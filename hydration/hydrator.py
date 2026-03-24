"""
Pinecone BYOC — data hydrator.

Reads records from a source, upserts into Pinecone in parallel batches,
and checkpoints progress to DynamoDB so interrupted runs can be resumed.

Sources:
  synthetic   — random normalized vectors (benchmarking / index warm-up)
  jsonl       — newline-delimited JSON: {"id": "...", "values": [...], "metadata": {...}}
  s3          — stream JSONL files from an S3 prefix or single object key

Checkpointing:
  Each completed batch is recorded in DynamoDB (hydration-checkpoints table).
  Rerunning with the same RUN_ID resumes from where it left off — already-
  completed batches are skipped.

Metrics:
  Each batch writes a row to hydration-metrics:
    vectors_written, wu, latency_ms, write_lsn, throughput_vps

Environment variables:
  Required:
    INDEX_HOST          — Pinecone index host
    PINECONE_API_KEY    — Pinecone API key

  Source:
    SOURCE_TYPE         — synthetic | jsonl | s3   (default: synthetic)
    SOURCE_PATH         — local file path or s3://bucket/key-or-prefix
    NAMESPACE           — Pinecone namespace        (default: "" = default ns)
    VECTOR_DIM          — embedding dimension       (default: 1024)
    TOTAL_VECTORS       — synthetic mode: vectors to write (default: 100_000)
    BATCH_SIZE          — vectors per logical batch  (default: 100)
    METADATA_EXTRA      — JSON string merged into every vector's metadata

  Execution:
    WORKERS             — parallel upsert threads   (default: 4)
    DRY_RUN             — true = validate, skip actual upserts (default: false)
    RUN_ID              — unique run identifier     (default: auto uuid4)

  AWS:
    AWS_REGION          — (default: us-east-1)
    CHECKPOINT_TABLE    — DynamoDB table            (default: hydration-checkpoints)
    METRICS_TABLE       — DynamoDB table            (default: hydration-metrics)
"""

import concurrent.futures
import json
import os
import signal
import time
import uuid
from decimal import Decimal
from typing import Iterator

import boto3
import numpy as np
from boto3.dynamodb.conditions import Key
from pinecone import Pinecone
from sklearn.preprocessing import normalize

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INDEX_HOST       = os.environ["INDEX_HOST"]
PINECONE_API_KEY = os.environ["PINECONE_API_KEY"]
AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")

SOURCE_TYPE      = os.environ.get("SOURCE_TYPE", "synthetic").lower()
SOURCE_PATH      = os.environ.get("SOURCE_PATH", "")
NAMESPACE        = os.environ.get("NAMESPACE", "")
VECTOR_DIM       = int(os.environ.get("VECTOR_DIM", "1024"))
TOTAL_VECTORS    = int(os.environ.get("TOTAL_VECTORS", "100_000".replace("_", "")))
BATCH_SIZE       = int(os.environ.get("BATCH_SIZE", "100"))
METADATA_EXTRA   = json.loads(os.environ.get("METADATA_EXTRA", "{}"))

WORKERS          = int(os.environ.get("WORKERS", "4"))
DRY_RUN          = os.environ.get("DRY_RUN", "false").lower() == "true"
RUN_ID           = os.environ.get("RUN_ID", str(uuid.uuid4()))

CHKPT_TABLE      = os.environ.get("CHECKPOINT_TABLE", "hydration-checkpoints")
METRICS_TABLE_   = os.environ.get("METRICS_TABLE", "hydration-metrics")

# Pinecone hard limits
_MAX_BATCH_BYTES   = 4_000_000
_MAX_BATCH_VECTORS = 100
PROGRESS_EVERY     = 5_000  # print progress every N vectors

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

ddb           = boto3.resource("dynamodb", region_name=AWS_REGION)
s3_client     = boto3.client("s3", region_name=AWS_REGION)
chkpt_table   = ddb.Table(CHKPT_TABLE)
metrics_table = ddb.Table(METRICS_TABLE_)

# ---------------------------------------------------------------------------
# Pinecone client
# ---------------------------------------------------------------------------

pc    = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(host=INDEX_HOST)

# ---------------------------------------------------------------------------
# Graceful shutdown on SIGTERM (ECS task stop)
# ---------------------------------------------------------------------------

_shutdown = False


def _on_sigterm(sig, frame):
    global _shutdown
    print("[hydrator] SIGTERM — finishing current batch then stopping", flush=True)
    _shutdown = True


signal.signal(signal.SIGTERM, _on_sigterm)

# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def load_completed_batches() -> set[str]:
    """Return set of batch_ids already committed for this RUN_ID."""
    items: list[dict] = []
    kwargs: dict = {
        "KeyConditionExpression": Key("run_id").eq(RUN_ID),
        "ProjectionExpression":   "batch_id",
    }
    while True:
        resp = chkpt_table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return {i["batch_id"] for i in items}


def mark_batch_done(batch_id: str, vector_count: int):
    chkpt_table.put_item(Item={
        "run_id":       RUN_ID,
        "batch_id":     batch_id,
        "vector_count": vector_count,
        "ts":           Decimal(str(time.time())),
    })

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def record_batch_metrics(
    batch_id: str,
    vector_count: int,
    latency_ms: int,
    wu: int,
    write_lsn: int | None,
    throughput_vps: float,
):
    item = {
        "run_id":         RUN_ID,
        "ts":             Decimal(str(time.time())),
        "batch_id":       batch_id,
        "vector_count":   vector_count,
        "latency_ms":     latency_ms,
        "wu":             wu,
        "throughput_vps": Decimal(str(round(throughput_vps, 2))),
        "source_type":    SOURCE_TYPE,
        "namespace":      NAMESPACE or "(default)",
    }
    if write_lsn is not None:
        item["write_lsn"] = write_lsn
    metrics_table.put_item(Item=item)

# ---------------------------------------------------------------------------
# LSN extraction (serverless only — silently absent on DRN indexes)
# ---------------------------------------------------------------------------

def _write_lsn(result) -> int | None:
    if not hasattr(result, "_response_info"):
        return None
    h = result._response_info.get("raw_headers", {})
    lsn = h.get("x-pinecone-request-lsn")
    return int(lsn) if lsn else None

# ---------------------------------------------------------------------------
# Smart upsert — respects Pinecone 4 MB / 100-vector sub-batch limits
# ---------------------------------------------------------------------------

def smart_upsert(vectors: list[dict]) -> tuple[int, int, int | None]:
    """
    Upsert a list of vectors, splitting into sub-batches as needed.
    Returns (total_vectors_upserted, total_wu, last_write_lsn).
    """
    batch:       list[dict] = []
    batch_bytes: int        = 0
    total:       int        = 0
    total_wu:    int        = 0
    last_lsn:    int | None = None

    def _flush():
        nonlocal total, total_wu, last_lsn
        result   = index.upsert(vectors=batch, namespace=NAMESPACE)
        usage    = getattr(result, "usage", None)
        wu       = int(getattr(usage, "write_units", 0) or 0)
        lsn      = _write_lsn(result)
        if lsn is not None:
            last_lsn = lsn
        total_wu += wu
        total    += len(batch)

    for vec in vectors:
        vec_bytes = len(json.dumps(vec).encode())
        if batch and (batch_bytes + vec_bytes > _MAX_BATCH_BYTES or len(batch) >= _MAX_BATCH_VECTORS):
            _flush()
            batch.clear()
            batch_bytes = 0
        batch.append(vec)
        batch_bytes += vec_bytes

    if batch:
        _flush()

    return total, total_wu, last_lsn

# ---------------------------------------------------------------------------
# Sources — yield batches of {"id", "values", "metadata"} dicts
# ---------------------------------------------------------------------------

def _parse_jsonl_stream(fileobj) -> Iterator[list[dict]]:
    """Yield BATCH_SIZE batches from a file-like object of JSONL."""
    batch: list[dict] = []
    for raw_line in fileobj:
        if isinstance(raw_line, bytes):
            raw_line = raw_line.decode("utf-8")
        line = raw_line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if "id" not in rec:
            raise ValueError(f"Record missing 'id': {line[:120]}")
        if "values" not in rec:
            raise ValueError(f"Record missing 'values' (pre-computed embeddings required): {line[:120]}")
        meta = {**rec.get("metadata", {}), "run_id": RUN_ID, **METADATA_EXTRA}
        batch.append({"id": str(rec["id"]), "values": rec["values"], "metadata": meta})
        if len(batch) >= BATCH_SIZE:
            yield batch
            batch = []
    if batch:
        yield batch


def _synthetic_source() -> Iterator[list[dict]]:
    """Yield batches of random l2-normalized vectors."""
    remaining = TOTAL_VECTORS
    while remaining > 0:
        n    = min(BATCH_SIZE, remaining)
        vecs = normalize(np.random.randn(n, VECTOR_DIM).astype("float32"), norm="l2")
        batch = [
            {
                "id":       str(uuid.uuid4()),
                "values":   vecs[i].tolist(),
                "metadata": {"source": "hydrator", "run_id": RUN_ID, **METADATA_EXTRA},
            }
            for i in range(n)
        ]
        yield batch
        remaining -= n


def _jsonl_source() -> Iterator[list[dict]]:
    with open(SOURCE_PATH, "rb") as f:
        yield from _parse_jsonl_stream(f)


def _s3_source() -> Iterator[list[dict]]:
    """Stream JSONL objects from an S3 URI (s3://bucket/key-or-prefix)."""
    path   = SOURCE_PATH.removeprefix("s3://")
    bucket, _, prefix = path.partition("/")

    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith((".jsonl", ".json", ".ndjson")):
                keys.append(obj["Key"])

    if not keys:
        raise ValueError(f"No JSONL files found at s3://{bucket}/{prefix}")

    print(f"[hydrator] {len(keys)} S3 objects at s3://{bucket}/{prefix}", flush=True)

    for key in sorted(keys):
        print(f"[hydrator] streaming s3://{bucket}/{key}", flush=True)
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        yield from _parse_jsonl_stream(resp["Body"])


def get_source() -> Iterator[list[dict]]:
    if SOURCE_TYPE == "synthetic":
        return _synthetic_source()
    if SOURCE_TYPE == "jsonl":
        return _jsonl_source()
    if SOURCE_TYPE == "s3":
        return _s3_source()
    raise ValueError(f"Unknown SOURCE_TYPE '{SOURCE_TYPE}' — must be synthetic | jsonl | s3")

# ---------------------------------------------------------------------------
# Worker: upsert one batch, checkpoint, emit metrics
# ---------------------------------------------------------------------------

def process_batch(batch_num: int, batch: list[dict], completed: set[str]) -> int:
    """Upsert a single batch. Returns number of vectors written."""
    batch_id = f"{RUN_ID}:{batch_num}"

    if batch_id in completed:
        return 0  # already committed — skip

    t0 = time.time()

    if DRY_RUN:
        # Validate record shape without upserting
        for vec in batch:
            assert len(vec["values"]) == VECTOR_DIM, (
                f"dim mismatch: got {len(vec['values'])}, expected {VECTOR_DIM}"
            )
        latency_ms = int((time.time() - t0) * 1000)
        print(f"[hydrator] DRY RUN batch={batch_num} validated {len(batch)} records", flush=True)
        return len(batch)

    total, wu, lsn = smart_upsert(batch)
    latency_ms     = int((time.time() - t0) * 1000)
    throughput     = total / max(latency_ms / 1000, 0.001)

    mark_batch_done(batch_id, total)
    record_batch_metrics(batch_id, total, latency_ms, wu, lsn, throughput)

    print(
        f"[hydrator] batch={batch_num} n={total} {latency_ms}ms "
        f"wu={wu} lsn={lsn} {throughput:.0f}v/s",
        flush=True,
    )
    return total

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(
        f"[hydrator] start — run_id={RUN_ID} source={SOURCE_TYPE} "
        f"dim={VECTOR_DIM} batch={BATCH_SIZE} workers={WORKERS} "
        f"namespace={NAMESPACE or '(default)'} dry_run={DRY_RUN}",
        flush=True,
    )
    if SOURCE_TYPE == "synthetic":
        print(f"[hydrator] synthetic mode — {TOTAL_VECTORS:,} vectors", flush=True)
    elif SOURCE_PATH:
        print(f"[hydrator] source path — {SOURCE_PATH}", flush=True)

    completed = load_completed_batches()
    if completed:
        print(f"[hydrator] resuming — {len(completed)} batches already committed", flush=True)

    source         = get_source()
    total_written  = 0
    total_batches  = 0
    next_progress  = PROGRESS_EVERY
    run_start      = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        pending: dict[concurrent.futures.Future, int] = {}

        for batch_num, batch in enumerate(source):
            if _shutdown:
                break

            fut = pool.submit(process_batch, batch_num, batch, completed)
            pending[fut] = batch_num
            total_batches += 1

            # Drain any finished futures to bound memory
            done = [f for f in pending if f.done()]
            for f in done:
                total_written += f.result()
                del pending[f]

            if total_written >= next_progress:
                elapsed = time.time() - run_start
                vps     = total_written / max(elapsed, 0.001)
                print(
                    f"[hydrator] progress — {total_written:,} vectors "
                    f"in {elapsed:.0f}s ({vps:.0f} v/s)",
                    flush=True,
                )
                next_progress += PROGRESS_EVERY

        # Wait for remaining in-flight batches
        for fut in concurrent.futures.as_completed(pending):
            total_written += fut.result()

    elapsed = time.time() - run_start
    vps     = total_written / max(elapsed, 0.001)
    status  = "interrupted" if _shutdown else "done"
    print(
        f"[hydrator] {status} — {total_written:,} vectors in {elapsed:.1f}s "
        f"({vps:.0f} v/s) batches={total_batches} run_id={RUN_ID}",
        flush=True,
    )


if __name__ == "__main__":
    main()
