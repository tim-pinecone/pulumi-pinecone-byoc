"""
Pinecone BYOC — continuous vector writer.
Writes WRITE_COUNT vectors every MIN_SLEEP_SECONDS–MAX_SLEEP_SECONDS seconds.

Metadata on every vector:
  kill=kill          — filter for bulk delete
  source=writer
  written_at=<epoch>
  dim=<int>
  segment=A|B|C      — present on 50/25/10% of batches respectively

Headers captured per batch:
  x-pinecone-request-lsn          — write LSN (monotonically increasing)
  x-pinecone-request-latency-ms   — server-side processing time

When /redrum/freshness_enabled is "true" in SSM, also writes freshness
records to DynamoDB. Always writes upsert metrics and recall samples.
"""

import json
import os
import random
import time
import uuid
from decimal import Decimal

import boto3
import numpy as np
from pinecone import Pinecone
from sklearn.preprocessing import normalize

INDEX_HOST        = os.environ["INDEX_HOST"]
PINECONE_API_KEY  = os.environ["PINECONE_API_KEY"]
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
VECTOR_DIM        = int(os.environ.get("VECTOR_DIM", "1024"))
WRITE_COUNT       = int(os.environ.get("WRITE_COUNT", "200"))
MIN_SLEEP         = int(os.environ.get("MIN_SLEEP_SECONDS", "60"))
MAX_SLEEP         = int(os.environ.get("MAX_SLEEP_SECONDS", "600"))
DYNAMO_TABLE      = os.environ.get("DYNAMO_TABLE", "redrum-freshness")
METRICS_TABLE     = os.environ.get("METRICS_TABLE", "redrum-metrics")
RECALL_TABLE      = os.environ.get("RECALL_TABLE", "redrum-recall")
SSM_FLAG_PATH     = os.environ.get("SSM_FLAG_PATH", "/redrum/freshness_enabled")

_MAX_BYTES   = 4_000_000
_MAX_VECTORS = 100

ssm           = boto3.client("ssm", region_name=AWS_REGION)
ddb           = boto3.resource("dynamodb", region_name=AWS_REGION)
fresh_table   = ddb.Table(DYNAMO_TABLE)
metrics_table = ddb.Table(METRICS_TABLE)
recall_table  = ddb.Table(RECALL_TABLE)


def freshness_enabled() -> bool:
    try:
        resp = ssm.get_parameter(Name=SSM_FLAG_PATH)
        return resp["Parameter"]["Value"].strip().lower() == "true"
    except Exception:
        return False


def roll_segment() -> str | None:
    r = random.random()
    if r < 0.50: return "A"
    if r < 0.75: return "B"
    if r < 0.85: return "C"
    return None


def _lsn_headers(result) -> tuple[int | None, int | None]:
    """Extract (write_lsn, server_latency_ms) from a response object."""
    if not hasattr(result, "_response_info"):
        return None, None
    headers = result._response_info.get("raw_headers", {})
    lsn = headers.get("x-pinecone-request-lsn")
    lat = headers.get("x-pinecone-request-latency-ms")
    return (int(lsn) if lsn else None), (int(lat) if lat else None)


def smart_upsert(index, vectors: list[dict]) -> tuple[int, int, int, list[str], int | None, int | None]:
    """
    Upsert in batches sized by JSON byte length.
    Returns (total_vectors, total_wu, total_ru, sampled_ids, write_lsn, avg_server_latency_ms).
    write_lsn: LSN from the final batch (highest assigned LSN for this upsert).
    avg_server_latency_ms: average server-side processing time across batches.
    """
    batch: list[dict] = []
    batch_bytes = 0
    total = 0
    total_wu = 0
    total_ru = 0
    sampled_ids: list[str] = []
    write_lsn: int | None = None
    server_lats: list[int] = []

    def _flush():
        nonlocal total, total_wu, total_ru, write_lsn
        result  = index.upsert(vectors=batch)
        usage   = getattr(result, "usage", None)
        wu      = int(getattr(usage, "write_units", 0) or 0)
        ru      = int(getattr(usage, "read_units",  0) or 0)
        lsn, server_lat = _lsn_headers(result)
        if lsn is not None:
            write_lsn = lsn          # last batch wins — highest LSN
        if server_lat is not None:
            server_lats.append(server_lat)
        total_wu += wu
        total_ru += ru
        sampled_ids.append(batch[0]["id"])
        total += len(batch)
        print(
            f"[writer] batch upserted {len(batch)} vectors "
            f"wu={wu} lsn={lsn} server_lat={server_lat}ms",
            flush=True,
        )

    for vec in vectors:
        vec_bytes = len(json.dumps(vec).encode("utf-8"))
        if batch and (batch_bytes + vec_bytes > _MAX_BYTES or len(batch) >= _MAX_VECTORS):
            _flush()
            batch.clear()
            batch_bytes = 0
        batch.append(vec)
        batch_bytes += vec_bytes

    if batch:
        _flush()

    avg_server_lat = int(sum(server_lats) / len(server_lats)) if server_lats else None
    return total, total_wu, total_ru, sampled_ids, write_lsn, avg_server_lat


def record_freshness(vector_ids: list[str], written_at: float):
    ts = Decimal(str(written_at))
    with fresh_table.batch_writer() as bw:
        for vid in vector_ids:
            bw.put_item(Item={"id": vid, "written_at": ts, "status": "pending"})
    print(f"[writer] freshness: recorded {len(vector_ids)} ids", flush=True)


def record_recall_samples(sampled_ids: list[str], written_at: float):
    ts = Decimal(str(written_at))
    with recall_table.batch_writer() as bw:
        for vid in sampled_ids:
            bw.put_item(Item={"id": vid, "written_at": ts, "status": "pending"})
    print(f"[writer] recall: sampled {len(sampled_ids)} ids", flush=True)


def record_upsert_metrics(
    latency_ms: int, wu: int, ru: int,
    vector_count: int, batch_count: int, segment: str | None,
    write_lsn: int | None, server_latency_ms: int | None,
):
    item = {
        "metric_type":       "upsert",
        "ts":                Decimal(str(time.time())),
        "latency_ms":        latency_ms,
        "vector_count":      vector_count,
        "batch_count":       batch_count,
        "wu":                wu,
        "ru":                ru,
        "segment":           segment or "none",
    }
    if write_lsn is not None:
        item["write_lsn"] = write_lsn
    if server_latency_ms is not None:
        item["server_latency_ms"] = server_latency_ms
        item["network_latency_ms"] = max(0, latency_ms - server_latency_ms)

    metrics_table.put_item(Item=item)
    print(
        f"[writer] metrics: latency={latency_ms}ms server={server_latency_ms}ms "
        f"wu={wu} lsn={write_lsn} segment={segment}",
        flush=True,
    )


pc    = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(host=INDEX_HOST)

print(
    f"[writer] started — dim={VECTOR_DIM} write_count={WRITE_COUNT} "
    f"sleep={MIN_SLEEP}-{MAX_SLEEP}s",
    flush=True,
)

while True:
    sleep_sec = random.randint(MIN_SLEEP, MAX_SLEEP)
    print(f"[writer] sleeping {sleep_sec}s", flush=True)
    time.sleep(sleep_sec)

    try:
        segment    = roll_segment()
        written_at = time.time()
        vecs       = normalize(np.random.randn(WRITE_COUNT, VECTOR_DIM).astype("float32"), norm="l2")
        ids        = [str(uuid.uuid4()) for _ in range(WRITE_COUNT)]

        vectors = []
        for i in range(WRITE_COUNT):
            meta = {"kill": "kill", "source": "writer", "written_at": written_at, "dim": VECTOR_DIM}
            if segment:
                meta["segment"] = segment
            vectors.append({"id": ids[i], "values": vecs[i].tolist(), "metadata": meta})

        t_upsert = time.time()
        total, total_wu, total_ru, sampled_ids, write_lsn, avg_server_lat = smart_upsert(index, vectors)
        upsert_latency_ms = int((time.time() - t_upsert) * 1000)

        print(
            f"[writer] done — {total} vectors in {upsert_latency_ms}ms "
            f"total_wu={total_wu} lsn={write_lsn}",
            flush=True,
        )

        record_upsert_metrics(
            upsert_latency_ms, total_wu, total_ru, total,
            len(sampled_ids), segment, write_lsn, avg_server_lat,
        )
        record_recall_samples(sampled_ids, written_at)

        if freshness_enabled():
            record_freshness(ids, written_at)

    except Exception as e:
        body = getattr(getattr(e, "body", None), "decode", lambda: str(getattr(e, "body", "")))()
        print(f"[writer] ERROR: {e} | body={body}", flush=True)
