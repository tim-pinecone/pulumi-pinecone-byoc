"""
Pinecone BYOC — continuous querier.
Runs QUERY_COUNT queries every MIN_SLEEP_SECONDS–MAX_SLEEP_SECONDS seconds.

Query distribution per cycle:
  50% → filter segment=A
  25% → filter segment=B
  10% → filter segment=C
  15% → no filter

Headers captured per query:
  x-pinecone-max-indexed-lsn      — highest LSN currently reflected in results
  x-pinecone-request-latency-ms   — server-side processing time

Records p50/p95/p99 for both client and server latency, network overhead,
max indexed LSN, and RU per cycle to redrum-metrics.
"""

import os
import random
import statistics
import time
from decimal import Decimal

import boto3
import numpy as np
from pinecone import Pinecone
from sklearn.preprocessing import normalize

INDEX_HOST        = os.environ["INDEX_HOST"]
PINECONE_API_KEY  = os.environ["PINECONE_API_KEY"]
AWS_REGION        = os.environ.get("AWS_REGION", "us-east-1")
VECTOR_DIM        = int(os.environ.get("VECTOR_DIM", "1024"))
QUERY_COUNT       = int(os.environ.get("QUERY_COUNT", "10"))
TOP_K             = int(os.environ.get("TOP_K", "10"))
MIN_SLEEP         = int(os.environ.get("MIN_SLEEP_SECONDS", "60"))
MAX_SLEEP         = int(os.environ.get("MAX_SLEEP_SECONDS", "600"))
METRICS_TABLE     = os.environ.get("METRICS_TABLE", "redrum-metrics")

ddb           = boto3.resource("dynamodb", region_name=AWS_REGION)
metrics_table = ddb.Table(METRICS_TABLE)

pc    = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(host=INDEX_HOST)


def build_filter_plan(n: int) -> list[str | None]:
    n_a    = round(n * 0.50)
    n_b    = round(n * 0.25)
    n_c    = round(n * 0.10)
    n_none = n - n_a - n_b - n_c
    plan   = ["A"] * n_a + ["B"] * n_b + ["C"] * n_c + [None] * n_none
    random.shuffle(plan)
    return plan


def pct(data: list[int], p: int) -> int:
    if len(data) < 2:
        return data[0] if data else 0
    qs = statistics.quantiles(data, n=100)
    return int(qs[min(p - 1, len(qs) - 1)])


def _query_headers(result) -> tuple[int | None, int | None]:
    """Extract (max_indexed_lsn, server_latency_ms) from a query response."""
    if not hasattr(result, "_response_info"):
        return None, None
    headers = result._response_info.get("raw_headers", {})
    lsn = headers.get("x-pinecone-max-indexed-lsn")
    lat = headers.get("x-pinecone-request-latency-ms")
    return (int(lsn) if lsn else None), (int(lat) if lat else None)


print(
    f"[querier] started — dim={VECTOR_DIM} query_count={QUERY_COUNT} "
    f"top_k={TOP_K} sleep={MIN_SLEEP}-{MAX_SLEEP}s",
    flush=True,
)

while True:
    sleep_sec = random.randint(MIN_SLEEP, MAX_SLEEP)
    print(f"[querier] sleeping {sleep_sec}s", flush=True)
    time.sleep(sleep_sec)

    try:
        plan            = build_filter_plan(QUERY_COUNT)
        client_lats     = []
        server_lats     = []
        lsn_values      = []
        total_ru        = 0
        seg_counts      = {"A": 0, "B": 0, "C": 0, "none": 0}

        for i, seg in enumerate(plan):
            vec        = normalize(np.random.randn(1, VECTOR_DIM).astype("float32"), norm="l2")[0].tolist()
            filter_arg = {"segment": {"$eq": seg}} if seg else None

            t0     = time.time()
            result = index.query(vector=vec, top_k=TOP_K, filter=filter_arg)
            client_lat_ms = int((time.time() - t0) * 1000)

            max_lsn, server_lat_ms = _query_headers(result)

            client_lats.append(client_lat_ms)
            if server_lat_ms is not None:
                server_lats.append(server_lat_ms)
            if max_lsn is not None:
                lsn_values.append(max_lsn)

            usage    = getattr(result, "usage", None)
            ru       = int(getattr(usage, "read_units", 0) or 0)
            total_ru += ru
            seg_counts[seg or "none"] += 1

            matches = result.get("matches", []) if isinstance(result, dict) else (result.matches or [])
            top     = matches[0] if matches else {}
            top_id  = top.get("id", "?") if isinstance(top, dict) else getattr(top, "id", "?")
            top_sc  = top.get("score", 0) if isinstance(top, dict) else getattr(top, "score", 0)

            print(
                f"[querier] query {i+1}/{QUERY_COUNT} seg={seg} "
                f"client={client_lat_ms}ms server={server_lat_ms}ms "
                f"lsn={max_lsn} ru={ru} top={top_id} score={top_sc:.6f}",
                flush=True,
            )

        # client latency percentiles
        p50_c = pct(client_lats, 50)
        p95_c = pct(client_lats, 95)
        p99_c = pct(client_lats, 99)

        # server latency percentiles
        p50_s = pct(server_lats, 50) if server_lats else None
        p95_s = pct(server_lats, 95) if server_lats else None

        # network overhead (median client - median server)
        network_ms = (p50_c - p50_s) if p50_s is not None else None

        # highest LSN seen this cycle
        max_indexed_lsn = max(lsn_values) if lsn_values else None

        print(
            f"[querier] cycle done — "
            f"p50={p50_c}ms (server={p50_s}ms net={network_ms}ms) "
            f"p95={p95_c}ms p99={p99_c}ms "
            f"max_lsn={max_indexed_lsn} total_ru={total_ru}",
            flush=True,
        )

        item = {
            "metric_type":        "query",
            "ts":                 Decimal(str(time.time())),
            "p50_ms":             p50_c,
            "p95_ms":             p95_c,
            "p99_ms":             p99_c,
            "min_ms":             min(client_lats),
            "max_ms":             max(client_lats),
            "query_count":        QUERY_COUNT,
            "total_ru":           total_ru,
            "queries_segment_a":  seg_counts["A"],
            "queries_segment_b":  seg_counts["B"],
            "queries_segment_c":  seg_counts["C"],
            "queries_no_filter":  seg_counts["none"],
        }
        if p50_s is not None:
            item["p50_server_ms"]  = p50_s
            item["p95_server_ms"]  = p95_s
        if network_ms is not None:
            item["p50_network_ms"] = network_ms
        if max_indexed_lsn is not None:
            item["max_indexed_lsn"] = max_indexed_lsn

        metrics_table.put_item(Item=item)

    except Exception as e:
        body = getattr(getattr(e, "body", None), "decode", lambda: str(getattr(e, "body", "")))()
        print(f"[querier] ERROR: {e} | body={body}", flush=True)
