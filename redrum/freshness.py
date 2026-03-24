#!/usr/bin/env python3
"""
freshness.py — Pinecone BYOC test bench monitor.

Usage:
  python freshness.py               # live dashboard (enables freshness tracking)
  python freshness.py --off         # disable freshness tracking and exit
  python freshness.py --stats       # print summary stats and exit
  python freshness.py --probe       # local upsert→poll latency probe
  python freshness.py --lambda-probe  # invoke tracker Lambda as prober
  python freshness.py --report      # generate full markdown benchmark report

Ctrl-C stops monitoring and disables freshness tracking.
"""

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
import numpy as np
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from dotenv import load_dotenv
from pinecone import Pinecone
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from sklearn.preprocessing import normalize

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")
AWS_PROFILE      = os.environ.get("AWS_PROFILE", "")
DYNAMO_TABLE     = os.environ.get("DYNAMO_TABLE", "redrum-freshness")
METRICS_TABLE    = os.environ.get("METRICS_TABLE", "redrum-metrics")
RECALL_TABLE     = os.environ.get("RECALL_TABLE", "redrum-recall")
STATS_TABLE      = os.environ.get("STATS_TABLE", "redrum-index-stats")
SSM_FLAG_PATH    = os.environ.get("SSM_FLAG_PATH", "/redrum/freshness_enabled")
REFRESH_SEC      = float(os.environ.get("DASHBOARD_REFRESH_SECONDS", "2"))
RECENT_ROWS      = int(os.environ.get("DASHBOARD_RECENT_ROWS", "12"))
INDEX_HOST       = os.environ.get("INDEX_HOST", "")
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", "")
VECTOR_DIM       = int(os.environ.get("VECTOR_DIM", "1024"))
PROBE_INTERVAL   = float(os.environ.get("PROBE_INTERVAL_SECONDS", "5"))
PROBE_POLL       = float(os.environ.get("PROBE_POLL_SECONDS", "0.1"))
LAMBDA_FUNCTION  = os.environ.get("LAMBDA_FUNCTION", "redrum-tracker")

session = boto3.Session(
    region_name=AWS_REGION,
    **({'profile_name': AWS_PROFILE} if AWS_PROFILE else {}),
)
ssm           = session.client("ssm")
ddb           = session.resource("dynamodb")
fresh_table   = ddb.Table(DYNAMO_TABLE)
metrics_table = ddb.Table(METRICS_TABLE)
recall_table  = ddb.Table(RECALL_TABLE)
stats_table   = ddb.Table(STATS_TABLE)


# ---------------------------------------------------------------------------
# SSM
# ---------------------------------------------------------------------------

def set_freshness(enabled: bool):
    value = "true" if enabled else "false"
    ssm.put_parameter(Name=SSM_FLAG_PATH, Value=value, Type="String", Overwrite=True)
    print(f"  freshness tracking {'ENABLED' if enabled else 'DISABLED'} ({SSM_FLAG_PATH} = {value})")


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def _scan_all(table, **kwargs) -> list[dict]:
    items = []
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def _query_all(table, **kwargs) -> list[dict]:
    items = []
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def fetch_recent_freshness(limit: int = RECENT_ROWS) -> list[dict]:
    items = _scan_all(fresh_table, FilterExpression=Attr("written_at").exists(), Limit=500)
    items.sort(key=lambda x: float(x.get("written_at", 0)), reverse=True)
    return items[:limit]


def fetch_all_freshness_latencies() -> list[int]:
    items = _scan_all(fresh_table, FilterExpression=Attr("status").eq("seen"),
                      ProjectionExpression="latency_ms")
    return [int(i["latency_ms"]) for i in items if "latency_ms" in i]


def fetch_query_metrics(limit: int = 500) -> list[dict]:
    return _query_all(
        metrics_table,
        KeyConditionExpression=Key("metric_type").eq("query"),
        ScanIndexForward=False,
        Limit=limit,
    )


def fetch_upsert_metrics(limit: int = 500) -> list[dict]:
    return _query_all(
        metrics_table,
        KeyConditionExpression=Key("metric_type").eq("upsert"),
        ScanIndexForward=False,
        Limit=limit,
    )


def fetch_recall_stats() -> dict:
    items = _scan_all(recall_table, FilterExpression=Attr("status").eq("tested"),
                      ProjectionExpression="recalled, recall_rank")
    if not items:
        return {"total": 0, "recalled": 0, "rate": 0.0, "avg_rank": None}
    total    = len(items)
    recalled = sum(1 for i in items if i.get("recalled"))
    ranks    = [int(i["recall_rank"]) for i in items if i.get("recalled") and i.get("recall_rank")]
    return {
        "total":    total,
        "recalled": recalled,
        "rate":     recalled / total if total else 0.0,
        "avg_rank": statistics.mean(ranks) if ranks else None,
    }


def fetch_latest_index_stats() -> dict | None:
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    resp  = stats_table.query(
        KeyConditionExpression=Key("date").eq(today),
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items", [])
    return items[0] if items else None


def fetch_index_stats_series(days: int = 7) -> list[dict]:
    from datetime import timedelta
    items = []
    for d in range(days):
        date = (datetime.now(tz=timezone.utc) - timedelta(days=d)).strftime("%Y-%m-%d")
        items.extend(_query_all(stats_table, KeyConditionExpression=Key("date").eq(date)))
    items.sort(key=lambda x: float(x.get("ts", 0)))
    return items


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def pct(values: list[int], p: int) -> str:
    if not values:
        return "—"
    if len(values) < 2:
        return f"{values[0] / 1000:.2f}s"
    return f"{statistics.quantiles(values, n=100)[min(p - 1, 98)] / 1000:.2f}s"


def fmt_ms(ms) -> str:
    return f"{int(ms) / 1000:.2f}s" if ms is not None else "—"


def fmt_ts(epoch) -> str:
    if epoch is None:
        return "—"
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).strftime("%H:%M:%S")


def status_text(status: str) -> Text:
    if status == "seen":
        return Text("✓ seen", style="green")
    if status == "timeout":
        return Text("✗ timeout", style="red")
    return Text("… pending", style="yellow")


# ---------------------------------------------------------------------------
# Live dashboard
# ---------------------------------------------------------------------------

def build_display(
    fresh_items: list[dict],
    fresh_latencies: list[int],
    query_metrics: list[dict],
    recall: dict,
    index_stats: dict | None,
) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="top", size=5),
        Layout(name="mid", size=7),
        Layout(name="bottom"),
    )
    layout["mid"].split_row(Layout(name="recall"), Layout(name="index"))

    # --- freshness stats ---
    timeout = sum(1 for i in fresh_items if i.get("status") == "timeout")
    pending = sum(1 for i in fresh_items if i.get("status") == "pending")
    layout["top"].update(Panel(
        f"  Freshness samples: [bold]{len(fresh_latencies)}[/bold]    "
        f"p50: [cyan]{pct(fresh_latencies, 50)}[/cyan]    "
        f"p95: [cyan]{pct(fresh_latencies, 95)}[/cyan]    "
        f"p99: [cyan]{pct(fresh_latencies, 99)}[/cyan]    "
        f"timeouts: [{'red' if timeout else 'green'}]{timeout}[/{'red' if timeout else 'green'}]    "
        f"pending: [yellow]{pending}[/yellow]",
        title="[bold]Pinecone Test Bench[/bold]",
        subtitle=f"updated {datetime.now().strftime('%H:%M:%S')}",
    ))

    # --- recall panel ---
    recall_pct = f"{recall['rate'] * 100:.1f}%" if recall["total"] else "—"
    avg_rank   = f"{recall['avg_rank']:.1f}" if recall.get("avg_rank") else "—"
    layout["recall"].update(Panel(
        f"  Rate:     [bold cyan]{recall_pct}[/bold cyan]\n"
        f"  Tested:   {recall['total']}\n"
        f"  Recalled: {recall['recalled']}\n"
        f"  Avg rank: {avg_rank}",
        title="Recall",
    ))

    # --- index size panel ---
    if index_stats:
        vc = int(index_stats.get("total_vector_count", 0))
        fl = float(index_stats.get("index_fullness", 0))
        layout["index"].update(Panel(
            f"  Vectors:  [bold cyan]{vc:,}[/bold cyan]\n"
            f"  Fullness: {fl * 100:.2f}%\n"
            f"  Updated:  {fmt_ts(index_stats.get('ts'))}",
            title="Index Size",
        ))
    else:
        layout["index"].update(Panel("  No data yet", title="Index Size"))

    # --- recent freshness records ---
    tbl = Table(show_header=True, header_style="bold", expand=True, show_lines=False)
    tbl.add_column("ID",      style="dim", width=10)
    tbl.add_column("Written", width=10)
    tbl.add_column("Seen",    width=10)
    tbl.add_column("Latency", width=10, justify="right")
    tbl.add_column("Status",  width=12)
    for item in fresh_items:
        vid     = item.get("id", "")[:8] + "…"
        latency = f"{int(item['latency_ms']) / 1000:.2f}s" if "latency_ms" in item else "—"
        tbl.add_row(
            vid,
            fmt_ts(item.get("written_at")),
            fmt_ts(item.get("seen_at")),
            latency,
            status_text(item.get("status", "pending")),
        )
    layout["bottom"].update(Panel(tbl, title="Recent Freshness Records"))
    return layout


def live_dashboard():
    console = Console()
    console.print("\n  Starting freshness tracking…")
    set_freshness(True)
    console.print("  Waiting for data (writer wakes up every 1–10 min)…\n")
    try:
        with Live(console=console, refresh_per_second=1 / REFRESH_SEC, screen=True) as live:
            while True:
                fresh_items     = fetch_recent_freshness()
                fresh_latencies = fetch_all_freshness_latencies()
                query_metrics   = fetch_query_metrics(limit=50)
                recall          = fetch_recall_stats()
                index_stats     = fetch_latest_index_stats()
                live.update(build_display(fresh_items, fresh_latencies, query_metrics, recall, index_stats))
                time.sleep(REFRESH_SEC)
    except KeyboardInterrupt:
        pass
    finally:
        console.print("\n  Stopping freshness tracking…")
        set_freshness(False)
        console.print("  Done.\n")


# ---------------------------------------------------------------------------
# --stats
# ---------------------------------------------------------------------------

def print_stats():
    latencies = fetch_all_freshness_latencies()
    if not latencies:
        print("No seen records yet.")
        return
    print(f"\nFreshness ({len(latencies)} samples)")
    for p in [50, 75, 95, 99]:
        print(f"  p{p:>2} : {pct(latencies, p)}")
    print(f"  min : {min(latencies)/1000:.2f}s  max : {max(latencies)/1000:.2f}s\n")


# ---------------------------------------------------------------------------
# --probe (local)
# ---------------------------------------------------------------------------

def run_probe():
    console = Console()
    if not INDEX_HOST or not PINECONE_API_KEY:
        console.print("[red]INDEX_HOST and PINECONE_API_KEY must be set[/red]")
        sys.exit(1)
    pc    = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index(host=INDEX_HOST)
    samples: list[int] = []
    console.print(f"\n  [bold]Local probe[/bold] — upsert every {PROBE_INTERVAL}s, poll every {int(PROBE_POLL*1000)}ms\n")
    try:
        n = 0
        while True:
            n  += 1
            vid = str(uuid.uuid4())
            vec = normalize(np.random.randn(1, VECTOR_DIM).astype("float32"), norm="l2")[0].tolist()
            index.upsert(vectors=[{"id": vid, "values": vec,
                                   "metadata": {"kill": "kill", "source": "probe"}}])
            t0 = time.time()
            deadline = t0 + 60
            seen = False
            while time.time() < deadline:
                try:
                    if vid in (index.fetch(ids=[vid]).vectors or {}):
                        latency_ms = int((time.time() - t0) * 1000)
                        samples.append(latency_ms)
                        seen = True
                        break
                except Exception:
                    pass
                time.sleep(PROBE_POLL)
            avg = statistics.mean(samples) if samples else 0
            p50 = statistics.median(samples) if samples else 0
            if seen:
                console.print(
                    f"  [{n:>4}] [green]{latency_ms:>6}ms[/green]   "
                    f"p50={p50/1000:.2f}s  avg={avg/1000:.2f}s"
                )
            else:
                console.print(f"  [{n:>4}] [red]timeout[/red]")
            time.sleep(PROBE_INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        if samples:
            console.print(f"\n  p50={statistics.median(samples)/1000:.2f}s  "
                          f"min={min(samples)/1000:.2f}s  max={max(samples)/1000:.2f}s\n")


# ---------------------------------------------------------------------------
# --lambda-probe
# ---------------------------------------------------------------------------

def run_lambda_probe():
    console = Console()
    lambda_client = session.client("lambda", config=Config(read_timeout=210, connect_timeout=10))
    console.print(f"\n  [bold]Lambda probe[/bold] — invoking [cyan]{LAMBDA_FUNCTION}[/cyan]")
    console.print("  3 min run · 1 vector every 2s · polling every 10ms\n")
    payload = json.dumps({
        "mode": "probe", "duration_seconds": 180,
        "upsert_interval_seconds": 2, "poll_interval_seconds": 0.01,
        "vector_timeout_seconds": 10,
    })
    try:
        resp = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION, InvocationType="RequestResponse",
            LogType="Tail", Payload=payload,
        )
    except Exception as e:
        console.print(f"[red]Invoke failed: {e}[/red]")
        return
    import base64
    raw = resp["Payload"].read()
    if resp.get("FunctionError"):
        console.print(f"[red]Lambda error:[/red] {raw.decode()}")
        if resp.get("LogResult"):
            console.print("\n[dim]Lambda logs:[/dim]")
            console.print(base64.b64decode(resp["LogResult"]).decode())
        return
    result  = json.loads(raw)
    samples = result.get("samples", [])
    if not samples:
        console.print("[yellow]No samples returned.[/yellow]")
        return
    console.print(f"  [bold]Results[/bold] ({result['count']} samples, {result['timeouts']} timeouts)\n")
    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="dim", width=6)
    tbl.add_column(style="cyan")
    tbl.add_row("p50", f"{result['p50_ms'] / 1000:.3f}s")
    if "p95_ms" in result:
        tbl.add_row("p95", f"{result['p95_ms'] / 1000:.3f}s")
        tbl.add_row("p99", f"{result['p99_ms'] / 1000:.3f}s")
    tbl.add_row("min", f"{result['min_ms'] / 1000:.3f}s")
    tbl.add_row("max", f"{result['max_ms'] / 1000:.3f}s")
    console.print(tbl)

    # LSN delta stats
    if result.get("lsn_delta_p50") is not None:
        console.print("\n  [dim]LSN delta (indexed_lsn − write_lsn when vector became visible)[/dim]")
        lsn_tbl = Table(show_header=False, box=None, padding=(0, 2))
        lsn_tbl.add_column(style="dim", width=6)
        lsn_tbl.add_column(style="cyan")
        lsn_tbl.add_row("p50", str(result["lsn_delta_p50"]))
        lsn_tbl.add_row("min", str(result["lsn_delta_min"]))
        lsn_tbl.add_row("max", str(result["lsn_delta_max"]))
        console.print(lsn_tbl)

    max_ms  = result["max_ms"]
    buckets = max(1, (max_ms // 1000) + 1)
    counts  = [0] * int(buckets)
    for ms in samples:
        counts[min(int(ms // 1000), len(counts) - 1)] += 1
    console.print("\n  [dim]Distribution (1s buckets)[/dim]")
    bar_max = max(counts)
    for i, c in enumerate(counts):
        bar = "█" * int(c / bar_max * 40) if bar_max else ""
        console.print(f"  {i:>3}s  {bar} {c}")
    console.print()


# ---------------------------------------------------------------------------
# --report
# ---------------------------------------------------------------------------

def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    head = "| " + " | ".join(headers) + " |"
    body = "\n".join("| " + " | ".join(str(c) for c in row) + " |" for row in rows)
    return "\n".join([head, sep, body])


def generate_report():
    console = Console()
    console.print("\n  Fetching data from DynamoDB…")

    now_str    = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = Path(f"report_{now_str}.md")

    fresh_latencies = fetch_all_freshness_latencies()
    query_metrics   = fetch_query_metrics(limit=1000)
    upsert_metrics  = fetch_upsert_metrics(limit=1000)
    recall          = fetch_recall_stats()
    index_series    = fetch_index_stats_series(days=7)

    lines: list[str] = []
    a = lines.append

    a(f"# Redrum Benchmark Report")
    a(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    a(f"\n---\n")

    # --- Summary ---
    a("## Summary\n")
    a(_md_table(
        ["Metric", "Value"],
        [
            ["Query cycles recorded", str(len(query_metrics))],
            ["Upsert cycles recorded", str(len(upsert_metrics))],
            ["Freshness samples", str(len(fresh_latencies))],
            ["Recall tests", str(recall["total"])],
            ["Recall rate", f"{recall['rate']*100:.1f}%"],
            ["Index stats data points", str(len(index_series))],
        ]
    ))

    # --- Freshness ---
    a("\n## Write-to-Read Freshness (DynamoDB/Lambda pipeline)\n")
    if fresh_latencies:
        a(_md_table(
            ["p50", "p75", "p95", "p99", "min", "max", "samples"],
            [[pct(fresh_latencies, 50), pct(fresh_latencies, 75),
              pct(fresh_latencies, 95), pct(fresh_latencies, 99),
              fmt_ms(min(fresh_latencies)), fmt_ms(max(fresh_latencies)),
              str(len(fresh_latencies))]],
        ))
    else:
        a("_No freshness data yet._")

    # --- Query latency overall ---
    a("\n## Query Latency\n")
    if query_metrics:
        all_p50 = [int(r["p50_ms"]) for r in query_metrics if "p50_ms" in r]
        all_p95 = [int(r["p95_ms"]) for r in query_metrics if "p95_ms" in r]
        all_p99 = [int(r["p99_ms"]) for r in query_metrics if "p99_ms" in r]
        a(_md_table(
            ["Metric", "median-of-cycles", "min", "max"],
            [
                ["p50", fmt_ms(statistics.median(all_p50)), fmt_ms(min(all_p50)), fmt_ms(max(all_p50))],
                ["p95", fmt_ms(statistics.median(all_p95)), fmt_ms(min(all_p95)), fmt_ms(max(all_p95))],
                ["p99", fmt_ms(statistics.median(all_p99)), fmt_ms(min(all_p99)), fmt_ms(max(all_p99))],
            ]
        ))

        # RU summary
        total_ru  = sum(int(r.get("total_ru", 0)) for r in query_metrics)
        total_q   = sum(int(r.get("query_count", 0)) for r in query_metrics)
        avg_ru_q  = total_ru / total_q if total_q else 0
        a(f"\n**Total RU consumed:** {total_ru:,}  |  **Avg RU/query:** {avg_ru_q:.1f}")

        # By filter segment
        a("\n### Query Latency by Filter Segment\n")
        seg_rows = []
        for seg, key in [("A (50%)", "queries_segment_a"), ("B (25%)", "queries_segment_b"),
                         ("C (10%)", "queries_segment_c"), ("none (15%)", "queries_no_filter")]:
            count = sum(int(r.get(key, 0)) for r in query_metrics)
            seg_rows.append([seg, str(count)])
        a(_md_table(["Segment", "Total queries run"], seg_rows))
    else:
        a("_No query metrics yet._")

    # --- Upsert performance ---
    a("\n## Upsert Performance\n")
    if upsert_metrics:
        lats = [int(r["latency_ms"]) for r in upsert_metrics if "latency_ms" in r]
        wus  = [int(r.get("wu", 0)) for r in upsert_metrics]
        a(_md_table(
            ["Metric", "median", "min", "max"],
            [
                ["Upsert latency (API time)", fmt_ms(statistics.median(lats)),
                 fmt_ms(min(lats)), fmt_ms(max(lats))],
                ["WU per cycle", f"{statistics.median(wus):.0f}",
                 str(min(wus)), str(max(wus))],
            ]
        ))
        total_wu = sum(wus)
        a(f"\n**Total WU consumed:** {total_wu:,}")

        seg_dist = {"A": 0, "B": 0, "C": 0, "none": 0}
        for r in upsert_metrics:
            seg_dist[r.get("segment", "none")] = seg_dist.get(r.get("segment", "none"), 0) + 1
        a("\n### Segment Distribution in Writes\n")
        a(_md_table(
            ["Segment", "Batches", "% of total"],
            [[s, str(c), f"{c/len(upsert_metrics)*100:.1f}%"] for s, c in seg_dist.items()],
        ))
    else:
        a("_No upsert metrics yet._")

    # --- Recall ---
    a("\n## Recall Accuracy\n")
    a(_md_table(
        ["Metric", "Value"],
        [
            ["Tests run", str(recall["total"])],
            ["Recalled (in top-K)", str(recall["recalled"])],
            ["Recall rate", f"{recall['rate']*100:.1f}%"],
            ["Avg rank (when recalled)", f"{recall['avg_rank']:.1f}" if recall["avg_rank"] else "—"],
        ]
    ))

    # --- Index size over time ---
    a("\n## Index Size Over Time\n")
    if index_series:
        rows = []
        for item in index_series[::max(1, len(index_series)//20)]:  # sample up to 20 rows
            ts  = float(item.get("ts", 0))
            dt  = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            vc  = int(item.get("total_vector_count", 0))
            fl  = float(item.get("index_fullness", 0))
            rows.append([dt, f"{vc:,}", f"{fl*100:.2f}%"])
        a(_md_table(["Timestamp (UTC)", "Vector Count", "Fullness"], rows))
    else:
        a("_No index stats yet._")

    # --- Latency vs index size ---
    a("\n## Query Latency vs Index Size\n")
    if query_metrics and index_series:
        # match each query cycle to nearest index stats by timestamp
        stat_ts  = [float(s.get("ts", 0)) for s in index_series]
        stat_vc  = [int(s.get("total_vector_count", 0)) for s in index_series]
        rows = []
        for qm in sorted(query_metrics, key=lambda x: float(x.get("ts", 0)))[::max(1, len(query_metrics)//15)]:
            qt = float(qm.get("ts", 0))
            # nearest index stat
            nearest_idx = min(range(len(stat_ts)), key=lambda i: abs(stat_ts[i] - qt))
            vc  = stat_vc[nearest_idx]
            p50 = fmt_ms(qm.get("p50_ms"))
            p95 = fmt_ms(qm.get("p95_ms"))
            rows.append([f"{vc:,}", p50, p95])
        a(_md_table(["Index Size", "p50 query", "p95 query"], rows))
    else:
        a("_Insufficient data for correlation._")

    # --- LSN freshness ---
    a("\n## LSN Freshness (Log Sequence Number)\n")
    a(
        "LSN (Log Sequence Number) tracks write visibility at the index level. "
        "Each upsert is assigned a `write_lsn`; query responses report the highest "
        "`max_indexed_lsn` currently reflected in results. A query whose "
        "`max_indexed_lsn` equals or exceeds a given `write_lsn` means that write "
        "is fully visible to queries.\n"
    )
    upsert_with_lsn = [r for r in upsert_metrics if r.get("write_lsn") is not None]
    query_with_lsn  = [r for r in query_metrics  if r.get("max_indexed_lsn") is not None]
    if upsert_with_lsn and query_with_lsn:
        # latest write LSN and latest max indexed LSN
        latest_write_lsn   = max(int(r["write_lsn"])       for r in upsert_with_lsn)
        latest_indexed_lsn = max(int(r["max_indexed_lsn"]) for r in query_with_lsn)
        lsn_lag            = latest_write_lsn - latest_indexed_lsn

        # match upserts → nearest query by time; compute how many ms until
        # max_indexed_lsn caught up (approximated by latency of query where lsn >= write_lsn)
        sorted_queries = sorted(query_with_lsn, key=lambda x: float(x.get("ts", 0)))
        visibility_lags: list[float] = []
        for um in upsert_with_lsn[:50]:  # sample up to 50 upserts
            wlsn = int(um["write_lsn"])
            wts  = float(um.get("ts", 0))
            # find the first query after this upsert where max_indexed_lsn >= write_lsn
            for qm in sorted_queries:
                if float(qm.get("ts", 0)) >= wts and int(qm["max_indexed_lsn"]) >= wlsn:
                    visibility_lags.append((float(qm["ts"]) - wts))
                    break

        a(_md_table(
            ["Metric", "Value"],
            [
                ["Upserts with LSN data",        str(len(upsert_with_lsn))],
                ["Query cycles with LSN data",   str(len(query_with_lsn))],
                ["Latest write_lsn",             str(latest_write_lsn)],
                ["Latest max_indexed_lsn",       str(latest_indexed_lsn)],
                ["Current LSN lag (write - indexed)", str(lsn_lag)],
            ]
        ))
        if visibility_lags:
            visibility_lags.sort()
            p50_vis = visibility_lags[len(visibility_lags) // 2]
            a(f"\n**Write-to-visibility latency** (time from upsert until a query cycle "
              f"reflects `max_indexed_lsn ≥ write_lsn`, n={len(visibility_lags)}):\n")
            a(_md_table(
                ["p50", "min", "max"],
                [[f"{p50_vis:.1f}s",
                  f"{min(visibility_lags):.1f}s",
                  f"{max(visibility_lags):.1f}s"]],
            ))
        else:
            a("\n_Insufficient overlapping timestamps to compute write-to-visibility latency._")
    else:
        a("_No LSN data yet. LSN tracking requires at least one writer and querier cycle "
          "after the latest deployment._")

    # --- Server vs client latency breakdown ---
    a("\n## Server vs Client Latency\n")
    a(
        "Client latency is measured end-to-end by the ECS task. "
        "Server latency comes from the `x-pinecone-request-latency-ms` response header "
        "(Pinecone processing time only). Network overhead = client − server.\n"
    )
    query_with_server  = [r for r in query_metrics  if r.get("p50_server_ms") is not None]
    upsert_with_server = [r for r in upsert_metrics if r.get("server_latency_ms") is not None]

    if query_with_server:
        client_p50s  = [int(r["p50_ms"])        for r in query_with_server]
        server_p50s  = [int(r["p50_server_ms"])  for r in query_with_server]
        network_p50s = [int(r.get("p50_network_ms", 0)) for r in query_with_server
                        if r.get("p50_network_ms") is not None]
        a("### Query Latency Breakdown\n")
        a(_md_table(
            ["Component", "median (of cycle medians)", "min", "max"],
            [
                ["Client (end-to-end)",
                 fmt_ms(statistics.median(client_p50s)),
                 fmt_ms(min(client_p50s)), fmt_ms(max(client_p50s))],
                ["Server (Pinecone processing)",
                 fmt_ms(statistics.median(server_p50s)),
                 fmt_ms(min(server_p50s)), fmt_ms(max(server_p50s))],
                ["Network overhead",
                 fmt_ms(statistics.median(network_p50s)) if network_p50s else "—",
                 fmt_ms(min(network_p50s)) if network_p50s else "—",
                 fmt_ms(max(network_p50s)) if network_p50s else "—"],
            ]
        ))
    else:
        a("_No query server-latency data yet._\n")

    if upsert_with_server:
        client_lats  = [int(r["latency_ms"])        for r in upsert_with_server]
        server_lats  = [int(r["server_latency_ms"])  for r in upsert_with_server]
        net_lats     = [int(r["network_latency_ms"]) for r in upsert_with_server
                        if r.get("network_latency_ms") is not None]
        a("\n### Upsert Latency Breakdown\n")
        a(_md_table(
            ["Component", "median", "min", "max"],
            [
                ["Client (end-to-end)",
                 fmt_ms(statistics.median(client_lats)),
                 fmt_ms(min(client_lats)), fmt_ms(max(client_lats))],
                ["Server (Pinecone processing)",
                 fmt_ms(statistics.median(server_lats)),
                 fmt_ms(min(server_lats)), fmt_ms(max(server_lats))],
                ["Network overhead",
                 fmt_ms(statistics.median(net_lats)) if net_lats else "—",
                 fmt_ms(min(net_lats)) if net_lats else "—",
                 fmt_ms(max(net_lats)) if net_lats else "—"],
            ]
        ))
    else:
        a("\n_No upsert server-latency data yet._")

    # write file
    report_path.write_text("\n".join(lines))
    console.print(f"\n  Report saved to [bold cyan]{report_path}[/bold cyan]\n")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pinecone test bench monitor")
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--off",          action="store_true", help="Disable tracking and exit")
    group.add_argument("--stats",        action="store_true", help="Print summary stats and exit")
    group.add_argument("--probe",        action="store_true", help="Local upsert→poll probe")
    group.add_argument("--lambda-probe", action="store_true", help="Invoke tracker Lambda as prober")
    group.add_argument("--report",       action="store_true", help="Generate full markdown benchmark report")
    args = parser.parse_args()

    if args.off:
        set_freshness(False)
    elif args.stats:
        print_stats()
    elif args.probe:
        run_probe()
    elif args.lambda_probe:
        run_lambda_probe()
    elif args.report:
        generate_report()
    else:
        live_dashboard()
