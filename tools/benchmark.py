"""
Measures the claim "async ingestion keeps the API fast" instead of asserting it.

Two designs, same machine, same kind of request:

  async   POST /v1/telemetry/ingest       -> hands the log to Kafka, returns 202.
                                             The LLM work happens later, off the request path.
  sync    POST /v1/telemetry/ingest-sync  -> the naive design: calls the LLM inline for
                                             every log, the client waits for the answer.

For each it fires N requests with C in flight at once and records every
request's latency, then reports p50/p95/p99, throughput and errors.

Usage (app must be running; sync mode needs GROQ_API_KEY set in the app):
    python tools/benchmark.py --mode async --requests 2000 --concurrency 50
    python tools/benchmark.py --mode sync  --requests 20   --concurrency 5
    python tools/benchmark.py --mode both

Keep the sync run small: every sync request is a real LLM call that uses
tokens and counts against Groq's free-tier rate limit (about 30 requests
a minute), which is exactly the cost the async design avoids.
Results are saved to results/benchmark-<time>.json.
"""
import argparse
import asyncio
import json
import math
import os
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"

SAMPLE_MESSAGES = [
    "Payment gateway timeout after {n}ms",
    "Database connection refused on replica db-{n}",
    "Disk usage at {n}% on node {n}",
    "Out of memory failure on container worker node clusters-{n}",
    "Cache eviction rate above threshold on node {n}",
]


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), math.ceil(pct / 100 * len(ordered))))
    return round(ordered[rank - 1], 2)


async def run_load(base_url: str, path: str, total: int, concurrency: int, headers: dict, tag: str) -> dict:
    latencies_ms: list[float] = []
    status_counts: dict[str, int] = {}
    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)

    async with httpx.AsyncClient(base_url=base_url, headers=headers, limits=limits, timeout=120.0) as client:
        async def one(i: int):
            payload = {
                "service": "benchmark",
                "level": "ERROR",
                "message": SAMPLE_MESSAGES[i % len(SAMPLE_MESSAGES)].format(n=i),
                "source": f"benchmark-{tag}",
            }
            async with semaphore:
                started = time.perf_counter()
                try:
                    response = await client.post(path, json=payload)
                    key = str(response.status_code)
                except httpx.HTTPError as e:
                    key = type(e).__name__
                elapsed = (time.perf_counter() - started) * 1000
            status_counts[key] = status_counts.get(key, 0) + 1
            if key in ("200", "202"):
                latencies_ms.append(elapsed)

        wall_started = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(total)))
        wall_s = time.perf_counter() - wall_started

    ok = len(latencies_ms)
    return {
        "endpoint": path,
        "requests": total,
        "concurrency": concurrency,
        "succeeded": ok,
        "status_counts": status_counts,
        "wall_time_s": round(wall_s, 2),
        "throughput_rps": round(ok / wall_s, 1) if wall_s else None,
        "latency_ms": {
            "p50": percentile(latencies_ms, 50),
            "p95": percentile(latencies_ms, 95),
            "p99": percentile(latencies_ms, 99),
            "max": round(max(latencies_ms), 2) if latencies_ms else None,
            "mean": round(sum(latencies_ms) / ok, 2) if ok else None,
        },
    }


def print_table(results: dict):
    print(f"\n{'design':<8}{'reqs':>7}{'conc':>6}{'ok':>7}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}{'req/s':>9}")
    for name, r in results.items():
        lat = r["latency_ms"]
        print(f"{name:<8}{r['requests']:>7}{r['concurrency']:>6}{r['succeeded']:>7}"
              f"{str(lat['p50']):>10}{str(lat['p95']):>10}{str(lat['p99']):>10}{str(r['throughput_rps']):>9}")
        if set(r["status_counts"]) - {"200", "202"}:
            print(f"        non-success responses: {r['status_counts']}")


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["async", "sync", "both"], default="both")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", ""))
    parser.add_argument("--requests", type=int, help="requests per design (default: 2000 async, 20 sync)")
    parser.add_argument("--concurrency", type=int, help="requests in flight (default: 50 async, 5 sync)")
    args = parser.parse_args()

    headers = {"X-API-Key": args.api_key} if args.api_key else {}
    results = {}
    started = time.time()

    if args.mode in ("async", "both"):
        print("Running async design (Kafka, 202 Accepted)...")
        results["async"] = await run_load(args.url, "/v1/telemetry/ingest",
                                          args.requests or 2000, args.concurrency or 50, headers, "async")
    if args.mode in ("sync", "both"):
        print("Running sync baseline (LLM inline on every request)...")
        results["sync"] = await run_load(args.url, "/v1/telemetry/ingest-sync",
                                         args.requests or 20, args.concurrency or 5, headers, "sync")

    print_table(results)

    async with httpx.AsyncClient(base_url=args.url, timeout=30.0) as client:
        summary = (await client.get("/metrics/summary", params={"since": started})).json()
    baseline_llm = summary["llm"]["sync_baseline"]
    if baseline_llm["calls"]:
        print(f"\nSync baseline used {baseline_llm['total_tokens']} tokens over {baseline_llm['calls']} LLM calls "
              f"(avg {baseline_llm['avg_tokens_per_call']} per log); LLM latency p50 "
              f"{baseline_llm['latency']['p50_ms']} ms.")

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"benchmark-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps({"kind": "benchmark", "started_at": started, "results": results,
                               "llm_usage_during_run": summary["llm"]}, indent=2))
    print(f"Saved {out}")
    print("Note: the async run also queued its logs for processing; see the dashboard or /metrics/summary.")


if __name__ == "__main__":
    asyncio.run(main())
