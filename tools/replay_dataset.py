"""
Replays a real, labelled log dataset into LogPulse, the way production
services would stream logs into it.

Dataset: Loghub (https://github.com/logpai/loghub), a public collection of
real system logs used in log-parsing research. Each *_2k file has 2,000
real log lines, and every line is labelled with the template it belongs to
(EventId), e.g. "mod_jk child workerEnv in error state <*>". That label is
the ground truth used to check whether LogPulse's learned rules group logs
correctly.

Usage (app must be running):
    python tools/replay_dataset.py --dataset Apache --limit 500 --rate 5
    python tools/replay_dataset.py --dataset OpenStack --split train      # after tools/split_dataset.py

For the full train/validation/test experiment use tools/run_experiment.py.
Results are saved to results/replay-<dataset>[-<split>]-<time>.json.
"""
import argparse
import csv
import json
import os
import time
from pathlib import Path

import httpx

LOGHUB_URL = "https://raw.githubusercontent.com/logpai/loghub/master/{name}/{name}_2k.log_structured.csv"
DATASETS = ["Apache", "OpenStack", "HDFS", "Linux", "Zookeeper", "Hadoop", "Spark", "BGL"]
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RESULTS_DIR = ROOT / "results"


def download_dataset(name: str) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / f"{name}_2k.log_structured.csv"
    if path.exists():
        return path
    url = LOGHUB_URL.format(name=name)
    print(f"Downloading {url}")
    response = httpx.get(url, timeout=60.0, follow_redirects=True)
    response.raise_for_status()
    path.write_bytes(response.content)
    return path


def split_path(dataset: str, split: str) -> Path:
    return DATA_DIR / "splits" / f"{dataset}_{split}.csv"


def load_rows(path: Path, dataset: str) -> list[dict]:
    """One LogPulse payload per dataset line; the template id rides along as ground truth."""
    rows = []
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            message = (row.get("Content") or "").strip()
            if not message:
                continue
            component = (row.get("Component") or "").strip()
            rows.append({
                "service": f"{dataset.lower()}-{component}" if component else dataset.lower(),
                "level": (row.get("Level") or "INFO").strip().upper() or "INFO",
                "message": message,
                "source": "loghub",
                "event_id": f"{dataset}:{row.get('EventId', '')}",
            })
    return rows


def load_recording(path: Path) -> list[dict]:
    """
    A session recorded from /v1/logs (OTLP_RECORD_FILE), optionally labelled by
    tools/label_with_drain.py. Order is kept, so a replay is the same session.
    """
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        row = {"service": r["service"], "level": r["level"], "message": r["message"], "source": "otlp-replay"}
        if r.get("event_id"):
            row["event_id"] = r["event_id"]
        rows.append(row)
    return rows


def wait_for_queue_to_drain(client: httpx.Client, base_url: str, since: float, timeout_s: float,
                            expected: int = 0) -> None:
    """
    Done means: Kafka has delivered every line that was sent (`expected` rows
    exist) AND nothing is still compiling. Checking only the compile queue
    reads results while lines are still in Kafka, and undercounts.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        summary = client.get(f"{base_url}/metrics/summary", params={"since": since}).json()
        metrics = client.get(f"{base_url}/metrics").json()
        stored = summary["logs"]["total"]
        if stored >= expected and summary["logs"]["pending"] == 0 and metrics.get("compile_queue_depth", 0) == 0:
            return
        print(f"  waiting: {stored}/{expected} lines stored, {summary['logs']['pending']} still compiling, "
              f"queue depth {metrics.get('compile_queue_depth')}")
        time.sleep(5)
    raise SystemExit(f"gave up after {timeout_s:.0f}s: not every line was processed, so the results would be incomplete")


def replay(rows: list[dict], base_url: str, api_key: str, rate: float, drain_timeout: float = 900.0) -> dict:
    """Sends rows at `rate` per second, waits for the pipeline, returns counts + the run's summary."""
    headers = {"X-API-Key": api_key} if api_key else {}
    started = time.time()
    sent = failed = 0
    interval = 1.0 / rate if rate > 0 else 0

    with httpx.Client(headers=headers, timeout=10.0) as client:
        for i, payload in enumerate(rows, 1):
            tick = time.perf_counter()
            try:
                response = client.post(f"{base_url}/v1/telemetry/ingest", json=payload)
                if response.status_code == 202:
                    sent += 1
                else:
                    failed += 1
                    print(f"  line {i}: HTTP {response.status_code} {response.text[:120]}")
            except httpx.HTTPError as e:
                failed += 1
                print(f"  line {i}: {type(e).__name__}: {e}")
            if i % 200 == 0:
                print(f"  sent {i}/{len(rows)}")
            sleep_for = interval - (time.perf_counter() - tick)
            if sleep_for > 0:
                time.sleep(sleep_for)

        time.sleep(2)
        wait_for_queue_to_drain(client, base_url, started, drain_timeout, expected=sent)
        summary = client.get(f"{base_url}/metrics/summary", params={"since": started}).json()

    return {"lines_sent": sent, "lines_failed": failed, "started_at": started, "summary": summary}


def print_summary(summary: dict) -> None:
    logs, naive, lat, ev = summary["logs"], summary["naive_comparison"], summary["latency"], summary["dataset_eval"]
    print(f"Logs handled          : {logs['handled']}  (cache hits {logs['cache_hit']}, LLM-compiled "
          f"{logs['llm_compiled']}, failed {logs['failed']}, unresolved {logs.get('unresolved', 0)})")
    print(f"Cache hit rate        : {summary['cache_hit_rate_pct']}%")
    print(f"Distinct templates    : {ev['distinct_templates']}")
    print(f"LLM calls             : {naive['actual_llm_calls']}  vs {naive['naive_llm_calls']} for one-call-per-log")
    print(f"LLM tokens            : {naive['actual_tokens']}  vs ~{naive['naive_tokens_estimate']} naive "
          f"-> {naive['token_reduction_pct']}% fewer")
    print(f"Rule precision        : {ev['rule_precision_pct']}% of {ev['cache_hits_checked']} checked cache hits "
          f"used a rule learned from the same template")
    print(f"Kafka queue lag       : p50 {lat['kafka_queue_lag']['p50_ms']} ms, p95 {lat['kafka_queue_lag']['p95_ms']} ms")
    print(f"Ingest->fix (hit)     : p50 {lat['ingest_to_fix_cache_hit']['p50_ms']} ms, "
          f"p95 {lat['ingest_to_fix_cache_hit']['p95_ms']} ms  ({logs['cache_hit_instant']} instant hits)")
    print(f"Ingest->fix (waited)  : p50 {lat['ingest_to_fix_hit_after_wait']['p50_ms']} ms, "
          f"p95 {lat['ingest_to_fix_hit_after_wait']['p95_ms']} ms  ({logs['cache_hit_after_wait']} hits that "
          f"queued behind a compile, no LLM call)")
    print(f"Ingest->fix (LLM)     : p50 {lat['ingest_to_fix_llm']['p50_ms']} ms, p95 {lat['ingest_to_fix_llm']['p95_ms']} ms")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", default="Apache", choices=DATASETS)
    parser.add_argument("--split", choices=["train", "val", "test"], help="replay one split from tools/split_dataset.py")
    parser.add_argument("--recording", type=Path, help="replay a recorded OTLP session (.jsonl) instead of a dataset")
    parser.add_argument("--limit", type=int, default=2000, help="how many lines to send")
    parser.add_argument("--rate", type=float, default=5.0, help="logs per second")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", ""))
    parser.add_argument("--drain-timeout", type=float, default=900.0)
    args = parser.parse_args()

    if args.recording:
        rows = load_recording(args.recording)[: args.limit]
        label = args.recording.stem
    else:
        if args.split:
            path = split_path(args.dataset, args.split)
            if not path.exists():
                raise SystemExit(f"{path} not found; run: python tools/split_dataset.py --dataset {args.dataset}")
        else:
            path = download_dataset(args.dataset)
        rows = load_rows(path, args.dataset)[: args.limit]
        label = f"{args.dataset}{'-' + args.split if args.split else ''}"
    templates = {r.get("event_id") for r in rows if r.get("event_id")}
    print(f"Replaying {len(rows)} {label} logs ({len(templates)} distinct templates) at {args.rate}/s")

    run = replay(rows, args.url, args.api_key, args.rate, args.drain_timeout)
    result = {"kind": "dataset_replay", "dataset": args.dataset, "split": args.split,
              "distinct_templates_sent": len(templates), "rate_per_s": args.rate, **run}
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"replay-{label}-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(result, indent=2))

    print(f"\nSent {run['lines_sent']}, failed {run['lines_failed']}\n=== Results ===")
    print_summary(run["summary"])
    print(f"\nSaved {out}")


if __name__ == "__main__":
    main()
