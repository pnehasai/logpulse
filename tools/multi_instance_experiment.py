"""
What does Redis actually buy when several copies of LogPulse run at once?

Starts N real app instances (default 3) in one Kafka consumer group, so each
instance owns one partition of a fresh topic and gets about 1/N of the logs.
Replays the same dataset twice:

  sharing ON   L1 -> Redis (L2) -> LLM. A rule learned by one instance is
               found by the others in Redis; the Redis lock stops two
               instances compiling the same message at the same time.
  sharing OFF  L1 -> LLM. Each instance only knows the rules it compiled
               itself, so every instance pays the LLM for every error type.

and compares LLM calls, tokens, duplicate compiles, and fix latency per
layer (L1 hit vs L2 hit vs LLM).

Each run gets its own SQLite file, Redis key and Kafka topic, so nothing
carries over between runs or from your normal data.

Needs Redis + Kafka (docker compose up -d) and GROQ_API_KEY in the environment.

    python tools/multi_instance_experiment.py --dataset Apache --limit 2000 --rate 20
    python tools/multi_instance_experiment.py --modes on        # only one setting
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

from replay_dataset import RESULTS_DIR, ROOT, download_dataset, load_recording, load_rows, replay

BASE_PORT = 8001


def create_topic(topic: str, partitions: int) -> None:
    """Create the run's topic with all its partitions BEFORE any instance starts.
    Otherwise the first client to touch it can auto-create it with 1 partition."""
    import asyncio

    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    async def go():
        admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
        await admin.start()
        try:
            await admin.create_topics([NewTopic(topic, partitions, 1)])
            for _ in range(20):
                info = (await admin.describe_topics([topic]) or [{}])[0]
                if not info.get("error_code") and len(info.get("partitions") or []) == partitions:
                    return
                await asyncio.sleep(0.5)
            raise SystemExit(f"topic {topic} did not come up with {partitions} partitions: {info}")
        finally:
            await admin.close()

    asyncio.run(go())
    print(f"  created topic {topic} with {partitions} partitions")


def start_instances(n: int, sharing: bool, run_id: str, log_dir: Path) -> list[subprocess.Popen]:
    create_topic(f"logpulse-{run_id}", n)
    # Create the shared database once, before n processes race to set it up.
    db_env = {**os.environ, "LOGPULSE_DB": str(RESULTS_DIR / f"{run_id}.db"), "PYTHONIOENCODING": "utf-8"}
    subprocess.run([sys.executable, "-c", "import app"], cwd=ROOT, env=db_env, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs = []
    for i in range(1, n + 1):
        env = {
            **os.environ,
            "INSTANCE_ID": f"app-{i}",
            "REDIS_SHARING": "1" if sharing else "0",
            "LOGPULSE_DB": str(RESULTS_DIR / f"{run_id}.db"),
            "LOGPULSE_REDIS_KEY": f"logpulse:{run_id}:rules",
            "KAFKA_TOPIC": f"logpulse-{run_id}",
            "KAFKA_TOPIC_PARTITIONS": str(n),
            "PYTHONIOENCODING": "utf-8",
        }
        log = (log_dir / f"{run_id}-app-{i}.log").open("w", encoding="utf-8")
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(BASE_PORT + i - 1)],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
        ))
    return procs


def wait_until_ready(procs: list[subprocess.Popen], timeout_s: float = 180) -> None:
    """Every instance answers, and the topic's n partitions are spread one per instance."""
    n = len(procs)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        dead = [i + 1 for i, p in enumerate(procs) if p.poll() is not None]
        if dead:
            raise SystemExit(f"instance(s) {dead} exited during startup; see results/multi-logs/")
        assigned = []
        try:
            for i in range(n):
                assigned.append(httpx.get(f"http://127.0.0.1:{BASE_PORT + i}/metrics", timeout=3).json()
                                ["kafka_partitions_assigned"])
        except (httpx.HTTPError, KeyError, ValueError):
            time.sleep(3)
            continue
        if all(len(a) == 1 for a in assigned) and len({a[0] for a in assigned}) == n:
            print(f"  ready: partitions per instance {assigned}")
            return
        print(f"  waiting for Kafka to spread partitions: {assigned}")
        time.sleep(3)
    raise SystemExit("instances did not become ready; see results/multi-logs/")


def wait_for_all_queues(n: int, since: float, timeout_s: float = 900) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        depths = [httpx.get(f"http://127.0.0.1:{BASE_PORT + i}/metrics", timeout=5).json()["compile_queue_depth"]
                  for i in range(n)]
        pending = httpx.get(f"http://127.0.0.1:{BASE_PORT}/metrics/summary", params={"since": since},
                            timeout=30).json()["logs"]["pending"]
        if pending == 0 and sum(depths) == 0:
            return
        print(f"  waiting: {pending} pending, queue depth per instance {depths}")
        time.sleep(5)


def stop(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            p.kill()


def run_mode(sharing: bool, rows: list[dict], n: int, rate: float, api_key: str, log_dir: Path) -> dict:
    run_id = f"multi-{'on' if sharing else 'off'}-{time.strftime('%Y%m%d-%H%M%S')}"
    print(f"\n=== Redis sharing {'ON' if sharing else 'OFF'}: {n} instances, run {run_id} ===")
    procs = start_instances(n, sharing, run_id, log_dir)
    try:
        wait_until_ready(procs)
        # The replay posts everything to instance 1's API; Kafka spreads it over all n consumers.
        run = replay(rows, f"http://127.0.0.1:{BASE_PORT}", api_key, rate)
        wait_for_all_queues(n, run["started_at"])
        run["summary"] = httpx.get(f"http://127.0.0.1:{BASE_PORT}/metrics/summary",
                                   params={"since": run["started_at"]}, timeout=30).json()
        run["summary"]["dataset_eval"].pop("by_template", None)
    finally:
        stop(procs)
    run.update({"run_id": run_id, "redis_sharing": sharing, "instances": n})
    return run


def print_comparison(results: dict) -> None:
    print("\nLookup = time of the cache check itself. LLM call = one Groq request.")
    print(f"{'sharing':<9}{'LLM calls':>10}{'tokens':>9}{'extra compiles':>16}{'L2 hits':>9}"
          f"{'L1 lookup p50':>15}{'L2 lookup p50':>15}{'LLM call p50':>14}{'precision':>11}")
    for name, run in results.items():
        s = run["summary"]
        lookups = s["latency"]["cache_lookup_by_result"]
        l2_hits = sum(i["l2_hits"] for i in s["instances"].values())

        def p50(key):
            value = lookups.get(key, {}).get("p50_ms")
            return f"{value} ms" if value is not None else "-"

        llm_p50 = s["llm"]["pipeline"]["latency"]["p50_ms"]
        llm_text = f"{llm_p50} ms" if llm_p50 is not None else "-"
        print(f"{name:<9}{s['llm']['pipeline']['calls']:>10}{s['llm']['pipeline']['total_tokens']:>9}"
              f"{s['duplicate_compiles']['extra_compiles_beyond_one_per_template']:>16}{l2_hits:>9}"
              f"{p50('L1_LOCAL_CACHE'):>15}{p50('L2_REDIS_FALLBACK'):>15}{llm_text:>14}"
              f"{str(s['dataset_eval']['rule_precision_pct']):>11}")
    for name, run in results.items():
        print(f"\nper instance, sharing {name}:")
        for inst, v in sorted(run["summary"]["instances"].items()):
            print(f"  {inst}: {v['logs']} logs, L1 hits {v['l1_hits']}, L2 hits {v['l2_hits']}, "
                  f"LLM calls {v['llm_calls']} ({v['llm_tokens']} tokens)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", default="Apache")
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--instances", type=int, default=3)
    parser.add_argument("--modes", default="on,off", help="on, off, or on,off")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", ""))
    parser.add_argument("--recording", type=Path, help="replay a recorded OTLP session (.jsonl) instead of a dataset")
    args = parser.parse_args()

    if not os.environ.get("GROQ_API_KEY"):
        raise SystemExit("GROQ_API_KEY is not set in this terminal")
    if args.recording:
        rows = load_recording(args.recording)[: args.limit]
        args.dataset = args.recording.stem
    else:
        rows = load_rows(download_dataset(args.dataset), args.dataset)[: args.limit]
    log_dir = RESULTS_DIR / "multi-logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    for mode in [m.strip() for m in args.modes.split(",") if m.strip()]:
        results[mode] = run_mode(mode == "on", rows, args.instances, args.rate, args.api_key, log_dir)

    out = RESULTS_DIR / f"multi-instance-{args.dataset}-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps({"kind": "multi_instance", "dataset": args.dataset, "lines": len(rows),
                               "rate_per_s": args.rate, "results": results}, indent=2))
    print_comparison(results)
    print(f"\nSaved {out}")


if __name__ == "__main__":
    main()
