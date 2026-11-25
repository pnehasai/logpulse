"""
Held-out evaluation: for each dataset, start a FRESH app (empty rules, its
own database, Redis key and Kafka topic), split the dataset 70/10/20 in time
order, and run tools/run_experiment.py (learn on train, rules frozen on
validation and test). Then stop the app.

"Held-out" means: the system was not tuned on these datasets. Run them once
on frozen code and report what comes out.

Needs Redis + Kafka (docker compose up -d) and GROQ_API_KEY in the environment.

    python tools/run_heldout.py --datasets HDFS,Zookeeper,Linux
"""
import argparse
import hashlib
import os
import subprocess
import sys
import time

import httpx

from replay_dataset import RESULTS_DIR, ROOT

PORT = 8000


def wait_for_app(proc: subprocess.Popen, timeout_s: float = 120) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc.poll() is not None:
            raise SystemExit("app exited during startup; see results/heldout-logs/")
        try:
            metrics = httpx.get(f"http://127.0.0.1:{PORT}/metrics", timeout=3).json()
            if metrics["kafka_partitions_assigned"]:
                return
        except (httpx.HTTPError, ValueError, KeyError):
            pass
        time.sleep(2)
    raise SystemExit("app did not become ready")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--datasets", default="HDFS,Zookeeper,Linux")
    parser.add_argument("--rate", type=float, default=20.0)
    args = parser.parse_args()
    if not os.environ.get("GROQ_API_KEY"):
        raise SystemExit("GROQ_API_KEY is not set in this terminal")

    code_hash = hashlib.sha256((ROOT / "app.py").read_bytes()).hexdigest()[:12]
    print(f"app.py fingerprint {code_hash} (same for every dataset = same frozen code)")
    log_dir = RESULTS_DIR / "heldout-logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    for dataset in [d.strip() for d in args.datasets.split(",") if d.strip()]:
        run_id = f"heldout-{dataset}-{time.strftime('%Y%m%d-%H%M%S')}"
        print(f"\n##### {dataset} ({run_id}) #####")
        subprocess.run([sys.executable, "tools/split_dataset.py", "--dataset", dataset], cwd=ROOT, check=True)
        env = {**os.environ, "LOGPULSE_DB": str(RESULTS_DIR / f"{run_id}.db"),
               "LOGPULSE_REDIS_KEY": f"logpulse:{run_id}:rules", "KAFKA_TOPIC": f"logpulse-{run_id}",
               "KAFKA_TOPIC_PARTITIONS": "1", "PYTHONIOENCODING": "utf-8", "APP_FINGERPRINT": code_hash}
        log = (log_dir / f"{run_id}.log").open("w", encoding="utf-8")
        app = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(PORT)],
                               cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            wait_for_app(app)
            subprocess.run([sys.executable, "tools/run_experiment.py", "--dataset", dataset, "--rate", str(args.rate),
                            "--url", f"http://127.0.0.1:{PORT}"], cwd=ROOT, check=True)
        finally:
            app.terminate()
            try:
                app.wait(timeout=15)
            except subprocess.TimeoutExpired:
                app.kill()


if __name__ == "__main__":
    main()
