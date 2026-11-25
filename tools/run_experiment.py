"""
The full evaluation: learn on train, then judge on data the system never saw.

  1. train  (70%)  learning ON  -> misses go to the LLM and become rules
  2. val    (10%)  learning OFF -> rules frozen; measures generalization
  3. test   (20%)  learning OFF -> rules frozen; the number to report

On val and test a miss is NOT sent to the LLM, so their hit rate shows how
well rules learned from past logs cover future logs. Misses are split into
"template was in train" (the rule was too narrow: a real miss) and "template
never appeared in train" (nothing could have covered it).

The app must start from an EMPTY rule set, or results leak from earlier runs:

    LOGPULSE_DB=experiment.db LOGPULSE_REDIS_KEY=logpulse:exp:rules uvicorn app:app

Then:
    python tools/split_dataset.py --dataset OpenStack
    python tools/run_experiment.py --dataset OpenStack

Saves results/experiment-<dataset>-<time>.json and prints a table.
"""
import argparse
import json
import os
import time

import httpx

from replay_dataset import RESULTS_DIR, load_rows, print_summary, replay, split_path

PHASES = [("train", True), ("val", False), ("test", False)]


def set_learning(base_url: str, api_key: str, enabled: bool) -> None:
    headers = {"X-API-Key": api_key} if api_key else {}
    response = httpx.post(f"{base_url}/admin/learning", json={"enabled": enabled}, headers=headers, timeout=10.0)
    response.raise_for_status()


def coverage(summary: dict, train_templates: set[str]) -> dict:
    """Hit rate restricted to logs whose template appeared in train."""
    seen_logs = seen_hits = unseen_logs = unseen_hits = 0
    for event_id, stats in summary["dataset_eval"]["by_template"].items():
        if event_id in train_templates:
            seen_logs += stats["logs"]
            seen_hits += stats["cache_hit"]
        else:
            unseen_logs += stats["logs"]
            unseen_hits += stats["cache_hit"]
    return {
        "logs_with_template_seen_in_train": seen_logs,
        "logs_with_template_not_in_train": unseen_logs,
        "hit_rate_on_seen_templates_pct": round(100 * seen_hits / seen_logs, 1) if seen_logs else None,
        # A never-seen type has no correct rule, so ANY hit on it is a wrong fix.
        "false_matches_on_unseen_templates": unseen_hits,
        "false_match_rate_on_unseen_pct": round(100 * unseen_hits / unseen_logs, 1) if unseen_logs else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", default="OpenStack")
    parser.add_argument("--rate", type=float, default=10.0, help="logs per second")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", ""))
    args = parser.parse_args()

    rows = {name: load_rows(split_path(args.dataset, name), args.dataset) for name, _ in PHASES}
    rules_before = len(httpx.get(f"{args.url}/rules", timeout=10.0).json()["rules"])
    if rules_before:
        raise SystemExit(f"The app already has {rules_before} rules, so this run would not start from zero.\n"
                         "Restart it with a fresh LOGPULSE_DB and LOGPULSE_REDIS_KEY (see this file's docstring).")

    train_templates = {r["event_id"] for r in rows["train"]}
    phases = {}
    try:
        for name, learning in PHASES:
            set_learning(args.url, args.api_key, learning)
            print(f"\n=== {name.upper()}: {len(rows[name])} lines, learning {'ON' if learning else 'OFF'} ===")
            run = replay(rows[name], args.url, args.api_key, args.rate)
            run["learning"] = learning
            run["templates"] = len({r["event_id"] for r in rows[name]})
            run["coverage"] = coverage(run["summary"], train_templates)
            run["summary"]["dataset_eval"].pop("by_template", None)  # bulky; coverage keeps what matters
            print_summary(run["summary"])
            phases[name] = run
    finally:
        set_learning(args.url, args.api_key, True)

    rules_learned = len(httpx.get(f"{args.url}/rules", timeout=10.0).json()["rules"])
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"experiment-{args.dataset}-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps({"kind": "experiment", "dataset": args.dataset, "rate_per_s": args.rate,
                               "rules_learned_in_train": rules_learned, "phases": phases}, indent=2))

    print(f"\n=== {args.dataset}: {rules_learned} rules learned on train ===")
    print(f"{'phase':<6}{'lines':>7}{'tmpl':>6}{'hit %':>8}{'hit % seen tmpl':>17}{'unseen-tmpl lines':>19}"
          f"{'false matches':>15}{'LLM calls':>11}{'tokens':>9}{'precision %':>13}")
    for name, run in phases.items():
        s, cov = run["summary"], run["coverage"]
        print(f"{name:<6}{s['logs']['handled']:>7}{run['templates']:>6}{str(s['cache_hit_rate_pct']):>8}"
              f"{str(cov['hit_rate_on_seen_templates_pct']):>17}{cov['logs_with_template_not_in_train']:>19}"
              f"{cov['false_matches_on_unseen_templates']:>15}"
              f"{s['llm']['pipeline']['calls']:>11}{s['llm']['pipeline']['total_tokens']:>9}"
              f"{str(s['dataset_eval']['rule_precision_pct']):>13}")
    print(f"\nSaved {out}")


if __name__ == "__main__":
    main()
