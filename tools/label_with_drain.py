"""
Labels a recorded session with an independent "type" for every incident,
so LogPulse's rule matching can be checked on logs that came with no labels
(e.g. the OpenTelemetry demo).

Labeller: Drain3 (https://github.com/logpai/Drain3), an open-source log
template miner from the same research line as the Loghub labels. It groups
log lines into templates, e.g. "Payment failed for order <NUM>", without
any LLM. LogPulse's regex rules come from an LLM, so agreement between the
two is evidence of correct grouping. Drain3 is itself a heuristic: where the
two disagree, look at the lines and decide which one was right.

Two passes: first every message is fed to Drain3 (templates can still merge
while it learns), then every message is matched against the final templates.

Usage:
    python tools/label_with_drain.py results/demo-recording.jsonl
Writes results/demo-recording.labelled.jsonl (adds event_id and drain_template)
and prints the templates found.
"""
import argparse
import json
from collections import Counter
from pathlib import Path

from drain3 import TemplateMiner
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig


def make_miner() -> TemplateMiner:
    config = TemplateMinerConfig()
    # Replace obvious variable values before grouping, as Drain3's own examples do.
    config.masking_instructions = [
        MaskingInstruction(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "UUID"),
        MaskingInstruction(r"\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?", "IP"),
        MaskingInstruction(r"\b0x[0-9a-fA-F]+\b", "HEX"),
        MaskingInstruction(r"\b[0-9a-fA-F]{16,}\b", "HEX"),
        MaskingInstruction(r"(?<![A-Za-z])-?\d+(?:\.\d+)?(?:ms|s|%)?(?![A-Za-z])", "NUM"),
    ]
    config.drain_sim_th = 0.5
    return TemplateMiner(config=config)


def label(records: list[dict]) -> tuple[list[dict], dict[int, str]]:
    miner = make_miner()
    for r in records:
        miner.add_log_message(r["message"])
    templates = {c.cluster_id: c.get_template() for c in miner.drain.clusters}
    labelled = []
    for r in records:
        cluster = miner.match(r["message"], full_search_strategy="fallback")
        cluster_id = cluster.cluster_id if cluster else None
        labelled.append({**r, "event_id": f"drain:{cluster_id}",
                         "drain_template": templates.get(cluster_id, "")})
    return labelled, templates


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("recording", type=Path)
    args = parser.parse_args()

    records = [json.loads(line) for line in args.recording.read_text(encoding="utf-8").splitlines() if line.strip()]
    labelled, templates = label(records)
    out = args.recording.with_suffix(".labelled.jsonl")
    out.write_text("".join(json.dumps(r) + "\n" for r in labelled), encoding="utf-8")

    counts = Counter(r["event_id"] for r in labelled)
    print(f"{len(records)} incidents, {len(templates)} Drain3 templates")
    for event_id, n in counts.most_common():
        cluster_id = int(event_id.split(":")[1]) if event_id.split(":")[1].isdigit() else None
        print(f"  {n:>5}  {event_id:<10} {templates.get(cluster_id, '?')[:110]}")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
