"""
Splits a Loghub dataset into train / validation / test (70 / 10 / 20 by default).

  train       the system learns from these: cache misses go to the LLM and
              become rules.
  validation  rules are frozen (learning off). Used to compare settings,
              e.g. prompt or redaction changes, without touching test.
  test        rules frozen, run once at the end: the number to report.

Default split is CHRONOLOGICAL: the first 70% of the file is train, the next
10% validation, the last 20% test. That matches how the system is used, since
it learns from past logs and is judged on future ones. A random split would
put near-identical neighbouring lines on both sides and inflate the result.
--mode random --seed N shuffles first, for comparison.

Writes data/splits/<Dataset>_<split>.csv and data/splits/<Dataset>_manifest.json.

Usage:
    python tools/split_dataset.py --dataset OpenStack
    python tools/split_dataset.py --dataset Apache --mode random --seed 42
"""
import argparse
import csv
import json
import random
from pathlib import Path

from replay_dataset import DATA_DIR, DATASETS, download_dataset

SPLIT_DIR = DATA_DIR / "splits"
SPLITS = ("train", "val", "test")


def split_rows(rows: list[dict], ratios=(0.7, 0.1, 0.2), mode: str = "chronological", seed: int = 42):
    """Pure function: rows in, {"train": [...], "val": [...], "test": [...]} out."""
    if abs(sum(ratios) - 1.0) > 1e-9:
        raise ValueError(f"ratios must add up to 1, got {ratios}")
    rows = list(rows)
    if mode == "random":
        random.Random(seed).shuffle(rows)
    elif mode != "chronological":
        raise ValueError(f"unknown mode {mode!r}")
    n = len(rows)
    train_end = round(n * ratios[0])
    val_end = train_end + round(n * ratios[1])
    return {"train": rows[:train_end], "val": rows[train_end:val_end], "test": rows[val_end:]}


def describe(parts: dict[str, list[dict]]) -> dict:
    """Counts per split, and how many templates in val/test never appear in train."""
    train_templates = {r["EventId"] for r in parts["train"]}
    info = {}
    for name, rows in parts.items():
        templates = {r["EventId"] for r in rows}
        unseen = templates - train_templates
        info[name] = {
            "lines": len(rows),
            "templates": len(templates),
            "templates_not_in_train": len(unseen) if name != "train" else 0,
            "lines_with_template_not_in_train": sum(r["EventId"] in unseen for r in rows) if name != "train" else 0,
        }
    return info


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", default="OpenStack", choices=DATASETS)
    parser.add_argument("--mode", default="chronological", choices=["chronological", "random"])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--ratios", default="0.7,0.1,0.2", help="train,val,test fractions")
    args = parser.parse_args()

    ratios = tuple(float(x) for x in args.ratios.split(","))
    source = download_dataset(args.dataset)
    with source.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = [r for r in reader if (r.get("Content") or "").strip()]

    parts = split_rows(rows, ratios, args.mode, args.seed)
    SPLIT_DIR.mkdir(parents=True, exist_ok=True)
    for name in SPLITS:
        with (SPLIT_DIR / f"{args.dataset}_{name}.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(parts[name])

    manifest = {"dataset": args.dataset, "source": source.name, "mode": args.mode,
                "seed": args.seed if args.mode == "random" else None, "ratios": ratios,
                "splits": describe(parts)}
    (SPLIT_DIR / f"{args.dataset}_manifest.json").write_text(json.dumps(manifest, indent=2))

    print(f"{args.dataset}: {len(rows)} lines, {args.mode} split {ratios}")
    for name, info in manifest["splits"].items():
        extra = "" if name == "train" else (f", {info['templates_not_in_train']} templates never seen in train "
                                            f"({info['lines_with_template_not_in_train']} lines)")
        print(f"  {name:<5} {info['lines']:>5} lines, {info['templates']:>3} templates{extra}")
    print(f"Wrote {SPLIT_DIR}")


if __name__ == "__main__":
    main()
