"""
Builds a review sheet for the rules LogPulse learned, so a person can judge
what no automatic metric can: is each LLM-written fix actually useful, and
does each rule match the right incidents?

One row per rule: the regex, the fix, the incident it was learned from, how
many incidents it matched, a few sample matches, and (if the session was
labelled with tools/label_with_drain.py) how many of those matches Drain3
put in a different template. Two columns are left empty for you:

  matches_right_incidents   yes / no
  fix_quality               useful / vague / wrong

Open the CSV in Excel or Google Sheets, fill in the two columns, then
summarise, e.g. "25 of 28 fixes rated useful, 28 of 28 rules match correctly".

Usage:
    python tools/make_review_sheet.py results/demo-live.db
"""
import argparse
import csv
import sqlite3
from pathlib import Path


def build_rows(db_path: Path, samples: int = 3) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rules = conn.execute(
            "SELECT r.id, r.regex_pattern, r.cached_fix, s.message AS source_message, s.service AS source_service, "
            "s.event_id AS source_event FROM compiled_rules_cache r LEFT JOIN system_logs s ON s.id = r.source_log_id "
            "ORDER BY r.id"
        ).fetchall()
        rows = []
        for rule in rules:
            matches = conn.execute(
                "SELECT message, service, event_id FROM system_logs WHERE matched_rule = ? AND routed_by != 'LLM_COMPILED'",
                (rule["regex_pattern"],),
            ).fetchall()
            distinct = list(dict.fromkeys(m["message"] for m in matches))
            other_template = sum(1 for m in matches
                                 if rule["source_event"] and m["event_id"] and m["event_id"] != rule["source_event"])
            rows.append({
                "rule_id": rule["id"],
                "service": rule["source_service"] or "",
                "learned_from": rule["source_message"] or "",
                "regex": rule["regex_pattern"],
                "fix": rule["cached_fix"],
                "times_reused": len(matches),
                "distinct_messages_matched": len(distinct),
                "sample_matches": " || ".join(distinct[:samples]),
                "matches_in_other_drain_template": other_template if rule["source_event"] else "",
                "matches_right_incidents": "",
                "fix_quality": "",
                "notes": "",
            })
        return rows
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    rows = build_rows(args.db)
    out = args.out or args.db.with_name(args.db.stem + "-review.csv")
    with out.open("w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig so Excel shows symbols correctly
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["rule_id"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} rules -> {out}")


if __name__ == "__main__":
    main()
