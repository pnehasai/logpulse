"""
Unit tests for compute_summary(): the numbers the project claims must come
out right for a database whose contents we control exactly. Uses a throwaway
SQLite file, never the real enterprise_telemetry.db.
"""
import sqlite3

import pytest

from app import _percentile, classify_log, compute_summary, init_db


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "summary.db")
    init_db(path)
    conn = sqlite3.connect(path)
    t = 1000.0
    logs = [
        # id 1: first log of template A, compiled by the LLM in 2 s
        (1, "LLM_COMPILED", "fix A", "D:E1", r"error \d+", t, t + 0.01, t + 2.0),
        # ids 2-4: later logs of template A, cache hits in 5 ms
        (2, "L1_LOCAL_CACHE", "fix A", "D:E1", r"error \d+", t + 1, t + 1.002, t + 1.005),
        (3, "L1_LOCAL_CACHE", "fix A", "D:E1", r"error \d+", t + 2, t + 2.002, t + 2.005),
        # a hit whose rule came from a DIFFERENT template: a precision miss
        (4, "L2_REDIS_FALLBACK", "fix A", "D:E2", r"error \d+", t + 3, t + 3.002, t + 3.005),
        # still waiting for the LLM
        (5, "KAFKA_ASYNC_PROCESSOR", "COMPILING...", "D:E3", None, t + 4, t + 4.002, None),
        # failed compile
        (6, "KAFKA_ASYNC_PROCESSOR", "COMPILE FAILED: bad regex", "D:E4", None, t + 5, t + 5.002, None),
    ]
    conn.executemany(
        "INSERT INTO system_logs (id, routed_by, ai_analysis, event_id, matched_rule, ingested_at, consumed_at, resolved_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        logs,
    )
    conn.execute("INSERT INTO compiled_rules_cache (regex_pattern, cached_fix, source_log_id) VALUES (?, ?, ?)",
                 (r"error \d+", "fix A", 1))
    conn.executemany(
        "INSERT INTO llm_calls (log_id, mode, attempt, prompt_tokens, completion_tokens, total_tokens, latency_ms, ok, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "pipeline", 1, 150, 50, 200, 900.0, 1, t),
            (6, "pipeline", 1, 150, 50, 200, 800.0, 1, t),
            (None, "sync_baseline", 1, 200, 50, 250, 1000.0, 1, t),
        ],
    )
    conn.commit()
    conn.close()
    return path


def test_outcomes_are_counted(db):
    s = compute_summary(db)
    assert s["logs"] == {"total": 6, "handled": 5, "cache_hit": 3, "llm_compiled": 1, "failed": 1, "pending": 1,
                         "unresolved": 0, "cache_hit_instant": 3, "cache_hit_after_wait": 0}
    assert s["cache_hit_rate_pct"] == 60.0


def test_token_reduction_uses_measured_baseline_tokens(db):
    n = compute_summary(db)["naive_comparison"]
    # naive: 5 handled logs x 250 tokens (the measured sync-baseline call) = 1250
    assert n["naive_tokens_estimate"] == 1250
    assert n["actual_tokens"] == 400
    assert n["actual_llm_calls"] == 2 and n["naive_llm_calls"] == 5
    assert n["token_reduction_pct"] == 68.0


def test_rule_precision_against_ground_truth(db):
    ev = compute_summary(db)["dataset_eval"]
    assert ev["distinct_templates"] == 4
    assert ev["cache_hits_checked"] == 3
    assert ev["cache_hits_same_template"] == 2
    assert ev["rule_precision_pct"] == 66.7


def test_latency_is_measured_from_ingest_time(db):
    lat = compute_summary(db)["latency"]
    assert lat["ingest_to_fix_cache_hit"]["p50_ms"] == pytest.approx(5.0, abs=0.01)
    assert lat["ingest_to_fix_llm"]["p50_ms"] == pytest.approx(2000.0, abs=0.01)
    assert lat["kafka_queue_lag"]["count"] == 6


def test_since_filters_to_one_run(db):
    s = compute_summary(db, since=1003.5)
    assert s["logs"]["total"] == 2  # ids 5 and 6


def test_percentile_nearest_rank():
    assert _percentile([], 50) is None
    assert _percentile([10, 20, 30, 40], 50) == 20
    assert _percentile(list(range(1, 101)), 95) == 95


def test_classify_old_rows():
    # Rows written before this change resolved by the LLM kept KAFKA_ASYNC_PROCESSOR.
    assert classify_log("KAFKA_ASYNC_PROCESSOR", "Restart the pool") == "llm_compiled"
    assert classify_log("REDIS_HOT_CACHE", "Restart the pool") == "cache_hit"
