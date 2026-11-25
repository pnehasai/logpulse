"""
Tests for the checks every LLM-written regex must pass before it is stored:
it compiles, it can't freeze the app (ReDoS), it isn't too broad, and any
code-style wrapper the LLM added is stripped first.
check_regex() runs real subprocesses here; no Groq, Redis or Kafka needed.
"""
import time

import pytest

from app import _clean_llm_regex, check_regex, is_regex_too_generic


def test_safe_pattern_passes_quickly():
    started = time.perf_counter()
    ok, reason = check_regex(r"Disk usage at \d+% on node \d+")
    assert ok and reason == "ok"
    assert time.perf_counter() - started < 3.0


def test_catastrophic_backtracking_is_rejected_by_the_timeout():
    # (a+)+$ takes exponential time on "aaaa...!": the classic ReDoS pattern.
    started = time.perf_counter()
    ok, reason = check_regex(r"(a+)+$", timeout_seconds=1.0)
    assert not ok and "ReDoS" in reason
    assert time.perf_counter() - started < 5.0  # the probe was killed, not left running


def test_invalid_syntax_is_rejected_with_the_reason():
    ok, reason = check_regex(r"(?<disk>Disk) usage")  # JavaScript-style named group
    assert not ok and reason.startswith("invalid regex syntax")


def test_null_byte_is_rejected():
    assert check_regex("abc\x00def") == (False, "invalid regex (contains a null byte)")


@pytest.mark.parametrize("pattern", [".*", r"\w*", "", r"\d*"])
def test_patterns_that_match_anything_are_too_generic(pattern):
    assert is_regex_too_generic(pattern)


def test_specific_pattern_is_not_too_generic():
    assert not is_regex_too_generic(r"Payment gateway timeout after \d+ms")


@pytest.mark.parametrize("raw, expected", [
    (r"r'Disk usage at \d+%'", r"Disk usage at \d+%"),
    (r'"Disk usage at \d+%"', r"Disk usage at \d+%"),
    (r"/Disk usage at \d+%/", r"Disk usage at \d+%"),
    (r"`Disk usage at \d+%`", r"Disk usage at \d+%"),
    (r"Disk usage at \d+%", r"Disk usage at \d+%"),  # already bare: unchanged
])
def test_code_style_wrappers_are_stripped(raw, expected):
    assert _clean_llm_regex(raw) == expected
