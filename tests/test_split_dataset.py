"""Unit tests for the train/validation/test split (tools/split_dataset.py)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from split_dataset import describe, split_rows  # noqa: E402


def _rows(n):
    return [{"LineId": str(i), "EventId": f"E{i % 7}", "Content": f"line {i}"} for i in range(n)]


def test_70_10_20_sizes_and_no_overlap():
    parts = split_rows(_rows(2000))
    assert [len(parts[k]) for k in ("train", "val", "test")] == [1400, 200, 400]
    ids = [r["LineId"] for k in ("train", "val", "test") for r in parts[k]]
    assert len(ids) == len(set(ids)) == 2000  # every line in exactly one split


def test_chronological_keeps_order():
    parts = split_rows(_rows(100))
    assert parts["train"][-1]["LineId"] == "69"
    assert parts["val"][0]["LineId"] == "70"
    assert parts["test"][0]["LineId"] == "80"


def test_random_is_reproducible_with_a_seed():
    a = split_rows(_rows(100), mode="random", seed=7)
    b = split_rows(_rows(100), mode="random", seed=7)
    assert a == b
    assert a != split_rows(_rows(100))  # and different from chronological


def test_bad_ratios_rejected():
    with pytest.raises(ValueError):
        split_rows(_rows(10), ratios=(0.5, 0.1, 0.1))


def test_describe_counts_templates_missing_from_train():
    parts = {"train": [{"EventId": "E1"}], "val": [{"EventId": "E1"}, {"EventId": "E2"}], "test": [{"EventId": "E3"}]}
    info = describe(parts)
    assert info["val"]["templates_not_in_train"] == 1 and info["val"]["lines_with_template_not_in_train"] == 1
    assert info["test"]["templates_not_in_train"] == 1
