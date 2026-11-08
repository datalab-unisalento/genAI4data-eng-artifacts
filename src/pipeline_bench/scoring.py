from __future__ import annotations

from collections import Counter
import json
from typing import Any

# Bump when the comparison rules change, and record it in every run file.
SCORER_VERSION = "2"


def _normalize(value: Any) -> Any:
    # JSON has one number type: a JavaScript runtime (for example n8n) returns 10.0 as 10.
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    return value


def _canonical(row: dict[str, Any]) -> str:
    return json.dumps(_normalize(row), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def score_rows(expected: list[dict[str, Any]], actual: list[dict[str, Any]]) -> dict[str, Any]:
    """Compare complete records as multisets, preserving duplicate-count semantics."""
    expected_counts = Counter(map(_canonical, expected))
    actual_counts = Counter(map(_canonical, actual))
    missing = list((expected_counts - actual_counts).elements())
    unexpected = list((actual_counts - expected_counts).elements())
    matched_count = len(expected) - len(missing)
    precision = matched_count / len(actual) if actual else float(not expected)
    recall = matched_count / len(expected) if expected else float(not actual)
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0.0
    )
    return {
        "passed": not missing and not unexpected,
        "expected_count": len(expected),
        "actual_count": len(actual),
        "matched_count": matched_count,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "missing_rows": [json.loads(row) for row in missing],
        "unexpected_rows": [json.loads(row) for row in unexpected],
    }
