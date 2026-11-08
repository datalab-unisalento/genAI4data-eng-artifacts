from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
FIXTURES_PATH = ROOT / "fixtures" / "benchmark.json"


class BenchmarkError(ValueError):
    """Raised when a benchmark configuration or result is invalid."""


def load_benchmark(path: Path = FIXTURES_PATH) -> dict[str, Any]:
    try:
        benchmark = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkError(f"Unable to load benchmark file {path}: {exc}") from exc
    if not isinstance(benchmark.get("tasks"), dict):
        raise BenchmarkError("Benchmark file must contain a 'tasks' object")
    if not isinstance(benchmark.get("sources"), dict):
        raise BenchmarkError("Benchmark file must contain a 'sources' object")
    root = path.resolve().parents[1]
    for source_id, source in benchmark["sources"].items():
        payload_file = root / source.get("payload_file", "")
        try:
            source["payload"] = json.loads(payload_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BenchmarkError(f"Unable to load source fixture {source_id} from {payload_file}: {exc}") from exc
        if not isinstance(source["payload"], dict):
            raise BenchmarkError(f"Source fixture {source_id} must contain a JSON object")
    return benchmark


def get_task(task_id: str, benchmark: dict[str, Any] | None = None) -> dict[str, Any]:
    benchmark = benchmark or load_benchmark()
    try:
        return benchmark["tasks"][task_id]
    except KeyError as exc:
        choices = ", ".join(sorted(benchmark["tasks"]))
        raise BenchmarkError(f"Unknown task {task_id!r}. Available tasks: {choices}") from exc


def expected_rows(task: dict[str, Any], benchmark: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    benchmark = benchmark or load_benchmark()
    rows = task.get("expected_rows")
    if rows is None and task.get("expected_file"):
        expected_path = ROOT / task["expected_file"]
        try:
            rows = json.loads(expected_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BenchmarkError(f"Unable to load expected output fixture {expected_path}: {exc}") from exc
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise BenchmarkError("Task must define expected_rows as a list of objects")
    return rows


def list_tasks(benchmark: dict[str, Any] | None = None) -> list[tuple[str, dict[str, Any]]]:
    benchmark = benchmark or load_benchmark()
    return sorted(benchmark["tasks"].items())
