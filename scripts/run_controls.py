#!/usr/bin/env python3
"""Run the hand-written reference artifacts through each platform and score them.

Every platform x task cell must score 100% here before model results from that
cell are trusted. A failing control points to the harness or the platform setup.

Output: benchmark-results/controls/<name>.json (per-cell records) and <name>.md
(a pass grid).

Usage:
    python scripts/run_controls.py --name reference-validation
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipeline_bench.benchmark import get_task, load_benchmark  # noqa: E402
from pipeline_bench.cli import _reset_sink, _score  # noqa: E402
from pipeline_bench.platforms import (  # noqa: E402
    PlatformError,
    cleanup_staged_artifact,
    run_airflow,
    run_dagster,
    run_kestra,
    run_n8n,
    run_nifi,
    run_prefect,
    start_recorder,
    stop_services,
)

RECORDER = "http://localhost:8090"
PLATFORMS = ["airflow", "n8n", "dagster", "prefect", "nifi", "kestra"]
TASKS = [
    "t1_open_meteo_ingestion",
    "t2_usgs_etl",
    "t3_open_meteo_join",
    "t4_daily_particulate_summary",
    "t5_trailing_three_hour_peak",
    "t6_earthquake_band_summary",
]
EXTENSIONS = {"airflow": "py", "dagster": "py", "prefect": "py", "kestra": "yml", "n8n": "json", "nifi": "groovy"}


def _run_platform(platform: str, artifact: Path, run_id: str, timeout: int) -> None:
    if platform == "airflow":
        run_airflow(artifact, run_id, timeout)
    elif platform == "n8n":
        run_n8n(artifact, timeout)
    elif platform == "dagster":
        run_dagster(artifact, run_id, timeout)
    elif platform == "prefect":
        run_prefect(artifact, run_id, timeout)
    elif platform == "nifi":
        run_nifi(artifact, run_id, timeout)
    elif platform == "kestra":
        run_kestra(artifact, run_id, timeout)
    else:
        raise ValueError(platform)


def run_cell(platform: str, task_id: str, benchmark: dict, timeout: int, keep: bool = False) -> dict:
    prefix = task_id.split("_")[0]
    run_id = f"ref-{prefix}"
    artifact = ROOT / "controls" / "reference" / platform / f"{prefix}.{EXTENSIONS[platform]}"
    record = {"platform": platform, "task": task_id, "run_id": run_id, "artifact": str(artifact.relative_to(ROOT))}
    started = time.monotonic()
    try:
        start_recorder()
        _reset_sink(RECORDER, run_id, 0)
        _run_platform(platform, artifact, run_id, timeout)
        evaluation = _score(RECORDER, run_id, 0, get_task(task_id, benchmark), benchmark)
        record.update({
            "passed": evaluation["passed"],
            "f1": evaluation["f1"],
            "expected_count": evaluation["expected_count"],
            "actual_count": evaluation["actual_count"],
            "matched_count": evaluation["matched_count"],
            "error": None,
        })
    except (PlatformError, RuntimeError, ValueError, OSError) as exc:
        record.update({"passed": False, "f1": None, "error": str(exc)[:500]})
    finally:
        record["duration_seconds"] = round(time.monotonic() - started, 1)
        # Same warm-service rule as the matrix: keep services after a healthy cell when the next cell
        # uses the same platform; stop after a platform failure or at the end of a platform group.
        if not (keep and record.get("error") is None):
            try:
                stop_services()
            except PlatformError as exc:
                record["cleanup_error"] = str(exc)[:300]
        cleanup_staged_artifact(platform, run_id, artifact)
    return record


def _grid(records: list[dict]) -> str:
    cells = {(r["task"], r["platform"]): r for r in records}
    lines = [
        "| Task | " + " | ".join(PLATFORMS) + " |",
        "|---" * (len(PLATFORMS) + 1) + "|",
    ]
    for task in TASKS:
        row = []
        for platform in PLATFORMS:
            record = cells.get((task, platform))
            if record is None:
                row.append("—")
            elif record["passed"]:
                row.append("pass")
            else:
                row.append("FAIL: " + (record.get("error") or f"f1={record.get('f1')}")[:60].replace("|", "/"))
        lines.append(f"| {task} | " + " | ".join(row) + " |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True, help="output file stem under benchmark-results/controls")
    parser.add_argument("--timeout", type=int, default=600, help="execution timeout per cell (seconds)")
    parser.add_argument("--platforms", nargs="+", choices=PLATFORMS, default=PLATFORMS)
    parser.add_argument("--tasks", nargs="+", choices=TASKS, default=TASKS)
    args = parser.parse_args()
    benchmark = load_benchmark()
    out_dir = ROOT / "benchmark-results" / "controls"
    out_dir.mkdir(parents=True, exist_ok=True)
    records = []
    cells = [(platform, task) for platform in args.platforms for task in args.tasks]
    total = len(cells)
    for index, (platform, task) in enumerate(cells, start=1):
        keep = index < total and cells[index][0] == platform
        print(f"[{index}/{total}] control platform={platform} task={task}", flush=True)
        record = run_cell(platform, task, benchmark, args.timeout, keep)
        records.append(record)
        status = "pass" if record["passed"] else f"FAIL ({record.get('error') or record.get('f1')})"
        print(f"    -> {status}", flush=True)
    (out_dir / f"{args.name}.json").write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
    grid = _grid(records)
    (out_dir / f"{args.name}.md").write_text(grid, encoding="utf-8")
    passed = sum(r["passed"] for r in records)
    print(f"Controls passed {passed}/{len(records)}")
    print(grid)
    return 0 if passed == len(records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
