#!/usr/bin/env python3
"""Re-score every logged attempt with the current scorer and report changed outcomes.

The recorder keeps the rows each sink received (benchmark-results/recorder/sink-events.jsonl).
This script compares the stored verdict of each attempt with a fresh score under the current
scorer, so a scorer change can be audited without re-running any model.

Usage:
    python scripts/rescore_runs.py --runs benchmark-results/runs/three-models-all-platforms
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipeline_bench.benchmark import expected_rows, get_task, load_benchmark  # noqa: E402
from pipeline_bench.scoring import SCORER_VERSION, score_rows  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=Path, action="append", required=True)
    parser.add_argument("--sink-log", type=Path, default=ROOT / "benchmark-results" / "recorder" / "sink-events.jsonl")
    parser.add_argument("--output", type=Path, default=ROOT / "benchmark-results" / "report" / "rescore_changes.csv")
    args = parser.parse_args()

    benchmark = load_benchmark()
    logged: dict[tuple[str, int], list] = {}
    for line in args.sink_log.read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        logged[(event["run_id"], event["attempt"])] = event["rows"]

    checked = 0
    changes = []
    for directory in args.runs:
        for path in sorted(directory.glob("*_1.json")):
            run = json.loads(path.read_text(encoding="utf-8"))
            task = get_task(run["task"], benchmark)
            for attempt in run.get("history", []):
                stored = attempt.get("evaluation")
                key = (run["run_id"], attempt["attempt"])
                if not stored or key not in logged:
                    continue
                fresh = score_rows(expected_rows(task, benchmark), logged[key])
                checked += 1
                if fresh["passed"] != stored["passed"]:
                    changes.append({
                        "run_id": run["run_id"], "attempt": attempt["attempt"], "platform": run["platform"],
                        "model": run["model"], "task": run["task"],
                        "stored_passed": stored["passed"], "rescored_passed": fresh["passed"],
                        "rescored_f1": round(fresh["f1"], 4), "scorer_version": SCORER_VERSION,
                    })

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "run_id", "attempt", "platform", "model", "task", "stored_passed", "rescored_passed",
            "rescored_f1", "scorer_version"])
        writer.writeheader()
        writer.writerows(changes)
    print(f"re-scored {checked} attempts with scorer v{SCORER_VERSION}; {len(changes)} verdicts changed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
