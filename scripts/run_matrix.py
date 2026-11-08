#!/usr/bin/env python3
"""Run configured task/platform/model cells serially and save per-run JSON results."""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TASKS = [
    "t1_open_meteo_ingestion", "t2_usgs_etl", "t3_open_meteo_join",
    "t4_daily_particulate_summary", "t5_trailing_three_hour_peak", "t6_earthquake_band_summary",
]
PLATFORMS = ["airflow", "n8n", "dagster", "prefect", "nifi", "kestra"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", required=True, help="Ollama model identifiers")
    parser.add_argument("--platforms", nargs="+", choices=PLATFORMS, default=PLATFORMS)
    parser.add_argument("--tasks", nargs="+", choices=TASKS, default=TASKS)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--repairs", type=int, default=2)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--top-p", type=float, default=1.0)
    parser.add_argument("--top-k", type=int, default=40)
    parser.add_argument("--repeat-penalty", type=float, default=1.1)
    parser.add_argument("--num-thread", type=int, default=0)
    parser.add_argument("--num-ctx", type=int, default=8192)
    parser.add_argument("--num-predict", type=int, default=4096)
    parser.add_argument("--keep-alive", default="30m")
    parser.add_argument("--think", action="store_true")
    parser.add_argument("--output-dir", help="Directory for per-run JSON results (default: benchmark-results/runs)")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be at least one")
    if args.repairs < 0 or args.repairs > 5:
        parser.error("--repairs must be between zero and five")
    if not math.isfinite(args.temperature) or args.temperature < 0:
        parser.error("--temperature must be finite and non-negative")
    if not 0 <= args.top_p <= 1:
        parser.error("--top-p must be between 0 and 1")
    if args.top_k < 0 or args.num_thread < 0 or args.num_ctx < 1 or args.num_predict < 1:
        parser.error("--top-k and --num-thread must be non-negative; --num-ctx and --num-predict must be positive")
    if not math.isfinite(args.repeat_penalty) or args.repeat_penalty <= 0:
        parser.error("--repeat-penalty must be finite and positive")
    if not args.keep_alive:
        parser.error("--keep-alive must not be empty")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    command_prefix = [sys.executable, "-m", "pipeline_bench.cli", "run"]
    # Platform is the outer loop, so each platform's services start once and are reused by every
    # model and task cell that follows (see docs/experiment-protocol.md, warm services).
    cells = [
        (platform, model, task, replicate)
        for platform in args.platforms
        for model in args.models
        for task in args.tasks
        for replicate in range(1, args.repeats + 1)
    ]
    failed = 0
    total = len(cells)
    for index, (platform, model, task, replicate) in enumerate(cells, start=1):
        next_platform = cells[index][0] if index < total else None
        keep = next_platform == platform
        run_id = f"{uuid.uuid4().hex[:8]}_{replicate}"
        print(f"[{index}/{total}] model={model} platform={platform} task={task} replicate={replicate}", flush=True)
        result = subprocess.run(
            command_prefix + [
                "--model", model,
                "--platform", platform,
                "--task", task,
                "--repairs", str(args.repairs),
                "--run-id", run_id,
                "--temperature", str(args.temperature),
                "--seed", str(args.seed),
                "--top-p", str(args.top_p),
                "--top-k", str(args.top_k),
                "--repeat-penalty", str(args.repeat_penalty),
                "--num-thread", str(args.num_thread),
                "--num-ctx", str(args.num_ctx),
                "--num-predict", str(args.num_predict),
                "--keep-alive", args.keep_alive,
                *(["--think"] if args.think else []),
                *(["--output-dir", args.output_dir] if args.output_dir else []),
                *(["--keep-services"] if keep else []),
            ],
            cwd=ROOT,
            env=env,
            check=False,
        )
        failed += result.returncode != 0
    print(f"Completed {total} runs; {failed} did not pass.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
