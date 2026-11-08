#!/usr/bin/env python3
"""Check whether repeated generations are identical under the fixed sampling settings.

The main grid uses temperature 0 and seed 42. If repeated generations for the same
prompt are byte-identical, one replicate per cell is enough. A control at
temperature 0.7 must differ across seeds, which shows the check can detect variation.

Usage:
    python scripts/check_determinism.py --models qwen3-coder:30b --tasks t1_open_meteo_ingestion
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipeline_bench.benchmark import get_task, load_benchmark  # noqa: E402
from pipeline_bench.ollama import generate_with_metrics, warm_up  # noqa: E402
from pipeline_bench.prompts import build_prompt  # noqa: E402

RECORDER = "http://recorder:8090"


def _options(temperature: float, seed: int) -> dict:
    # Same defaults as the CLI.
    return {
        "temperature": temperature,
        "seed": seed,
        "top_p": 1.0,
        "top_k": 40,
        "repeat_penalty": 1.1,
        "num_thread": 0,
        "num_ctx": 8192,
        "num_predict": 4096,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", required=True)
    parser.add_argument("--tasks", nargs="+", required=True)
    parser.add_argument("--platform", default="airflow")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, default=ROOT / "benchmark-results" / "determinism" / "determinism.json")
    args = parser.parse_args()
    benchmark = load_benchmark()
    results = []
    for model in args.models:
        for task_id in args.tasks:
            prompt = build_prompt(
                task_id, get_task(task_id, benchmark), benchmark, args.platform, RECORDER, "determinism", attempt=0
            )
            for label, temperature, seeds in (
                ("temperature_0_fixed_seed", 0.0, [42] * args.repeats),
                ("temperature_0.7_varied_seed", 0.7, list(range(1, args.repeats + 1))),
            ):
                hashes = []
                for seed in seeds:
                    warm_up(model, "http://localhost:11434", 600, "30m")
                    text, _ = generate_with_metrics(
                        prompt, model, "http://localhost:11434", 600, _options(temperature, seed), "30m", False
                    )
                    hashes.append(hashlib.sha256(text.strip().encode()).hexdigest()[:16])
                identical = len(set(hashes)) == 1
                results.append({"model": model, "task": task_id, "setting": label, "hashes": hashes, "identical": identical})
                print(f"{model} {task_id} {label}: {'identical' if identical else 'differ'} {hashes}", flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    fixed = [r for r in results if r["setting"] == "temperature_0_fixed_seed"]
    print(f"temperature-0 cells identical: {sum(r['identical'] for r in fixed)}/{len(fixed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
