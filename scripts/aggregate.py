#!/usr/bin/env python3
"""Aggregate per-run result JSON files into a compact CSV summary."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def _mean_metric(runs: list[dict[str, Any]], name: str) -> float | None:
    values = [
        run.get("metrics", {}).get(name)
        for run in runs
        if isinstance(run.get("metrics", {}).get(name), (int, float))
    ]
    return sum(values) / len(values) if values else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=Path("benchmark-results/runs"))
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/summary/summary.csv"))
    args = parser.parse_args()
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(args.results_dir.glob("*.json")):
        try:
            result = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f"Cannot read result file {path}: {exc}") from exc
        if {"task", "platform", "model", "status", "attempts"} <= result.keys():
            configuration = json.dumps(result.get("configuration", {}), sort_keys=True)
            grouped[(result["task"], result["platform"], result["model"], configuration)].append(result)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=[
            "task", "platform", "model", "configuration_json", "runs", "first_attempt_pass_rate",
            "pass_rate", "mean_attempts", "mean_generation_seconds",
            "mean_execution_seconds", "mean_total_seconds", "mean_prompt_tokens",
            "mean_output_tokens", "mean_cost_estimate_usd", "mean_cpu_percent",
            "max_peak_memory_bytes", "mean_gpu_utilization_percent",
            "max_peak_gpu_memory_bytes", "mean_estimated_gpu_energy_wh",
            "mean_source_coverage", "mean_source_requests", "mean_sink_posts",
            "mean_sink_rows_accepted", "model_request_errors", "invalid_artifacts", "runtime_errors",
            "platform_errors", "harness_errors", "incorrect_outputs",
            "resource_error_runs", "mean_best_f1",
        ])
        writer.writeheader()
        for (task, platform, model, configuration), runs in sorted(grouped.items()):
            first_attempts = [
                run["metrics"]["first_attempt_passed"]
                for run in runs
                if isinstance(run.get("metrics"), dict)
                and isinstance(run["metrics"].get("first_attempt_passed"), bool)
            ]
            peak_memory_values = [
                run["metrics"]["peak_memory_bytes"]
                for run in runs
                if isinstance(run.get("metrics"), dict)
                and isinstance(run["metrics"].get("peak_memory_bytes"), int)
            ]
            resource_error_runs = sum(
                any(
                    attempt.get("container_resources", {}).get("measurement_error")
                    for attempt in run.get("history", [])
                )
                for run in runs
            )
            category_counts = {
                category: sum(
                    run.get("failure_categories", {}).get(category, 0) for run in runs
                )
                for category in (
                    "model_request_error",
                    "invalid_artifact",
                    "runtime_error",
                    "platform_error",
                    "harness_error",
                    "incorrect_output",
                )
            }
            peak_gpu_memory_values = [
                run["metrics"]["peak_gpu_memory_bytes"]
                for run in runs
                if isinstance(run.get("metrics"), dict)
                and isinstance(run["metrics"].get("peak_gpu_memory_bytes"), int)
            ]
            def format_mean(name: str) -> str:
                value = _mean_metric(runs, name)
                return f"{value:.4f}" if value is not None else ""

            writer.writerow({
                "task": task,
                "platform": platform,
                "model": model,
                "configuration_json": configuration,
                "runs": len(runs),
                "first_attempt_pass_rate": (
                    f"{sum(first_attempts) / len(first_attempts):.4f}" if first_attempts else ""
                ),
                "pass_rate": f"{sum(run['status'] == 'passed' for run in runs) / len(runs):.4f}",
                "mean_attempts": f"{sum(run['attempts'] for run in runs) / len(runs):.2f}",
                "mean_generation_seconds": format_mean("generation_duration_seconds"),
                "mean_execution_seconds": format_mean("execution_duration_seconds"),
                "mean_total_seconds": format_mean("total_duration_seconds"),
                "mean_prompt_tokens": format_mean("prompt_tokens"),
                "mean_output_tokens": format_mean("output_tokens"),
                "mean_cost_estimate_usd": format_mean("cost_estimate_usd"),
                "mean_cpu_percent": format_mean("mean_cpu_percent"),
                "max_peak_memory_bytes": max(peak_memory_values, default=""),
                "mean_gpu_utilization_percent": format_mean("mean_gpu_utilization_percent"),
                "max_peak_gpu_memory_bytes": max(peak_gpu_memory_values, default=""),
                "mean_estimated_gpu_energy_wh": format_mean("estimated_gpu_energy_wh"),
                "mean_source_coverage": format_mean("source_coverage"),
                "mean_source_requests": format_mean("source_request_count"),
                "mean_sink_posts": format_mean("sink_post_count"),
                "mean_sink_rows_accepted": format_mean("sink_rows_accepted"),
                "model_request_errors": category_counts["model_request_error"],
                "invalid_artifacts": category_counts["invalid_artifact"],
                "runtime_errors": category_counts["runtime_error"],
                "platform_errors": category_counts["platform_error"],
                "harness_errors": category_counts["harness_error"],
                "incorrect_outputs": category_counts["incorrect_output"],
                "resource_error_runs": resource_error_runs,
                "mean_best_f1": format_mean("best_f1"),
            })
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
