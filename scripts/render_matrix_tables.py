#!/usr/bin/env python3
"""Render one markdown table per task (metrics as rows, platforms as columns) from per-run JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PLATFORM_ORDER = ["airflow", "n8n", "dagster", "prefect", "nifi", "kestra"]
TASK_ORDER = ["t1_open_meteo_ingestion", "t2_usgs_etl", "t3_open_meteo_join"]


def _get(value: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _format(value: Any, digits: int = 3) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    if isinstance(value, list):
        return ", ".join(value) if value else "none"
    return str(value)


def _gb(value: Any) -> Any:
    return None if value is None else value / 1_000_000_000


def _metrics(run: dict[str, Any]) -> dict[str, Any]:
    last = run["history"][-1] if run.get("history") else {}
    evaluation = last.get("evaluation") or {}
    metrics = run.get("metrics", {})
    generation = last.get("generation_metrics") or {}
    resources = last.get("container_resources") or {}
    requests = last.get("request_metrics") or {}
    failures = run.get("failure_categories") or {}
    detail = (last.get("execution_error") or "").replace("\n", " ")
    return {
        "Status": run.get("status"),
        "Attempts": run.get("attempts"),
        "Failure category": ", ".join(failures) if failures else None,
        "Failure detail (truncated)": (detail[:110] + "…") if len(detail) > 110 else (detail or None),
        "First-attempt passed": metrics.get("first_attempt_passed"),
        "Best F1": metrics.get("best_f1"),
        "Precision": evaluation.get("precision"),
        "Recall": evaluation.get("recall"),
        "F1": evaluation.get("f1"),
        "Expected rows": evaluation.get("expected_count"),
        "Actual rows": evaluation.get("actual_count"),
        "Matched rows": evaluation.get("matched_count"),
        "Source coverage": requests.get("source_coverage", metrics.get("source_coverage")),
        "Source requests": requests.get("source_request_count", metrics.get("source_request_count")),
        "Sink POSTs": requests.get("sink_post_count", metrics.get("sink_post_count")),
        "Sink rows accepted": requests.get("sink_rows_accepted", metrics.get("sink_rows_accepted")),
        "Generation time (s)": metrics.get("generation_duration_seconds"),
        "Ollama model load (s)": generation.get("model_load_duration_seconds"),
        "Ollama total time (s)": generation.get("ollama_total_duration_seconds"),
        "Execution time (s)": metrics.get("execution_duration_seconds"),
        "End-to-end time (s)": metrics.get("total_duration_seconds"),
        "Prompt tokens": metrics.get("prompt_tokens"),
        "Output tokens": metrics.get("output_tokens"),
        "Output tokens/s": generation.get("output_tokens_per_second"),
        "Cost (USD)": metrics.get("cost_estimate_usd"),
        "Mean container CPU (%)": metrics.get("mean_cpu_percent", resources.get("mean_cpu_percent")),
        "Peak container CPU (%)": metrics.get("peak_cpu_percent", resources.get("peak_cpu_percent")),
        "Peak container memory (GB)": _gb(metrics.get("peak_memory_bytes", resources.get("peak_memory_bytes"))),
        "Resource samples": metrics.get("resource_sample_count", resources.get("sample_count")),
        "Mean GPU utilization (%)": metrics.get("mean_gpu_utilization_percent", resources.get("mean_gpu_utilization_percent")),
        "Peak GPU memory (GB)": _gb(metrics.get("peak_gpu_memory_bytes", resources.get("peak_gpu_memory_bytes"))),
        "Estimated GPU energy (Wh)": metrics.get("estimated_gpu_energy_wh", resources.get("estimated_gpu_energy_wh")),
    }


def render(results_dirs: list[Path]) -> str:
    # Later directories replace earlier ones for the same model, task, and platform.
    runs: dict[tuple[str, str, str], dict[str, Any]] = {}
    for results_dir in results_dirs:
        for path in sorted(results_dir.glob("*_1.json")):
            run = json.loads(path.read_text(encoding="utf-8"))
            runs[(run["model"], run["task"], run["platform"])] = run
    models = sorted({model for model, _, _ in runs})
    sections = []
    for model in models:
        for task in TASK_ORDER:
            platforms = [p for p in PLATFORM_ORDER if (model, task, p) in runs]
            if not platforms:
                continue
            rows = {p: _metrics(runs[(model, task, p)]) for p in platforms}
            metric_names = list(rows[platforms[0]].keys())
            lines = [f"### {model} — {task}", "", "| Metric | " + " | ".join(platforms) + " |", "|---" * (len(platforms) + 1) + "|"]
            for name in metric_names:
                lines.append(f"| {name} | " + " | ".join(_format(rows[p][name]) for p in platforms) + " |")
            sections.append("\n".join(lines))
    return "\n\n".join(sections) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, action="append", required=True,
                        help="repeat to merge several directories; later ones win")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(args.results_dir), encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
