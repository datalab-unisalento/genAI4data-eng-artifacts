#!/usr/bin/env python3
"""Build every table, statistic, and figure for the paper from the raw run files.

Run with the analysis environment (needs pandas, scipy, statsmodels, matplotlib):

    python scripts/make_report.py

Outputs go to benchmark-results/report/. Raw run files are never modified.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import statsmodels.api as sm  # noqa: E402
import statsmodels.formula.api as smf  # noqa: E402
from scipy.stats import chi2  # noqa: E402
from statsmodels.stats.contingency_tables import mcnemar  # noqa: E402
from statsmodels.stats.multitest import multipletests  # noqa: E402
from statsmodels.stats.proportion import proportion_confint  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from render_matrix_tables import render  # noqa: E402

PLATFORMS = ["airflow", "n8n", "dagster", "prefect", "nifi", "kestra"]
TASKS = [
    "t1_open_meteo_ingestion", "t2_usgs_etl", "t3_open_meteo_join",
    "t4_daily_particulate_summary", "t5_trailing_three_hour_peak", "t6_earthquake_band_summary",
]
# Categories derived from the stored error text, for runs written before the category split.
LEGACY_RULES = [
    ("runtime_error", ("processor failed", "DAG run ended in state", "Kestra execution ended", "execute --id")),
    ("invalid_artifact", ("has issues", "did not load DAG", "did not validate", "must set", "not valid JSON",
                          "returned HTTP 422", "SyntaxError", "ImportError", "ModuleNotFoundError",
                          "NameError", "Invalid entity", "must declare", "must include", "must contain",
                          "must have", "must be a JSON", "Model did not return valid", "Model returned an empty",
                          "must return", "Each n8n node")),
]


def _load_runs(dirs: list[Path]) -> list[dict[str, Any]]:
    runs = []
    for directory in dirs:
        for path in sorted(directory.glob("*_1.json")):
            run = json.loads(path.read_text(encoding="utf-8"))
            run["_source"] = directory.name
            runs.append(run)
    return runs


def _derive_category(attempt: dict[str, Any], legacy: bool) -> str | None:
    category = attempt.get("failure_category")
    if category is None:
        return None
    if not legacy:
        return category
    if category == "invalid_generated_artifact":
        return "invalid_artifact"
    if category != "platform_error":
        return category
    message = attempt.get("execution_error") or ""
    for derived, markers in LEGACY_RULES:
        if any(marker in message for marker in markers):
            return derived
    return "platform_error"


def _run_row(run: dict[str, Any]) -> dict[str, Any]:
    history = run.get("history", [])
    last = history[-1] if history else {}
    evaluations = [h.get("evaluation") for h in history if h.get("evaluation")]
    first = history[0].get("evaluation") if history else None
    metrics = run.get("metrics", {})
    generation = last.get("generation_metrics") or {}
    config = run.get("configuration", {})
    legacy = "prompt_version" not in config
    categories = [_derive_category(h, legacy) for h in history]
    return {
        "source": run["_source"],
        "run_id": run["run_id"],
        "model": run["model"],
        "platform": run["platform"],
        "task": run["task"],
        "status": run["status"],
        "passed": run["status"] == "passed",
        "attempts": run["attempts"],
        "first_attempt_passed": bool(first and first.get("passed")),
        "best_f1": max((e["f1"] for e in evaluations), default=None),
        "first_attempt_f1": first.get("f1") if first else None,
        "final_precision": (last.get("evaluation") or {}).get("precision"),
        "final_recall": (last.get("evaluation") or {}).get("recall"),
        "final_f1": (last.get("evaluation") or {}).get("f1"),
        "delivered_rows": (last.get("evaluation") or {}).get("actual_count"),
        "expected_rows": (last.get("evaluation") or {}).get("expected_count"),
        "final_category": categories[-1] if categories else None,
        "categories": ";".join(c for c in categories if c),
        "generation_seconds": metrics.get("generation_duration_seconds"),
        "execution_seconds": metrics.get("execution_duration_seconds"),
        "end_to_end_seconds": metrics.get("total_duration_seconds"),
        "prompt_tokens": metrics.get("prompt_tokens"),
        "output_tokens": metrics.get("output_tokens"),
        "output_tokens_per_second": generation.get("output_tokens_per_second"),
        "mean_cpu_percent": metrics.get("mean_cpu_percent"),
        "peak_memory_gb": (metrics.get("peak_memory_bytes") or 0) / 1e9 or None,
        "mean_gpu_utilization_percent": metrics.get("mean_gpu_utilization_percent"),
        "peak_gpu_memory_gb": (metrics.get("peak_gpu_memory_bytes") or 0) / 1e9 or None,
        "gpu_energy_wh": metrics.get("estimated_gpu_energy_wh"),
        "prompt_version": config.get("prompt_version", "unversioned"),
        "scorer_version": config.get("scorer_version", "unversioned"),
        "temperature": (config.get("ollama_generation_options") or {}).get("temperature"),
        "seed": (config.get("ollama_generation_options") or {}).get("seed"),
    }


def _wilson(successes: int, n: int) -> tuple[float | None, float | None]:
    if n == 0:
        return None, None
    low, high = proportion_confint(successes, n, method="wilson")
    return float(low), float(high)


def _rate_table(df: pd.DataFrame, by: str) -> pd.DataFrame:
    rows = []
    for key, group in df.groupby(by):
        n = len(group)
        k = int(group["passed"].sum())
        low, high = _wilson(k, n)
        rows.append({by: key, "passed": k, "cells": n, "pass_rate": k / n, "ci95_low": low, "ci95_high": high,
                     "first_attempt_pass_rate": group["first_attempt_passed"].mean()})
    return pd.DataFrame(rows)


def _pairwise_mcnemar(df: pd.DataFrame) -> pd.DataFrame:
    wide = df.pivot_table(index=["model", "task"], columns="platform", values="passed", aggfunc="first")
    pairs, pvals = [], []
    for i, a in enumerate(PLATFORMS):
        for b in PLATFORMS[i + 1:]:
            if a not in wide or b not in wide:
                continue
            both = wide[[a, b]].dropna().astype(bool)
            table = [[int((both[a] & both[b]).sum()), int((both[a] & ~both[b]).sum())],
                     [int((~both[a] & both[b]).sum()), int((~both[a] & ~both[b]).sum())]]
            discordant = table[0][1] + table[1][0]
            p = float(mcnemar(table, exact=discordant < 25).pvalue) if discordant else 1.0
            pairs.append((a, b, table[0][1], table[1][0], p))
            pvals.append(p)
    if not pairs:
        return pd.DataFrame()
    _, adjusted, _, _ = multipletests(pvals, method="holm")
    return pd.DataFrame([
        {"platform_a": a, "platform_b": b, "a_only_passes": ab, "b_only_passes": ba,
         "p_value": p, "p_holm": padj}
        for (a, b, ab, ba, p), padj in zip(pairs, adjusted)
    ])


def _cochran_q(df: pd.DataFrame) -> dict[str, float]:
    wide = df.pivot_table(index=["model", "task"], columns="platform", values="passed", aggfunc="first").dropna()
    matrix = wide.astype(int).to_numpy()
    k = matrix.shape[1]
    n_blocks = matrix.shape[0]
    column = matrix.sum(axis=0)
    row = matrix.sum(axis=1)
    total = matrix.sum()
    denominator = k * total - (row ** 2).sum()
    if denominator == 0:
        return {"Q": float("nan"), "df": k - 1, "p_value": float("nan"), "blocks": n_blocks}
    q = (k - 1) * (k * (column ** 2).sum() - total ** 2) / denominator
    return {"Q": float(q), "df": k - 1, "p_value": float(1 - chi2.cdf(q, k - 1)), "blocks": n_blocks}


def _logistic(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.assign(passed=df["passed"].astype(int))
    try:
        fit = smf.glm("passed ~ C(platform, Treatment('airflow')) + C(model) + C(task)",
                      data=frame, family=sm.families.Binomial()).fit()
    except Exception as exc:  # separation or singular design
        return pd.DataFrame([{"term": "error", "note": str(exc)[:200]}])
    conf = fit.conf_int()
    if not np.isfinite(conf.values).all() or np.abs(fit.params.values).max() > 15:
        # Complete or quasi-complete separation (for example a platform that passes every cell).
        return pd.DataFrame([{"term": "all", "note": "complete separation: odds ratios not estimable; "
                                                      "use the paired tests"}])
    return pd.DataFrame({
        "term": fit.params.index,
        "odds_ratio": np.exp(fit.params.values),
        "ci95_low": np.exp(conf[0].values),
        "ci95_high": np.exp(conf[1].values),
        "p_value": fit.pvalues.values,
    })


def _heatmap(df: pd.DataFrame, path: Path) -> None:
    models = sorted(df["model"].unique())
    fig, axes = plt.subplots(1, len(PLATFORMS), figsize=(3 * len(PLATFORMS), 2.8), sharey=True)
    for ax, platform in zip(axes, PLATFORMS):
        grid = np.full((len(models), len(TASKS)), np.nan)
        for i, model in enumerate(models):
            for j, task in enumerate(TASKS):
                match = df[(df.model == model) & (df.task == task) & (df.platform == platform)]
                if len(match):
                    grid[i, j] = float(match["passed"].iloc[0])
        ax.imshow(grid, vmin=0, vmax=1, cmap="RdYlGn", aspect="auto")
        ax.set_title(platform)
        ax.set_xticks(range(len(TASKS)), [t.split("_")[0] for t in TASKS])
        ax.set_yticks(range(len(models)), [m.split(":")[0] for m in models])
    fig.suptitle("Pass (green) and fail (red) per model, task, and platform (one replicate)")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _variance_summary(directory: Path) -> pd.DataFrame | None:
    if not directory.exists():
        return None
    runs = _load_runs([directory])
    if not runs:
        return None
    df = pd.DataFrame([_run_row(r) for r in runs])
    rows = []
    for key, group in df.groupby(["model", "platform", "task"]):
        n = len(group)
        k = int(group["passed"].sum())
        low, high = _wilson(k, n)
        rows.append({"model": key[0], "platform": key[1], "task": key[2], "seeds": n, "passed": k,
                     "pass_rate": k / n, "ci95_low": low, "ci95_high": high})
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--main", type=Path, default=ROOT / "benchmark-results" / "runs" / "three-models-prompt-v2")
    parser.add_argument("--variance", type=Path, default=ROOT / "benchmark-results" / "runs" / "variance-temp07")
    parser.add_argument("--v1", type=Path, default=ROOT / "benchmark-results" / "runs" / "three-models-all-platforms")
    parser.add_argument("--out", type=Path, default=ROOT / "benchmark-results" / "report")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    main_runs = _load_runs([args.main])
    if not main_runs:
        print(f"no runs in {args.main}", file=sys.stderr)
        return 1
    df = pd.DataFrame([_run_row(r) for r in main_runs])
    df.to_csv(args.out / "master_runs.csv", index=False)
    (args.out / "tables_main.md").write_text(render([args.main]), encoding="utf-8")

    stats: dict[str, Any] = {"n_runs": len(df), "passed": int(df["passed"].sum()),
                             "prompt_versions": sorted(df["prompt_version"].unique()),
                             "scorer_versions": sorted(df["scorer_version"].unique())}
    by_platform = _rate_table(df, "platform").set_index("platform").reindex(PLATFORMS).reset_index()
    by_platform["platform"] = PLATFORMS
    by_model = _rate_table(df, "model")
    by_task = _rate_table(df, "task")
    failures = (df.assign(category=df["categories"].str.split(";")).explode("category")
                .query("category.notna() and category != ''")
                .groupby(["platform", "category"]).size().unstack(fill_value=0))
    repair = (df.groupby("platform")[["first_attempt_passed", "passed"]].mean()
              .rename(columns={"passed": "eventual_pass_rate"}).reindex(PLATFORMS))
    mc = _pairwise_mcnemar(df)
    q = _cochran_q(df)
    logit = _logistic(df)

    with pd.ExcelWriter(args.out / "stats.xlsx") as writer:
        for name, table in [("by_platform", by_platform), ("by_model", by_model), ("by_task", by_task),
                            ("failure_categories", failures.reset_index()), ("repair", repair.reset_index()),
                            ("mcnemar_pairs", mc), ("logistic", logit)]:
            table.to_excel(writer, sheet_name=name, index=False)

    lines = ["# Statistics (main grid)", "",
             f"Runs: {len(df)}; passed: {int(df['passed'].sum())}; prompt versions: {stats['prompt_versions']}; "
             f"scorer versions: {stats['scorer_versions']}", "",
             "## Pass rate per platform (Wilson 95% CI)", "",
             "| Platform | Passed | Cells | Pass rate | 95% CI | First-attempt pass rate |",
             "|---|---|---|---|---|---|"]
    for _, r in by_platform.iterrows():
        lines.append(f"| {r['platform']} | {r['passed']} | {r['cells']} | {r['pass_rate']:.2f} | "
                     f"[{r['ci95_low']:.2f}, {r['ci95_high']:.2f}] | {r['first_attempt_pass_rate']:.2f} |")
    lines += ["", "## Pass rate per model (Wilson 95% CI)", "", "| Model | Passed | Cells | Pass rate | 95% CI |",
              "|---|---|---|---|---|"]
    for _, r in by_model.iterrows():
        lines.append(f"| {r['model']} | {r['passed']} | {r['cells']} | {r['pass_rate']:.2f} | "
                     f"[{r['ci95_low']:.2f}, {r['ci95_high']:.2f}] |")
    lines += ["", "## Pass rate per task (Wilson 95% CI)", "", "| Task | Passed | Cells | Pass rate | 95% CI |",
              "|---|---|---|---|---|"]
    for _, r in by_task.iterrows():
        lines.append(f"| {r['task']} | {r['passed']} | {r['cells']} | {r['pass_rate']:.2f} | "
                     f"[{r['ci95_low']:.2f}, {r['ci95_high']:.2f}] |")
    lines += ["", f"## Cochran's Q across platforms (blocks = model x task, n = {q['blocks']})", "",
              f"Q = {q['Q']:.2f}, df = {q['df']}, p = {q['p_value']:.4g}", ""]
    lines += ["## Pairwise McNemar tests (Holm-adjusted)", ""]
    if len(mc):
        lines += ["| Platform A | Platform B | A only | B only | p | p (Holm) |", "|---|---|---|---|---|---|"]
        for _, r in mc.iterrows():
            lines.append(f"| {r['platform_a']} | {r['platform_b']} | {r['a_only_passes']} | {r['b_only_passes']} | "
                         f"{r['p_value']:.4g} | {r['p_holm']:.4g} |")
    lines += ["", "## Logistic regression (pass ~ platform + model + task)", ""]
    lines += ["| Term | Odds ratio | 95% CI | p |", "|---|---|---|---|"]
    for _, r in logit.iterrows():
        if "odds_ratio" in r and not pd.isna(r.get("odds_ratio")):
            lines.append(f"| {r['term']} | {r['odds_ratio']:.3g} | [{r['ci95_low']:.3g}, {r['ci95_high']:.3g}] | {r['p_value']:.4g} |")
        else:
            lines.append(f"| {r['term']} | {r.get('note', '')} | | |")
    lines += ["", "## Failure categories per platform (attempts, all repair rounds)", "",
              failures.to_markdown() if len(failures) else "none", ""]
    (args.out / "stats.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    stats.update({"cochran_q": q, "by_platform": by_platform.to_dict("records")})
    (args.out / "stats.json").write_text(json.dumps(stats, indent=2, default=str) + "\n", encoding="utf-8")

    _heatmap(df, args.out / "pass_heatmap.png")

    variance = _variance_summary(args.variance)
    if variance is not None:
        variance.to_csv(args.out / "variance_summary.csv", index=False)

    v1_runs = _load_runs([args.v1])
    if v1_runs:
        v1 = pd.DataFrame([_run_row(r) for r in v1_runs])
        v1.to_csv(args.out / "master_runs_v1_ablation.csv", index=False)

    print(f"wrote report to {args.out}: {len(df)} main runs, {int(df['passed'].sum())} passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
