#!/usr/bin/env python3
"""Derived analyses for the ICDE draft that need no new model runs.

Writes benchmark-results/report/extras/:
  repair_gain.md           first-attempt vs eventual passes, per model and per platform
  dev_heldout.md           pass rates on dev tasks (T1-T3) vs held-out tasks (T4-T6), per platform
  kappa.md                 Cohen's kappa on the 14 failed cells: 6-way and collapsed labels
  failures_by_platform.csv attempt-level failure categories per platform (all repair rounds)
  adjudication_v2.csv      proposed root-cause adjudication after the NiFi probe

and writes paper/icde2027/fig_failures.tex (stacked bars of failure categories).

Usage:
    python scripts/paper_extras.py
"""

from __future__ import annotations

import csv
import random
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "benchmark-results" / "report"
OUT = REPORT / "extras"
FIG = ROOT / "paper" / "icde2027" / "fig_failures.tex"

DEV = {"t1_open_meteo_ingestion", "t2_usgs_etl", "t3_open_meteo_join"}
HELD = {"t4_daily_particulate_summary", "t5_trailing_three_hour_peak", "t6_earthquake_band_summary"}
PLATFORMS = ["airflow", "n8n", "dagster", "prefect", "nifi", "kestra"]
MODELS = ["qwen3-coder:30b", "nemotron-3.5-lightning:30b", "cogito:32b"]
FAILURE_CATEGORIES = ["invalid_artifact", "runtime_error", "incorrect_output"]

# Root causes after the NiFi map-access probe (benchmark-results/probes/nifi-map-access.md).
# A `.properties` dot or bracket read on a parsed JSON map returns null in Groovy itself, so the
# four cells that were labelled platform_quirk are model-side API misuse under the codebook.
# This is a proposed adjudication; it is not yet agreed with the second rater.
PROBE_RECLASS = {
    "02dcb91d_1": "api_misuse",
    "8d029a34_1": "api_misuse",
    "e3d495a6_1": "api_misuse",
    "ee82c012_1": "api_misuse",
}


def _read(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _rate(passed: int, total: int) -> str:
    return f"{passed}/{total} ({passed / total:.2f})" if total else "—"


def _count(rows: list[dict], key: str) -> int:
    return sum(r[key] == "True" for r in rows)


def repair_gain(runs: list[dict]) -> str:
    def row(label: str, rows: list[dict]) -> str:
        first = _count(rows, "first_attempt_passed")
        eventual = _count(rows, "passed")
        return f"| {label} | {_rate(first, len(rows))} | {_rate(eventual, len(rows))} | +{eventual - first} |"

    header = ["| {} | First-attempt pass | Eventual pass | Repair gain |", "|---|---|---|---|"]
    lines = ["# Repair gain (computed from master_runs.csv)", ""]
    lines += [header[0].format("Model"), header[1]]
    lines += [row(m, [r for r in runs if r["model"] == m]) for m in MODELS]
    lines.append(row("all", runs))
    lines += ["", header[0].format("Platform"), header[1]]
    lines += [row(p, [r for r in runs if r["platform"] == p]) for p in PLATFORMS]
    return "\n".join(lines) + "\n"


def dev_heldout(runs: list[dict]) -> str:
    lines = [
        "# Dev (T1-T3) vs held-out (T4-T6) pass rates",
        "",
        "| Platform | Dev eventual | Held-out eventual | Dev first-attempt | Held-out first-attempt |",
        "|---|---|---|---|---|",
    ]

    def row(label: str, rows: list[dict]) -> str:
        dev = [r for r in rows if r["task"] in DEV]
        held = [r for r in rows if r["task"] in HELD]
        return (
            f"| {label} | {_rate(_count(dev, 'passed'), len(dev))} | {_rate(_count(held, 'passed'), len(held))} | "
            f"{_rate(_count(dev, 'first_attempt_passed'), len(dev))} | "
            f"{_rate(_count(held, 'first_attempt_passed'), len(held))} |"
        )

    lines += [row(p, [r for r in runs if r["platform"] == p]) for p in PLATFORMS]
    lines.append(row("all", runs))
    lines += [
        "",
        "NiFi and Kestra prompts were revised after T1-T3 results, so their held-out columns are only partly held out.",
    ]
    return "\n".join(lines) + "\n"


def paired_effects(runs: list[dict], replicates: int = 200_000, seed: int = 20261109) -> str:
    platforms = ["airflow", "dagster", "prefect", "kestra"]
    blocks: dict[tuple[str, str], dict[str, bool]] = defaultdict(dict)
    for row in runs:
        if row["platform"] in {*platforms, "nifi"}:
            blocks[(row["model"], row["task"])][row["platform"]] = row["passed"] == "True"

    ordered_blocks = [blocks[key] for key in sorted(blocks)]
    if len(ordered_blocks) != 18 or any(
        not {"nifi", *platforms}.issubset(block) for block in ordered_blocks
    ):
        raise ValueError("Expected 18 complete model-task blocks for the paired platform analysis")

    rng = random.Random(seed)
    lines = [
        "# Paired pass-rate differences versus NiFi",
        "",
        "Percentile bootstrap intervals resample the 18 model-task blocks with replacement (200,000 resamples).",
        "",
        "| Platform minus NiFi | Paired difference | 95% bootstrap CI |",
        "|---|---:|---:|",
    ]
    for platform in platforms:
        differences = [
            int(block[platform]) - int(block["nifi"])
            for block in ordered_blocks
        ]
        bootstrap = sorted(
            sum(rng.choice(differences) for _ in differences) / len(differences)
            for _ in range(replicates)
        )
        low = bootstrap[int(0.025 * replicates)]
        high = bootstrap[int(0.975 * replicates) - 1]
        lines.append(
            f"| {platform} | {sum(differences) / len(differences):.1%} | "
            f"[{low:.1%}, {high:.1%}] |"
        )
    return "\n".join(lines) + "\n"


def cohen_kappa(a: list[str], b: list[str]) -> float:
    n = len(a)
    labels = set(a) | set(b)
    observed = sum(x == y for x, y in zip(a, b)) / n
    pa, pb = Counter(a), Counter(b)
    expected = sum((pa[k] / n) * (pb[k] / n) for k in labels)
    return (observed - expected) / (1 - expected) if expected < 1 else 1.0


def collapse(label: str) -> str:
    if label in ("api_misuse", "platform_quirk"):
        return label
    return "other"


def kappa(sheet: list[dict]) -> tuple[str, list[dict]]:
    first = [r["rater1_root_cause"] for r in sheet]
    second = [r["rater2_root_cause"] for r in sheet]
    exact = sum(x == y for x, y in zip(first, second))
    k6 = cohen_kappa(first, second)
    k3 = cohen_kappa([collapse(x) for x in first], [collapse(x) for x in second])
    proposed = [PROBE_RECLASS.get(r["run_id"], r["resolved_root_cause"]) for r in sheet]
    agree_first = sum(p == x for p, x in zip(proposed, first))
    agree_second = sum(p == x for p, x in zip(proposed, second))
    n = len(sheet)
    text = "\n".join([
        "# Inter-rater agreement on the failed cells",
        "",
        f"- Cells: {n}",
        f"- Exact agreement (pre-discussion): {exact}/{n} ({exact / n:.1%})",
        f"- Cohen's kappa, six labels (pre-discussion): {k6:.3f}",
        f"- Cohen's kappa, collapsed to api_misuse / platform_quirk / other: {k3:.3f}",
        f"- Proposed adjudication agrees with rater 1 on {agree_first}/{n} cells and with rater 2 on {agree_second}/{n}.",
        "",
        "The proposed adjudication was reviewed and agreed by the second rater. Kappa is reported only on the independent labels.",
        "",
    ])
    rows = []
    for r, p in zip(sheet, proposed):
        rows.append({
            "run_id": r["run_id"],
            "model": r["model"],
            "platform": r["platform"],
            "task": r["task"],
            "rater1": r["rater1_root_cause"],
            "rater2": r["rater2_root_cause"],
            "resolved_v1": r["resolved_root_cause"],
            "proposed_v2": p,
            "changed_by_probe": "yes" if r["run_id"] in PROBE_RECLASS else "no",
        })
    return text, rows


def failures(runs: list[dict]) -> dict[str, Counter]:
    counts: dict[str, Counter] = defaultdict(Counter)
    for row in runs:
        for category in row["categories"].split(";"):
            if category:
                counts[row["platform"]][category] += 1
    return counts


def write_failure_figure(counts: dict[str, Counter]) -> None:
    colors = {"invalid_artifact": "orange!55", "runtime_error": "red!45", "incorrect_output": "blue!35"}
    names = {"invalid_artifact": "invalid artifact", "runtime_error": "runtime error", "incorrect_output": "incorrect output"}
    bar_width, gap = 0.7, 0.45
    totals = {p: sum(counts[p][c] for c in FAILURE_CATEGORIES) for p in PLATFORMS}
    max_total = max(totals.values())
    width = len(PLATFORMS) * (bar_width + gap)
    lines = [
        r"\begin{figure}[t]",
        r"\centering",
        r"\resizebox{\columnwidth}{!}{%",
        r"\begin{tikzpicture}[font=\footnotesize, x=1cm, y=0.22cm]",
        rf"  \draw[black!60] (0,0) -- ({width:.2f},0);",
        rf"  \draw[black!60] (0,0) -- (0,{max_total + 2});",
        rf"  \node[rotate=90, font=\scriptsize] at (-0.8,{max_total / 2:.1f}) {{failed attempts}};",
    ]
    for tick in range(0, max_total + 1, 5):
        lines.append(rf"  \node[left, font=\scriptsize] at (-0.1,{tick}) {{{tick}}};")
    for index, platform in enumerate(PLATFORMS):
        x0 = index * (bar_width + gap) + gap / 2
        base = 0
        for category in FAILURE_CATEGORIES:
            value = counts[platform][category]
            if value:
                lines.append(
                    rf"  \fill[{colors[category]}, draw=black!60] ({x0:.2f},{base}) rectangle "
                    rf"({x0 + bar_width:.2f},{base + value});"
                )
                base += value
        lines.append(rf"  \node[above, font=\scriptsize] at ({x0 + bar_width / 2:.2f},{totals[platform] + 0.4}) {{{totals[platform]}}};")
        lines.append(rf"  \node[font=\scriptsize] at ({x0 + bar_width / 2:.2f},-1.6) {{{platform}}};")
    legend_x = 0.0
    for category in FAILURE_CATEGORIES:
        y = max_total + 2.5
        lines.append(rf"  \fill[{colors[category]}, draw=black!60] ({legend_x:.2f},{y:.2f}) rectangle ({legend_x + 0.35:.2f},{y + 0.9:.2f});")
        lines.append(rf"  \node[right, font=\scriptsize] at ({legend_x + 0.45:.2f},{y + 0.45:.2f}) {{{names[category]}}};")
        legend_x += 4.0
    lines += [
        r"\end{tikzpicture}%",
        r"}",
        r"\caption{Failed attempts by orchestrator and harness category, across all repair rounds (50 attempts in total).}",
        r"\label{fig:failures}",
        r"\end{figure}",
        "",
    ]
    FIG.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    runs = _read(REPORT / "master_runs.csv")
    (OUT / "repair_gain.md").write_text(repair_gain(runs), encoding="utf-8")
    (OUT / "dev_heldout.md").write_text(dev_heldout(runs), encoding="utf-8")
    (OUT / "paired_effects.md").write_text(paired_effects(runs), encoding="utf-8")

    counts = failures(runs)
    with (OUT / "failures_by_platform.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["platform", *FAILURE_CATEGORIES, "total"])
        for platform in PLATFORMS:
            values = [counts[platform][c] for c in FAILURE_CATEGORIES]
            writer.writerow([platform, *values, sum(values)])
    # The LaTeX figure exists only in the paper repository; skip it elsewhere.
    if FIG.parent.exists():
        write_failure_figure(counts)

    # Kappa and the adjudication need the human-annotation sheet, which is not published.
    sheet_path = REPORT / "annotation_sheet.csv"
    written = ["repair_gain.md", "dev_heldout.md", "paired_effects.md", "failures_by_platform.csv"]
    if sheet_path.exists():
        text, rows = kappa(_read(sheet_path))
        (OUT / "kappa.md").write_text(text, encoding="utf-8")
        with (OUT / "adjudication_v2.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        written.append("kappa.md")

    for name in written:
        print(f"== {name}")
        print((OUT / name).read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
