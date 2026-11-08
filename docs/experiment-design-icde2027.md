# Experiment design for ICDE 2027 (EAB track)

Status: plan, written 2026-11-02. The submission deadline is **11 November 2026, 5:00 PM Pacific** (round 2).

## 1. Call requirements

Source: [ICDE 2027 research papers call](https://icde2027.github.io/cf-research-papers.html), as reported by an automated fetch of the page. Verify the wording on the page before submission.

- **Track:** submit as Experiment, Analysis, and Benchmark (EAB). The title needs the prefix "[Experiment, Analysis, and Benchmark]", which is removed in the camera-ready version. The paper must "provide fundamentally new insight into the strengths and weaknesses of existing methods."
- **Artifacts:** EAB papers "MUST provide all artifacts necessary to reproduce the results. No exceptions." Artifacts must be on an open service such as a public GitHub repository.
- **Resubmission:** a rejected paper cannot be submitted to ICDE for one year. Round 2 is the only realistic deadline this cycle.
- **AI disclosure:** LLM use for running experiments must be declared in the acknowledgments.
- **Length:** 12 pages, excluding references and the AI-content declaration. No appendix.
- **Review:** single-blind; at least three reviewers; accept or reject with no revisions.

## 2. What the paper must show

The insight must come from the experiments. The claims to build the paper on:

1. **Platform effect.** The same model's success rate across six orchestrators (Airflow, n8n, Dagster, Prefect, NiFi, Kestra), per task.
2. **Failure taxonomy.** Where models fail, and whether each failure is a model error or a platform quirk.
3. **Repair value.** How much execution feedback improves each model, by platform and task.
4. **Size and cost.** Success against model size, and cost per successful pipeline (tokens, time, estimated GPU energy).

## 3. Design

### 3.1 Harness validation (first, blocking)

Every platform × task cell needs a hand-written reference artifact that scores 100% through the same harness. Until then, a model failure cannot be distinguished from a harness failure.

- Current state: controls exist only for T1 on Airflow, NiFi, and Kestra, plus the Airflow control under `controls/`.
- Required: 6 platforms × 6 tasks = 36 controls.
- Validation must run before each experiment batch, and a failing control blocks the batch.

This check would have caught the recorder bug described in §6.

### 3.2 Grid

| Axis | Choice |
| --- | --- |
| Platforms | All six: airflow, n8n, dagster, prefect, nifi, kestra |
| Tasks | T1–T6. T1–T3 are development tasks; T4–T6 are held out (not used while tuning prompts). |
| Models | A size ladder in one family (for example Qwen3 at 0.6B, 1.7B, 4B, 8B, 14B, 32B) plus two or three other families and one code specialist. Target eight models. |
| Temperature | 0 with seed 42 for the main grid |
| Replicates | One per cell for the main grid |
| Repairs | Budget of 2. Report both first-attempt pass and eventual pass. |

The current three models (qwen3-coder:30b, nemotron-3.5-lightning:30b, cogito:32b) are all 30B-class, so they cannot show size effects on their own.

**Replicates.** At temperature 0 with a fixed seed, five replicates of the same cell are close to duplicates, so they add little information. The README's five-replicate plan should change. Run a separate variance study instead: temperature 0.7, five seeds, on a subset of cells. First confirm that temperature-0 replicates are identical in practice.

**Disk.** About 11 GB is free. Models larger than that cannot be pulled until space is freed.

### 3.3 Dev and held-out split

T1–T3 were used while tuning prompts. T4–T6 were written after the prompts were fixed and have not been used for tuning, so they serve as the held-out set. Freeze the prompt templates and the grid before the final run, and record the git revision of the frozen state.

### 3.4 Metrics

- **Outcome:** exact sink correctness (duplicate-sensitive precision, recall, F1), pass rate, row-count match.
- **Process:** first-attempt pass, eventual pass after repairs, attempts to success, invalid-artifact rate, platform-error rate, and the failure-category breakdown already logged per run.
- **Efficiency:** generation tokens, generation time, execution time, container CPU and memory, GPU utilization, and estimated GPU energy. Report cost per successful pipeline.

### 3.5 Statistics

- Success rates with Wilson confidence intervals.
- Platform comparison on the same model and task: McNemar's test for two platforms, Cochran's Q across six. Correct for multiple comparisons with Holm.
- Size effect: logistic regression on log model size, with task as a random effect.
- Report effect sizes alongside p-values.

### 3.6 Failure annotation

Take about 150 failed cells, have two people label root causes against a fixed codebook (model error, platform quirk, harness error, ambiguous), and report inter-rater agreement with Cohen's kappa. This section carries the "weaknesses" claim.

### 3.7 Reproducibility

- Pin every Docker image by digest. The Python base images and the recorder's base image are currently unpinned tags.
- Record the Ollama version and model digest for each run (already logged).
- Archive the code, fixtures, reference outputs, results, and generated artifacts on Zenodo with a DOI. Link it from the paper.
- Provide one command that reproduces a single cell, and one that rebuilds all tables from the raw run files.
- Re-run a subset on a second machine to confirm the results are reproducible outside this workstation.

### 3.8 Reference model (optional)

One frontier API model would give a ceiling for the comparison. The current design is local-only by choice. If that stays, the paper should say why.

## 4. Nine-day plan

| Days | Work |
| --- | --- |
| 1–2 | 36 reference artifacts, each passing through the harness. Pin images by digest. Add T4–T6 to the matrix. |
| 2–5 | Main grid on the model ladder: temperature 0, one replicate, repairs on. |
| 4–6 | Variance study on a subset. Failure annotation. |
| 6–8 | Statistics, tables, figures. Results and discussion. |
| 8–9 | Artifact release with DOI. Acknowledgments and AI disclosure. |

The main-grid run is the bottleneck. Validation has to pass first so the run is trustworthy.

## 5. Scope risk

A full grid of eight models × six platforms × six tasks is about 288 cells before repairs, and each cell takes minutes. Measure the real per-cell time from the restarted run before fixing the model list. If the measured time does not fit the schedule, cut the model ladder first and keep all six platforms and all six tasks.

## 6. Incident: invalid three-model run

The first three-model run (54 cells) was invalid. The recorder could not write its log files, because I had moved them into `benchmark-results/recorder/`, owned by the host user, while the container runs with `cap_drop: ALL`. Every cell failed with `harness_error` before any model or platform behaviour was measured. The run is archived under `benchmark-results/runs/invalid-three-models-recorder-permissions/` with an explanatory README. The fix was to make the two log files writable (`chmod o+w`), and a one-cell check then passed. The valid run was restarted into `benchmark-results/runs/three-models-all-platforms/`.

The fix is a workaround. A longer-term fix is to run the recorder as the host user, or to keep its logs in a directory it owns.
