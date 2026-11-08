# Experiment protocol

This document fixes the protocol for the results in the paper. Anything that changed after results were first seen is listed in §6 with its reason.

## 1. Scope

- **Platforms (6):** Airflow 2.10.5, n8n 1.82.3, Dagster 1.10.5, Prefect 3.2.14, NiFi 2.12.0, Kestra 1.3.45. Images are pinned by digest in `docker-compose.yml` and in the two Dockerfiles.
- **Tasks (6):** T1–T3 (ingestion, ETL, join) and T4–T6 (daily aggregate, trailing-window peak, earthquake band summary). Source data are archived fixtures (`fixtures/sources`), with checksums in `fixtures/manifest.json`.
- **Models (3):** qwen3-coder:30b, nemotron-3.5-lightning:30b, cogito:32b, all served by Ollama on one NVIDIA RTX A6000. A size ladder is deferred to later work.
- **Main grid:** 3 × 6 × 6 = 108 cells, one replicate each, temperature 0, seed 42, up to two repairs.
- **Variance study:** qwen3-coder:30b, all six platforms, tasks T2 and T5, temperature 0.7, five seeds (60 cells). Reported separately from the main grid.

## 2. Generation settings

Recorded in every run file under `configuration`:

- `temperature 0`, `seed 42`, `top_p 1`, `top_k 40`, `repeat_penalty 1.1`, `num_ctx 8192`, `num_predict 4096`, reasoning disabled.
- **Warm-up:** each measured generation is preceded by a one-token call (`ollama.warm_up`). Under identical settings, the first generation after a cold model load differed from later calls, while warm calls matched. `scripts/check_determinism.py` shows this: with warm-up, temperature-0 repeats were identical for both tasks tested (3 of 3 repeats each). The result is in `benchmark-results/determinism/determinism.json`.
- **Keep-alive:** 30 minutes, so a model stays loaded between cells.

## 3. Prompts

- `PROMPT_VERSION = "v2"`, recorded in every run file.
- v2 differs from v1 in two places: the NiFi instructions require reading the HTTP response (Java only sends a streamed body when the response is read), and the Kestra instructions show the task structure with `id`.

## 4. Scoring

- `SCORER_VERSION = "2"`, recorded in every run file.
- Records are compared as complete JSON objects, as multisets, so duplicates count. Integer-valued floats are normalized before comparison, so JavaScript runtimes (n8n) and Python runtimes agree on `10` and `10.0`.
- `scripts/rescore_runs.py` re-scores every logged attempt and lists any verdict that changes.

## 5. Failure categories

Each failed attempt has exactly one category, set from where the failure happened:

| Category | Meaning | Set by |
|---|---|---|
| `model_request_error` | The model call failed | generation stage |
| `invalid_artifact` | The artifact cannot be loaded, parsed, or validated by its platform | `ArtifactError`: n8n JSON and schema validation, Kestra flow rejected (HTTP 422), Airflow DAG not loaded, NiFi processor invalid (for example Groovy compile errors), import and compile errors in Dagster and Prefect |
| `runtime_error` | The artifact loaded and ran, then failed | `ExecutionError`: NiFi processor exception, Airflow DAG run failed, Kestra execution failed, n8n execution failure that is not a validation error |
| `platform_error` | Infrastructure or service failure, not the artifact | `PlatformError`: timeouts, service not ready, Docker or Compose failures |
| `harness_error` | An exception in the harness itself | any other exception |
| `incorrect_output` | The run finished but the sink data do not match the reference | scorer |

Dagster and Prefect distinguish load failures from runtime failures by their error text (`_ARTIFACT_LOAD_MARKERS` in `platforms.py`). That is a heuristic and is one of the items the failure annotation (§7) checks.

## 6. Changes made after first results (disclosed)

1. **Prompt v1 → v2.** The v1 results led to the NiFi and Kestra prompt changes in §3. Results under v1 are kept as an ablation (`benchmark-results/runs/three-models-all-platforms`, and the n8n re-run directory). The main results use v2.
2. **n8n template.** Every v1 n8n cell failed because the template's trigger cannot be started from the CLI. The template was changed to a manual trigger and the affected n8n cells were re-run. The superseded cells are kept under `superseded-n8n-skeleton-bug/`.
3. **Scorer.** Integer-valued floats are normalized (§4). Re-scoring v1 attempts changed no verdicts.
4. **Failure categories.** The v1 category `platform_error` covered artifact problems and infrastructure problems alike. v2 separates them (§5). v1 category counts in the report are derived from the stored error text and are marked as such.
5. **Fixes found by the reference controls, before any v2 result existed.** These are harness or template bugs, so they change the protocol, and all of them are included in v2:
   - **n8n Merge node.** With two sources, the Code node could run before the second source had executed, so T3 could not pass on n8n for any model. The template now has a Merge node that waits for every source (`prompts.py`).
   - **Recorder preflight.** The recorder runs as the host user, and every run checks that it can write its log directory before the first cell starts.
   - **Failure classes.** Load failures and runtime failures are raised as separate exception types (§5). Two helper names were undefined in an earlier version and were fixed before any v2 run.
   - **Warm-up and keep-alive** (§2) were added before the v2 run.

Fixes to the reference artifacts only (the controls check the harness, not the models, so these do not change the prompts):
   - **Kestra reference** did not call its entry point, so it delivered no rows.
   - **NiFi reference** declared the same Groovy variable twice (a compile error), and read the USGS `properties` key with dot or bracket access, which NiFi's Groovy resolves to the map's own property instead of the key. The reference now uses `get('properties')`.

**Platform finding for the paper.** In NiFi 2.12's `ExecuteGroovyScript`, `map['properties']` and `map.properties` return `null` for JSON-parsed maps, while `map.get('properties')` returns the value. Model-written NiFi artifacts that use the first two forms will fail for that reason alone. The prompt does not warn about it, so the failure is recorded as a platform quirk combined with an under-specified prompt, and the failure annotation (`docs/failure-codebook.md`) checks it.

## 7. Dev and held-out tasks

- T1–T3 were the development tasks. Prompt wording was tuned on them, including the v2 NiFi and Kestra changes, which came from T1–T3 results.
- T4–T6 were written after the v1 prompts were fixed and were not used for tuning. Their results under the NiFi and Kestra prompts are therefore not fully held out, because those two prompts were changed after seeing T1–T3 results. The paper must state this.

## 8. Reference controls

Each platform × task cell has a hand-written reference artifact (`controls/reference/`, generated by `scripts/build_reference_artifacts.py`). `scripts/run_controls.py` runs all 36 through the same platform runners and scoring. The controls are the validation gate: a cell whose control does not score 100% is not used for model results until the harness is fixed.

## 9. Cost

Dollar costs are not reported; the cost tariff is not set (`null` in every run). The paper reports tokens, time, and estimated GPU energy instead.

## 10. Analysis

`scripts/make_report.py` builds every table and statistic from the raw run files:

- pass rate per platform, model, and task, with Wilson 95% confidence intervals;
- Cochran's Q across the six platforms, with blocks = model × task;
- pairwise McNemar tests between platforms, with Holm adjustment;
- a logistic regression (pass ~ platform + model + task), reported only when it is estimable (it is not when a platform passes or fails every cell, and the paired tests are reported instead);
- failure categories per platform, repair behaviour (first-attempt vs eventual pass), and a pass/fail heatmap.

It runs under `python`, which has pandas, scipy, statsmodels, matplotlib, and openpyxl.

## 11. Reproducibility

- Every image is pinned by digest. The recorder and the Dagster/Prefect runner are built from the pinned base images.
- Every run file records the Ollama version, the model digest, the git revision and worktree state, and the source-tree hash.
- `fixtures/manifest.json` records the checksum of every fixture and reference output, and a unit test checks it.
- Results, controls, and the report are tracked in `benchmark-results/`.
