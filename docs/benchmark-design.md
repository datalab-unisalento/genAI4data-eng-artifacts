# Data-engineering benchmark: related work, dataset design, and metrics

Status: working design document. Items marked *(unverified)* come from secondary sources and need checking against the original paper or page before they are cited.

## 1. Related work

Several benchmarks already evaluate AI agents on data-engineering work. Each covers a different slice of the problem.

| Benchmark | What it evaluates | Relation to this project |
| --- | --- | --- |
| [ELT-Bench](https://arxiv.org/abs/2504.04808) | 100 end-to-end ELT pipelines over 835 source tables, with dbt models loaded into Snowflake. The best reported agent built 3.9% of data models. | Closest in scope, but dbt/SQL on one warehouse rather than orchestrator-native pipelines. |
| [ELT-Bench-Verified](https://arxiv.org/pdf/2603.29399) | An audit of ELT-Bench. It argues that benchmark-attributable errors understate agent capability. | Relevant for dataset quality checks. Raw ELT-Bench numbers should be read with it. |
| [data-eng-bench](https://www.snowflake.com/en/blog/engineering/data-eng-bench-data-engineering-agent-benchmark/) (Snowflake with Bespoke Labs) | 103 dbt tasks on a retail warehouse, verified row by row against a reference. | Same verification idea as ours (materialized rows checked against a reference), but dbt-only. |
| [ADE-bench](https://www.getdbt.com/blog/ade-bench-dbt-data-benchmarking) (dbt Labs) | Agent evaluation on real dbt projects and databases. | dbt-only. |
| [Astronomer data-engineering benchmark](https://www.astronomer.io/blog/astronomer-data-engineering-benchmark.md) | Coding-agent work on Airflow. *(unverified)* | Airflow only. |
| [Spider 2.0](https://arxiv.org/abs/2411.07763) | 632 enterprise text-to-SQL workflows over BigQuery and Snowflake. | Covers the SQL step only. |
| [Pipeline generation study](https://www.researchsquare.com/article/rs-6786102/v1) | Airflow (code-based), Azure Data Factory (low-code), and Databricks (hybrid). Models did much better on Airflow. *(unverified)* | Closest to our multi-platform framing. |

**Gap.** Most of this work targets dbt or SQL against one warehouse, or a single orchestrator. The gap this project can claim is narrower: *execution-based, orchestrator-native pipelines compared across several platforms under one contract, scored on the records that reach the sink.* This project already has that harness across Airflow, n8n, Dagster, Prefect, NiFi, and Kestra. The paper should state the gap this way, not as "no benchmark exists."

## 2. Dataset design

### 2.1 Unit of a task

Each task is a JSON entry in `fixtures/benchmark.json` with these fields:

| Field | Purpose |
| --- | --- |
| `id` | Stable identifier, such as `t4_daily_particulate_summary`. |
| `name`, `description` | Human-readable title and summary. |
| `prompt` | Task wording with a `{platform}` placeholder. |
| `source_ids` | Archived sources the task reads. |
| `required_fields` | Fields every output record must contain. |
| `instructions` | Exact transformation rules, including rounding and inclusion rules. |
| `expected_file` | Golden output produced by a reference implementation. |

Planned additions for the full dataset: `tier` (see 2.2), `operations` (for example `filter`, `join`, `window`, `aggregate`), `split` (`dev` or `heldout`), and `reference_script`.

### 2.2 Difficulty tiers

| Tier | Operations | Current tasks |
| --- | --- | --- |
| Ingestion | Fetch, flatten columnar or nested JSON, coerce types | T1 |
| Transform | Derived fields, conditional bucketing, unit handling | T2 |
| Combine | Inner join on a shared key, union, deduplication | T3 |
| Aggregate | Group-by counts and extrema, per-date summaries | T4, T6 |
| Window | Trailing-window computations over ordered records | T5 |
| Operational | Incremental loads with watermarks, HTTP 429/503 retries, schema drift, idempotent reruns | Not yet built |

### 2.3 Exact-match outputs

The scorer compares records by exact JSON equality, so any float that a model rounds differently counts as a failure. Tasks therefore use only:

- counts, calendar dates, and identifiers,
- raw source values (such as a maximum), which need no arithmetic,
- explicit rules when a value is derived.

Tasks that need a computed float (for example a mean) should either specify the rounding exactly or avoid the computation. Current reference outputs contain no computed floats.

### 2.4 Reference outputs

`scripts/build_reference_outputs.py` computes the expected outputs for T4–T6 from the archived fixtures and writes them to `fixtures/expected/`. Its `--check` mode fails when a committed output differs from the generator. A unit test runs the same check.

Reference outputs must come from code that is independent of any model output. They are written once, reviewed, and committed. Any change to a reference output is a benchmark version change and is recorded in the fixture manifest.

### 2.5 Splits

Tasks marked `dev` may be used while developing prompts. Tasks marked `heldout` are never used during prompt development and are reported separately, to limit overfitting to the prompt templates. The current batch is all `dev`; the held-out split is not yet defined.

### 2.6 Coverage

The current fixtures are Open-Meteo air-quality observations and USGS earthquakes, both for 2024-01-01 to 2024-01-02. That limits how much the results generalise. Planned additions are other public domains (transport, retail, health) with their own fixtures and checksums, so that a platform's results do not depend on one dataset.

### 2.7 First batch (implemented)

| Task | Tier | Source | Output | Rows |
| --- | --- | --- | --- | --- |
| T4 daily particulate summary | Aggregate | Open-Meteo particulate | One record per date: hour count, maximum pm2_5, maximum pm10 | 2 |
| T5 trailing three-hour peak | Window | Open-Meteo particulate | One record per hour from the third onward: pm2_5 and its trailing three-hour maximum | 46 |
| T6 earthquake band summary | Aggregate | USGS earthquakes | One record per magnitude band: event count, largest magnitude, largest depth | 2 |

These three tasks are not yet in the matrix driver, which runs only T1–T3 (`TASKS` in `scripts/run_matrix.py`). Adding them is a one-line change, to be made after the current experiment finishes.

## 3. Metrics

**Outcome**
- Exact sink correctness: duplicate-sensitive precision, recall, and F1 over complete records.
- Pass rate: the share of cells whose records match exactly.
- Field-level accuracy and row-count match.

**Pipeline health**
- First-attempt pass, eventual pass after repairs, and attempts to success or exhaustion.
- Invalid-artifact rate (the artifact does not parse or load on its platform).
- Platform-error rate, and the failure-category breakdown already logged per run (`model_request_error`, `invalid_generated_artifact`, `platform_error`, `harness_error`, `incorrect_output`).

**Efficiency**
- Generation tokens, generation time, execution time, and end-to-end time.
- Container CPU and memory, GPU utilization, and estimated GPU energy. Only the GPU energy figure is an estimate; the CPU figures are sampled.

**Stability**
- Variance across replicates, with bootstrap confidence intervals. The design target is five replicates per cell.

**Cross-platform consistency**
- Whether a model that passes on one orchestrator passes on the others, per task.

Most of these metrics are already logged for each run. What remains is the task suite, reference outputs, splits, and the analysis script.

## 4. Open questions

- Whether the held-out split should contain tasks from a new domain, to test generalisation beyond the air-quality and earthquake fixtures.
- Whether operational tasks (retries, watermarks) can be scored on the sink alone, or need recorder-level checks such as request counts.
- Whether to version the benchmark (for example `v1.0`) before the paper is submitted.
