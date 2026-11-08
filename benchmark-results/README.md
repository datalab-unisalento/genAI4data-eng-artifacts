# Benchmark results

This curated release contains raw run records for the prompt-v1 ablation, the prompt-v2 main grid, the n8n reruns, and the temperature-variance study. Each JSON record stores the cell configuration, environment, attempt history, and metrics; generated artifacts from each attempt are stored alongside it.

`report/` contains the derived main-grid and variance tables and statistics. The paper's primary results use `runs/three-models-prompt-v2/`; the v1 runs are retained as an ablation. `controls/reference-validation-final.json` records the final 36-cell reference-control validation.

Pending human-annotation sheets, smoke runs, superseded controls, and recorder request/sink logs are excluded from this public snapshot.
