# LLM-generated data-pipeline benchmark artifacts

This repository contains the code, fixtures, reference implementations, and experiment records for an execution-based benchmark of LLM-generated data pipelines across six orchestrators.

## Contents

- `src/pipeline_bench/`: generation, execution, and scoring harness.
- `scripts/`: experiment, validation, and report-generation utilities.
- `controls/reference/`: hand-written reference pipelines for the platform/task cells.
- `fixtures/`: archived Open-Meteo and USGS input data, expected outputs, and checksums.
- `benchmark-results/runs/`: raw per-cell records for the main prompt-v2 grid, the prompt-v1 ablation, its n8n reruns, and the temperature-variance study.
- `benchmark-results/report/`: aggregate tables, statistics, and model/platform results derived from the run records.
- `docs/experiment-protocol.md`: protocol, deviations, and analysis details.

Drafts, reference-paper PDFs, collaboration materials, recorder logs, smoke runs, and the pending human-annotation sheets are not included.

## Reproduce

The harness requires Python 3.11+, Docker Compose, Ollama, and a compatible NVIDIA GPU with enough memory for the selected model. Images are pinned by digest in the Docker configuration. Install the package and run the unit tests:

```sh
python3 -m pip install -e .
python3 -m unittest discover -s tests
```

To regenerate the report from the included run records, install the analysis dependencies (`numpy`, `pandas`, `scipy`, `statsmodels`, `matplotlib`, and `openpyxl`) and run:

```sh
python3 scripts/make_report.py
```

To execute the full experiment protocol, configure Ollama and Docker, then run `bash scripts/run_experiments.sh`. This requires substantial GPU time; it writes new run records under `benchmark-results/runs/`.

The fixture sources are Open-Meteo Air Quality API (`https://air-quality-api.open-meteo.com/`) and USGS FDSN Earthquake Catalog (`https://earthquake.usgs.gov/fdsnws/event/1/`). The archived inputs and their metadata are in `fixtures/sources/`.

No license file is included. Making this repository public does not by itself grant permission to reuse its code or data.
