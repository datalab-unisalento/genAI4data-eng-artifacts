#!/usr/bin/env bash
# Run the paper's experiments in protocol order. See docs/experiment-protocol.md.
#
# 1. Main grid: 3 models x 6 platforms x 6 tasks, one replicate, temperature 0, seed 42, 2 repairs.
# 2. Variance study: qwen3-coder:30b, 6 platforms, tasks T2 and T5, temperature 0.7, seeds 1-5.
#
# Refuses to start unless the final reference controls all pass.
set -euo pipefail
cd "$(dirname "$0")/.."

CONTROLS=benchmark-results/controls/reference-validation-final.json
python3 - "$CONTROLS" <<'PY'
import json, sys
records = json.load(open(sys.argv[1]))
failed = [f"{r['platform']}/{r['task']}" for r in records if not r["passed"]]
if failed or len(records) != 36:
    sys.exit(f"controls not fully passing ({len(records)} cells): {failed}")
print("controls: 36/36 pass")
PY

MODELS=(qwen3-coder:30b nemotron-3.5-lightning:30b cogito:32b)
PLATFORMS=(airflow n8n dagster prefect nifi kestra)
TASKS=(t1_open_meteo_ingestion t2_usgs_etl t3_open_meteo_join t4_daily_particulate_summary t5_trailing_three_hour_peak t6_earthquake_band_summary)

MAIN=benchmark-results/runs/three-models-prompt-v2
mkdir -p "$MAIN"
python3 scripts/run_matrix.py --models "${MODELS[@]}" --platforms "${PLATFORMS[@]}" --tasks "${TASKS[@]}" \
  --repeats 1 --repairs 2 --temperature 0 --seed 42 --output-dir "$MAIN" || true  # failed cells are results, not driver errors

VARIANCE=benchmark-results/runs/variance-temp07
mkdir -p "$VARIANCE"
for seed in 1 2 3 4 5; do
  python3 scripts/run_matrix.py --models qwen3-coder:30b --platforms "${PLATFORMS[@]}" \
    --tasks t2_usgs_etl t5_trailing_three_hour_peak --repeats 1 --repairs 2 \
    --temperature 0.7 --seed "$seed" --output-dir "$VARIANCE" || true
done
echo "experiments finished"
