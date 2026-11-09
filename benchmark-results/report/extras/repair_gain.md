# Repair gain (computed from master_runs.csv)

| Model | First-attempt pass | Eventual pass | Repair gain |
|---|---|---|---|
| qwen3-coder:30b | 33/36 (0.92) | 34/36 (0.94) | +1 |
| nemotron-3.5-lightning:30b | 26/36 (0.72) | 29/36 (0.81) | +3 |
| cogito:32b | 29/36 (0.81) | 31/36 (0.86) | +2 |
| all | 88/108 (0.81) | 94/108 (0.87) | +6 |

| Platform | First-attempt pass | Eventual pass | Repair gain |
|---|---|---|---|
| airflow | 17/18 (0.94) | 18/18 (1.00) | +1 |
| n8n | 12/18 (0.67) | 15/18 (0.83) | +3 |
| dagster | 17/18 (0.94) | 17/18 (0.94) | +0 |
| prefect | 18/18 (1.00) | 18/18 (1.00) | +0 |
| nifi | 6/18 (0.33) | 8/18 (0.44) | +2 |
| kestra | 18/18 (1.00) | 18/18 (1.00) | +0 |
