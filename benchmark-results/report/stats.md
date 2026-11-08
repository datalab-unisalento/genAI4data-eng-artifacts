# Statistics (main grid)

Runs: 108; passed: 94; prompt versions: ['v2']; scorer versions: ['2']

## Pass rate per platform (Wilson 95% CI)

| Platform | Passed | Cells | Pass rate | 95% CI | First-attempt pass rate |
|---|---|---|---|---|---|
| airflow | 18 | 18 | 1.00 | [0.82, 1.00] | 0.94 |
| n8n | 15 | 18 | 0.83 | [0.61, 0.94] | 0.67 |
| dagster | 17 | 18 | 0.94 | [0.74, 0.99] | 0.94 |
| prefect | 18 | 18 | 1.00 | [0.82, 1.00] | 1.00 |
| nifi | 8 | 18 | 0.44 | [0.25, 0.66] | 0.33 |
| kestra | 18 | 18 | 1.00 | [0.82, 1.00] | 1.00 |

## Pass rate per model (Wilson 95% CI)

| Model | Passed | Cells | Pass rate | 95% CI |
|---|---|---|---|---|
| cogito:32b | 31 | 36 | 0.86 | [0.71, 0.94] |
| nemotron-3.5-lightning:30b | 29 | 36 | 0.81 | [0.65, 0.90] |
| qwen3-coder:30b | 34 | 36 | 0.94 | [0.82, 0.98] |

## Pass rate per task (Wilson 95% CI)

| Task | Passed | Cells | Pass rate | 95% CI |
|---|---|---|---|---|
| t1_open_meteo_ingestion | 17 | 18 | 0.94 | [0.74, 0.99] |
| t2_usgs_etl | 14 | 18 | 0.78 | [0.55, 0.91] |
| t3_open_meteo_join | 15 | 18 | 0.83 | [0.61, 0.94] |
| t4_daily_particulate_summary | 17 | 18 | 0.94 | [0.74, 0.99] |
| t5_trailing_three_hour_peak | 17 | 18 | 0.94 | [0.74, 0.99] |
| t6_earthquake_band_summary | 14 | 18 | 0.78 | [0.55, 0.91] |

## Cochran's Q across platforms (blocks = model x task, n = 18)

Q = 36.25, df = 5, p = 8.465e-07

## Pairwise McNemar tests (Holm-adjusted)

| Platform A | Platform B | A only | B only | p | p (Holm) |
|---|---|---|---|---|---|
| airflow | n8n | 3 | 0 | 0.25 | 1 |
| airflow | dagster | 1 | 0 | 1 | 1 |
| airflow | prefect | 0 | 0 | 1 | 1 |
| airflow | nifi | 10 | 0 | 0.001953 | 0.0293 |
| airflow | kestra | 0 | 0 | 1 | 1 |
| n8n | dagster | 1 | 3 | 0.625 | 1 |
| n8n | prefect | 0 | 3 | 0.25 | 1 |
| n8n | nifi | 8 | 1 | 0.03906 | 0.4297 |
| n8n | kestra | 0 | 3 | 0.25 | 1 |
| dagster | prefect | 0 | 1 | 1 | 1 |
| dagster | nifi | 9 | 0 | 0.003906 | 0.04688 |
| dagster | kestra | 0 | 1 | 1 | 1 |
| prefect | nifi | 10 | 0 | 0.001953 | 0.0293 |
| prefect | kestra | 0 | 0 | 1 | 1 |
| nifi | kestra | 0 | 10 | 0.001953 | 0.0293 |

## Logistic regression (pass ~ platform + model + task)

| Term | Odds ratio | 95% CI | p |
|---|---|---|---|
| all | complete separation: odds ratios not estimable; use the paired tests | | |

## Failure categories per platform (attempts, all repair rounds)

| platform   |   incorrect_output |   invalid_artifact |   runtime_error |
|:-----------|-------------------:|-------------------:|----------------:|
| airflow    |                  2 |                  0 |               0 |
| dagster    |                  1 |                  1 |               1 |
| n8n        |                  3 |                 10 |               0 |
| nifi       |                  9 |                  7 |              16 |

