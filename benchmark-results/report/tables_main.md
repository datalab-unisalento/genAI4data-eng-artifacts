### cogito:32b — t1_open_meteo_ingestion

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | passed | passed | passed | passed | passed |
| Attempts | 1 | 1 | 1 | 1 | 2 | 1 |
| Failure category | — | — | — | — | invalid_artifact | — |
| Failure detail (truncated) | — | — | — | — | — | — |
| First-attempt passed | yes | yes | yes | yes | — | yes |
| Best F1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Precision | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Recall | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| F1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Expected rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Actual rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Matched rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Source coverage | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink POSTs | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink rows accepted | 48 | 48 | 48 | 48 | 48 | 48 |
| Generation time (s) | 12.048 | 25.997 | 9.378 | 11.868 | 24.433 | 10.872 |
| Ollama model load (s) | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 |
| Ollama total time (s) | 12.044 | 25.991 | 9.374 | 11.864 | 12.778 | 10.868 |
| Execution time (s) | 27.889 | 21.071 | 20.332 | 27.792 | 364.701 | 18.408 |
| End-to-end time (s) | 68.636 | 52.760 | 54.313 | 46.229 | 409.383 | 53.579 |
| Prompt tokens | 732 | 1297 | 721 | 680 | 2935 | 739 |
| Output tokens | 346 | 741 | 266 | 342 | 667 | 307 |
| Output tokens/s | 30.219 | 29.708 | 30.294 | 30.333 | 29.589 | 29.891 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 19.344 | 16.430 | 0.020 | 0.031 | 18.970 | 8.977 |
| Peak container CPU (%) | 154.510 | 130.670 | 0.190 | 0.200 | 285.080 | 51.730 |
| Peak container memory (GB) | 1.994 | 0.356 | 0.031 | 0.024 | 1.504 | 1.748 |
| Resource samples | 22 | 17 | 26 | 22 | 122 | 18 |
| Mean GPU utilization (%) | 21.682 | 44.412 | 21.385 | 30.091 | 10.942 | 15.889 |
| Peak GPU memory (GB) | 26.090 | 26.092 | 26.088 | 26.085 | 26.092 | 26.090 |
| Estimated GPU energy (Wh) | 2.058 | 2.733 | 1.793 | 1.659 | 5.030 | 1.793 |

### cogito:32b — t2_usgs_etl

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | passed | failed | passed | failed | passed |
| Attempts | 1 | 1 | 3 | 1 | 3 | 1 |
| Failure category | — | — | runtime_error, incorrect_output, invalid_artifact | — | incorrect_output, invalid_artifact | — |
| Failure detail (truncated) | — | — | docker compose --profile dagster run --rm -T --no-deps python-runner sh -c mkdir -p "$DAGSTER_HOME" && exec da… | — | — | — |
| First-attempt passed | yes | yes | — | yes | no | yes |
| Best F1 | 1.000 | 1.000 | 0.000 | 1.000 | 0.000 | 1.000 |
| Precision | 1.000 | 1.000 | — | 1.000 | 0.000 | 1.000 |
| Recall | 1.000 | 1.000 | — | 1.000 | 0.000 | 1.000 |
| F1 | 1.000 | 1.000 | — | 1.000 | 0.000 | 1.000 |
| Expected rows | 49 | 49 | — | 49 | 49 | 49 |
| Actual rows | 49 | 49 | — | 49 | 49 | 49 |
| Matched rows | 49 | 49 | — | 49 | 0 | 49 |
| Source coverage | 1.000 | 1.000 | 0.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 1 | 1 | 0 | 1 | 1 | 1 |
| Sink POSTs | 1 | 1 | 0 | 1 | 1 | 1 |
| Sink rows accepted | 49 | 49 | 0 | 49 | 49 | 49 |
| Generation time (s) | 14.702 | 26.860 | 42.108 | 13.457 | 53.167 | 13.557 |
| Ollama model load (s) | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 |
| Ollama total time (s) | 14.697 | 26.853 | 14.618 | 13.452 | 16.618 | 13.552 |
| Execution time (s) | 26.572 | 20.741 | 38.500 | 27.915 | 435.026 | 18.090 |
| End-to-end time (s) | 42.059 | 48.117 | 103.582 | 42.267 | 517.765 | 33.102 |
| Prompt tokens | 733 | 1298 | 7688 | 687 | 7735 | 746 |
| Output tokens | 427 | 767 | 1070 | 390 | 1395 | 387 |
| Output tokens/s | 30.279 | 29.725 | 28.779 | 30.323 | 29.596 | 29.873 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 20.788 | 15.616 | 0.034 | 0.039 | 25.930 | 9.021 |
| Peak container CPU (%) | 131.280 | 124.940 | 0.230 | 0.240 | 425.540 | 27.530 |
| Peak container memory (GB) | 1.983 | 0.303 | 0.031 | 0.023 | 1.495 | 1.616 |
| Resource samples | 14 | 16 | 20 | 19 | 144 | 11 |
| Mean GPU utilization (%) | 32.071 | 46.938 | 53.927 | 28.500 | 19.444 | 33.545 |
| Peak GPU memory (GB) | 22.342 | 22.352 | 22.352 | 22.342 | 22.352 | 22.344 |
| Estimated GPU energy (Wh) | 1.669 | 2.658 | 4.236 | 1.617 | 8.104 | 1.484 |

### cogito:32b — t3_open_meteo_join

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | failed | passed | passed | passed | passed |
| Attempts | 1 | 3 | 1 | 1 | 1 | 1 |
| Failure category | — | invalid_artifact | — | — | — | — |
| Failure detail (truncated) | — | n8n sink node 'Sink' must set parameters.sendBody=true, parameters.specifyBody='json', and parameters.jsonBody… | — | — | — | — |
| First-attempt passed | yes | — | yes | yes | yes | yes |
| Best F1 | 1.000 | — | 1.000 | 1.000 | 1.000 | 1.000 |
| Precision | 1.000 | — | 1.000 | 1.000 | 1.000 | 1.000 |
| Recall | 1.000 | — | 1.000 | 1.000 | 1.000 | 1.000 |
| F1 | 1.000 | — | 1.000 | 1.000 | 1.000 | 1.000 |
| Expected rows | 48 | — | 48 | 48 | 48 | 48 |
| Actual rows | 48 | — | 48 | 48 | 48 | 48 |
| Matched rows | 48 | — | 48 | 48 | 48 | 48 |
| Source coverage | 1.000 | 0.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 2 | 0 | 2 | 2 | 2 | 2 |
| Sink POSTs | 1 | 0 | 1 | 1 | 1 | 1 |
| Sink rows accepted | 48 | 0 | 48 | 48 | 48 | 48 |
| Generation time (s) | 23.223 | 122.625 | 19.337 | 21.552 | 24.064 | 21.512 |
| Ollama model load (s) | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 |
| Ollama total time (s) | 23.218 | 41.411 | 19.332 | 21.547 | 24.059 | 21.505 |
| Execution time (s) | 26.750 | 27.023 | 20.898 | 28.981 | 18.911 | 18.462 |
| End-to-end time (s) | 50.379 | 175.363 | 42.043 | 51.836 | 44.935 | 40.403 |
| Prompt tokens | 925 | 7963 | 928 | 880 | 947 | 941 |
| Output tokens | 675 | 3393 | 552 | 628 | 694 | 616 |
| Output tokens/s | 30.094 | 29.025 | 29.773 | 30.125 | 29.801 | 29.731 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 19.288 | 0.202 | 0.014 | 0.033 | 12.244 | 5.985 |
| Peak container CPU (%) | 111.750 | 3.110 | 0.020 | 0.310 | 149.400 | 29.560 |
| Peak container memory (GB) | 1.952 | 0.203 | 0.024 | 0.023 | 1.378 | 1.613 |
| Resource samples | 17 | 21 | 20 | 24 | 15 | 12 |
| Mean GPU utilization (%) | 44.529 | 78.583 | 42.300 | 37.960 | 49.867 | 50.462 |
| Peak GPU memory (GB) | 22.348 | 22.352 | 22.352 | 22.352 | 22.352 | 22.348 |
| Estimated GPU energy (Wh) | 2.444 | 10.761 | 2.115 | 2.335 | 2.395 | 2.174 |

### nemotron-3.5-lightning:30b — t1_open_meteo_ingestion

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | passed | passed | passed | failed | passed |
| Attempts | 1 | 1 | 1 | 1 | 3 | 1 |
| Failure category | — | — | — | — | runtime_error | — |
| Failure detail (truncated) | — | — | — | — | NiFi processor failed: ExecuteGroovyScript[id=abfcfa59-01a1-1000-fddb-95d0886416e7] java.lang.IllegalArgumentE… | — |
| First-attempt passed | yes | yes | yes | yes | — | yes |
| Best F1 | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| Precision | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| Recall | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| F1 | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| Expected rows | 48 | 48 | 48 | 48 | — | 48 |
| Actual rows | 48 | 48 | 48 | 48 | — | 48 |
| Matched rows | 48 | 48 | 48 | 48 | — | 48 |
| Source coverage | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink POSTs | 1 | 1 | 1 | 1 | 0 | 1 |
| Sink rows accepted | 48 | 48 | 48 | 48 | 0 | 48 |
| Generation time (s) | 2.457 | 5.033 | 2.110 | 2.077 | 7.869 | 2.060 |
| Ollama model load (s) | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 |
| Ollama total time (s) | 2.454 | 5.029 | 2.107 | 2.073 | 2.524 | 2.058 |
| Execution time (s) | 27.633 | 21.059 | 21.094 | 27.626 | 144.900 | 17.431 |
| End-to-end time (s) | 67.284 | 65.721 | 62.589 | 69.418 | 227.184 | 58.989 |
| Prompt tokens | 758 | 1334 | 740 | 696 | 3327 | 766 |
| Output tokens | 369 | 770 | 303 | 301 | 1089 | 293 |
| Output tokens/s | 178.685 | 171.469 | 172.357 | 172.324 | 178.096 | 171.414 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 16.410 | 7.605 | 0.019 | 0.025 | 56.970 | 8.113 |
| Peak container CPU (%) | 115.940 | 119.850 | 0.170 | 0.180 | 402.160 | 42.570 |
| Peak container memory (GB) | 1.858 | 0.371 | 0.031 | 0.031 | 1.476 | 1.732 |
| Resource samples | 22 | 21 | 29 | 32 | 62 | 18 |
| Mean GPU utilization (%) | 3.045 | 16.095 | 8.833 | 5.394 | 5.547 | 6.842 |
| Peak GPU memory (GB) | 26.083 | 42.091 | 42.087 | 42.083 | 42.087 | 42.087 |
| Estimated GPU energy (Wh) | 1.343 | 1.666 | 1.421 | 1.478 | 3.229 | 1.316 |

### nemotron-3.5-lightning:30b — t2_usgs_etl

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | passed | passed | passed | failed | passed |
| Attempts | 1 | 1 | 1 | 1 | 3 | 1 |
| Failure category | — | — | — | — | invalid_artifact, runtime_error | — |
| Failure detail (truncated) | — | — | — | — | NiFi processor failed: ExecuteGroovyScript[id=ac041af9-01a1-1000-d3bd-bc508d453157] groovy.lang.MissingPropert… | — |
| First-attempt passed | yes | yes | yes | yes | — | yes |
| Best F1 | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| Precision | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| Recall | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| F1 | 1.000 | 1.000 | 1.000 | 1.000 | — | 1.000 |
| Expected rows | 49 | 49 | 49 | 49 | — | 49 |
| Actual rows | 49 | 49 | 49 | 49 | — | 49 |
| Matched rows | 49 | 49 | 49 | 49 | — | 49 |
| Source coverage | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink POSTs | 1 | 1 | 1 | 1 | 0 | 1 |
| Sink rows accepted | 49 | 49 | 49 | 49 | 0 | 49 |
| Generation time (s) | 2.775 | 5.000 | 2.402 | 2.420 | 11.787 | 2.626 |
| Ollama model load (s) | 0.004 | 0.002 | 0.002 | 0.001 | 0.003 | 0.001 |
| Ollama total time (s) | 2.771 | 4.996 | 2.398 | 2.417 | 4.304 | 2.619 |
| Execution time (s) | 29.385 | 20.997 | 20.508 | 27.119 | 419.382 | 17.844 |
| End-to-end time (s) | 33.019 | 26.366 | 23.256 | 30.919 | 478.837 | 20.818 |
| Prompt tokens | 776 | 1348 | 750 | 723 | 5121 | 790 |
| Output tokens | 426 | 785 | 366 | 373 | 1642 | 402 |
| Output tokens/s | 173.908 | 174.727 | 175.961 | 177.051 | 159.595 | 175.483 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 19.804 | 27.939 | 0.038 | 0.031 | 28.792 | 12.793 |
| Peak container CPU (%) | 115.970 | 130.420 | 0.230 | 0.230 | 455.280 | 23.060 |
| Peak container memory (GB) | 1.879 | 0.366 | 0.031 | 0.030 | 1.526 | 1.732 |
| Resource samples | 11 | 9 | 10 | 15 | 135 | 6 |
| Mean GPU utilization (%) | 5.091 | 7.444 | 6.000 | 4.600 | 3.906 | 8.714 |
| Peak GPU memory (GB) | 26.083 | 26.090 | 26.083 | 26.083 | 26.090 | 26.085 |
| Estimated GPU energy (Wh) | 0.636 | 0.702 | 0.580 | 0.653 | 4.331 | 0.547 |

### nemotron-3.5-lightning:30b — t3_open_meteo_join

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | failed | passed | passed | failed | passed |
| Attempts | 1 | 3 | 1 | 1 | 3 | 1 |
| Failure category | — | invalid_artifact | — | — | runtime_error | — |
| Failure detail (truncated) | — | n8n connection 'Benchmark Trigger'.main has an invalid destination node | — | — | NiFi processor failed: ExecuteGroovyScript[id=ac0701d2-01a1-1000-a0c2-d3baa130fa6b] groovy.lang.MissingMethodE… | — |
| First-attempt passed | yes | — | yes | yes | — | yes |
| Best F1 | 1.000 | — | 1.000 | 1.000 | — | 1.000 |
| Precision | 1.000 | — | 1.000 | 1.000 | — | 1.000 |
| Recall | 1.000 | — | 1.000 | 1.000 | — | 1.000 |
| F1 | 1.000 | — | 1.000 | 1.000 | — | 1.000 |
| Expected rows | 48 | — | 48 | 48 | — | 48 |
| Actual rows | 48 | — | 48 | 48 | — | 48 |
| Matched rows | 48 | — | 48 | 48 | — | 48 |
| Source coverage | 1.000 | 0.000 | 1.000 | 1.000 | 0.500 | 1.000 |
| Source requests | 2 | 0 | 2 | 2 | 1 | 2 |
| Sink POSTs | 1 | 0 | 1 | 1 | 0 | 1 |
| Sink rows accepted | 48 | 0 | 48 | 48 | 0 | 48 |
| Generation time (s) | 3.318 | 23.091 | 3.584 | 3.481 | 18.653 | 3.871 |
| Ollama model load (s) | 0.004 | 0.002 | 0.002 | 0.003 | 0.001 | 0.001 |
| Ollama total time (s) | 3.314 | 8.490 | 3.580 | 3.478 | 6.711 | 3.867 |
| Execution time (s) | 26.538 | 5.285 | 21.667 | 27.538 | 109.589 | 18.432 |
| End-to-end time (s) | 30.207 | 42.399 | 26.551 | 31.852 | 178.748 | 22.650 |
| Prompt tokens | 961 | 5790 | 947 | 901 | 8285 | 967 |
| Output tokens | 520 | 3666 | 570 | 536 | 2702 | 612 |
| Output tokens/s | 174.745 | 172.611 | 176.183 | 170.752 | 168.585 | 174.041 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 35.009 | 0.050 | 0.052 | 1.395 | 87.817 | 10.115 |
| Peak container CPU (%) | 144.680 | 0.090 | 0.270 | 20.330 | 381.630 | 20.950 |
| Peak container memory (GB) | 1.821 | 0.257 | 0.023 | 0.024 | 1.447 | 1.733 |
| Resource samples | 9 | 4 | 12 | 15 | 37 | 8 |
| Mean GPU utilization (%) | 6.900 | 51.087 | 5.308 | 4.467 | 15.737 | 7.750 |
| Peak GPU memory (GB) | 26.088 | 26.092 | 26.088 | 26.085 | 26.090 | 26.090 |
| Estimated GPU energy (Wh) | 0.658 | 1.576 | 0.683 | 0.673 | 2.806 | 0.626 |

### qwen3-coder:30b — t1_open_meteo_ingestion

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | passed | passed | passed | passed | passed |
| Attempts | 1 | 1 | 1 | 1 | 1 | 1 |
| Failure category | — | — | — | — | — | — |
| Failure detail (truncated) | — | — | — | — | — | — |
| First-attempt passed | yes | yes | yes | yes | yes | yes |
| Best F1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Precision | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Recall | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| F1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Expected rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Actual rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Matched rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Source coverage | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink POSTs | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink rows accepted | 48 | 48 | 48 | 48 | 48 | 48 |
| Generation time (s) | 3.084 | 5.691 | 2.412 | 2.648 | 2.814 | 2.845 |
| Ollama model load (s) | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 | 0.001 |
| Ollama total time (s) | 3.079 | 5.685 | 2.407 | 2.644 | 2.810 | 2.841 |
| Execution time (s) | 61.181 | 14.346 | 10.715 | 17.481 | 44.719 | 17.628 |
| End-to-end time (s) | 72.720 | 42.414 | 36.084 | 43.573 | 70.803 | 47.563 |
| Prompt tokens | 732 | 1300 | 727 | 680 | 742 | 745 |
| Output tokens | 400 | 732 | 311 | 341 | 341 | 347 |
| Output tokens/s | 139.141 | 136.732 | 142.282 | 139.764 | 131.071 | 132.469 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 71.579 | 47.802 | 1.430 | 3.720 | 92.804 | 162.768 |
| Peak container CPU (%) | 253.210 | 140.350 | 5.480 | 25.750 | 310.220 | 400.310 |
| Peak container memory (GB) | 1.632 | 0.475 | 0.031 | 0.031 | 1.374 | 1.645 |
| Resource samples | 20 | 5 | 4 | 7 | 14 | 6 |
| Mean GPU utilization (%) | 9.062 | 16.031 | 10.500 | 8.941 | 5.732 | 8.176 |
| Peak GPU memory (GB) | 19.746 | 42.089 | 42.083 | 42.078 | 42.080 | 42.083 |
| Estimated GPU energy (Wh) | 0.992 | 1.146 | 0.969 | 1.083 | 1.285 | 1.117 |

### qwen3-coder:30b — t2_usgs_etl

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | passed | passed | passed | failed | passed |
| Attempts | 1 | 1 | 1 | 1 | 3 | 1 |
| Failure category | — | — | — | — | incorrect_output, invalid_artifact | — |
| Failure detail (truncated) | — | — | — | — | — | — |
| First-attempt passed | yes | yes | yes | yes | no | yes |
| Best F1 | 1.000 | 1.000 | 1.000 | 1.000 | 0.000 | 1.000 |
| Precision | 1.000 | 1.000 | 1.000 | 1.000 | 0.000 | 1.000 |
| Recall | 1.000 | 1.000 | 1.000 | 1.000 | 0.000 | 1.000 |
| F1 | 1.000 | 1.000 | 1.000 | 1.000 | 0.000 | 1.000 |
| Expected rows | 49 | 49 | 49 | 49 | 49 | 49 |
| Actual rows | 49 | 49 | 49 | 49 | 49 | 49 |
| Matched rows | 49 | 49 | 49 | 49 | 0 | 49 |
| Source coverage | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink POSTs | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink rows accepted | 49 | 49 | 49 | 49 | 49 | 49 |
| Generation time (s) | 3.237 | 7.133 | 2.895 | 3.163 | 14.928 | 3.177 |
| Ollama model load (s) | 0.001 | 0.002 | 0.002 | 0.002 | 0.001 | 0.003 |
| Ollama total time (s) | 3.233 | 7.124 | 2.891 | 3.158 | 3.925 | 3.171 |
| Execution time (s) | 29.552 | 20.381 | 20.675 | 27.707 | 450.983 | 17.894 |
| End-to-end time (s) | 33.100 | 27.827 | 23.843 | 31.170 | 494.517 | 22.811 |
| Prompt tokens | 736 | 1303 | 731 | 681 | 7906 | 749 |
| Output tokens | 429 | 916 | 363 | 420 | 1760 | 397 |
| Output tokens/s | 140.172 | 134.080 | 133.255 | 140.559 | 133.260 | 132.557 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 32.163 | 20.552 | 0.036 | 0.039 | 26.106 | 29.553 |
| Peak container CPU (%) | 147.340 | 127.550 | 0.230 | 0.200 | 398.890 | 53.300 |
| Peak container memory (GB) | 1.710 | 0.414 | 0.030 | 0.031 | 1.403 | 1.710 |
| Resource samples | 11 | 9 | 11 | 14 | 145 | 7 |
| Mean GPU utilization (%) | 6.818 | 16.000 | 6.545 | 4.933 | 5.329 | 9.000 |
| Peak GPU memory (GB) | 19.746 | 42.089 | 42.083 | 42.078 | 42.087 | 42.083 |
| Estimated GPU energy (Wh) | 0.611 | 0.854 | 0.623 | 0.679 | 4.805 | 0.581 |

### qwen3-coder:30b — t3_open_meteo_join

| Metric | airflow | n8n | dagster | prefect | nifi | kestra |
|---|---|---|---|---|---|---|
| Status | passed | passed | passed | passed | passed | passed |
| Attempts | 1 | 1 | 1 | 1 | 1 | 1 |
| Failure category | — | — | — | — | — | — |
| Failure detail (truncated) | — | — | — | — | — | — |
| First-attempt passed | yes | yes | yes | yes | yes | yes |
| Best F1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Precision | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Recall | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| F1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Expected rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Actual rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Matched rows | 48 | 48 | 48 | 48 | 48 | 48 |
| Source coverage | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| Source requests | 2 | 2 | 2 | 2 | 2 | 2 |
| Sink POSTs | 1 | 1 | 1 | 1 | 1 | 1 |
| Sink rows accepted | 48 | 48 | 48 | 48 | 48 | 48 |
| Generation time (s) | 4.963 | 9.703 | 4.687 | 5.140 | 4.945 | 4.665 |
| Ollama model load (s) | 0.001 | 0.001 | 0.002 | 0.007 | 0.002 | 0.002 |
| Ollama total time (s) | 4.958 | 9.695 | 4.671 | 5.134 | 4.940 | 4.654 |
| Execution time (s) | 26.440 | 20.576 | 20.250 | 27.533 | 19.023 | 17.758 |
| End-to-end time (s) | 33.612 | 32.690 | 25.228 | 32.991 | 25.969 | 23.865 |
| Prompt tokens | 925 | 1822 | 923 | 884 | 947 | 945 |
| Output tokens | 669 | 1253 | 626 | 703 | 629 | 604 |
| Output tokens/s | 140.433 | 134.303 | 139.745 | 142.208 | 132.417 | 135.414 |
| Cost (USD) | — | — | — | — | — | — |
| Mean container CPU (%) | 33.833 | 16.153 | 0.034 | 0.117 | 17.153 | 13.857 |
| Peak container CPU (%) | 151.050 | 120.890 | 0.250 | 1.440 | 119.760 | 36.140 |
| Peak container memory (GB) | 1.795 | 0.407 | 0.023 | 0.024 | 1.430 | 1.722 |
| Resource samples | 11 | 10 | 12 | 15 | 9 | 8 |
| Mean GPU utilization (%) | 6.818 | 20.000 | 12.083 | 9.438 | 7.778 | 9.375 |
| Peak GPU memory (GB) | 19.750 | 42.089 | 42.087 | 42.083 | 42.087 | 42.087 |
| Estimated GPU energy (Wh) | 0.725 | 1.084 | 0.724 | 0.802 | 0.668 | 0.625 |
