# Paired pass-rate differences versus NiFi

Percentile bootstrap intervals resample the 18 model-task blocks with replacement (200,000 resamples).

| Platform minus NiFi | Paired difference | 95% bootstrap CI |
|---|---:|---:|
| airflow | 55.6% | [33.3%, 77.8%] |
| dagster | 50.0% | [27.8%, 72.2%] |
| prefect | 55.6% | [33.3%, 77.8%] |
| kestra | 55.6% | [33.3%, 77.8%] |
