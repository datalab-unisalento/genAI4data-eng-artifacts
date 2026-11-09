# Dev (T1-T3) vs held-out (T4-T6) pass rates

| Platform | Dev eventual | Held-out eventual | Dev first-attempt | Held-out first-attempt |
|---|---|---|---|---|
| airflow | 9/9 (1.00) | 9/9 (1.00) | 9/9 (1.00) | 8/9 (0.89) |
| n8n | 7/9 (0.78) | 8/9 (0.89) | 7/9 (0.78) | 5/9 (0.56) |
| dagster | 8/9 (0.89) | 9/9 (1.00) | 8/9 (0.89) | 9/9 (1.00) |
| prefect | 9/9 (1.00) | 9/9 (1.00) | 9/9 (1.00) | 9/9 (1.00) |
| nifi | 4/9 (0.44) | 4/9 (0.44) | 3/9 (0.33) | 3/9 (0.33) |
| kestra | 9/9 (1.00) | 9/9 (1.00) | 9/9 (1.00) | 9/9 (1.00) |
| all | 46/54 (0.85) | 48/54 (0.89) | 45/54 (0.83) | 43/54 (0.80) |

NiFi and Kestra prompts were revised after T1-T3 results, so their held-out columns are only partly held out.
