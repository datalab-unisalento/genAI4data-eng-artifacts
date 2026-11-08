from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

SOURCE_URL = "http://recorder:8090/source/open-meteo/particulate?run_id=12e2b2b9_1&attempt=0"
SINK_URL = "http://recorder:8090/sink/12e2b2b9_1?attempt=0"


def fetch_and_post():
    resp = urllib.request.urlopen(SOURCE_URL)
    data = json.loads(resp.read().decode('utf-8'))

    hourly_times = data['hourly']['time']
    pm25 = data['hourly']['pm2_5']
    pm10 = data['hourly']['pm10']

    date_groups = {}
    for i, t in enumerate(hourly_times):
        date_key = t[:10]
        if date_key not in date_groups:
            date_groups[date_key] = []
        date_groups[date_key].append(i)

    records = []
    for date_key, indices in date_groups.items():
        pm25_vals = [pm25[idx] for idx in indices]
        pm10_vals = [pm10[idx] for idx in indices]
        record = {
            "date": date_key,
            "hours": len(indices),
            "pm2_5_max": max(pm25_vals),
            "pm10_max": max(pm10_vals)
        }
        records.append(record)

    body = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(
        SINK_URL,
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    urllib.request.urlopen(req)


with DAG(dag_id='bench_12e2b2b9_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id='t4_daily_particulate_summary', python_callable=fetch_and_post)
