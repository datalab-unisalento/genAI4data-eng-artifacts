from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

SOURCE_URL = "http://recorder:8090/source/open-meteo/particulate?run_id=4d0c5a76_1&attempt=0"
SINK_URL = "http://recorder:8090/sink/4d0c5a76_1?attempt=0"


def fetch_and_post():
    resp = urllib.request.urlopen(SOURCE_URL)
    data = json.loads(resp.read().decode('utf-8'))

    times = data['hourly']['time']
    pm25 = data['hourly']['pm2_5']
    pm10 = data['hourly']['pm10']

    groups = {}
    for i, t in enumerate(times):
        date = t[:10]
        if date not in groups:
            groups[date] = []
        groups[date].append(i)

    records = []
    for date, indices in sorted(groups.items()):
        pm25_vals = [pm25[idx] for idx in indices]
        pm10_vals = [pm10[idx] for idx in indices]
        rec = {
            "date": date,
            "hours": len(indices),
            "pm2_5_max": max(pm25_vals),
            "pm10_max": max(pm10_vals)
        }
        records.append(rec)

    body = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(SINK_URL, data=body, headers={'Content-Type': 'application/json'}, method='POST')
    urllib.request.urlopen(req)


with DAG(dag_id='bench_4d0c5a76_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id='t4_daily_particulate_summary', python_callable=fetch_and_post)
