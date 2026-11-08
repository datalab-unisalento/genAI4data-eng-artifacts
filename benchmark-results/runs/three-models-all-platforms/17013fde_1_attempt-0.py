from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

SOURCE_URL = "http://recorder:8090/source/open-meteo/particulate?run_id=17013fde_1&attempt=0"
SINK_URL = "http://recorder:8090/sink/17013fde_1?attempt=0"


def process_and_post():
    resp = urllib.request.urlopen(SOURCE_URL)
    raw = resp.read().decode("utf-8")
    data = json.loads(raw)

    times = data["hourly"]["time"]
    pm25_values = data["hourly"]["pm2_5"]

    records = []
    for i in range(2, len(times)):
        window = pm25_values[i - 2 : i + 1]
        peak = max(window)
        record = {
            "time": times[i],
            "pm2_5": pm25_values[i],
            "pm2_5_peak_3h": peak,
        }
        records.append(record)

    payload = json.dumps(records).encode("utf-8")
    req = urllib.request.Request(
        SINK_URL, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    urllib.request.urlopen(req)


with DAG(dag_id="bench_17013fde_1", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id="t5_trailing_three_hour_peak", python_callable=process_and_post)
