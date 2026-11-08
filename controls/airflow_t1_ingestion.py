import json
import urllib.request
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def ingest_particulate_records():
    source_url = (
        "http://recorder:8090/source/open-meteo/particulate"
        "?run_id=control-t1&attempt=0"
    )
    sink_url = "http://recorder:8090/sink/control-t1?attempt=0"

    with urllib.request.urlopen(source_url, timeout=30) as response:
        payload = json.loads(response.read())

    hourly = payload["hourly"]
    times = hourly["time"]
    pm10_values = hourly["pm10"]
    pm2_5_values = hourly["pm2_5"]
    if not (len(times) == len(pm10_values) == len(pm2_5_values)):
        raise ValueError("Open-Meteo hourly arrays have inconsistent lengths")

    records = [
        {"time": time_value, "pm10": pm10, "pm2_5": pm2_5}
        for time_value, pm10, pm2_5 in zip(times, pm10_values, pm2_5_values)
    ]
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 202:
            raise RuntimeError(f"Sink rejected records with HTTP {response.status}")


with DAG(
    dag_id="bench_control_t1",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="ingest_open_meteo_particulates",
        python_callable=ingest_particulate_records,
    )
