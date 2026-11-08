from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


import json
import urllib.request

SOURCES = {"particulate": "http://recorder:8090/source/open-meteo/particulate?run_id=ref-t5&attempt=0"}
SINK_URL = "http://recorder:8090/sink/ref-t5?attempt=0"


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read())


def transform(s):
    h = s["particulate"]["hourly"]
    v = h["pm2_5"]
    return [{"time": h["time"][i], "pm2_5": v[i], "pm2_5_peak_3h": max(v[i - 2:i + 1])}
            for i in range(2, len(v))]


def deliver(records):
    request = urllib.request.Request(
        SINK_URL,
        data=json.dumps(records).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()


def ingest():
    data = {name: fetch(url) for name, url in SOURCES.items()}
    deliver(transform(data))


with DAG(
    dag_id="bench_ref_t5",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="ingest", python_callable=ingest)
