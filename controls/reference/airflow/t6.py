from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


import json
import urllib.request

SOURCES = {"usgs": "http://recorder:8090/source/usgs/earthquakes?run_id=ref-t6&attempt=0"}
SINK_URL = "http://recorder:8090/sink/ref-t6?attempt=0"


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read())


def transform(s):
    bands = {}
    for f in s["usgs"]["features"]:
        mag = f["properties"]["mag"]
        depth = f["geometry"]["coordinates"][2]
        name = "minor" if mag < 4.0 else ("moderate" if mag < 5.0 else "major")
        b = bands.setdefault(name, {"event_count": 0, "max_magnitude": None, "max_depth_km": None})
        b["event_count"] += 1
        b["max_magnitude"] = mag if b["max_magnitude"] is None else max(b["max_magnitude"], mag)
        b["max_depth_km"] = depth if b["max_depth_km"] is None else max(b["max_depth_km"], depth)
    return [{"magnitude_band": n, **bands[n]} for n in ["minor", "moderate", "major"] if n in bands]


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
    dag_id="bench_ref_t6",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="ingest", python_callable=ingest)
