from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


import json
import urllib.request

SOURCES = {"usgs": "http://recorder:8090/source/usgs/earthquakes?run_id=ref-t2&attempt=0"}
SINK_URL = "http://recorder:8090/sink/ref-t2?attempt=0"


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read())


def transform(s):
    out = []
    for f in s["usgs"]["features"]:
        mag = f["properties"]["mag"]
        lon, lat, depth = f["geometry"]["coordinates"]
        out.append({
            "id": f["id"], "magnitude": mag, "place": f["properties"]["place"],
            "time_ms": f["properties"]["time"], "longitude": lon, "latitude": lat, "depth_km": depth,
            "magnitude_band": "minor" if mag < 4.0 else ("moderate" if mag < 5.0 else "major"),
        })
    return out


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
    dag_id="bench_ref_t2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="ingest", python_callable=ingest)
