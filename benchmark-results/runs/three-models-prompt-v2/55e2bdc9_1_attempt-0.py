from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

MAG_MINOR_THRESHOLD = 4.0
MODERATE_THRESHOLD = 5.0


def process_earthquakes():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=55e2bdc9_1&attempt=0"
    sink_url = "http://recorder:8090/sink/55e2bdc9_1?attempt=0"

    req = urllib.request.urlopen(source_url)
    data = json.loads(req.read().decode('utf-8'))

    bands = {}
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        mag = props.get("mag")
        if mag is None:
            continue
        depth = feature.get("geometry", {}).get("coordinates", [0, 0, 0])[2]

        if mag < MAG_MINOR_THRESHOLD:
            band = "minor"
        elif mag < MODERATE_THRESHOLD:
            band = "moderate"
        else:
            band = "major"

        if band not in bands:
            bands[band] = []
        bands[band].append({
            "mag": mag,
            "depth_km": depth
        })

    records = []
    for band_name, events in bands.items():
        count = len(events)
        max_mag = max(e["mag"] for e in events)
        max_depth = max(e["depth_km"] for e in events)
        records.append({
            "magnitude_band": band_name,
            "event_count": count,
            "max_magnitude": max_mag,
            "max_depth_km": max_depth
        })

    body = json.dumps(records).encode('utf-8')
    req_sink = urllib.request.Request(
        sink_url,
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    urllib.request.urlopen(req_sink)


with DAG(dag_id='bench_55e2bdc9_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id="t6_earthquake_band_summary",
        python_callable=process_earthquakes
    )
