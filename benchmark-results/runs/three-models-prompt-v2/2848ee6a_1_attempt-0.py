from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def task_callable():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=2848ee6a_1&attempt=0"
    sink_url = "http://recorder:8090/sink/2848ee6a_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode('utf-8'))

    # Initialize summary records by band
    bands = {
        "minor": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "moderate": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "major": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')}
    }

    # Process each feature
    for feature in geojson_data.get("features", []):
        mag = feature["properties"]["mag"]
        depth = feature["geometry"]["coordinates"][2]

        # Determine band and update summary
        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        record = bands[band]
        record["event_count"] += 1
        record["max_magnitude"] = max(record["max_magnitude"], mag)
        record["max_depth_km"] = max(record["max_depth_km"], depth)

    # Filter out empty bands and format output
    records = []
    for band, data in bands.items():
        if data["event_count"] > 0:
            records.append({
                "magnitude_band": band,
                "event_count": data["event_count"],
                "max_magnitude": data["max_magnitude"],
                "max_depth_km": data["max_depth_km"]
            })

    # Send to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_2848ee6a_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t6_earthquake_band_summary',
        python_callable=task_callable
    )
