from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def extract_transform_load():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=6f963f0e_1&attempt=0"
    sink_url = "http://recorder:8090/sink/6f963f0e_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in geojson_data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        coordinates = geometry['coordinates']

        magnitude_band = 'minor' if properties['mag'] < 4.0 else ('moderate' if 4.0 <= properties['mag'] < 5.0 else 'major')

        record = {
            "id": feature['id'],
            "magnitude": properties['mag'],
            "place": properties['place'],
            "time_ms": properties['time'],
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth_km": coordinates[2],
            "magnitude_band": magnitude_band
        }
        records.append(record)

    # Post to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_6f963f0e_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='t2_usgs_etl',
        python_callable=extract_transform_load
    )
