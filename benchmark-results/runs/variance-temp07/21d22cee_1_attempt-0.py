from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def usgs_etl_task():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=21d22cee_1&attempt=0"
    sink_url = "http://recorder:8090/sink/21d22cee_1?attempt=0"

    # Fetch the source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in geojson_data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        coordinates = geometry['coordinates']

        # Derive magnitude_band
        mag = properties['mag']
        if mag < 4.0:
            magnitude_band = 'minor'
        elif mag < 5.0:
            magnitude_band = 'moderate'
        else:
            magnitude_band = 'major'

        record = {
            "id": feature['id'],
            "magnitude": mag,
            "place": properties['place'],
            "time_ms": properties['time'],
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth_km": coordinates[2],
            "magnitude_band": magnitude_band
        }

        records.append(record)

    # Send to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_21d22cee_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t2_usgs_etl',
        python_callable=usgs_etl_task
    )
