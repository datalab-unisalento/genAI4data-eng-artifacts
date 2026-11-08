from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def etl_task():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=20e82670_1&attempt=0"
    sink_url = "http://recorder:8090/sink/20e82670_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in data['features']:
        props = feature['properties']
        geom = feature['geometry']
        
        magnitude = props['mag']
        place = props['place']
        time_ms = props['time']
        longitude, latitude, depth_km = geom['coordinates']

        # Derive magnitude_band
        if magnitude < 4.0:
            magnitude_band = 'minor'
        elif magnitude < 5.0:
            magnitude_band = 'moderate'
        else:
            magnitude_band = 'major'

        record = {
            "id": feature['id'],
            "magnitude": magnitude,
            "place": place,
            "time_ms": time_ms,
            "longitude": longitude,
            "latitude": latitude,
            "depth_km": depth_km,
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

with DAG(dag_id='bench_20e82670_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t2_usgs_etl',
        python_callable=etl_task
    )
