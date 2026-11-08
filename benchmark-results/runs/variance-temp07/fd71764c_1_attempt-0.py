from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def usgs_etl_task():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=fd71764c_1&attempt=0"
    sink_url = "http://recorder:8090/sink/fd71764c_1?attempt=0"

    # Fetch the source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in data['features']:
        prop = feature['properties']
        geom = feature['geometry']
        
        record = {
            'id': feature['id'],
            'magnitude': prop['mag'],
            'place': prop['place'],
            'time_ms': prop['time'],
            'longitude': geom['coordinates'][0],
            'latitude': geom['coordinates'][1],
            'depth_km': geom['coordinates'][2],
            'magnitude_band': (
                'minor' if prop['mag'] < 4.0 else
                'moderate' if 4.0 <= prop['mag'] < 5.0 else
                'major'
            )
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

with DAG(dag_id='bench_fd71764c_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t2_usgs_etl',
        python_callable=usgs_etl_task
    )
