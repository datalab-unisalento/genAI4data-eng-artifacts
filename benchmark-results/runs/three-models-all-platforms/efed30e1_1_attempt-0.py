from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def etl_task():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=efed30e1_1&attempt=0"
    sink_url = "http://recorder:8090/sink/efed30e1_1?attempt=0"

    resp = urllib.request.urlopen(source_url)
    data = json.loads(resp.read().decode('utf-8'))

    records = []
    for feature in data['features']:
        props = feature['properties']
        geom = feature['geometry']
        coords = geom['coordinates']
        magnitude = props['mag']
        if magnitude < 4.0:
            mag_band = 'minor'
        elif magnitude < 5.0:
            mag_band = 'moderate'
        else:
            mag_band = 'major'
        record = {
            'id': feature['id'],
            'magnitude': magnitude,
            'place': props['place'],
            'time_ms': props['time'],
            'longitude': coords[0],
            'latitude': coords[1],
            'depth_km': coords[2],
            'magnitude_band': mag_band
        }
        records.append(record)

    body = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(sink_url, data=body, headers={'Content-Type': 'application/json'}, method='POST')
    urllib.request.urlopen(req)

with DAG(dag_id='bench_efed30e1_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id='t2_usgs_etl', python_callable=etl_task)
