from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_earthquakes():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=380858ae_1&attempt=0"
    sink_url = "http://recorder:8090/sink/380858ae_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in data['features']:
        record = {
            'id': feature['id'],
            'magnitude': feature['properties']['mag'],
            'place': feature['properties']['place'],
            'time_ms': feature['properties']['time'],
            'longitude': feature['geometry']['coordinates'][0],
            'latitude': feature['geometry']['coordinates'][1],
            'depth_km': feature['geometry']['coordinates'][2]
        }
        
        if record['magnitude'] < 4.0:
            record['magnitude_band'] = 'minor'
        elif 4.0 <= record['magnitude'] < 5.0:
            record['magnitude_band'] = 'moderate'
        else:
            record['magnitude_band'] = 'major'

        records.append(record)

    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_380858ae_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t2_usgs_etl = PythonOperator(
        task_id='t2_usgs_etl',
        python_callable=process_earthquakes
    )
