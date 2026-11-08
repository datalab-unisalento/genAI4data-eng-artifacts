from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_earthquakes():
    # Fetch source data
    with urllib.request.urlopen("http://recorder:8090/source/usgs/earthquakes?run_id=9da70624_1&attempt=0") as response:
        data = json.load(response)
    
    records = []
    for feature in data['features']:
        # Extract coordinates
        coords = feature['geometry']['coordinates']
        
        # Create record with required fields
        record = {
            'id': feature['id'],
            'magnitude': feature['properties']['mag'],
            'place': feature['properties']['place'],
            'time_ms': feature['properties']['time'],
            'longitude': coords[0],
            'latitude': coords[1], 
            'depth_km': coords[2]
        }
        
        # Determine magnitude band
        mag = record['magnitude']
        if mag < 4.0:
            record['magnitude_band'] = 'minor'
        elif 4.0 <= mag < 5.0:
            record['magnitude_band'] = 'moderate'
        else:
            record['magnitude_band'] = 'major'
            
        records.append(record)
    
    # Post to sink
    request = urllib.request.Request(
        "http://recorder:8090/sink/9da70624_1?attempt=0",
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_9da70624_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t2_usgs_etl',
        python_callable=process_earthquakes
    )
