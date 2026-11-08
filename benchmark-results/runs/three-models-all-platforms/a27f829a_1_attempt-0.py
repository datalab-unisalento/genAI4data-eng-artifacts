from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

USGS_EARTHQUAKE_URL = "http://recorder:8090/source/usgs/earthquakes?run_id=a27f829a_1&attempt=0"
SINK_URL = "http://recorder:8090/sink/a27f829a_1?attempt=0"

def classify_and_post():
    response = urllib.request.urlopen(USGS_EARTHQUAKE_URL)
    data = json.loads(response.read().decode('utf-8'))
    
    bands = {}
    for feature in data['features']:
        mag = feature['properties']['mag']
        depth = feature['geometry']['coordinates'][2]
        
        if mag < 4.0:
            band = 'minor'
        elif mag < 5.0:
            band = 'moderate'
        else:
            band = 'major'
            
        if band not in bands:
            bands[band] = []
        bands[band].append({
            'mag': mag,
            'depth_km': depth
        })
    
    records = []
    for band, events in bands.items():
        count = len(events)
        max_mag = max(e['mag'] for e in events)
        max_depth = max(e['depth_km'] for e in events)
        records.append({
            'magnitude_band': band,
            'event_count': count,
            'max_magnitude': max_mag,
            'max_depth_km': max_depth
        })
    
    payload = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(SINK_URL, data=payload, headers={'Content-Type': 'application/json'}, method='POST')
    urllib.request.urlopen(req)

with DAG(dag_id='bench_a27f829a_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id='t6_earthquake_band_summary', python_callable=classify_and_post)
