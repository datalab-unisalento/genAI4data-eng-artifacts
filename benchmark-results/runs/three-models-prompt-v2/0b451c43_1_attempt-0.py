from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_earthquakes():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=0b451c43_1&attempt=0"
    sink_url = "http://recorder:8090/sink/0b451c43_1?attempt=0"

    # Fetch earthquake data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Initialize band statistics
    bands = {
        'minor': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')},
        'moderate': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')},
        'major': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')}
    }

    # Process each earthquake
    for feature in data['features']:
        mag = feature['properties']['mag']
        depth = feature['geometry']['coordinates'][2]
        
        if mag < 4.0:
            band = 'minor'
        elif mag >= 4.0 and mag <= 5.0:
            band = 'moderate'
        else:
            band = 'major'

        bands[band]['count'] += 1
        bands[band]['max_mag'] = max(bands[band]['max_mag'], mag)
        bands[band]['max_depth'] = max(bands[band]['max_depth'], depth)

    # Prepare output records
    records = []
    for band, stats in bands.items():
        if stats['count'] > 0:
            record = {
                'magnitude_band': band,
                'event_count': stats['count'],
                'max_magnitude': stats['max_mag'],
                'max_depth_km': stats['max_depth']
            }
            records.append(record)

    # Post results to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_0b451c43_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t6_earthquake_band_summary = PythonOperator(
        task_id='t6_earthquake_band_summary',
        python_callable=process_earthquakes
    )
