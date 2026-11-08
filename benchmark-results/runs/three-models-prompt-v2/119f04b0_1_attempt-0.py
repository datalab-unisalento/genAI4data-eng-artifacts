from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def ingest_open_meteo_particulate():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=119f04b0_1&attempt=0"
    sink_url = "http://recorder:8090/sink/119f04b0_1?attempt=0"

    # Fetch data from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract hourly data
    hourly_data = data['hourly']
    times = hourly_data['time']
    pm10_values = hourly_data['pm10']
    pm2_5_values = hourly_data['pm2_5']

    # Convert to records
    records = []
    for i in range(len(times)):
        record = {
            'time': times[i],
            'pm10': pm10_values[i],
            'pm2_5': pm2_5_values[i]
        }
        records.append(record)

    # Send data to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_119f04b0_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_open_meteo_particulate',
        python_callable=ingest_open_meteo_particulate
    )
