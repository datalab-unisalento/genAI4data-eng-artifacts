from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=61eb3852_1&attempt=0"
    sink_url = "http://recorder:8090/sink/61eb3852_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract pm2.5 values and times
    pm2_5_values = data['hourly']['pm2_5']
    times = data['hourly']['time']

    records = []
    for i in range(2, len(pm2_5_values)):
        # Get the current hour and two previous hours
        window = pm2_5_values[i-2:i+1]
        peak = max(window)
        
        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": peak
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

with DAG(dag_id='bench_61eb3852_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t5_trailing_three_hour_peak',
        python_callable=process_particulate_data
    )
