from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=a3691a7d_1&attempt=0"
    sink_url = "http://recorder:8090/sink/a3691a7d_1?attempt=0"

    # Fetch data from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract hourly pm2.5 values and times
    times = data['hourly']['time']
    pm2_5_values = data['hourly']['pm2_5']

    records = []

    # Process each hour starting from the third one (index 2)
    for i in range(2, len(times)):
        # Get the current and previous two hours' pm2.5 values
        window_pm2_5 = pm2_5_values[i-2:i+1]
        peak_3h = max(window_pm2_5)

        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": peak_3h
        }

        records.append(record)

    # Send results to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass  # Response is not used

with DAG(dag_id='bench_a3691a7d_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='process_particulate_data',
        python_callable=process_particulate_data
    )
