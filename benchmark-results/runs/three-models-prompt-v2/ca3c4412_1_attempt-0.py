from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=ca3c4412_1&attempt=0"
    sink_url = "http://recorder:8090/sink/ca3c4412_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly = data['hourly']
    times = hourly['time']
    pm2_5_values = hourly['pm2_5']

    records = []

    for i in range(2, len(times)):
        # Get the current and previous two hours' pm2.5 values
        window_pm2_5 = pm2_5_values[i-2:i+1]
        peak = max(window_pm2_5)

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

with DAG(dag_id='bench_ca3c4412_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='t5_trailing_three_hour_peak',
        python_callable=process_particulate_data
    )
