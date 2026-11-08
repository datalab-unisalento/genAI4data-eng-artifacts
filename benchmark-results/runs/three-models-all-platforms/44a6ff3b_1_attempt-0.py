from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=44a6ff3b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/44a6ff3b_1?attempt=0"

    # Fetch data from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly_pm2_5 = data['hourly']['pm2_5']
    hourly_time = data['hourly']['time']

    records = []

    for i in range(2, len(hourly_pm2_5)):
        # Get pm2.5 values for current and previous two hours
        window_values = hourly_pm2_5[i-2:i+1]
        peak_value = max(window_values)

        record = {
            "time": hourly_time[i],
            "pm2_5": hourly_pm2_5[i],
            "pm2_5_peak_3h": peak_value
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
        pass

with DAG(dag_id='bench_44a6ff3b_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='t5_trailing_three_hour_peak',
        python_callable=process_particulate_data
    )
