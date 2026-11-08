from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=8902842b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/8902842b_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    pm2_5_values = data['hourly']['pm2_5']
    times = data['hourly']['time']

    records = []

    for i in range(2, len(pm2_5_values)):
        current_pm2_5 = pm2_5_values[i]
        previous_two = pm2_5_values[i-2:i+1]
        peak_3h = max(previous_two)

        record = {
            "time": times[i],
            "pm2_5": current_pm2_5,
            "pm2_5_peak_3h": peak_3h
        }

        records.append(record)

    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_8902842b_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    task = PythonOperator(
        task_id='process_particulate_data',
        python_callable=process_particulate_data
    )
