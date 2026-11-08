from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def process_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=9043e845_1&attempt=0"
    sink_url = "http://recorder:8090/sink/9043e845_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    times = data['hourly']['time']
    pm2_5_values = data['hourly']['pm2_5']

    records = []
    
    for i in range(2, len(times)):
        current_time = times[i]
        max_pm2_5 = max(pm2_5_values[i-2:i+1])
        
        record = {
            "time": current_time,
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": max_pm2_5
        }
        records.append(record)

    request = urllib.request.Request(
        sink_url, 
        data=json.dumps({"records": records}).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_9043e845_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id="t5_trailing_three_hour_peak",
        python_callable=process_data
    )
