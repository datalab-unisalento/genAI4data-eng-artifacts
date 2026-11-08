from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=fbe4ea72_1&attempt=0"
    sink_url = "http://recorder:8090/sink/fbe4ea72_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    times = data['hourly']['time']
    pm2_5_values = data['hourly']['pm2_5']

    records = []
    
    for i in range(2, len(times)):
        current_time = times[i]
        current_pm2_5 = pm2_5_values[i]
        
        # Get max of current and previous 2 hours
        window = pm2_5_values[i-2:i+1]
        peak_3h = max(window)
        
        record = {
            "time": current_time,
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

with DAG(dag_id='bench_fbe4ea72_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id="t5_trailing_three_hour_peak",
        python_callable=process_particulate_data
    )
