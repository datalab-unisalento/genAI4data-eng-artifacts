from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json
from collections import defaultdict

def process_particulate_summary():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=3dd03f50_1&attempt=0"
    sink_url = "http://recorder:8090/sink/3dd03f50_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly_records = list(zip(data['hourly']['time'], data['hourly']['pm2_5'], data['hourly']['pm10']))
    
    daily_summary = defaultdict(lambda: {
        'date': '',
        'hours': 0,
        'pm2_5_max': float('-inf'),
        'pm10_max': float('-inf')
    })

    for time_str, pm2_5_val, pm10_val in hourly_records:
        date_key = time_str[:10]  # First 10 characters as date
        record = daily_summary[date_key]
        record['date'] = date_key
        record['hours'] += 1
        record['pm2_5_max'] = max(record['pm2_5_max'], pm2_5_val)
        record['pm10_max'] = max(record['pm10_max'], pm10_val)

    records = list(daily_summary.values())
    
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_3dd03f50_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t4_daily_particulate_summary',
        python_callable=process_particulate_summary
    )
