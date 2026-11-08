from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=5b74e4be_1&attempt=0"
    sink_url = "http://recorder:8090/sink/5b74e4be_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly_data = data['hourly']
    times = hourly_data['time']
    pm25_values = hourly_data['pm2_5']
    pm10_values = hourly_data['pm10']

    # Group by date and calculate maxima
    summaries = {}
    for time, pm25, pm10 in zip(times, pm25_values, pm10_values):
        date_key = time[:10]
        if date_key not in summaries:
            summaries[date_key] = {
                'date': date_key,
                'hours': 0,
                'pm2_5_max': float('-inf'),
                'pm10_max': float('-inf')
            }
        
        summary = summaries[date_key]
        summary['hours'] += 1
        summary['pm2_5_max'] = max(summary['pm2_5_max'], pm25)
        summary['pm10_max'] = max(summary['pm10_max'], pm10)

    # Convert to list of records
    records = list(summaries.values())

    # Post to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_5b74e4be_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id="t4_daily_particulate_summary",
        python_callable=process_particulate_data
    )
