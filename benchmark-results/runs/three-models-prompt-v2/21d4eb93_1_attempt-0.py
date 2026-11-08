from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def process_particulate_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=21d4eb93_1&attempt=0"
    sink_url = "http://recorder:8090/sink/21d4eb93_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly_data = list(zip(data['hourly']['time'], 
                          data['hourly']['pm10'],
                          data['hourly']['pm2_5']))
    
    daily_summary = {}
    for time_str, pm10_val, pm25_val in hourly_data:
        date_key = time_str[:10]
        
        if date_key not in daily_summary:
            daily_summary[date_key] = {
                'hours': 0,
                'pm10_max': float('-inf'),
                'pm2_5_max': float('-inf')
            }
            
        daily_summary[date_key]['hours'] += 1
        daily_summary[date_key]['pm10_max'] = max(daily_summary[date_key]['pm10_max'], pm10_val)
        daily_summary[date_key]['pm2_5_max'] = max(daily_summary[date_key]['pm2_5_max'], pm25_val)

    records = []
    for date, summary in daily_summary.items():
        record = {
            'date': date,
            'hours': summary['hours'],
            'pm10_max': summary['pm10_max'],
            'pm2_5_max': summary['pm2_5_max']
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

with DAG(dag_id='bench_21d4eb93_1', 
         start_date=datetime(2024, 1, 1), 
         schedule=None, 
         catchup=False) as dag:

    daily_summary_task = PythonOperator(
        task_id='t4_daily_particulate_summary',
        python_callable=process_particulate_data
    )
