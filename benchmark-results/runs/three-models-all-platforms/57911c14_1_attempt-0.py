from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def process_data():
    # Fetch particulate data
    with urllib.request.urlopen("http://recorder:8090/source/open-meteo/particulate?run_id=57911c14_1&attempt=0") as response:
        particulate_data = json.loads(response.read().decode('utf-8'))
    
    # Fetch gases data
    with urllib.request.urlopen("http://recorder:8090/source/open-meteo/gases?run_id=57911c14_1&attempt=0") as response:
        gases_data = json.loads(response.read().decode('utf-8'))
    
    # Convert hourly data to records
    particulate_records = [
        {
            'time': time,
            'pm10': pm10,
            'pm2_5': pm2_5
        }
        for time, pm10, pm2_5 in zip(
            particulate_data['hourly']['time'],
            particulate_data['hourly']['pm10'],
            particulate_data['hourly']['pm2_5']
        )
    ]
    
    gases_records = [
        {
            'time': time,
            'nitrogen_dioxide': no2,
            'ozone': ozone
        }
        for time, no2, ozone in zip(
            gases_data['hourly']['time'],
            gases_data['hourly']['nitrogen_dioxide'],
            gases_data['hourly']['ozone']
        )
    ]
    
    # Create dictionaries for easier joining
    particulate_dict = {r['time']: r for r in particulate_records}
    gases_dict = {r['time']: r for r in gases_records}
    
    # Inner join on time
    joined_records = []
    for time, particulate_record in particulate_dict.items():
        if time in gases_dict:
            joined_record = {
                'time': time,
                'pm10': particulate_record['pm10'],
                'pm2_5': particulate_record['pm2_5'],
                'nitrogen_dioxide': gases_dict[time]['nitrogen_dioxide'],
                'ozone': gases_dict[time]['ozone']
            }
            joined_records.append(joined_record)
    
    # Post to sink
    request = urllib.request.Request(
        "http://recorder:8090/sink/57911c14_1?attempt=0",
        data=json.dumps({"records": joined_records}).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

with DAG(dag_id='bench_57911c14_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t3_open_meteo_join = PythonOperator(
        task_id='t3_open_meteo_join',
        python_callable=process_data
    )
