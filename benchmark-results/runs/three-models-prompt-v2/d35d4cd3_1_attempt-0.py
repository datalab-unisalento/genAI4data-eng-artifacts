from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def t3_open_meteo_join():
    # Fetch particulate data
    particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=d35d4cd3_1&attempt=0"
    with urllib.request.urlopen(particulate_url) as response:
        particulate_data = json.loads(response.read().decode('utf-8'))

    # Fetch gas data
    gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=d35d4cd3_1&attempt=0"
    with urllib.request.urlopen(gases_url) as response:
        gases_data = json.loads(response.read().decode('utf-8'))

    # Convert hourly data to records
    particulate_records = []
    for i, time in enumerate(particulate_data['hourly']['time']):
        record = {
            'time': time,
            'pm10': particulate_data['hourly']['pm10'][i],
            'pm2_5': particulate_data['hourly']['pm2_5'][i]
        }
        particulate_records.append(record)

    gases_records = []
    for i, time in enumerate(gases_data['hourly']['time']):
        record = {
            'time': time,
            'nitrogen_dioxide': gases_data['hourly']['nitrogen_dioxide'][i],
            'ozone': gases_data['hourly']['ozone'][i]
        }
        gases_records.append(record)

    # Create a mapping from time to records for efficient lookup
    gases_map = {record['time']: record for record in gases_records}

    # Inner join on time
    joined_records = []
    for p_record in particulate_records:
        time = p_record['time']
        if time in gases_map:
            g_record = gases_map[time]
            combined_record = {
                'time': time,
                'pm10': p_record['pm10'],
                'pm2_5': p_record['pm2_5'],
                'nitrogen_dioxide': g_record['nitrogen_dioxide'],
                'ozone': g_record['ozone']
            }
            joined_records.append(combined_record)

    # Post to sink
    sink_url = "http://recorder:8090/sink/d35d4cd3_1?attempt=0"
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(joined_records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass  # Response is not used

with DAG(dag_id='bench_d35d4cd3_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t3_open_meteo_join',
        python_callable=t3_open_meteo_join
    )
