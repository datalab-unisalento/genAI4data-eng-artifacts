from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def ingest_open_meteo_particulate():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=7206427b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/7206427b_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly_data = data['hourly']
    time_list = hourly_data['time']
    pm10_list = hourly_data['pm10']
    pm2_5_list = hourly_data['pm2_5']

    records = []
    for i in range(len(time_list)):
        record = {
            "time": time_list[i],
            "pm10": pm10_list[i],
            "pm2_5": pm2_5_list[i]
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

with DAG(dag_id='bench_7206427b_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t1_open_meteo_ingestion',
        python_callable=ingest_open_meteo_particulate
    )
