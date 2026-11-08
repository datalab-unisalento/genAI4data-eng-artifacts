from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def process_open_meteo_data():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=1d4e513e_1&attempt=0"
    sink_url = "http://recorder:8090/sink/1d4e513e_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for i in range(len(data['hourly']['time'])):
        record = {
            'time': data['hourly']['time'][i],
            'pm10': data['hourly']['pm10'][i],
            'pm2_5': data['hourly']['pm2_5'][i]
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

with DAG(dag_id='bench_1d4e513e_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(
        task_id='t1_open_meteo_ingestion',
        python_callable=process_open_meteo_data
    )
