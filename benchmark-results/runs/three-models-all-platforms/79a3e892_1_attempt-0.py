from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def ingest_open_meteo():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=79a3e892_1&attempt=0"
    sink_url = "http://recorder:8090/sink/79a3e892_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    times = data['hourly']['time']
    pm10_values = data['hourly']['pm10']
    pm25_values = data['hourly']['pm2_5']

    records = []
    for i in range(len(times)):
        record = {
            "time": times[i],
            "pm10": pm10_values[i],
            "pm2_5": pm25_values[i]
        }
        records.append(record)

    body = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(sink_url, data=body, headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req) as response:
        pass

with DAG(dag_id='bench_79a3e892_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t1_open_meteo_ingestion = PythonOperator(
        task_id='t1_open_meteo_ingestion',
        python_callable=ingest_open_meteo
    )
