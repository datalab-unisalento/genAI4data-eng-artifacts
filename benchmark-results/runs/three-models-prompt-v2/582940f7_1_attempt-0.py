from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def ingest_and_post():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=582940f7_1&attempt=0"
    sink_url = "http://recorder:8090/sink/582940f7_1?attempt=0"

    resp = urllib.request.urlopen(source_url)
    raw = resp.read().decode('utf-8')
    data = json.loads(raw)

    times = data['hourly']['time']
    pm10s = data['hourly']['pm10']
    pm25s = data['hourly']['pm2_5']

    records = []
    for i in range(len(times)):
        record = {
            "time": times[i],
            "pm10": pm10s[i],
            "pm2_5": pm25s[i]
        }
        records.append(record)

    body = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(sink_url, data=body, headers={'Content-Type': 'application/json'}, method='POST')
    urllib.request.urlopen(req)

with DAG(dag_id='bench_582940f7_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t1_open_meteo_ingestion = PythonOperator(
        task_id='t1_open_meteo_ingestion',
        python_callable=ingest_and_post
    )
