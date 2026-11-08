from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def join_and_post():
    src_part = urllib.request.urlopen(
        "http://recorder:8090/source/open-meteo/particulate?run_id=01a8f53c_1&attempt=0"
    ).read()
    src_gas = urllib.request.urlopen(
        "http://recorder:8090/source/open-meteo/gases?run_id=01a8f53c_1&attempt=0"
    ).read()

    p = json.loads(src_part)
    g = json.loads(src_gas)

    times_p = set(p["hourly"]["time"])
    times_g = set(g["hourly"]["time"])
    common = sorted(times_p & times_g)

    pm10_map = dict(zip(p["hourly"]["time"], p["hourly"]["pm10"]))
    pm25_map = dict(zip(p["hourly"]["time"], p["hourly"]["pm2_5"]))
    no2_map = dict(zip(g["hourly"]["time"], g["hourly"]["nitrogen_dioxide"]))
    o3_map = dict(zip(g["hourly"]["time"], g["hourly"]["ozone"]))

    records = []
    for t in common:
        records.append({
            "time": t,
            "pm10": pm10_map[t],
            "pm2_5": pm25_map[t],
            "nitrogen_dioxide": no2_map[t],
            "ozone": o3_map[t]
        })

    sink_url = "http://recorder:8090/sink/01a8f53c_1?attempt=0"
    req = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    urllib.request.urlopen(req)

with DAG(dag_id='bench_01a8f53c_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id='t3_open_meteo_join', python_callable=join_and_post)
