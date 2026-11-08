from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

PARTICULATE_URL = "http://recorder:8090/source/open-meteo/particulate?run_id=9e06f308_1&attempt=0"
GASES_URL = "http://recorder:8090/source/open-meteo/gases?run_id=9e06f308_1&attempt=0"
SINK_URL = "http://recorder:8090/sink/9e06f308_1?attempt=0"


def _join_and_post():
    with urllib.request.urlopen(PARTICULATE_URL) as resp:
        p_data = json.loads(resp.read().decode("utf-8"))
    with urllib.request.urlopen(GASES_URL) as resp:
        g_data = json.loads(resp.read().decode("utf-8"))

    p_times = set(p_data["hourly"]["time"])
    g_times = set(g_data["hourly"]["time"])
    common_times = sorted(p_times & g_times)

    records = []
    for t in common_times:
        p_idx = p_data["hourly"]["time"].index(t)
        g_idx = g_data["hourly"]["time"].index(t)
        record = {
            "time": t,
            "pm10": p_data["hourly"]["pm10"][p_idx],
            "pm2_5": p_data["hourly"]["pm2_5"][p_idx],
            "nitrogen_dioxide": g_data["hourly"]["nitrogen_dioxide"][g_idx],
            "ozone": g_data["hourly"]["ozone"][g_idx],
        }
        records.append(record)

    req = urllib.request.Request(
        SINK_URL,
        data=json.dumps(records).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req)


with DAG(dag_id='bench_9e06f308_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t3_open_meteo_join = PythonOperator(
        task_id="t3_open_meteo_join",
        python_callable=_join_and_post,
    )
