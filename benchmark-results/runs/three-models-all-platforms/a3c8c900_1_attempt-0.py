from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json

def t3_open_meteo_join():
    # Fetch particulate data
    particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=a3c8c900_1&attempt=0"
    with urllib.request.urlopen(particulate_url) as response:
        particulate_data = json.loads(response.read().decode('utf-8'))

    # Fetch gases data
    gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=a3c8c900_1&attempt=0"
    with urllib.request.urlopen(gases_url) as response:
        gases_data = json.loads(response.read().decode('utf-8'))

    # Convert hourly data to records
    particulate_records = [
        {
            "time": time,
            "pm10": pm10_val,
            "pm2_5": pm2_5_val
        }
        for time, pm10_val, pm2_5_val in zip(
            particulate_data["hourly"]["time"],
            particulate_data["hourly"]["pm10"],
            particulate_data["hourly"]["pm2_5"]
        )
    ]

    gases_records = [
        {
            "time": time,
            "nitrogen_dioxide": no2_val,
            "ozone": ozone_val
        }
        for time, no2_val, ozone_val in zip(
            gases_data["hourly"]["time"],
            gases_data["hourly"]["nitrogen_dioxide"],
            gases_data["hourly"]["ozone"]
        )
    ]

    # Create lookup dict from gases records by time
    gases_lookup = {record["time"]: record for record in gases_records}

    # Inner join on time
    joined_records = []
    for p_record in particulate_records:
        time_key = p_record["time"]
        if time_key in gases_lookup:
            g_record = gases_lookup[time_key]
            combined = {
                "time": time_key,
                "pm10": p_record["pm10"],
                "pm2_5": p_record["pm2_5"],
                "nitrogen_dioxide": g_record["nitrogen_dioxide"],
                "ozone": g_record["ozone"]
            }
            joined_records.append(combined)

    # Post to sink
    sink_url = "http://recorder:8090/sink/a3c8c900_1?attempt=0"
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(joined_records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass  # Response is not used

with DAG(dag_id='bench_a3c8c900_1', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(
        task_id='t3_open_meteo_join',
        python_callable=t3_open_meteo_join
    )
