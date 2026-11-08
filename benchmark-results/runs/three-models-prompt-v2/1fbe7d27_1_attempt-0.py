from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

def process_data():
    # Fetch particulate data
    with urllib.request.urlopen('http://recorder:8090/source/open-meteo/particulate?run_id=1fbe7d27_1&attempt=0') as response:
        particulate_data = json.loads(response.read().decode())
    
    # Fetch gases data
    with urllib.request.urlopen('http://recorder:8090/source/open-meteo/gases?run_id=1fbe7d27_1&attempt=0') as response:
        gases_data = json.loads(response.read().decode())

    # Convert hourly arrays to records for particulate data
    particulate_records = []
    times = particulate_data['hourly']['time']
    pm10_values = particulate_data['hourly']['pm10']
    pm2_5_values = particulate_data['hourly']['pm2_5']
    
    for i in range(len(times)):
        particulate_records.append({
            'time': times[i],
            'pm10': pm10_values[i],
            'pm2_5': pm2_5_values[i]
        })

    # Convert hourly arrays to records for gases data
    gases_records = []
    times = gases_data['hourly']['time']
    no2_values = gases_data['hourly']['nitrogen_dioxide']
    ozone_values = gases_data['hourly']['ozone']
    
    for i in range(len(times)):
        gases_records.append({
            'time': times[i],
            'nitrogen_dioxide': no2_values[i],
            'ozone': ozone_values[i]
        })

    # Perform inner join on time
    joined_records = []
    particulate_dict = {r['time']: r for r in particulate_records}
    
    for gas_record in gases_records:
        if gas_record['time'] in particulate_dict:
            combined_record = {
                'time': gas_record['time'],
                'pm10': particulate_dict[gas_record['time']]['pm10'],
                'pm2_5': particulate_dict[gas_record['time']]['pm2_5'],
                'nitrogen_dioxide': gas_record['nitrogen_dioxide'],
                'ozone': gas_record['ozone']
            }
            joined_records.append(combined_record)

    # Post to sink
    request = urllib.request.Request(
        'http://recorder:8090/sink/1fbe7d27_1?attempt=0',
        data=json.dumps(joined_records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

with DAG(
    dag_id='bench_1fbe7d27_1',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    process_task = PythonOperator(
        task_id='t3_open_meteo_join',
        python_callable=process_data
    )
