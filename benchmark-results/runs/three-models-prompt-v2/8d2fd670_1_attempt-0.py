from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=8d2fd670_1&attempt=0"
    gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=8d2fd670_1&attempt=0"
    sink_url = "http://recorder:8090/sink/8d2fd670_1?attempt=0"

    # Fetch particulate data
    with urllib.request.urlopen(particulate_url) as response:
        particulate_data = json.loads(response.read().decode())

    # Fetch gases data
    with urllib.request.urlopen(gases_url) as response:
        gases_data = json.loads(response.read().decode())

    # Convert hourly columns to records for particulates
    particulate_records = []
    times = particulate_data['hourly']['time']
    pm10_values = particulate_data['hourly']['pm10']
    pm2_5_values = particulate_data['hourly']['pm2_5']

    for i in range(len(times)):
        record = {
            'time': times[i],
            'pm10': pm10_values[i],
            'pm2_5': pm2_5_values[i]
        }
        particulate_records.append(record)

    # Convert hourly columns to records for gases
    gases_records = []
    times = gases_data['hourly']['time']
    no2_values = gases_data['hourly']['nitrogen_dioxide']
    ozone_values = gases_data['hourly']['ozone']

    for i in range(len(times)):
        record = {
            'time': times[i],
            'nitrogen_dioxide': no2_values[i],
            'ozone': ozone_values[i]
        }
        gases_records.append(record)

    # Create a mapping from time to records for both datasets
    particulate_map = {record['time']: record for record in particulate_records}
    gases_map = {record['time']: record for record in gases_records}

    # Inner join on time
    joined_records = []
    common_times = set(particulate_map.keys()) & set(gases_map.keys())

    for time in sorted(common_times):
        p_record = particulate_map[time]
        g_record = gases_map[time]
        combined_record = {**p_record, **g_record}
        joined_records.append(combined_record)

    # POST to sink
    if joined_records:
        data = json.dumps(joined_records).encode('utf-8')
        req = urllib.request.Request(sink_url, data=data)
        req.add_header('Content-Type', 'application/json')
        with urllib.request.urlopen(req) as response:
            pass

@job
def bench_8d2fd670_1():
    ingest_pipeline()
