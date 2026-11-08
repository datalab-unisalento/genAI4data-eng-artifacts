from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=a1a52cae_1&attempt=0"
    gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=a1a52cae_1&attempt=0"
    sink_url = "http://recorder:8090/sink/a1a52cae_1?attempt=0"

    # Fetch particulate data
    with urllib.request.urlopen(particulate_url) as response:
        particulate_data = json.loads(response.read().decode())

    # Fetch gases data
    with urllib.request.urlopen(gases_url) as response:
        gases_data = json.loads(response.read().decode())

    # Convert hourly arrays to records for particulates
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

    # Convert hourly arrays to records for gases
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

    # Create lookup dict from gases records by time
    gases_lookup = {record['time']: record for record in gases_records}

    # Inner join: only keep records where both particulate and gas data exist
    joined_records = []
    for p_record in particulate_records:
        time_key = p_record['time']
        if time_key in gases_lookup:
            g_record = gases_lookup[time_key]
            combined = {**p_record, **g_record}
            joined_records.append(combined)

    # POST to sink
    payload = {'records': joined_records} if joined_records else []
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(sink_url, data=data)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_a1a52cae_1():
    ingest_pipeline()
