from dagster import op, job
import urllib.request
import json
from collections import defaultdict

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=14343d67_1&attempt=0"
    sink_url = "http://recorder:8090/sink/14343d67_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read())

    hourly_records = list(zip(data['hourly']['time'], data['hourly']['pm2_5'], data['hourly']['pm10']))

    daily_summary = defaultdict(lambda: {
        'date': None,
        'hours': 0,
        'pm2_5_max': float('-inf'),
        'pm10_max': float('-inf')
    })

    for time_str, pm2_5_val, pm10_val in hourly_records:
        date_key = time_str[:10]  # First 10 characters as date
        record = daily_summary[date_key]
        record['date'] = date_key
        record['hours'] += 1
        if pm2_5_val > record['pm2_5_max']:
            record['pm2_5_max'] = pm2_5_val
        if pm10_val > record['pm10_max']:
            record['pm10_max'] = pm10_val

    # Prepare output records
    output_records = list(daily_summary.values())

    # POST to sink
    req = urllib.request.Request(
        url=sink_url,
        data=json.dumps(output_records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_14343d67_1():
    ingest_pipeline()
