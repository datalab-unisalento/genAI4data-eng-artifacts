from dagster import op, job
import urllib.request
import json
from collections import defaultdict

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=2358e214_1&attempt=0"
    sink_url = "http://recorder:8090/sink/2358e214_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode())

    hourly_records = data['hourly']
    time_list = hourly_records['time']
    pm10_list = hourly_records['pm10']
    pm2_5_list = hourly_records['pm2_5']

    # Group by date and compute maxima
    daily_summary = defaultdict(lambda: {
        'hours': 0,
        'pm2_5_max': float('-inf'),
        'pm10_max': float('-inf')
    })

    for i, time_str in enumerate(time_list):
        date = time_str[:10]  # First 10 characters as date
        pm10_val = pm10_list[i]
        pm2_5_val = pm2_5_list[i]

        summary = daily_summary[date]
        summary['hours'] += 1
        summary['pm2_5_max'] = max(summary['pm2_5_max'], pm2_5_val)
        summary['pm10_max'] = max(summary['pm10_max'], pm10_val)

    # Prepare records for sink
    records = []
    for date, values in daily_summary.items():
        record = {
            'date': date,
            'hours': values['hours'],
            'pm2_5_max': values['pm2_5_max'],
            'pm10_max': values['pm10_max']
        }
        records.append(record)

    # POST to sink
    request_data = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(sink_url, data=request_data)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as response:
        pass  # Response ignored

@job
def bench_2358e214_1():
    ingest_pipeline()
