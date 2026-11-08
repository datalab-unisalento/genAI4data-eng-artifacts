from dagster import op, job

import json
import urllib.request

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=dbda7165_1&attempt=0"
    sink_url = "http://recorder:8090/sink/dbda7165_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    times = data["hourly"]["time"]
    pm2_5_values = data["hourly"]["pm2_5"]

    results = []
    for i in range(2, len(times)):
        current_time = times[i]
        window_pm2_5 = pm2_5_values[i-2:i+1]
        peak_value = max(window_pm2_5)
        
        record = {
            "time": current_time,
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": peak_value
        }
        results.append(record)

    request = urllib.request.Request(
        sink_url,
        data=json.dumps(results).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_dbda7165_1():
    ingest_pipeline()
