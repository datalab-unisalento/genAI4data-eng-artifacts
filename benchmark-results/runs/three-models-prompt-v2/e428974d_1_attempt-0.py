from dagster import op, job

import json
import urllib.request

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=e428974d_1&attempt=0"
    sink_url = "http://recorder:8090/sink/e428974d_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    times = data["hourly"]["time"]
    pm2_5_values = data["hourly"]["pm2_5"]

    results = []
    for i in range(2, len(times)):
        current_time = times[i]
        window_pm2_5 = pm2_5_values[i-2:i+1]
        max_pm2_5 = max(window_pm2_5)
        
        record = {
            "time": current_time,
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": max_pm2_5
        }
        results.append(record)

    request = urllib.request.Request(
        sink_url,
        data=json.dumps({"records": results}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_e428974d_1():
    ingest_pipeline()
