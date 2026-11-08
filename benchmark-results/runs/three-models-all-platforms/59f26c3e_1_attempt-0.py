from dagster import op, job

import json
import urllib.request
from datetime import datetime

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=59f26c3e_1&attempt=0"
    sink_url = "http://recorder:8090/sink/59f26c3e_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    hourly_data = list(zip(data["hourly"]["time"], data["hourly"]["pm10"], data["hourly"]["pm2_5"]))
    
    daily_summaries = {}
    for time_str, pm10_val, pm2_5_val in hourly_data:
        date_key = time_str[:10]
        
        if date_key not in daily_summaries:
            daily_summaries[date_key] = {
                "date": date_key,
                "hours": 0,
                "pm10_max": float('-inf'),
                "pm2_5_max": float('-inf')
            }
            
        summary = daily_summaries[date_key]
        summary["hours"] += 1
        summary["pm10_max"] = max(summary["pm10_max"], pm10_val)
        summary["pm2_5_max"] = max(summary["pm2_5_max"], pm2_5_val)

    records = list(daily_summaries.values())
    
    request = urllib.request.Request(
        sink_url,
        data=json.dumps({"records": records}).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_59f26c3e_1():
    ingest_pipeline()
