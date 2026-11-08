from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=637031bf_1&attempt=0"
    sink_url = "http://recorder:8090/sink/637031bf_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode())

    hourly = data['hourly']
    times = hourly['time']
    pm2_5_values = hourly['pm2_5']

    results = []

    for i in range(2, len(times)):
        # Get the current hour and previous two hours
        window_pm2_5 = pm2_5_values[i-2:i+1]
        max_pm2_5 = max(window_pm2_5)

        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": max_pm2_5
        }

        results.append(record)

    # Post to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(results).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_637031bf_1():
    ingest_pipeline()
