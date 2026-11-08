from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=96da9c50_1&attempt=0"
    sink_url = "http://recorder:8090/sink/96da9c50_1?attempt=0"

    # Fetch data from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read())

    hourly = data['hourly']
    times = hourly['time']
    pm2_5_values = hourly['pm2_5']

    results = []

    # Process starting from the third hour (index 2)
    for i in range(2, len(times)):
        # Get pm2.5 values for current and previous two hours
        window_pm2_5 = pm2_5_values[i-2:i+1]
        peak = max(window_pm2_5)

        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": peak
        }

        results.append(record)

    # Post to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(results).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_96da9c50_1():
    ingest_pipeline()
