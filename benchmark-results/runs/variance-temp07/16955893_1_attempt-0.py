from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=16955893_1&attempt=0"
    sink_url = "http://recorder:8090/sink/16955893_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read())

    hourly = data['hourly']
    times = hourly['time']
    pm2_5_values = hourly['pm2_5']

    results = []

    # Process from third hour onward (index 2)
    for i in range(2, len(times)):
        # Get the current and previous two hours' pm2.5 values
        window_pm2_5 = pm2_5_values[i-2:i+1]  # e.g., [i-2, i-1, i]
        peak = max(window_pm2_5)

        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": peak
        }

        results.append(record)

    # Post to sink
    request = urllib.request.Request(
        url=sink_url,
        data=json.dumps(results).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_16955893_1():
    ingest_pipeline()
