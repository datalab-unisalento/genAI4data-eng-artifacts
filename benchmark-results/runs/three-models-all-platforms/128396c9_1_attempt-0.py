from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=128396c9_1&attempt=0"
    sink_url = "http://recorder:8090/sink/128396c9_1?attempt=0"

    # Fetch data from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read())

    # Extract hourly data
    times = data["hourly"]["time"]
    pm10_values = data["hourly"]["pm10"]
    pm2_5_values = data["hourly"]["pm2_5"]

    # Convert to records
    records = []
    for i in range(len(times)):
        record = {
            "time": times[i],
            "pm10": pm10_values[i],
            "pm2_5": pm2_5_values[i]
        }
        records.append(record)

    # Post to sink
    request_data = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(sink_url, data=request_data)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_128396c9_1():
    ingest_pipeline()
