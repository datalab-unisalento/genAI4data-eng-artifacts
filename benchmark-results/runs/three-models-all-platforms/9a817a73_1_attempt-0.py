from dagster import op, job

import json
import urllib.request


@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=9a817a73_1&attempt=0"
    sink_url = "http://recorder:8090/sink/9a817a73_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode())

    hourly = source_data["hourly"]
    times = hourly["time"]
    pm25_values = hourly["pm2_5"]

    records = []
    for i in range(2, len(times)):
        window = pm25_values[i - 2:i + 1]
        peak = max(window)
        record = {
            "time": times[i],
            "pm2_5": pm25_values[i],
            "pm2_5_peak_3h": peak
        }
        records.append(record)

    body = json.dumps(records).encode("utf-8")

    req = urllib.request.Request(
        sink_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(req) as response:
        _ = response.read()


@job
def bench_9a817a73_1():
    ingest_pipeline()
