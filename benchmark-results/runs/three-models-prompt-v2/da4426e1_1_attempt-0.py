from dagster import op, job

import json
import urllib.request


@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=da4426e1_1&attempt=0"
    sink_url = "http://recorder:8090/sink/da4426e1_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode())

    hourly = source_data["hourly"]
    times = hourly["time"]
    pm10s = hourly["pm10"]
    pm25s = hourly["pm2_5"]

    records = []
    for i in range(len(times)):
        record = {
            "time": times[i],
            "pm10": pm10s[i],
            "pm2_5": pm25s[i]
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
def bench_da4426e1_1():
    ingest_pipeline()
