from dagster import op, job

import json
import urllib.request


@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=f54b410f_1&attempt=0"
    sink_url = "http://recorder:8090/sink/f54b410f_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode())

    times = source_data["hourly"]["time"]
    pm10s = source_data["hourly"]["pm10"]
    pm25s = source_data["hourly"]["pm2_5"]

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
        return response.read().decode()


@job
def bench_f54b410f_1():
    ingest_pipeline()
