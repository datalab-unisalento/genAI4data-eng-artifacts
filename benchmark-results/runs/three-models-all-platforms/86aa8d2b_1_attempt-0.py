from dagster import op, job

import json
import urllib.request

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=86aa8d2b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/86aa8d2b_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    records = []
    for i in range(len(data["hourly"]["time"])):
        record = {
            "time": data["hourly"]["time"][i],
            "pm10": data["hourly"]["pm10"][i],
            "pm2_5": data["hourly"]["pm2_5"][i]
        }
        records.append(record)

    request = urllib.request.Request(
        sink_url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"records": records}).encode("utf-8")
    )

    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_86aa8d2b_1():
    ingest_pipeline()
