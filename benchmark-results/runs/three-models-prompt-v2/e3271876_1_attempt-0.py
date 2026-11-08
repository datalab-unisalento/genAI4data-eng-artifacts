from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=e3271876_1&attempt=0"
    sink_url = "http://recorder:8090/sink/e3271876_1?attempt=0"
    req = urllib.request.Request(source_url)
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
    hourly = data["hourly"]
    times = hourly["time"]
    pm25_values = hourly["pm2_5"]
    records = []
    for i in range(2, len(pm25_values)):
        peak_val = max(pm25_values[i], pm25_values[i-1], pm25_values[i-2])
        records.append({
            "time": times[i],
            "pm2_5": pm25_values[i],
            "pm2_5_peak_3h": peak_val
        })
    body = json.dumps(records).encode("utf-8")
    sink_req = urllib.request.Request(
        sink_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(sink_req) as resp:
        pass

@job
def bench_e3271876_1():
    ingest_pipeline()
