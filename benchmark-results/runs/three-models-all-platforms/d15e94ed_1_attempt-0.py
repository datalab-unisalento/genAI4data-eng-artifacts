from dagster import op, job

import json
import urllib.request
import collections


@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=d15e94ed_1&attempt=0"
    sink_url = "http://recorder:8090/sink/d15e94ed_1?attempt=0"

    with urllib.request.urlopen(source_url) as resp:
        data = json.loads(resp.read().decode())

    hourly = data["hourly"]
    times = hourly["time"]
    pm25 = hourly["pm2_5"]
    pm10 = hourly["pm10"]

    date_groups = collections.defaultdict(list)
    for i, t in enumerate(times):
        date_key = t[:10]
        date_groups[date_key].append(i)

    records = []
    for date_key, indices in date_groups.items():
        pm25_vals = [pm25[i] for i in indices]
        pm10_vals = [pm10[i] for i in indices]
        record = {
            "date": date_key,
            "hours": len(indices),
            "pm2_5_max": max(pm25_vals),
            "pm10_max": max(pm10_vals)
        }
        records.append(record)

    body = json.dumps(records).encode("utf-8")
    req = urllib.request.Request(sink_url, data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req)


@job
def bench_d15e94ed_1():
    ingest_pipeline()
