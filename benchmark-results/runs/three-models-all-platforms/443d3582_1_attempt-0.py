from dagster import op, job

import json
import urllib.request


@op
def ingest_pipeline():
    open_meteo_particulate = "http://recorder:8090/source/open-meteo/particulate?run_id=443d3582_1&attempt=0"
    open_meteo_gases = "http://recorder:8090/source/open-meteo/gases?run_id=443d3582_1&attempt=0"
    sink_url = "http://recorder:8090/sink/443d3582_1?attempt=0"

    req_part = urllib.request.Request(open_meteo_particulate)
    with urllib.request.urlopen(req_part) as resp:
        part_data = json.loads(resp.read().decode())

    req_gas = urllib.request.Request(open_meteo_gases)
    with urllib.request.urlopen(req_gas) as resp:
        gas_data = json.loads(resp.read().decode())

    times_pm = part_data["hourly"]["time"]
    pm10_vals = part_data["hourly"]["pm10"]
    pm25_vals = part_data["hourly"]["pm2_5"]

    times_gas = gas_data["hourly"]["time"]
    no2_vals = gas_data["hourly"]["nitrogen_dioxide"]
    o3_vals = gas_data["hourly"]["ozone"]

    time_set_pm = set(times_pm)
    time_set_gas = set(times_gas)
    common_times = sorted(time_set_pm & time_set_gas)

    records = []
    for t in common_times:
        idx_pm = times_pm.index(t)
        idx_gas = times_gas.index(t)
        record = {
            "time": t,
            "pm10": pm10_vals[idx_pm],
            "pm2_5": pm25_vals[idx_pm],
            "nitrogen_dioxide": no2_vals[idx_gas],
            "ozone": o3_vals[idx_gas]
        }
        records.append(record)

    body = json.dumps(records).encode("utf-8")
    req_sink = urllib.request.Request(
        sink_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req_sink) as resp:
        pass


@job
def bench_443d3582_1():
    ingest_pipeline()
