from dagster import op, job

import json
import urllib.request


@op
def ingest_pipeline():
    open_meteo_particulate = "http://recorder:8090/source/open-meteo/particulate?run_id=8c1f799d_1&attempt=0"
    open_meteo_gases = "http://recorder:8090/source/open-meteo/gases?run_id=8c1f799d_1&attempt=0"
    sink_url = "http://recorder:8090/sink/8c1f799d_1?attempt=0"

    req_part = urllib.request.Request(open_meteo_particulate)
    resp_part = urllib.request.urlopen(req_part)
    data_part = json.loads(resp_part.read().decode())

    req_gases = urllib.request.Request(open_meteo_gases)
    resp_gases = urllib.request.urlopen(req_gases)
    data_gases = json.loads(resp_gases.read().decode())

    times_p = data_part["hourly"]["time"]
    pm10_p = data_part["hourly"]["pm10"]
    pm25_p = data_part["hourly"]["pm2_5"]

    times_g = data_gases["hourly"]["time"]
    no2_g = data_gases["hourly"]["nitrogen_dioxide"]
    o3_g = data_gases["hourly"]["ozone"]

    dict_p = {t: (p, pm2) for t, p, pm2 in zip(times_p, pm10_p, pm25_p)}
    dict_g = {t: (n, o) for t, n, o in zip(times_g, no2_g, o3_g)}

    common_times = set(dict_p.keys()) & set(dict_g.keys())

    records = []
    for t in sorted(common_times):
        p_val, pm_val = dict_p[t]
        n_val, o_val = dict_g[t]
        records.append({
            "time": t,
            "pm10": p_val,
            "pm2_5": pm_val,
            "nitrogen_dioxide": n_val,
            "ozone": o_val
        })

    body = json.dumps(records).encode("utf-8")

    req_sink = urllib.request.Request(
        sink_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    urllib.request.urlopen(req_sink)


@job
def bench_8c1f799d_1():
    ingest_pipeline()
