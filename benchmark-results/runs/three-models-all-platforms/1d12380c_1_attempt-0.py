from prefect import flow
import json
import urllib.request


@flow
def bench_1d12380c_1():
    source_particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=1d12380c_1&attempt=0"
    source_gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=1d12380c_1&attempt=0"
    sink_url = "http://recorder:8090/sink/1d12380c_1?attempt=0"

    with urllib.request.urlopen(source_particulate_url) as resp:
        p_data = json.loads(resp.read().decode("utf-8"))
    with urllib.request.urlopen(source_gases_url) as resp:
        g_data = json.loads(resp.read().decode("utf-8"))

    p_times = p_data["hourly"]["time"]
    p_pm10 = p_data["hourly"]["pm10"]
    p_pm25 = p_data["hourly"]["pm2_5"]

    g_times = g_data["hourly"]["time"]
    g_no2 = g_data["hourly"]["nitrogen_dioxide"]
    g_o3 = g_data["hourly"]["ozone"]

    p_dict = dict(zip(p_times, zip(p_pm10, p_pm25)))
    g_dict = dict(zip(g_times, zip(g_no2, g_o3)))

    common_times = set(p_dict.keys()) & set(g_dict.keys())

    records = []
    for t in sorted(common_times):
        pm10_val, pm25_val = p_dict[t]
        no2_val, o3_val = g_dict[t]
        records.append({
            "time": t,
            "pm10": pm10_val,
            "pm2_5": pm25_val,
            "nitrogen_dioxide": no2_val,
            "ozone": o3_val
        })

    body = json.dumps(records)
    request = urllib.request.Request(
        sink_url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(request) as resp:
        resp.read()


if __name__ == "__main__":
    bench_1d12380c_1()
