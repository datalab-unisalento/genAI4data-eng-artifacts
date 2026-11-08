from prefect import flow
import json
import urllib.request


@flow
def bench_8dd40f58_1():
    source_particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=8dd40f58_1&attempt=0"
    source_gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=8dd40f58_1&attempt=0"
    sink_url = "http://recorder:8090/sink/8dd40f58_1?attempt=0"

    with urllib.request.urlopen(source_particulate_url) as resp:
        p_data = json.loads(resp.read().decode("utf-8"))
    with urllib.request.urlopen(source_gases_url) as resp:
        g_data = json.loads(resp.read().decode("utf-8"))

    # Extract hourly arrays from each source
    p_times = p_data["hourly"]["time"]
    p_pm10 = p_data["hourly"]["pm10"]
    p_pm25 = p_data["hourly"]["pm2_5"]

    g_times = g_data["hourly"]["time"]
    g_no2 = g_data["hourly"]["nitrogen_dioxide"]
    g_o3 = g_data["hourly"]["ozone"]

    # Build lookup maps: time -> values for gases
    g_lookup = {}
    for i, t in enumerate(g_times):
        g_lookup[t] = (g_no2[i], g_o3[i])

    # Inner join on exact time, preserving duplicate counts
    records = []
    for i, t in enumerate(p_times):
        if t in g_lookup:
            no2, o3 = g_lookup[t]
            records.append({
                "time": t,
                "pm10": p_pm10[i],
                "pm2_5": p_pm25[i],
                "nitrogen_dioxide": no2,
                "ozone": o3
            })

    body = json.dumps(records)
    request = urllib.request.Request(
        sink_url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(request) as resp:
        _ = resp.read()


if __name__ == "__main__":
    bench_8dd40f58_1()
