import json
import urllib.request
from prefect import flow


@flow
def bench_b25545e8_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=b25545e8_1&attempt=0"
    sink_url = "http://recorder:8090/sink/b25545e8_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode("utf-8"))

    hourly = data["hourly"]
    times = hourly["time"]
    pm10_values = hourly["pm10"]
    pm25_values = hourly["pm2_5"]

    date_groups = {}
    for i, ts in enumerate(times):
        date_key = ts[:10]
        if date_key not in date_groups:
            date_groups[date_key] = {"count": 0, "pm25": [], "pm10": []}
        date_groups[date_key]["count"] += 1
        date_groups[date_key]["pm25"].append(pm25_values[i])
        date_groups[date_key]["pm10"].append(pm10_values[i])

    records = []
    for date_key, group in date_groups.items():
        record = {
            "date": date_key,
            "hours": group["count"],
            "pm2_5_max": max(group["pm25"]),
            "pm10_max": max(group["pm10"]),
        }
        records.append(record)

    payload = json.dumps(records)
    req = urllib.request.Request(
        sink_url,
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as response:
        _ = response.read()


if __name__ == "__main__":
    bench_b25545e8_1()
