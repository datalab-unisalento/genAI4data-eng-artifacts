from prefect import flow
import json
import urllib.request


@flow
def bench_e01e82e3_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=e01e82e3_1&attempt=0"
    sink_url = "http://recorder:8090/sink/e01e82e3_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode("utf-8"))

    times = source_data["hourly"]["time"]
    pm25_values = source_data["hourly"]["pm2_5"]

    records = []
    for i in range(2, len(times)):
        window = pm25_values[i - 2 : i + 1]
        peak = max(window)
        record = {
            "time": times[i],
            "pm2_5": pm25_values[i],
            "pm2_5_peak_3h": peak,
        }
        records.append(record)

    body = json.dumps(records).encode("utf-8")
    request = urllib.request.Request(
        sink_url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    urllib.request.urlopen(request)


if __name__ == "__main__":
    bench_e01e82e3_1()
