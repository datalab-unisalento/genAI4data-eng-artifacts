from prefect import flow
import json
import urllib.request


@flow
def bench_bd7b37fa_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=bd7b37fa_1&attempt=0"
    sink_url = "http://recorder:8090/sink/bd7b37fa_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode("utf-8"))

    hourly = source_data["hourly"]
    times = hourly["time"]
    pm10_values = hourly["pm10"]
    pm25_values = hourly["pm2_5"]

    records = []
    for i in range(len(times)):
        record = {
            "time": times[i],
            "pm10": pm10_values[i],
            "pm2_5": pm25_values[i]
        }
        records.append(record)

    request_body = json.dumps(records).encode("utf-8")
    request = urllib.request.Request(
        sink_url,
        data=request_body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    urllib.request.urlopen(request)


if __name__ == "__main__":
    bench_bd7b37fa_1()
