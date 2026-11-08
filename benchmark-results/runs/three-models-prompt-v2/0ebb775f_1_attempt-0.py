from prefect import flow
import json
import urllib.request


@flow
def bench_0ebb775f_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=0ebb775f_1&attempt=0"
    sink_url = "http://recorder:8090/sink/0ebb775f_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode("utf-8"))

    times = source_data["hourly"]["time"]
    pm25_values = source_data["hourly"]["pm2_5"]

    records = []
    for i in range(2, len(pm25_values)):
        current_pm25 = pm25_values[i]
        previous_pm25_1 = pm25_values[i - 1]
        previous_pm25_2 = pm25_values[i - 2]
        peak_3h = max(current_pm25, previous_pm25_1, previous_pm25_2)
        record = {
            "time": times[i],
            "pm2_5": current_pm25,
            "pm2_5_peak_3h": peak_3h
        }
        records.append(record)

    json_body = json.dumps(records)
    request = urllib.request.Request(
        sink_url,
        data=json_body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(request) as response:
        _ = response.read()


if __name__ == "__main__":
    bench_0ebb775f_1()
