from prefect import flow
import json
import urllib.request

@flow(name="bench_54a94ea1_1")
def bench_54a94ea1_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=54a94ea1_1&attempt=0"
    sink_url = "http://recorder:8090/sink/54a94ea1_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly = data['hourly']
    times = hourly['time']
    pm2_5_values = hourly['pm2_5']

    records = []

    for i in range(2, len(times)):
        # Get the current hour and previous two hours
        window_pm2_5 = pm2_5_values[i-2:i+1]
        peak_3h = max(window_pm2_5)

        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": peak_3h
        }

        records.append(record)

    # Post results to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    with urllib.request.urlopen(request) as response:
        pass  # Request sent successfully

if __name__ == '__main__':
    bench_54a94ea1_1()
