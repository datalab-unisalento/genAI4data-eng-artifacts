from prefect import flow
import json
import urllib.request

@flow(name="bench_8574aeaf_1")
def bench_8574aeaf_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=8574aeaf_1&attempt=0"
    sink_url = "http://recorder:8090/sink/8574aeaf_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly = data['hourly']
    times = hourly['time']
    pm2_5_values = hourly['pm2_5']

    records = []

    # Process each hour starting from the third (index 2)
    for i in range(2, len(times)):
        # Get pm2.5 values for current and previous two hours
        window_pm2_5 = pm2_5_values[i-2:i+1]
        max_pm2_5 = max(window_pm2_5)

        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": max_pm2_5
        }
        records.append(record)

    # Send results to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass  # Response is not used

if __name__ == '__main__':
    bench_8574aeaf_1()
