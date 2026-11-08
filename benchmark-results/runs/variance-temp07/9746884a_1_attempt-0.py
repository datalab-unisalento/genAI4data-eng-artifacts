from prefect import flow
import json
import urllib.request

@flow(name="bench_9746884a_1")
def bench_9746884a_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=9746884a_1&attempt=0"
    sink_url = "http://recorder:8090/sink/9746884a_1?attempt=0"

    # Fetch data from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly_data = data['hourly']
    times = hourly_data['time']
    pm2_5_values = hourly_data['pm2_5']

    records = []

    # Process each hour starting from the third one (index 2)
    for i in range(2, len(times)):
        current_time = times[i]
        current_pm2_5 = pm2_5_values[i]

        # Compute maximum pm2_5 over the current and two previous hours
        max_pm2_5 = max(pm2_5_values[i-2], pm2_5_values[i-1], current_pm2_5)

        record = {
            "time": current_time,
            "pm2_5": current_pm2_5,
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
        pass

if __name__ == '__main__':
    bench_9746884a_1()
