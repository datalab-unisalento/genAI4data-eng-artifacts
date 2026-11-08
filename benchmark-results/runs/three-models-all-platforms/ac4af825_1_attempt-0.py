from prefect import flow
import json
import urllib.request

@flow(name="bench_ac4af825_1")
def bench_ac4af825_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=ac4af825_1&attempt=0"
    sink_url = "http://recorder:8090/sink/ac4af825_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract hourly data
    times = data['hourly']['time']
    pm2_5_values = data['hourly']['pm2_5']

    records = []
    for i in range(2, len(times)):
        # Get the current hour and previous two hours
        window_pm2_5 = pm2_5_values[i-2:i+1]
        max_pm2_5 = max(window_pm2_5)
        
        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": max_pm2_5
        }
        records.append(record)

    # Send to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_ac4af825_1()
