import json
import urllib.request
from prefect import flow

@flow(name="bench_fb380cdb_1")
def bench_fb380cdb_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=fb380cdb_1&attempt=0"
    sink_url = "http://recorder:8090/sink/fb380cdb_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly = data['hourly']
    times = hourly['time']
    pm2_5_values = hourly['pm2_5']

    records = []
    for i in range(2, len(times)):
        # Get the current and previous two hours' pm2.5 values
        window_pm2_5 = pm2_5_values[i-2:i+1]
        peak_3h = max(window_pm2_5)

        record = {
            "time": times[i],
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": peak_3h
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
    bench_fb380cdb_1()
