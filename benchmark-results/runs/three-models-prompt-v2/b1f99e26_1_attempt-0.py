from prefect import flow
import json
import urllib.request

@flow(name="bench_b1f99e26_1")
def bench_b1f99e26_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=b1f99e26_1&attempt=0"
    sink_url = "http://recorder:8090/sink/b1f99e26_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract hourly data
    time_list = data['hourly']['time']
    pm10_list = data['hourly']['pm10']
    pm2_5_list = data['hourly']['pm2_5']

    # Convert to records
    records = []
    for i in range(len(time_list)):
        record = {
            "time": time_list[i],
            "pm10": pm10_list[i],
            "pm2_5": pm2_5_list[i]
        }
        records.append(record)

    # Post to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_b1f99e26_1()
