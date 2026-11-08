from prefect import flow
import json
import urllib.request

@flow
def bench_6297069b_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=6297069b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/6297069b_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract hourly arrays
    time_array = data['hourly']['time']
    pm10_array = data['hourly']['pm10']
    pm2_5_array = data['hourly']['pm2_5']

    # Create records preserving exact values
    records = [
        {
            "time": time,
            "pm10": pm10,
            "pm2_5": pm2_5
        }
        for time, pm10, pm2_5 in zip(time_array, pm10_array, pm2_5_array)
    ]

    # Post records to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_6297069b_1()
