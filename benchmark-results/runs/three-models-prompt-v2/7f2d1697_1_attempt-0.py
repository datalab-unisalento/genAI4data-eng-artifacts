from prefect import flow
import json
import urllib.request

@flow
def bench_7f2d1697_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=7f2d1697_1&attempt=0"
    sink_url = "http://recorder:8090/sink/7f2d1697_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    times = data['hourly']['time']
    pm2_5_values = data['hourly']['pm2_5']

    records = []
    for i in range(2, len(times)):
        current_time = times[i]
        max_pm2_5 = max(pm2_5_values[i-2:i+1])
        
        record = {
            "time": current_time,
            "pm2_5": pm2_5_values[i],
            "pm2_5_peak_3h": max_pm2_5
        }
        records.append(record)

    request = urllib.request.Request(
        sink_url, 
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_7f2d1697_1()
