from prefect import flow
import json
import urllib.request
from collections import defaultdict


@flow
def bench_d1bb226c_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=d1bb226c_1&attempt=0"
    sink_url = "http://recorder:8090/sink/d1bb226c_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode('utf-8'))

    hourly = source_data['hourly']
    times = hourly['time']
    pm10_values = hourly['pm10']
    pm25_values = hourly['pm2_5']

    date_groups = defaultdict(list)
    for i, t in enumerate(times):
        date_str = t[:10]
        date_groups[date_str].append(i)

    records = []
    for date_str, indices in date_groups.items():
        pm25_day = [pm25_values[i] for i in indices]
        pm10_day = [pm10_values[i] for i in indices]
        record = {
            "date": date_str,
            "hours": len(indices),
            "pm2_5_max": max(pm25_day),
            "pm10_max": max(pm10_day)
        }
        records.append(record)

    payload = json.dumps(records)
    request = urllib.request.Request(
        sink_url,
        data=payload.encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    urllib.request.urlopen(request)


if __name__ == '__main__':
    bench_d1bb226c_1()
