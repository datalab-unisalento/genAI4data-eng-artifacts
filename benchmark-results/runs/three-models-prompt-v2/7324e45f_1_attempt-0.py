from prefect import flow
import json
import urllib.request
from datetime import datetime

@flow(name="bench_7324e45f_1")
def daily_particulate_summary():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=7324e45f_1&attempt=0"
    sink_url = "http://recorder:8090/sink/7324e45f_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    hourly_records = list(zip(data['hourly']['time'], data['hourly']['pm2_5'], data['hourly']['pm10']))

    daily_summary = {}

    for time_str, pm2_5_val, pm10_val in hourly_records:
        date_key = time_str[:10]  # First 10 characters as date

        if date_key not in daily_summary:
            daily_summary[date_key] = {
                'date': date_key,
                'hours': 0,
                'pm2_5_max': float('-inf'),
                'pm10_max': float('-inf')
            }

        daily_summary[date_key]['hours'] += 1
        daily_summary[date_key]['pm2_5_max'] = max(daily_summary[date_key]['pm2_5_max'], pm2_5_val)
        daily_summary[date_key]['pm10_max'] = max(daily_summary[date_key]['pm10_max'], pm10_val)

    records = list(daily_summary.values())

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
    daily_particulate_summary()
