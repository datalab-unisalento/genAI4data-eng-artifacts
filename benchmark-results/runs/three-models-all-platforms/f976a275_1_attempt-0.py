from prefect import flow
import json
import urllib.request
from datetime import datetime

@flow(name="bench_f976a275_1")
def bench_f976a275_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=f976a275_1&attempt=0"
    sink_url = "http://recorder:8090/sink/f976a275_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract hourly records
    times = data['hourly']['time']
    pm10_values = data['hourly']['pm10']
    pm2_5_values = data['hourly']['pm2_5']

    # Group by date and compute summary
    daily_summary = {}

    for i, time_str in enumerate(times):
        date_key = time_str[:10]  # First 10 characters as date

        if date_key not in daily_summary:
            daily_summary[date_key] = {
                'date': date_key,
                'hours': 0,
                'pm2_5_max': float('-inf'),
                'pm10_max': float('-inf')
            }

        # Increment hour count
        daily_summary[date_key]['hours'] += 1

        # Update max values
        pm10_val = pm10_values[i]
        pm2_5_val = pm2_5_values[i]

        if pm10_val > daily_summary[date_key]['pm10_max']:
            daily_summary[date_key]['pm10_max'] = pm10_val

        if pm2_5_val > daily_summary[date_key]['pm2_5_max']:
            daily_summary[date_key]['pm2_5_max'] = pm2_5_val

    # Convert to list of records
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
    bench_f976a275_1()
