from prefect import flow
import json
import urllib.request

@flow
def bench_b1feeea4_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=b1feeea4_1&attempt=0"
    sink_url = "http://recorder:8090/sink/b1feeea4_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Extract hourly data
    times = data['hourly']['time']
    pm25_values = data['hourly']['pm2_5']
    pm10_values = data['hourly']['pm10']

    # Group by date and calculate summaries
    daily_summaries = {}
    for time, pm25, pm10 in zip(times, pm25_values, pm10_values):
        date_key = time[:10]
        if date_key not in daily_summaries:
            daily_summaries[date_key] = {
                'date': date_key,
                'hours': 0,
                'pm2_5_max': float('-inf'),
                'pm10_max': float('-inf')
            }
        
        summary = daily_summaries[date_key]
        summary['hours'] += 1
        summary['pm2_5_max'] = max(summary['pm2_5_max'], pm25)
        summary['pm10_max'] = max(summary['pm10_max'], pm10)

    # Convert to list of records
    records = list(daily_summaries.values())

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
    bench_b1feeea4_1()
