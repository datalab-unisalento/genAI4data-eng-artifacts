from prefect import flow
import json
import urllib.request

@flow
def bench_97d5480a_1():
    source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=97d5480a_1&attempt=0"
    sink_url = "http://recorder:8090/sink/97d5480a_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    # Extract hourly data
    times = data["hourly"]["time"]
    pm2_5_values = data["hourly"]["pm2_5"]
    pm10_values = data["hourly"]["pm10"]

    # Group by date and calculate maxima
    summaries = {}
    for time, pm2_5, pm10 in zip(times, pm2_5_values, pm10_values):
        date = time[:10]
        if date not in summaries:
            summaries[date] = {
                "date": date,
                "hours": 0,
                "pm2_5_max": float('-inf'),
                "pm10_max": float('-inf')
            }
        
        summary = summaries[date]
        summary["hours"] += 1
        summary["pm2_5_max"] = max(summary["pm2_5_max"], pm2_5)
        summary["pm10_max"] = max(summary["pm10_max"], pm10)

    # Convert to list of records
    records = list(summaries.values())

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
    bench_97d5480a_1()
