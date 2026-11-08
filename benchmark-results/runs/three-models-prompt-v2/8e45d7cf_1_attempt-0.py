from prefect import flow
import json
import urllib.request

@flow
def bench_8e45d7cf_1():
    # Fetch particulate data
    with urllib.request.urlopen("http://recorder:8090/source/open-meteo/particulate?run_id=8e45d7cf_1&attempt=0") as response:
        particulate_data = json.load(response)
    
    # Convert hourly arrays to records for particulate
    particulate_records = []
    times = particulate_data["hourly"]["time"]
    pm10s = particulate_data["hourly"]["pm10"]
    pm2_5s = particulate_data["hourly"]["pm2_5"]
    
    for i in range(len(times)):
        particulate_records.append({
            "time": times[i],
            "pm10": pm10s[i],
            "pm2_5": pm2_5s[i]
        })
    
    # Fetch gases data
    with urllib.request.urlopen("http://recorder:8090/source/open-meteo/gases?run_id=8e45d7cf_1&attempt=0") as response:
        gases_data = json.load(response)
    
    # Convert hourly arrays to records for gases
    gases_records = []
    times = gases_data["hourly"]["time"]
    no2s = gases_data["hourly"]["nitrogen_dioxide"]
    ozones = gases_data["hourly"]["ozone"]
    
    for i in range(len(times)):
        gases_records.append({
            "time": times[i],
            "nitrogen_dioxide": no2s[i],
            "ozone": ozones[i]
        })
    
    # Create dictionaries for joining
    particulate_dict = {r["time"]: r for r in particulate_records}
    gases_dict = {r["time"]: r for r in gases_records}
    
    # Inner join on time
    joined_records = []
    for time, particulate_record in particulate_dict.items():
        if time in gases_dict:
            gas_record = gases_dict[time]
            joined_records.append({
                "time": time,
                "pm10": particulate_record["pm10"],
                "pm2_5": particulate_record["pm2_5"],
                "nitrogen_dioxide": gas_record["nitrogen_dioxide"],
                "ozone": gas_record["ozone"]
            })
    
    # Post to sink
    request = urllib.request.Request(
        "http://recorder:8090/sink/8e45d7cf_1?attempt=0",
        data=json.dumps(joined_records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_8e45d7cf_1()
