from dagster import op, job
import json
import urllib.request

@op
def ingest_pipeline():
    # Fetch particulate data
    with urllib.request.urlopen("http://recorder:8090/source/open-meteo/particulate?run_id=430d3202_1&attempt=0") as response:
        particulate_data = json.load(response)
    
    # Fetch gases data
    with urllib.request.urlopen("http://recorder:8090/source/open-meteo/gases?run_id=430d3202_1&attempt=0") as response:
        gases_data = json.load(response)

    # Convert hourly arrays to records for particulate data
    particulate_records = [
        {
            "time": time,
            "pm10": pm10,
            "pm2_5": pm2_5
        }
        for time, pm10, pm2_5 in zip(
            particulate_data["hourly"]["time"],
            particulate_data["hourly"]["pm10"],
            particulate_data["hourly"]["pm2_5"]
        )
    ]

    # Convert hourly arrays to records for gases data
    gases_records = [
        {
            "time": time,
            "nitrogen_dioxide": no2,
            "ozone": ozone
        }
        for time, no2, ozone in zip(
            gases_data["hourly"]["time"],
            gases_data["hourly"]["nitrogen_dioxide"],
            gases_data["hourly"]["ozone"]
        )
    ]

    # Create dictionaries for efficient lookup
    particulate_dict = {r["time"]: r for r in particulate_records}
    gases_dict = {r["time"]: r for r in gases_records}

    # Inner join on exact time values
    joined_records = []
    for time, gas_record in gases_dict.items():
        if time in particulate_dict:
            joined_record = {
                "time": time,
                **particulate_dict[time],
                **gas_record
            }
            joined_records.append(joined_record)

    # Post to sink
    request = urllib.request.Request(
        url="http://recorder:8090/sink/430d3202_1?attempt=0",
        data=json.dumps({"records": joined_records}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_430d3202_1():
    ingest_pipeline()
