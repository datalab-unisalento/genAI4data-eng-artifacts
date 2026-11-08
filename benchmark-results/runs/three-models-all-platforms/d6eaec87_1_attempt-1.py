from dagster import op, job

import json
import urllib.request

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=d6eaec87_1&attempt=1"
    sink_url = "http://recorder:8090/sink/d6eaec87_1?attempt=1"

    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    records = []
    for feature in data["features"]:
        magnitude = feature["properties"]["mag"]
        
        # Determine magnitude_band
        if magnitude < 4.0:
            magnitude_band = "minor"
        elif 4.0 <= magnitude < 5.0:
            magnitude_band = "moderate"
        else:
            magnitude_band = "major"

        record = {
            "id": feature["properties"]["mag"],
            "magnitude": magnitude,
            "place": feature["properties"]["place"],
            "time_ms": feature["properties"]["time"],
            "longitude": feature["geometry"]["coordinates"][0],
            "latitude": feature["geometry"]["coordinates"][1],
            "depth_km": feature["geometry"]["coordinates"][2],
            "magnitude_band": magnitude_band
        }
        records.append(record)

    # Prepare the request body
    payload = {"records": records}
    
    # Create and send POST request
    req = urllib.request.Request(
        sink_url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_d6eaec87_1():
    ingest_pipeline()
