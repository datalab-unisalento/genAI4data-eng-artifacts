from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=12da8cb3_1&attempt=0"
    sink_url = "http://recorder:8090/sink/12da8cb3_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode())

    # Initialize band summary
    bands = {
        "minor": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "moderate": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "major": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')}
    }

    # Process each feature
    for feature in geojson_data["features"]:
        mag = feature["properties"]["mag"]
        depth = feature["geometry"]["coordinates"][2]

        # Classify magnitude band
        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        # Update summary for the band
        bands[band]["event_count"] += 1
        bands[band]["max_magnitude"] = max(bands[band]["max_magnitude"], mag)
        bands[band]["max_depth_km"] = max(bands[band]["max_depth_km"], depth)

    # Prepare output records (only bands with events)
    result_records = []
    for band, summary in bands.items():
        if summary["event_count"] > 0:
            record = {
                "magnitude_band": band,
                "event_count": summary["event_count"],
                "max_magnitude": summary["max_magnitude"],
                "max_depth_km": summary["max_depth_km"]
            }
            result_records.append(record)

    # POST to sink
    request_data = json.dumps(result_records).encode('utf-8')
    req = urllib.request.Request(sink_url, data=request_data)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_12da8cb3_1():
    ingest_pipeline()
