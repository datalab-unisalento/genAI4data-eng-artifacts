from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=dc50bc51_1&attempt=0"
    sink_url = "http://recorder:8090/sink/dc50bc51_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode())

    # Initialize band summary
    bands = {
        "minor": {"count": 0, "max_mag": float('-inf'), "max_depth": float('-inf')},
        "moderate": {"count": 0, "max_mag": float('-inf'), "max_depth": float('-inf')},
        "major": {"count": 0, "max_mag": float('-inf'), "max_depth": float('-inf')}
    }

    # Process each feature
    for feature in geojson_data["features"]:
        mag = feature["properties"]["mag"]
        depth = feature["geometry"]["coordinates"][2]

        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        bands[band]["count"] += 1
        bands[band]["max_mag"] = max(bands[band]["max_mag"], mag)
        bands[band]["max_depth"] = max(bands[band]["max_depth"], depth)

    # Prepare output records
    records = []
    for band, stats in bands.items():
        if stats["count"] > 0:
            record = {
                "magnitude_band": band,
                "event_count": stats["count"],
                "max_magnitude": stats["max_mag"],
                "max_depth_km": stats["max_depth"]
            }
            records.append(record)

    # Post to sink
    data = json.dumps(records).encode('utf-8')
    req = urllib.request.Request(sink_url, data=data)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_dc50bc51_1():
    ingest_pipeline()
