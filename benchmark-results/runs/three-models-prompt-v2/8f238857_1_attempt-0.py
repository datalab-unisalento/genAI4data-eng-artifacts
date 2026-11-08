from prefect import flow
import json
import urllib.request

@flow(name="bench_8f238857_1")
def bench_8f238857_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=8f238857_1&attempt=0"
    sink_url = "http://recorder:8090/sink/8f238857_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode('utf-8'))

    # Initialize summary dictionary
    bands = {
        "minor": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "moderate": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "major": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')}
    }

    # Process each feature
    for feature in geojson_data['features']:
        mag = feature['properties']['mag']
        depth = feature['geometry']['coordinates'][2]

        # Determine magnitude band and update summary
        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        bands[band]["event_count"] += 1
        if mag > bands[band]["max_magnitude"]:
            bands[band]["max_magnitude"] = mag
        if depth > bands[band]["max_depth_km"]:
            bands[band]["max_depth_km"] = depth

    # Prepare records for sink
    records = []
    for band, stats in bands.items():
        if stats["event_count"] > 0:
            record = {
                "magnitude_band": band,
                "event_count": stats["event_count"],
                "max_magnitude": stats["max_magnitude"],
                "max_depth_km": stats["max_depth_km"]
            }
            records.append(record)

    # Send to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_8f238857_1()
