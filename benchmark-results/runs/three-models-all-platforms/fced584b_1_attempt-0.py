from prefect import flow
import json
import urllib.request

@flow(name="bench_fced584b_1")
def bench_fced584b_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=fced584b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/fced584b_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Initialize summary records by magnitude band
    bands = {
        "minor": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "moderate": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')},
        "major": {"event_count": 0, "max_magnitude": float('-inf'), "max_depth_km": float('-inf')}
    }

    # Process each feature
    for feature in data['features']:
        mag = feature['properties']['mag']
        depth = feature['geometry']['coordinates'][2]

        # Determine magnitude band and update summary
        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        record = bands[band]
        record['event_count'] += 1
        if mag > record['max_magnitude']:
            record['max_magnitude'] = mag
        if depth > record['max_depth_km']:
            record['max_depth_km'] = depth

    # Build final records list with required fields
    records = []
    for band, values in bands.items():
        if values['event_count'] > 0:
            records.append({
                "magnitude_band": band,
                "event_count": values['event_count'],
                "max_magnitude": values['max_magnitude'],
                "max_depth_km": values['max_depth_km']
            })

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
    bench_fced584b_1()
