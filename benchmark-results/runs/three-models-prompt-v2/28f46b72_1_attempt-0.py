from prefect import flow
import json
import urllib.request

@flow
def bench_28f46b72_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=28f46b72_1&attempt=0"
    sink_url = "http://recorder:8090/sink/28f46b72_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    
    for feature in data['features']:
        magnitude_band = "minor"
        if feature['properties']['mag'] >= 4.0 and feature['properties']['mag'] < 5.0:
            magnitude_band = "moderate"
        elif feature['properties']['mag'] >= 5.0:
            magnitude_band = "major"

        record = {
            'id': feature['id'],
            'magnitude': feature['properties']['mag'],
            'place': feature['properties']['place'],
            'time_ms': feature['properties']['time'],
            'longitude': feature['geometry']['coordinates'][0],
            'latitude': feature['geometry']['coordinates'][1],
            'depth_km': feature['geometry']['coordinates'][2],
            'magnitude_band': magnitude_band
        }
        
        records.append(record)

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
    bench_28f46b72_1()
