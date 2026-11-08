from prefect import flow
import json
import urllib.request

@flow(name="bench_3f89bbb3_1")
def bench_3f89bbb3_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=3f89bbb3_1&attempt=0"
    sink_url = "http://recorder:8090/sink/3f89bbb3_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in geojson_data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        coordinates = geometry['coordinates']

        magnitude_band = "minor"
        if 4.0 <= properties['mag'] < 5.0:
            magnitude_band = "moderate"
        elif properties['mag'] >= 5.0:
            magnitude_band = "major"

        record = {
            'id': feature['id'],
            'magnitude': properties['mag'],
            'place': properties['place'],
            'time_ms': properties['time'],
            'longitude': coordinates[0],
            'latitude': coordinates[1],
            'depth_km': coordinates[2],
            'magnitude_band': magnitude_band
        }

        records.append(record)

    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_3f89bbb3_1()
