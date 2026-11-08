from prefect import flow
import json
import urllib.request

@flow(name="bench_2185a214_1")
def bench_2185a214_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=2185a214_1&attempt=0"
    sink_url = "http://recorder:8090/sink/2185a214_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        coordinates = geometry['coordinates']

        magnitude = properties['mag']
        if magnitude < 4.0:
            magnitude_band = 'minor'
        elif magnitude < 5.0:
            magnitude_band = 'moderate'
        else:
            magnitude_band = 'major'

        record = {
            "id": feature['id'],
            "magnitude": magnitude,
            "place": properties['place'],
            "time_ms": properties['time'],
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth_km": coordinates[2],
            "magnitude_band": magnitude_band
        }
        records.append(record)

    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    urllib.request.urlopen(request)

if __name__ == '__main__':
    bench_2185a214_1()
