from prefect import flow
import json
import urllib.request

@flow(name="bench_119a2bc0_1")
def bench_119a2bc0_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=119a2bc0_1&attempt=0"
    sink_url = "http://recorder:8090/sink/119a2bc0_1?attempt=0"

    # Fetch GeoJSON from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in data['features']:
        props = feature['properties']
        geom = feature['geometry']
        coords = geom['coordinates']

        # Determine magnitude_band
        mag = props['mag']
        if mag < 4.0:
            magnitude_band = 'minor'
        elif mag < 5.0:
            magnitude_band = 'moderate'
        else:
            magnitude_band = 'major'

        record = {
            "id": feature['id'],
            "magnitude": mag,
            "place": props['place'],
            "time_ms": props['time'],
            "longitude": coords[0],
            "latitude": coords[1],
            "depth_km": coords[2],
            "magnitude_band": magnitude_band
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
    bench_119a2bc0_1()
