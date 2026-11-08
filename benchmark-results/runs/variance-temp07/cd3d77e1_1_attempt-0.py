import json
import urllib.request
from prefect import flow

@flow(name="bench_cd3d77e1_1")
def bench_cd3d77e1_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=cd3d77e1_1&attempt=0"
    sink_url = "http://recorder:8090/sink/cd3d77e1_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in geojson_data['features']:
        properties = feature['properties']
        geometry = feature['geometry']
        coordinates = geometry['coordinates']

        magnitude = properties['mag']
        place = properties['place']
        time_ms = properties['time']
        longitude = coordinates[0]
        latitude = coordinates[1]
        depth_km = coordinates[2] if len(coordinates) > 2 else None

        # Derive magnitude_band
        if magnitude < 4.0:
            magnitude_band = 'minor'
        elif magnitude < 5.0:
            magnitude_band = 'moderate'
        else:
            magnitude_band = 'major'

        record = {
            "id": feature['id'],
            "magnitude": magnitude,
            "place": place,
            "time_ms": time_ms,
            "longitude": longitude,
            "latitude": latitude,
            "depth_km": depth_km,
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
    bench_cd3d77e1_1()
