from prefect import flow
import json
import urllib.request


@flow
def bench_88525330_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=88525330_1&attempt=0"
    sink_url = "http://recorder:8090/sink/88525330_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in data["features"]:
        props = feature["properties"]
        geom = feature["geometry"]
        magnitude = props["mag"]
        if magnitude < 4.0:
            magnitude_band = "minor"
        elif magnitude < 5.0:
            magnitude_band = "moderate"
        else:
            magnitude_band = "major"

        record = {
            "id": feature["id"],
            "magnitude": magnitude,
            "place": props["place"],
            "time_ms": props["time"],
            "longitude": geom["coordinates"][0],
            "latitude": geom["coordinates"][1],
            "depth_km": geom["coordinates"][2],
            "magnitude_band": magnitude_band
        }
        records.append(record)

    body = json.dumps(records).encode('utf-8')
    request = urllib.request.Request(sink_url, data=body, headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(request) as response:
        pass


if __name__ == '__main__':
    bench_88525330_1()
