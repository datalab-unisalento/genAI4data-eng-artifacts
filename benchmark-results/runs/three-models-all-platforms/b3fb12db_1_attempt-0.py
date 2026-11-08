import json
import urllib.request
from prefect import flow


@flow
def bench_b3fb12db_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=b3fb12db_1&attempt=0"
    sink_url = "http://recorder:8090/sink/b3fb12db_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode("utf-8"))

    bands = {}
    for feature in data["features"]:
        mag = feature["properties"]["mag"]
        depth = feature["geometry"]["coordinates"][2]
        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        if band not in bands:
            bands[band] = []
        bands[band].append({"mag": mag, "depth_km": depth})

    records = []
    for band, events in bands.items():
        event_count = len(events)
        max_magnitude = max(e["mag"] for e in events)
        max_depth_km = max(e["depth_km"] for e in events)
        records.append({
            "magnitude_band": band,
            "event_count": event_count,
            "max_magnitude": max_magnitude,
            "max_depth_km": max_depth_km
        })

    body = json.dumps(records)
    request = urllib.request.Request(
        sink_url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(request) as response:
        _ = response.read()


if __name__ == "__main__":
    bench_b3fb12db_1()
