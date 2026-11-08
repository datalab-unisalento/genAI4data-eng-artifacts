from dagster import op, job

import urllib.request
import json


@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=5805bd2f_1&attempt=0"
    sink_url = "http://recorder:8090/sink/5805bd2f_1?attempt=0"

    with urllib.request.urlopen(source_url) as resp:
        data = json.loads(resp.read())

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
        bands.setdefault(band, []).append({
            "mag": mag,
            "depth_km": depth
        })

    records = []
    for band, events in bands.items():
        if not events:
            continue
        event_count = len(events)
        max_magnitude = max(e["mag"] for e in events)
        max_depth_km = max(e["depth_km"] for e in events)
        records.append({
            "magnitude_band": band,
            "event_count": event_count,
            "max_magnitude": max_magnitude,
            "max_depth_km": max_depth_km
        })

    body = json.dumps(records).encode("utf-8")
    req = urllib.request.Request(sink_url, data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req)


@job
def bench_5805bd2f_1():
    ingest_pipeline()
