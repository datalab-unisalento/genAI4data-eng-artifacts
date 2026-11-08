from dagster import op, job

import urllib.request
import json


@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=6908bb77_1&attempt=0"
    sink_url = "http://recorder:8090/sink/6908bb77_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        source_data = json.loads(response.read().decode())

    records = []
    for feature in source_data["features"]:
        props = feature["properties"]
        geom = feature["geometry"]
        mag = props["mag"]
        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        record = {
            "id": feature["id"],
            "magnitude": mag,
            "place": props["place"],
            "time_ms": props["time"],
            "longitude": geom["coordinates"][0],
            "latitude": geom["coordinates"][1],
            "depth_km": geom["coordinates"][2],
            "magnitude_band": band
        }
        records.append(record)

    body = json.dumps(records).encode("utf-8")
    req = urllib.request.Request(sink_url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode())


@job
def bench_6908bb77_1():
    ingest_pipeline()
