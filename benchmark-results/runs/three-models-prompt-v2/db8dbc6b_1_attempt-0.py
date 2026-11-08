from dagster import op, job

import urllib.request
import json


@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=db8dbc6b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/db8dbc6b_1?attempt=0"

    with urllib.request.urlopen(source_url) as resp:
        data = json.loads(resp.read().decode())

    records = []
    for feature in data["features"]:
        props = feature["properties"]
        geom = feature["geometry"]
        coords = geom["coordinates"]

        magnitude = props["mag"]
        if magnitude < 4.0:
            mag_band = "minor"
        elif magnitude < 5.0:
            mag_band = "moderate"
        else:
            mag_band = "major"

        record = {
            "id": feature["id"],
            "magnitude": magnitude,
            "place": props["place"],
            "time_ms": props["time"],
            "longitude": coords[0],
            "latitude": coords[1],
            "depth_km": coords[2],
            "magnitude_band": mag_band
        }
        records.append(record)

    body = json.dumps(records).encode("utf-8")
    req = urllib.request.Request(
        sink_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


@job
def bench_db8dbc6b_1():
    ingest_pipeline()
