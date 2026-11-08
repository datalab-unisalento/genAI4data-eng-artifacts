from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=5abc4bae_1&attempt=0"
    sink_url = "http://recorder:8090/sink/5abc4bae_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode())

    records = []
    for feature in geojson_data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]
        coordinates = geometry["coordinates"]

        record = {
            "id": feature["id"],
            "magnitude": properties["mag"],
            "place": properties["place"],
            "time_ms": properties["time"],
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth_km": coordinates[2],
            "magnitude_band": (
                "minor" if properties["mag"] < 4.0 else
                "moderate" if 4.0 <= properties["mag"] < 5.0 else
                "major"
            )
        }
        records.append(record)

    # Post to sink
    data = json.dumps({"records": records}).encode("utf-8")
    req = urllib.request.Request(sink_url, data=data)
    req.add_header("Content-Type", "application/json")

    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_5abc4bae_1():
    ingest_pipeline()
