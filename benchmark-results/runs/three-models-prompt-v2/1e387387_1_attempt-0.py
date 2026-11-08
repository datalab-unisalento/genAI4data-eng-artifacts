from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=1e387387_1&attempt=0"
    sink_url = "http://recorder:8090/sink/1e387387_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode())

    records = []
    for feature in geojson_data["features"]:
        props = feature["properties"]
        geom = feature["geometry"]
        
        record = {
            "id": feature["id"],
            "magnitude": props["mag"],
            "place": props["place"],
            "time_ms": props["time"],
            "longitude": geom["coordinates"][0],
            "latitude": geom["coordinates"][1],
            "depth_km": geom["coordinates"][2],
            "magnitude_band": (
                "minor" if props["mag"] < 4.0 else
                "moderate" if props["mag"] < 5.0 else
                "major"
            )
        }
        records.append(record)

    # Post to sink
    payload = {"records": records}
    json_data = json.dumps(payload).encode('utf-8')
    
    req = urllib.request.Request(sink_url, data=json_data)
    req.add_header('Content-Type', 'application/json')
    
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_1e387387_1():
    ingest_pipeline()
