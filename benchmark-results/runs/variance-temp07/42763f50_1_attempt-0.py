from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=42763f50_1&attempt=0"
    sink_url = "http://recorder:8090/sink/42763f50_1?attempt=0"

    # Fetch the source data
    with urllib.request.urlopen(source_url) as response:
        geojson_data = json.loads(response.read().decode())

    records = []
    for feature in geojson_data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]
        
        record = {
            "id": feature["id"],
            "magnitude": properties["mag"],
            "place": properties["place"],
            "time_ms": properties["time"],
            "longitude": geometry["coordinates"][0],
            "latitude": geometry["coordinates"][1],
            "depth_km": geometry["coordinates"][2],
        }
        
        # Derive magnitude_band
        mag = record["magnitude"]
        if mag < 4.0:
            record["magnitude_band"] = "minor"
        elif mag < 5.0:
            record["magnitude_band"] = "moderate"
        else:
            record["magnitude_band"] = "major"

        records.append(record)

    # Post to sink
    payload = {"records": records}
    json_data = json.dumps(payload).encode('utf-8')
    
    req = urllib.request.Request(sink_url, data=json_data)
    req.add_header('Content-Type', 'application/json')
    
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_42763f50_1():
    ingest_pipeline()
