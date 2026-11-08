from dagster import op, job
import urllib.request
import json

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=28e565ee_1&attempt=0"
    sink_url = "http://recorder:8090/sink/28e565ee_1?attempt=0"

    # Fetch data from source
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read())

    records = []
    for feature in data['features']:
        props = feature['properties']
        geom = feature['geometry']
        
        record = {
            'id': feature['id'],
            'magnitude': props['mag'],
            'place': props['place'],
            'time_ms': props['time'],
            'longitude': geom['coordinates'][0],
            'latitude': geom['coordinates'][1],
            'depth_km': geom['coordinates'][2]
        }
        
        mag = record['magnitude']
        if mag < 4.0:
            record['magnitude_band'] = 'minor'
        elif mag < 5.0:
            record['magnitude_band'] = 'moderate'
        else:
            record['magnitude_band'] = 'major'

        records.append(record)

    # Post to sink
    payload = json.dumps({'records': records}).encode('utf-8')
    req = urllib.request.Request(sink_url, data=payload)
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as response:
        pass

@job
def bench_28e565ee_1():
    ingest_pipeline()
