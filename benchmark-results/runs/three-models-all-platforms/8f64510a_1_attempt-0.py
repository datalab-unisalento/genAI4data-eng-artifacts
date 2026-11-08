from dagster import op, job

import json
import urllib.request

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=8f64510a_1&attempt=0"
    sink_url = "http://recorder:8090/sink/8f64510a_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    # Initialize band statistics
    bands = {
        'minor': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')},
        'moderate': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')},
        'major': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')}
    }

    # Process each earthquake event
    for feature in data['features']:
        mag = feature['properties']['mag']
        depth = feature['geometry']['coordinates'][2]

        if mag < 4.0:
            band = bands['minor']
        elif 4.0 <= mag < 5.0:
            band = bands['moderate']
        else:
            band = bands['major']

        band['count'] += 1
        band['max_mag'] = max(band['max_mag'], mag)
        band['max_depth'] = max(band['max_depth'], depth)

    # Prepare output records
    records = []
    for band_name, stats in bands.items():
        if stats['count'] > 0:
            records.append({
                'magnitude_band': band_name,
                'event_count': stats['count'],
                'max_magnitude': stats['max_mag'],
                'max_depth_km': stats['max_depth']
            })

    # Post results to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_8f64510a_1():
    ingest_pipeline()
