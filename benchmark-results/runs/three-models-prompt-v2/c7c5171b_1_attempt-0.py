from dagster import op, job

import json
import urllib.request

@op
def ingest_pipeline():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=c7c5171b_1&attempt=0"
    sink_url = "http://recorder:8090/sink/c7c5171b_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.load(response)

    bands = {
        'minor': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')},
        'moderate': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')},
        'major': {'count': 0, 'max_mag': -float('inf'), 'max_depth': -float('inf')}
    }

    for feature in data['features']:
        mag = feature['properties']['mag']
        depth = feature['geometry']['coordinates'][2]

        if mag < 4.0:
            band = bands['minor']
        elif mag >= 5.0:
            band = bands['major']
        else:
            band = bands['moderate']

        band['count'] += 1
        band['max_mag'] = max(band['max_mag'], mag)
        band['max_depth'] = max(band['max_depth'], depth)

    results = []
    for band_name, stats in bands.items():
        if stats['count'] > 0:
            results.append({
                'magnitude_band': band_name,
                'event_count': stats['count'],
                'max_magnitude': stats['max_mag'],
                'max_depth_km': stats['max_depth']
            })

    request = urllib.request.Request(
        sink_url,
        data=json.dumps(results).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

@job
def bench_c7c5171b_1():
    ingest_pipeline()
