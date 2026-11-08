from prefect import flow
import json
import urllib.request

@flow
def bench_3424cdfa_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=3424cdfa_1&attempt=0"
    sink_url = "http://recorder:8090/sink/3424cdfa_1?attempt=0"

    # Fetch source data
    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode('utf-8'))

    records = []
    for feature in data['features']:
        coords = feature['geometry']['coordinates']
        
        record = {
            'id': feature['id'],
            'magnitude': feature['properties']['mag'],
            'place': feature['properties']['place'],
            'time_ms': feature['properties']['time'],
            'longitude': coords[0],
            'latitude': coords[1], 
            'depth_km': coords[2]
        }

        # Determine magnitude band
        mag = record['magnitude']
        if mag < 4.0:
            record['magnitude_band'] = 'minor'
        elif 4.0 <= mag < 5.0:
            record['magnitude_band'] = 'moderate'
        else:
            record['magnitude_band'] = 'major'

        records.append(record)

    # Post to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_3424cdfa_1()
