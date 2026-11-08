from prefect import flow
import json
import urllib.request

@flow(name="bench_ec95a67f_1")
def bench_ec95a67f_1():
    source_particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=ec95a67f_1&attempt=0"
    source_gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=ec95a67f_1&attempt=0"
    sink_url = "http://recorder:8090/sink/ec95a67f_1?attempt=0"

    # Fetch particulate data
    with urllib.request.urlopen(source_particulate_url) as response:
        particulate_data = json.loads(response.read().decode('utf-8'))

    # Fetch gases data
    with urllib.request.urlopen(source_gases_url) as response:
        gases_data = json.loads(response.read().decode('utf-8'))

    # Convert hourly columns to records for particulates
    particulate_records = []
    times = particulate_data['hourly']['time']
    pm10_values = particulate_data['hourly']['pm10']
    pm2_5_values = particulate_data['hourly']['pm2_5']

    for i in range(len(times)):
        record = {
            'time': times[i],
            'pm10': pm10_values[i],
            'pm2_5': pm2_5_values[i]
        }
        particulate_records.append(record)

    # Convert hourly columns to records for gases
    gases_records = []
    times = gases_data['hourly']['time']
    no2_values = gases_data['hourly']['nitrogen_dioxide']
    o3_values = gases_data['hourly']['ozone']

    for i in range(len(times)):
        record = {
            'time': times[i],
            'nitrogen_dioxide': no2_values[i],
            'ozone': o3_values[i]
        }
        gases_records.append(record)

    # Create lookup dict from gases records by time
    gases_lookup = {record['time']: record for record in gases_records}

    # Inner join on exact time
    final_records = []
    for p_record in particulate_records:
        time_key = p_record['time']
        if time_key in gases_lookup:
            g_record = gases_lookup[time_key]
            combined_record = {
                'time': time_key,
                'pm10': p_record['pm10'],
                'pm2_5': p_record['pm2_5'],
                'nitrogen_dioxide': g_record['nitrogen_dioxide'],
                'ozone': g_record['ozone']
            }
            final_records.append(combined_record)

    # Post to sink
    request = urllib.request.Request(
        sink_url,
        data=json.dumps(final_records).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(request) as response:
        pass

if __name__ == '__main__':
    bench_ec95a67f_1()
