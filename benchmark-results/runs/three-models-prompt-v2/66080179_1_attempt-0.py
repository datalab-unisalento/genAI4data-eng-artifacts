from prefect import flow
import json
import urllib.request

@flow(name="bench_66080179_1")
def bench_66080179_1():
    source_particulate_url = "http://recorder:8090/source/open-meteo/particulate?run_id=66080179_1&attempt=0"
    source_gases_url = "http://recorder:8090/source/open-meteo/gases?run_id=66080179_1&attempt=0"
    sink_url = "http://recorder:8090/sink/66080179_1?attempt=0"

    # Fetch particulate data
    with urllib.request.urlopen(source_particulate_url) as response:
        particulate_data = json.loads(response.read().decode('utf-8'))

    # Fetch gases data
    with urllib.request.urlopen(source_gases_url) as response:
        gases_data = json.loads(response.read().decode('utf-8'))

    # Convert hourly columns to records for particulates
    particulate_records = []
    time_list = particulate_data['hourly']['time']
    pm10_list = particulate_data['hourly']['pm10']
    pm2_5_list = particulate_data['hourly']['pm2_5']

    for i in range(len(time_list)):
        record = {
            'time': time_list[i],
            'pm10': pm10_list[i],
            'pm2_5': pm2_5_list[i]
        }
        particulate_records.append(record)

    # Convert hourly columns to records for gases
    gases_records = []
    time_list = gases_data['hourly']['time']
    nitrogen_dioxide_list = gases_data['hourly']['nitrogen_dioxide']
    ozone_list = gases_data['hourly']['ozone']

    for i in range(len(time_list)):
        record = {
            'time': time_list[i],
            'nitrogen_dioxide': nitrogen_dioxide_list[i],
            'ozone': ozone_list[i]
        }
        gases_records.append(record)

    # Create a mapping from time to records for gases
    gases_map = {record['time']: record for record in gases_records}

    # Inner join on exact time values
    final_records = []
    for p_record in particulate_records:
        time_key = p_record['time']
        if time_key in gases_map:
            g_record = gases_map[time_key]
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
    bench_66080179_1()
