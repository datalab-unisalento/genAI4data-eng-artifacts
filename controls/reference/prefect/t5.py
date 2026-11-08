from prefect import flow

import json
import urllib.request

SOURCES = {"particulate": "http://recorder:8090/source/open-meteo/particulate?run_id=ref-t5&attempt=0"}
SINK_URL = "http://recorder:8090/sink/ref-t5?attempt=0"


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read())


def transform(s):
    h = s["particulate"]["hourly"]
    v = h["pm2_5"]
    return [{"time": h["time"][i], "pm2_5": v[i], "pm2_5_peak_3h": max(v[i - 2:i + 1])}
            for i in range(2, len(v))]


def deliver(records):
    request = urllib.request.Request(
        SINK_URL,
        data=json.dumps(records).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()


def run():
    data = {name: fetch(url) for name, url in SOURCES.items()}
    deliver(transform(data))


@flow(name="bench_ref_t5")
def bench_ref_t5():
    run()


if __name__ == "__main__":
    bench_ref_t5()
