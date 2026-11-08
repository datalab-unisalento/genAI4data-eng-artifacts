import json
import urllib.request
from prefect import flow


@flow
def bench_c6f41053_1():
    source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=c6f41053_1&attempt=0"
    sink_url = "http://recorder:8090/sink/c6f41053_1?attempt=0"

    with urllib.request.urlopen(source_url) as response:
        data = json.loads(response.read().decode("utf-8"))

    bands = {}
    for feature in data["features"]:
        mag = feature["properties"]["mag"]
        depth = feature["geometry"]["coordinates"][2]
        if mag < 4.0:
            band = "minor"
        elif mag < 5.0:
            band = "moderate"
        else:
            band = "major"

        if band not in bands:
            bands[band] = {"count": 0, "max_mag": -float("inf"), "max_depth": -float("inf")}
        bands[band]["count"] += 1
        if mag > bands[band]["max_mag"]:
            bands[band]["max_mag"] = mag
        if depth > bands[band]["max_depth"]:
            bands[band]["max_depth"] = depth

    records = []
    for band, info in bands.items():
        if info["count"] > 0:
            records.append({
                "magnitude_band": band,
                "event_count": info["count"],
                "max_magnitude": info["max_mag"],
                "max_depth_km": info["max_depth"]
            })

    body = json.dumps(records)
    request = urllib.request.Request(
        sink_url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(request) as response:
        _ = response.read().decode("utf-8")


if __name__ == "__main__":
    bench_c6f41053_1()
