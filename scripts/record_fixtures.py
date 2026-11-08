#!/usr/bin/env python3
"""Download dated source responses and write source plus expected-output fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "fixtures"
SOURCE_DIR = FIXTURE_DIR / "sources"
EXPECTED_DIR = FIXTURE_DIR / "expected"
OPEN_METEO_ENDPOINT = "https://air-quality-api.open-meteo.com/v1/air-quality"
USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def fetch_json(url: str) -> tuple[dict[str, Any], bytes]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "pipeline-generation-benchmark/0.1 (fixture archival)"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"Unable to fetch fixture from {url}: {exc}") from exc
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Source returned invalid JSON at {url}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object from {url}")
    return payload, body


def save_source(source_id: str, description: str, url: str, payload: dict[str, Any], response: bytes) -> dict[str, Any]:
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    payload_path = SOURCE_DIR / f"{source_id}.json"
    payload_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    metadata = {
        "source_id": source_id,
        "description": description,
        "request_url": url,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "http_status": 200,
        "response_sha256": hashlib.sha256(response).hexdigest(),
        "saved_json_sha256": hashlib.sha256(payload_path.read_bytes()).hexdigest(),
        "payload_file": str(payload_path.relative_to(ROOT)),
        "payload_bytes": payload_path.stat().st_size,
    }
    metadata_path = SOURCE_DIR / f"{source_id}.metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return metadata


def hourly_records(payload: dict[str, Any], variables: list[str]) -> list[dict[str, Any]]:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
        raise RuntimeError("Open-Meteo fixture is missing hourly.time")
    times = hourly["time"]
    for variable in variables:
        if not isinstance(hourly.get(variable), list) or len(hourly[variable]) != len(times):
            raise RuntimeError(f"Open-Meteo hourly.{variable} is missing or has a different length")
    return [
        {"time": timestamp, **{variable: hourly[variable][index] for variable in variables}}
        for index, timestamp in enumerate(times)
    ]


def earthquake_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    features = payload.get("features")
    if not isinstance(features, list):
        raise RuntimeError("USGS fixture is missing a features array")
    rows = []
    for feature in features:
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") or []
        magnitude = properties.get("mag")
        if magnitude is None or len(coordinates) < 3:
            continue
        band = "minor" if magnitude < 4.0 else "moderate" if magnitude < 5.0 else "major"
        rows.append({
            "id": feature.get("id"),
            "magnitude": magnitude,
            "place": properties.get("place"),
            "time_ms": properties.get("time"),
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth_km": coordinates[2],
            "magnitude_band": band,
        })
    return rows


def save_expected(task_id: str, rows: list[dict[str, Any]]) -> str:
    EXPECTED_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPECTED_DIR / f"{task_id}.json"
    path.write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return str(path.relative_to(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2024-01-02", help="Inclusive date, per Open-Meteo API")
    parser.add_argument("--latitude", type=float, default=37.7749)
    parser.add_argument("--longitude", type=float, default=-122.4194)
    parser.add_argument("--usgs-min-magnitude", type=float, default=4.5)
    args = parser.parse_args()

    common_open_meteo = {
        "latitude": args.latitude,
        "longitude": args.longitude,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "timezone": "GMT",
    }
    particulate_url = OPEN_METEO_ENDPOINT + "?" + urllib.parse.urlencode({
        **common_open_meteo,
        "hourly": "pm10,pm2_5",
    })
    gases_url = OPEN_METEO_ENDPOINT + "?" + urllib.parse.urlencode({
        **common_open_meteo,
        "hourly": "nitrogen_dioxide,ozone",
    })
    usgs_url = USGS_ENDPOINT + "?" + urllib.parse.urlencode({
        "format": "geojson",
        "starttime": args.start_date,
        "endtime": args.end_date,
        "minmagnitude": args.usgs_min_magnitude,
        "orderby": "time-asc",
    })

    source_specs = [
        (
            "open_meteo_particulate",
            "Open-Meteo hourly PM10 and PM2.5 observations",
            particulate_url,
        ),
        (
            "open_meteo_gases",
            "Open-Meteo hourly nitrogen dioxide and ozone observations",
            gases_url,
        ),
        (
            "usgs_earthquakes",
            "USGS earthquake events as GeoJSON",
            usgs_url,
        ),
    ]
    payloads: dict[str, dict[str, Any]] = {}
    source_metadata = []
    for source_id, description, url in source_specs:
        payload, response = fetch_json(url)
        payloads[source_id] = payload
        metadata = save_source(source_id, description, url, payload, response)
        source_metadata.append(metadata)
        print(f"Saved {source_id}: {metadata['payload_bytes']} bytes, SHA-256 {metadata['saved_json_sha256']}")

    particulate_rows = hourly_records(payloads["open_meteo_particulate"], ["pm10", "pm2_5"])
    gas_rows = hourly_records(payloads["open_meteo_gases"], ["nitrogen_dioxide", "ozone"])
    gas_by_time = {row["time"]: row for row in gas_rows}
    joined_rows = [
        {**row, **gas_by_time[row["time"]]}
        for row in particulate_rows
        if row["time"] in gas_by_time
    ]
    usgs_rows = earthquake_records(payloads["usgs_earthquakes"])

    task_rows = {
        "t1_open_meteo_ingestion": particulate_rows,
        "t2_usgs_etl": usgs_rows,
        "t3_open_meteo_join": joined_rows,
    }
    expected_metadata = {}
    for task_id, rows in task_rows.items():
        expected_metadata[task_id] = {
            "expected_file": save_expected(task_id, rows),
            "expected_count": len(rows),
        }

    capture = {
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "request_parameters": {
            "latitude": args.latitude,
            "longitude": args.longitude,
            "open_meteo_start_date": args.start_date,
            "open_meteo_end_date_inclusive": args.end_date,
            "open_meteo_timezone": "GMT",
            "usgs_starttime": args.start_date,
            "usgs_endtime": args.end_date,
            "usgs_minmagnitude": args.usgs_min_magnitude,
            "usgs_orderby": "time-asc",
        },
        "sources": source_metadata,
        "expected_outputs": expected_metadata,
    }
    (FIXTURE_DIR / "capture.json").write_text(json.dumps(capture, indent=2) + "\n", encoding="utf-8")
    print(f"Saved capture manifest to {FIXTURE_DIR / 'capture.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
