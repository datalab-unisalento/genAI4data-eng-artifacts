#!/usr/bin/env python3
"""Compute reference outputs for the extended benchmark tasks from the archived source fixtures.

Outputs contain only counts, calendar dates, and raw source values, so exact sink
comparison does not depend on floating-point rounding conventions.

Usage:
    python scripts/build_reference_outputs.py          # write fixtures/expected/t4-t6
    python scripts/build_reference_outputs.py --check  # fail if committed outputs differ
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "fixtures" / "sources"
EXPECTED = ROOT / "fixtures" / "expected"


def _load(name: str) -> dict[str, Any]:
    return json.loads((SOURCES / name).read_text(encoding="utf-8"))


def daily_particulate_summary(hourly: dict[str, Any]) -> list[dict[str, Any]]:
    days: dict[str, dict[str, Any]] = {}
    for time, pm10, pm2_5 in zip(hourly["time"], hourly["pm10"], hourly["pm2_5"]):
        day = days.setdefault(time[:10], {"hours": 0, "pm2_5_max": None, "pm10_max": None})
        day["hours"] += 1
        day["pm2_5_max"] = pm2_5 if day["pm2_5_max"] is None else max(day["pm2_5_max"], pm2_5)
        day["pm10_max"] = pm10 if day["pm10_max"] is None else max(day["pm10_max"], pm10)
    return [
        {"date": date, "hours": day["hours"], "pm2_5_max": day["pm2_5_max"], "pm10_max": day["pm10_max"]}
        for date, day in sorted(days.items())
    ]


def trailing_three_hour_peak(hourly: dict[str, Any]) -> list[dict[str, Any]]:
    times, values = hourly["time"], hourly["pm2_5"]
    return [
        {"time": times[i], "pm2_5": values[i], "pm2_5_peak_3h": max(values[i - 2 : i + 1])}
        for i in range(2, len(values))
    ]


def _magnitude_band(magnitude: float) -> str:
    if magnitude < 4.0:
        return "minor"
    if magnitude < 5.0:
        return "moderate"
    return "major"


def earthquake_band_summary(geojson: dict[str, Any]) -> list[dict[str, Any]]:
    bands: dict[str, dict[str, Any]] = {}
    for feature in geojson["features"]:
        magnitude = feature["properties"]["mag"]
        depth = feature["geometry"]["coordinates"][2]
        if magnitude is None or depth is None:
            raise ValueError(f"Feature {feature.get('id')} has no magnitude or depth")
        band = bands.setdefault(
            _magnitude_band(magnitude),
            {"event_count": 0, "max_magnitude": None, "max_depth_km": None},
        )
        band["event_count"] += 1
        band["max_magnitude"] = magnitude if band["max_magnitude"] is None else max(band["max_magnitude"], magnitude)
        band["max_depth_km"] = depth if band["max_depth_km"] is None else max(band["max_depth_km"], depth)
    order = ["minor", "moderate", "major"]
    return [
        {"magnitude_band": name, **bands[name]}
        for name in order
        if name in bands
    ]


def build() -> dict[str, list[dict[str, Any]]]:
    particulate = _load("open_meteo_particulate.json")
    earthquakes = _load("usgs_earthquakes.json")
    return {
        "t4_daily_particulate_summary.json": daily_particulate_summary(particulate["hourly"]),
        "t5_trailing_three_hour_peak.json": trailing_three_hour_peak(particulate["hourly"]),
        "t6_earthquake_band_summary.json": earthquake_band_summary(earthquakes),
    }


def _serialize(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, indent=2, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if committed outputs differ")
    args = parser.parse_args()
    failed = False
    for name, rows in build().items():
        path = EXPECTED / name
        text = _serialize(rows)
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != text:
                print(f"MISMATCH {path.relative_to(ROOT)}", file=sys.stderr)
                failed = True
            else:
                print(f"ok {path.relative_to(ROOT)}")
        else:
            path.write_text(text, encoding="utf-8")
            print(f"wrote {path.relative_to(ROOT)} ({len(rows)} rows)")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
