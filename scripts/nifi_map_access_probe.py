#!/usr/bin/env python3
"""Probe how NiFi's ExecuteGroovyScript resolves `.properties` on JSON maps.

Four probe artifacts read the T2 USGS fixture and post one row per feature
(`id`, `magnitude`). Each differs only in the expression that reads
`feature.properties.mag`. A fifth probe checks the same Groovy expressions on
maps built in the script, to separate NiFi behaviour from Groovy language behaviour.

Outcome per probe: number of rows delivered, number with a non-null magnitude,
and the raw NiFi error text if the processor failed.

Output: benchmark-results/probes/nifi-map-access.json and .md
Usage:
    python scripts/nifi_map_access_probe.py
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipeline_bench.cli import _reset_sink  # noqa: E402
from pipeline_bench.platforms import (  # noqa: E402
    PlatformError,
    cleanup_staged_artifact,
    run_nifi,
    start_recorder,
    stop_services,
)

RECORDER = "http://localhost:8090"
PROBE_DIR = ROOT / "controls" / "probes" / "nifi"
OUT_DIR = ROOT / "benchmark-results" / "probes"

# Expression that reads feature.properties.mag, varied only in how `properties` is accessed.
FEATURE_VARIANTS = {
    "dot": "f.properties.mag",
    "bracket": "f.properties['mag']",
    "get_get": "f.get('properties').get('mag')",
    "get_dot": "f.get('properties').mag",
}

TEMPLATE = """import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def usgs = new JsonSlurper().parseText(new URL("http://recorder:8090/source/usgs/earthquakes?run_id={run_id}&attempt=0").text)
def rows = usgs.features.collect {{ f -> [id: f.id, magnitude: {expr}] }}

def connection = new URL("http://recorder:8090/sink/{run_id}?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") {{ it << JsonOutput.toJson(rows) }}
if (connection.responseCode >= 300) {{
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
"""

# Same accesses on a map built in the script (plain) and on a parsed JSON map (parsed).
SEMANTICS_TEMPLATE = """import groovy.json.JsonOutput

def plain = [properties: [mag: 5.0], mag: 1.0]
def parsed = new groovy.json.JsonSlurper().parseText('{{"properties": {{"mag": 5.0}}, "mag": 1.0}}')
def checks = [
    [check: "plain_map_dot_properties_mag", value: String.valueOf(plain.properties?.mag)],
    [check: "parsed_map_dot_properties_mag", value: String.valueOf(parsed.properties?.mag)],
    [check: "parsed_map_bracket_properties_mag", value: String.valueOf(parsed['properties']?.get('mag'))],
    [check: "parsed_map_get_properties_mag", value: String.valueOf(parsed.get('properties')?.get('mag'))],
]

def connection = new URL("http://recorder:8090/sink/{run_id}?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") {{ it << JsonOutput.toJson(checks) }}
if (connection.responseCode >= 300) {{
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
"""


def _write_artifacts() -> dict[str, Path]:
    PROBE_DIR.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, Path] = {}
    for name, expr in FEATURE_VARIANTS.items():
        path = PROBE_DIR / f"map_access_{name}.groovy"
        path.write_text(TEMPLATE.format(run_id=f"probe-{name}", expr=expr), encoding="utf-8")
        artifacts[name] = path
    path = PROBE_DIR / "semantics.groovy"
    path.write_text(SEMANTICS_TEMPLATE.format(run_id="probe-semantics"), encoding="utf-8")
    artifacts["semantics"] = path
    return artifacts


def _sink_rows(run_id: str) -> list[dict]:
    with urllib.request.urlopen(f"{RECORDER}/sink/{run_id}?attempt=0", timeout=30) as response:
        return json.loads(response.read().decode("utf-8")).get("rows", [])


def run_probe(name: str, artifact: Path) -> dict:
    run_id = f"probe-{name}"
    record: dict = {"probe": name, "artifact": str(artifact.relative_to(ROOT)), "error": None}
    started = time.monotonic()
    rows: list[dict] = []
    try:
        start_recorder()
        _reset_sink(RECORDER, run_id, 0)
        run_nifi(artifact, run_id, 300)
    except (PlatformError, RuntimeError, ValueError, OSError) as exc:
        record["error"] = str(exc)[:500]
    finally:
        record["duration_seconds"] = round(time.monotonic() - started, 1)
        try:
            rows = _sink_rows(run_id)
        except (OSError, ValueError) as exc:
            record["sink_error"] = str(exc)[:200]
        record["rows"] = rows
        try:
            stop_services()
        except PlatformError as exc:
            record["cleanup_error"] = str(exc)[:300]
        cleanup_staged_artifact("nifi", run_id, artifact)
    if name != "semantics":
        record["delivered"] = len(rows)
        record["non_null_magnitude"] = sum(1 for row in rows if row.get("magnitude") is not None)
    return record


def _markdown(records: list[dict]) -> str:
    lines = [
        "| Probe | Expression | Delivered rows | Non-null magnitude | Processor error |",
        "|---|---|---|---|---|",
    ]
    for record in records:
        if record["probe"] == "semantics":
            continue
        expr = FEATURE_VARIANTS[record["probe"]]
        error = (record["error"] or "—").replace("|", "/")[:120]
        lines.append(
            f"| {record['probe']} | `{expr}` | {record.get('delivered', 0)} | "
            f"{record.get('non_null_magnitude', 0)} | {error} |"
        )
    semantics = next(r for r in records if r["probe"] == "semantics")
    lines += ["", "Groovy semantics check (same NiFi processor):", "", "| Check | Value |", "|---|---|"]
    for row in semantics["rows"]:
        lines.append(f"| {row['check']} | {row['value']} |")
    if semantics["error"]:
        lines.append(f"\nSemantics probe error: {semantics['error']}")
    return "\n".join(lines) + "\n"


def main() -> int:
    artifacts = _write_artifacts()
    records = []
    for name, artifact in artifacts.items():
        print(f"probe {name}", flush=True)
        records.append(run_probe(name, artifact))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "nifi-map-access.json").write_text(json.dumps(records, indent=2), encoding="utf-8")
    (OUT_DIR / "nifi-map-access.md").write_text(_markdown(records), encoding="utf-8")
    print((OUT_DIR / "nifi-map-access.md").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
