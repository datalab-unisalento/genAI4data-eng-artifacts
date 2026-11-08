#!/usr/bin/env python3
"""Generate hand-written reference artifacts for every platform x task cell.

Each artifact implements the task correctly on its platform. They are the
validation controls: a cell whose reference artifact does not score 100% points
to a harness problem, not a model problem.

Output: controls/reference/<platform>/<task_prefix>.<ext>, one file per cell,
with run id ref-<task_prefix> (for example ref-t1).

Usage:
    python scripts/build_reference_artifacts.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipeline_bench.benchmark import get_task, load_benchmark  # noqa: E402
from pipeline_bench.prompts import build_prompt  # noqa: E402

OUT = ROOT / "controls" / "reference"
TASKS = [
    "t1_open_meteo_ingestion",
    "t2_usgs_etl",
    "t3_open_meteo_join",
    "t4_daily_particulate_summary",
    "t5_trailing_three_hour_peak",
    "t6_earthquake_band_summary",
]
PLATFORMS = {"airflow": "py", "dagster": "py", "prefect": "py", "kestra": "yml", "n8n": "json", "nifi": "groovy"}
RECORDER = "http://recorder:8090"

# Source ids each task reads, in order. n8n names its HTTP nodes "Source 1", "Source 2", ...
SOURCES = {
    "t1": ["open_meteo_particulate"],
    "t2": ["usgs_earthquakes"],
    "t3": ["open_meteo_particulate", "open_meteo_gases"],
    "t4": ["open_meteo_particulate"],
    "t5": ["open_meteo_particulate"],
    "t6": ["usgs_earthquakes"],
}
SOURCE_KEY = {
    "open_meteo_particulate": "particulate",
    "open_meteo_gases": "gases",
    "usgs_earthquakes": "usgs",
}

# ---------- transforms, one per language ----------

PY_TRANSFORM = {
    "t1": '''def transform(s):
    h = s["particulate"]["hourly"]
    return [{"time": t, "pm10": p10, "pm2_5": p25} for t, p10, p25 in zip(h["time"], h["pm10"], h["pm2_5"])]''',
    "t2": '''def transform(s):
    out = []
    for f in s["usgs"]["features"]:
        mag = f["properties"]["mag"]
        lon, lat, depth = f["geometry"]["coordinates"]
        out.append({
            "id": f["id"], "magnitude": mag, "place": f["properties"]["place"],
            "time_ms": f["properties"]["time"], "longitude": lon, "latitude": lat, "depth_km": depth,
            "magnitude_band": "minor" if mag < 4.0 else ("moderate" if mag < 5.0 else "major"),
        })
    return out''',
    "t3": '''def transform(s):
    p = s["particulate"]["hourly"]
    g = s["gases"]["hourly"]
    gases = {t: (no2, o3) for t, no2, o3 in zip(g["time"], g["nitrogen_dioxide"], g["ozone"])}
    out = []
    for t, p10, p25 in zip(p["time"], p["pm10"], p["pm2_5"]):
        if t in gases:
            out.append({"time": t, "pm10": p10, "pm2_5": p25,
                        "nitrogen_dioxide": gases[t][0], "ozone": gases[t][1]})
    return out''',
    "t4": '''def transform(s):
    h = s["particulate"]["hourly"]
    days = {}
    for t, p10, p25 in zip(h["time"], h["pm10"], h["pm2_5"]):
        d = days.setdefault(t[:10], {"hours": 0, "pm2_5_max": None, "pm10_max": None})
        d["hours"] += 1
        d["pm2_5_max"] = p25 if d["pm2_5_max"] is None else max(d["pm2_5_max"], p25)
        d["pm10_max"] = p10 if d["pm10_max"] is None else max(d["pm10_max"], p10)
    return [{"date": k, **v} for k, v in sorted(days.items())]''',
    "t5": '''def transform(s):
    h = s["particulate"]["hourly"]
    v = h["pm2_5"]
    return [{"time": h["time"][i], "pm2_5": v[i], "pm2_5_peak_3h": max(v[i - 2:i + 1])}
            for i in range(2, len(v))]''',
    "t6": '''def transform(s):
    bands = {}
    for f in s["usgs"]["features"]:
        mag = f["properties"]["mag"]
        depth = f["geometry"]["coordinates"][2]
        name = "minor" if mag < 4.0 else ("moderate" if mag < 5.0 else "major")
        b = bands.setdefault(name, {"event_count": 0, "max_magnitude": None, "max_depth_km": None})
        b["event_count"] += 1
        b["max_magnitude"] = mag if b["max_magnitude"] is None else max(b["max_magnitude"], mag)
        b["max_depth_km"] = depth if b["max_depth_km"] is None else max(b["max_depth_km"], depth)
    return [{"magnitude_band": n, **bands[n]} for n in ["minor", "moderate", "major"] if n in bands]''',
}

JS_TRANSFORM = {
    "t1": '''const h = $('Source 1').first().json.hourly;
const records = h.time.map((t, i) => ({ time: t, pm10: h.pm10[i], pm2_5: h.pm2_5[i] }));''',
    "t2": '''const records = $('Source 1').first().json.features.map((f) => {
  const mag = f.properties.mag;
  const [lon, lat, depth] = f.geometry.coordinates;
  return { id: f.id, magnitude: mag, place: f.properties.place, time_ms: f.properties.time,
           longitude: lon, latitude: lat, depth_km: depth,
           magnitude_band: mag < 4.0 ? 'minor' : (mag < 5.0 ? 'moderate' : 'major') };
});''',
    "t3": '''const p = $('Source 1').first().json.hourly;
const g = $('Source 2').first().json.hourly;
const gases = {};
g.time.forEach((t, i) => { gases[t] = { no2: g.nitrogen_dioxide[i], o3: g.ozone[i] }; });
const records = [];
p.time.forEach((t, i) => {
  if (Object.prototype.hasOwnProperty.call(gases, t)) {
    records.push({ time: t, pm10: p.pm10[i], pm2_5: p.pm2_5[i],
                   nitrogen_dioxide: gases[t].no2, ozone: gases[t].o3 });
  }
});''',
    "t4": '''const h = $('Source 1').first().json.hourly;
const days = {};
h.time.forEach((t, i) => {
  const d = t.slice(0, 10);
  if (!days[d]) days[d] = { hours: 0, pm2_5_max: null, pm10_max: null };
  days[d].hours += 1;
  const p25 = h.pm2_5[i], p10 = h.pm10[i];
  days[d].pm2_5_max = days[d].pm2_5_max === null ? p25 : Math.max(days[d].pm2_5_max, p25);
  days[d].pm10_max = days[d].pm10_max === null ? p10 : Math.max(days[d].pm10_max, p10);
});
const records = Object.keys(days).sort().map((d) => ({ date: d, ...days[d] }));''',
    "t5": '''const h = $('Source 1').first().json.hourly;
const v = h.pm2_5;
const records = [];
for (let i = 2; i < v.length; i++) {
  records.push({ time: h.time[i], pm2_5: v[i], pm2_5_peak_3h: Math.max(v[i - 2], v[i - 1], v[i]) });
}''',
    "t6": '''const bands = {};
for (const f of $('Source 1').first().json.features) {
  const mag = f.properties.mag;
  const depth = f.geometry.coordinates[2];
  const name = mag < 4.0 ? 'minor' : (mag < 5.0 ? 'moderate' : 'major');
  if (!bands[name]) bands[name] = { event_count: 0, max_magnitude: null, max_depth_km: null };
  const b = bands[name];
  b.event_count += 1;
  b.max_magnitude = b.max_magnitude === null ? mag : Math.max(b.max_magnitude, mag);
  b.max_depth_km = b.max_depth_km === null ? depth : Math.max(b.max_depth_km, depth);
}
const records = ['minor', 'moderate', 'major'].filter((n) => bands[n]).map((n) => ({ magnitude_band: n, ...bands[n] }));''',
}

GROOVY_TRANSFORM = {
    "t1": '''def h = s.particulate.hourly
def records = (0..<h.time.size()).collect { [time: h.time[it], pm10: h.pm10[it], pm2_5: h.pm2_5[it]] }''',
    "t2": '''def records = s.usgs.features.collect { f ->
    def mag = f.get('properties').get('mag')
    def c = f.geometry.coordinates
    [id: f.id, magnitude: mag, place: f.get('properties').get('place'), time_ms: f.get('properties').get('time'),
     longitude: c[0], latitude: c[1], depth_km: c[2],
     magnitude_band: mag < 4.0 ? "minor" : (mag < 5.0 ? "moderate" : "major")]
}''',
    "t3": '''def p = s.particulate.hourly
def g = s.gases.hourly
def gasIndex = [:]
(0..<g.time.size()).each { gasIndex[g.time[it]] = [no2: g.nitrogen_dioxide[it], o3: g.ozone[it]] }
def records = []
(0..<p.time.size()).each { i ->
    def t = p.time[i]
    if (gasIndex.containsKey(t)) {
        records << [time: t, pm10: p.pm10[i], pm2_5: p.pm2_5[i],
                    nitrogen_dioxide: gasIndex[t].no2, ozone: gasIndex[t].o3]
    }
}''',
    "t4": '''def h = s.particulate.hourly
def days = [:]
(0..<h.time.size()).each { i ->
    def d = h.time[i].substring(0, 10)
    if (!days.containsKey(d)) days[d] = [hours: 0, pm2_5_max: null, pm10_max: null]
    days[d].hours += 1
    def p25 = h.pm2_5[i]
    def p10 = h.pm10[i]
    days[d].pm2_5_max = days[d].pm2_5_max == null ? p25 : Math.max(days[d].pm2_5_max, p25)
    days[d].pm10_max = days[d].pm10_max == null ? p10 : Math.max(days[d].pm10_max, p10)
}
def records = days.keySet().sort().collect { [date: it] + days[it] }''',
    "t5": '''def h = s.particulate.hourly
def v = h.pm2_5
def records = (2..<v.size()).collect { i ->
    [time: h.time[i], pm2_5: v[i], pm2_5_peak_3h: [v[i - 2], v[i - 1], v[i]].max()]
}''',
    "t6": '''def bands = [:]
s.usgs.features.each { f ->
    def mag = f.get('properties').get('mag')
    def depth = f.geometry.coordinates[2]
    def name = mag < 4.0 ? "minor" : (mag < 5.0 ? "moderate" : "major")
    if (!bands.containsKey(name)) bands[name] = [event_count: 0, max_magnitude: null, max_depth_km: null]
    def b = bands[name]
    b.event_count += 1
    b.max_magnitude = b.max_magnitude == null ? mag : Math.max(b.max_magnitude, mag)
    b.max_depth_km = b.max_depth_km == null ? depth : Math.max(b.max_depth_km, depth)
}
def records = ["minor", "moderate", "major"].findAll { bands.containsKey(it) }.collect { [magnitude_band: it] + bands[it] }''',
}


def _indent(text: str, spaces: int) -> str:
    return "\n".join((" " * spaces + line) if line else "" for line in text.splitlines())


def _sources_py(task: str, run_id: str) -> str:
    entries = ", ".join(
        f'"{SOURCE_KEY[sid]}": "{RECORDER}/source/{_path(sid)}?run_id={run_id}&attempt=0"'
        for sid in SOURCES[task]
    )
    return "{" + entries + "}"


def _path(source_id: str) -> str:
    return {
        "open_meteo_particulate": "open-meteo/particulate",
        "open_meteo_gases": "open-meteo/gases",
        "usgs_earthquakes": "usgs/earthquakes",
    }[source_id]


def _python_fetch_and_post(task: str, run_id: str) -> str:
    return f'''import json
import urllib.request

SOURCES = {_sources_py(task, run_id)}
SINK_URL = "{RECORDER}/sink/{run_id}?attempt=0"


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read())


{PY_TRANSFORM[task]}


def deliver(records):
    request = urllib.request.Request(
        SINK_URL,
        data=json.dumps(records).encode("utf-8"),
        headers={{"Content-Type": "application/json"}},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()


def run():
    data = {{name: fetch(url) for name, url in SOURCES.items()}}
    deliver(transform(data))'''


def airflow(task: str, run_id: str) -> str:
    dag_id = f"bench_{run_id.replace('-', '_')}"
    return f'''from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


{_indent(_python_fetch_and_post(task, run_id).replace("def run():", "def ingest():").replace("    data = {", "    data = {"), 0)}


with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="ingest", python_callable=ingest)
'''


def dagster(task: str, run_id: str) -> str:
    job = f"bench_{run_id.replace('-', '_')}"
    body = _python_fetch_and_post(task, run_id).replace("import json\nimport urllib.request\n", "import json\nimport urllib.request\n", 1)
    body = body.replace("def run():", "def _run():")
    return f'''from dagster import op, job

{body}


@op
def ingest_pipeline():
    _run()


@job
def {job}():
    ingest_pipeline()
'''


def prefect(task: str, run_id: str) -> str:
    flow_name = f"bench_{run_id.replace('-', '_')}"
    body = _python_fetch_and_post(task, run_id)
    return f'''from prefect import flow

{body}


@flow(name="{flow_name}")
def {flow_name}():
    run()


if __name__ == "__main__":
    {flow_name}()
'''


def kestra(task: str, run_id: str) -> str:
    flow_id = f"bench_{run_id.replace('-', '_')}"
    script = _indent(_python_fetch_and_post(task, run_id) + "\n\n\nrun()\n", 6)
    return f'''id: {flow_id}
namespace: bench
tasks:
  - id: ingest
    type: io.kestra.plugin.scripts.python.Script
    runner: PROCESS
    script: |
{script}
'''


def n8n(task: str, run_id: str, benchmark: dict) -> str:
    skeleton_prompt = build_prompt(
        task, get_task(task, benchmark), benchmark, "n8n", RECORDER, run_id, attempt=0
    )
    skeleton, _ = json.JSONDecoder().raw_decode(skeleton_prompt.split("Canonical n8n workflow skeleton:\n", 1)[1])
    for node in skeleton["nodes"]:
        if node["name"] == "Transform":
            node["parameters"]["jsCode"] = JS_TRANSFORM[task.split("_")[0]] + "\nreturn [{ json: { records } }];"
    return json.dumps(skeleton, indent=2) + "\n"


def nifi(task: str, run_id: str) -> str:
    fetch_lines = []
    for sid in SOURCES[task]:
        key = SOURCE_KEY[sid]
        fetch_lines.append(f'def {key} = fetch("{RECORDER}/source/{_path(sid)}?run_id={run_id}&attempt=0")')
    sources = "\n".join(fetch_lines)
    names = ", ".join(f"{SOURCE_KEY[sid]}: {SOURCE_KEY[sid]}" for sid in SOURCES[task])
    return f'''import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def fetch = {{ url -> new JsonSlurper().parseText(new URL(url).text) }}
{sources}
def s = [{names}]

{GROOVY_TRANSFORM[task]}

def connection = new URL("{RECORDER}/sink/{run_id}?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") {{ it << JsonOutput.toJson(records) }}
if (connection.responseCode >= 300) {{
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
'''


def build(out_root: Path = OUT) -> list[Path]:
    benchmark = load_benchmark()
    written = []
    for task in TASKS:
        prefix = task.split("_")[0]
        run_id = f"ref-{prefix}"
        generators = {
            "airflow": lambda: airflow(prefix, run_id),
            "dagster": lambda: dagster(prefix, run_id),
            "prefect": lambda: prefect(prefix, run_id),
            "kestra": lambda: kestra(prefix, run_id),
            "n8n": lambda: n8n(task, run_id, benchmark),
            "nifi": lambda: nifi(prefix, run_id),
        }
        for platform, ext in PLATFORMS.items():
            path = out_root / platform / f"{prefix}.{ext}"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(generators[platform](), encoding="utf-8")
            written.append(path)
    return written


if __name__ == "__main__":
    for path in build():
        print("wrote", path.relative_to(ROOT))
