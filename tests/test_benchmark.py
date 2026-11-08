from __future__ import annotations

import csv
import json
import subprocess
import sys
import threading
import unittest
from contextlib import redirect_stdout
from http.server import ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from urllib.request import Request, urlopen
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipeline_bench.benchmark import expected_rows, get_task, list_tasks, load_benchmark
from pipeline_bench.cli import _artifact_text, _classify_failure, _run
from pipeline_bench.ollama import generate_with_metrics
from pipeline_bench.platforms import (
    ArtifactError,
    DockerResourceMonitor,
    ExecutionError,
    PlatformError,
    _parse_docker_bytes,
    _validate_kestra_flow,
    _validate_n8n_workflow,
)
from pipeline_bench.prompts import build_prompt
from pipeline_bench.recorder import RecorderState, make_handler
from pipeline_bench.scoring import score_rows


class BenchmarkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.benchmark = load_benchmark()

    def test_defines_three_tasks_with_expected_rows(self) -> None:
        self.assertEqual(len(list_tasks(self.benchmark)), 6)
        self.assertEqual(len(expected_rows(get_task("t3_open_meteo_join", self.benchmark), self.benchmark)), 48)

    def test_fixture_manifest_matches_files_on_disk(self) -> None:
        import hashlib
        manifest = json.loads((ROOT / "fixtures" / "manifest.json").read_text(encoding="utf-8"))
        for relative, digest in manifest["files"].items():
            actual = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
            self.assertEqual(actual, digest, relative)

    def test_extended_reference_outputs_match_generator(self) -> None:
        sys.path.insert(0, str(ROOT / "scripts"))
        from build_reference_outputs import build
        for filename, rows in build().items():
            committed = json.loads((ROOT / "fixtures" / "expected" / filename).read_text(encoding="utf-8"))
            self.assertEqual(committed, rows, filename)
        self.assertEqual(len(expected_rows(get_task("t5_trailing_three_hour_peak", self.benchmark), self.benchmark)), 46)

    def test_join_fixture_uses_only_shared_timestamps(self) -> None:
        rows = expected_rows(get_task("t3_open_meteo_join", self.benchmark), self.benchmark)
        self.assertEqual(len(rows), 48)
        self.assertEqual(rows[0]["time"], "2024-01-01T00:00")
        self.assertEqual(rows[-1]["time"], "2024-01-02T23:00")
        self.assertEqual(set(rows[0]), {"time", "pm10", "pm2_5", "nitrogen_dioxide", "ozone"})

    def test_scoring_treats_integer_valued_floats_as_equal(self) -> None:
        # n8n (JavaScript) returns 10.0 as 10; both must score as the same number.
        result = score_rows([{"pm2_5": 10.0}], [{"pm2_5": 10}])
        self.assertTrue(result["passed"])

    def test_exact_multiset_scoring_ignores_order_but_checks_duplicates(self) -> None:
        expected = [{"key": 1}, {"key": 1}, {"key": 2}]
        self.assertTrue(score_rows(expected, list(reversed(expected)))["passed"])
        duplicate_error = score_rows(expected, [{"key": 1}, {"key": 2}, {"key": 2}])
        self.assertFalse(duplicate_error["passed"])
        self.assertEqual(duplicate_error["missing_rows"], [{"key": 1}])
        self.assertEqual(duplicate_error["unexpected_rows"], [{"key": 2}])
        self.assertAlmostEqual(duplicate_error["precision"], 2 / 3)
        self.assertAlmostEqual(duplicate_error["recall"], 2 / 3)
        self.assertAlmostEqual(duplicate_error["f1"], 2 / 3)
        self.assertEqual(score_rows([], [])["f1"], 1.0)
        self.assertEqual(score_rows([{"key": 1}], [])["f1"], 0.0)

    def test_ollama_generation_returns_latency_token_and_throughput_metrics(self) -> None:
        payload = {
            "response": "generated",
            "total_duration": 2_500_000_000,
            "load_duration": 500_000_000,
            "prompt_eval_count": 100,
            "eval_count": 20,
            "eval_duration": 1_000_000_000,
        }
        with patch(
            "pipeline_bench.ollama.urllib.request.urlopen",
            return_value=StringIO(json.dumps(payload)),
        ) as urlopen_mock:
            text, metrics = generate_with_metrics(
                "prompt",
                "test-model",
                options={"temperature": 0.0, "seed": 42, "top_p": 1.0},
                think=False,
            )
        self.assertEqual(text, "generated")
        self.assertEqual(metrics["prompt_tokens"], 100)
        self.assertEqual(metrics["output_tokens"], 20)
        self.assertEqual(metrics["ollama_total_duration_seconds"], 2.5)
        self.assertEqual(metrics["output_tokens_per_second"], 20)
        self.assertGreaterEqual(metrics["generation_duration_seconds"], 0)
        request = urlopen_mock.call_args.args[0]
        body = json.loads(request.data)
        self.assertEqual(body["options"], {"temperature": 0.0, "seed": 42, "top_p": 1.0})
        self.assertEqual(body["keep_alive"], "5m")
        self.assertFalse(body["think"])

    def test_docker_memory_sizes_convert_to_bytes(self) -> None:
        self.assertEqual(_parse_docker_bytes("1.5 MiB"), 1_572_864)
        self.assertEqual(_parse_docker_bytes("28.12MiB"), 29_485_957)
        self.assertEqual(_parse_docker_bytes("0B"), 0)
        self.assertEqual(_parse_docker_bytes("2 GB"), 2_000_000_000)

    def test_aggregate_reports_gpu_coverage_and_failure_categories(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            runs = root / "runs"
            runs.mkdir()
            result = {
                "task": "t1",
                "platform": "airflow",
                "model": "test-model",
                "status": "failed",
                "attempts": 2,
                "configuration": {"ollama_generation_options": {"temperature": 0.0, "seed": 42}},
                "metrics": {
                    "first_attempt_passed": False,
                    "generation_duration_seconds": 1.0,
                    "execution_duration_seconds": 2.0,
                    "total_duration_seconds": 3.0,
                    "source_coverage": 0.5,
                    "mean_gpu_utilization_percent": 20.0,
                    "peak_gpu_memory_bytes": 1000,
                    "estimated_gpu_energy_wh": 0.01,
                    "best_f1": 0.5,
                },
                "failure_categories": {"incorrect_output": 1, "platform_error": 1},
                "history": [{"container_resources": {"measurement_error": None}}],
            }
            (runs / "run.json").write_text(json.dumps(result), encoding="utf-8")
            output = root / "summary.csv"
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "aggregate.py"),
                    "--results-dir", str(runs),
                    "--output", str(output),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            with output.open(encoding="utf-8", newline="") as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(row["incorrect_outputs"], "1")
            self.assertEqual(row["platform_errors"], "1")
            self.assertEqual(row["mean_source_coverage"], "0.5000")
            self.assertEqual(row["max_peak_gpu_memory_bytes"], "1000")
            self.assertIn('"seed": 42', row["configuration_json"])

    def test_gpu_monitor_records_utilization_memory_and_power_estimate(self) -> None:
        monitor = DockerResourceMonitor()
        monitor._gpu_supported = True
        output = "Example GPU,550.1,50,1024,100.0\n"
        with (
            patch("pipeline_bench.platforms.subprocess.run", return_value=SimpleNamespace(stdout=output)),
            patch("pipeline_bench.platforms.time.monotonic", side_effect=[10.0, 11.0]),
        ):
            monitor._sample_gpu()
            monitor._sample_gpu()
        self.assertEqual(monitor._gpu_names, ["Example GPU"])
        self.assertEqual(monitor._gpu_utilization, [50.0, 50.0])
        self.assertEqual(monitor._peak_gpu_memory_bytes, 1_073_741_824)
        self.assertAlmostEqual(monitor._gpu_energy_wh, 100 / 3600)

    def test_prompt_includes_correct_platform_and_contract(self) -> None:
        task = get_task("t1_open_meteo_ingestion", self.benchmark)
        prompt = build_prompt("t1_open_meteo_ingestion", task, self.benchmark, "airflow", "http://recorder:8090", "abc")
        self.assertIn("Python Apache Airflow DAG", prompt)
        self.assertIn("from airflow import DAG", prompt)
        self.assertIn("from airflow.operators.python import PythonOperator", prompt)
        self.assertIn("pm2_5", prompt)
        self.assertIn("http://recorder:8090/sink/abc?attempt=0", prompt)
        self.assertIn("run_id=abc", prompt)
        self.assertIn("response_schema", prompt)
        self.assertNotIn("10.9", prompt)
        self.assertNotIn("payload_shape", prompt)

    def test_prompts_name_platform_entrypoint_imports(self) -> None:
        task = get_task("t1_open_meteo_ingestion", self.benchmark)
        dagster_prompt = build_prompt(
            "t1_open_meteo_ingestion", task, self.benchmark, "dagster",
            "http://recorder:8090", "dagster-test",
        )
        prefect_prompt = build_prompt(
            "t1_open_meteo_ingestion", task, self.benchmark, "prefect",
            "http://recorder:8090", "prefect-test",
        )
        self.assertIn("from dagster import op, job", dagster_prompt)
        self.assertIn("exactly `@op` (not `@op()`)", dagster_prompt)
        self.assertIn("exact line `from dagster import op, job`", dagster_prompt)
        self.assertIn("Do not use Dagster context, resources", dagster_prompt)
        self.assertIn("from prefect import flow", prefect_prompt)
        self.assertIn("do not import any other Prefect symbols", prefect_prompt)
        self.assertIn("urllib.request.Request(sink_url", prefect_prompt)

    def test_nifi_and_kestra_prompts_name_platform_entrypoints(self) -> None:
        task = get_task("t1_open_meteo_ingestion", self.benchmark)
        nifi_prompt = build_prompt(
            "t1_open_meteo_ingestion", task, self.benchmark, "nifi",
            "http://recorder:8090", "nifi-test",
        )
        kestra_prompt = build_prompt(
            "t1_open_meteo_ingestion", task, self.benchmark, "kestra",
            "http://recorder:8090", "kestra-test",
        )
        self.assertIn("ExecuteGroovyScript", nifi_prompt)
        self.assertIn("session.transfer(flowFile, REL_SUCCESS)", nifi_prompt)
        self.assertIn("id: bench_kestra_test", kestra_prompt)
        self.assertIn("namespace: bench", kestra_prompt)
        self.assertIn("io.kestra.plugin.scripts.python.Script", kestra_prompt)
        self.assertIn("runner: PROCESS", kestra_prompt)

    def test_failure_categories_separate_artifact_runtime_and_platform_errors(self) -> None:
        self.assertEqual(_classify_failure("generation", RuntimeError("x")), "model_request_error")
        self.assertEqual(_classify_failure("artifact_validation", ValueError("x")), "invalid_artifact")
        self.assertEqual(_classify_failure("platform_execution", ArtifactError("x")), "invalid_artifact")
        self.assertEqual(_classify_failure("platform_execution", ExecutionError("x")), "runtime_error")
        self.assertEqual(_classify_failure("platform_execution", PlatformError("x")), "platform_error")
        self.assertEqual(_classify_failure("platform_execution", KeyError("x")), "harness_error")

    def test_n8n_validation_errors_are_artifact_errors(self) -> None:
        with self.assertRaises(ArtifactError):
            _validate_n8n_workflow({"id": "x", "nodes": []})

    def test_kestra_flow_must_declare_run_specific_id_and_namespace(self) -> None:
        flow = "id: bench_run_1\nnamespace: bench\ntasks:\n  - id: hello\n    type: x\n"
        _validate_kestra_flow(flow, "bench_run_1")
        with self.assertRaises(PlatformError):
            _validate_kestra_flow(flow, "bench_other")
        with self.assertRaises(PlatformError):
            _validate_kestra_flow(flow.replace("namespace: bench", "namespace: other"), "bench_run_1")
        with self.assertRaises(PlatformError):
            _validate_kestra_flow("tasks:\n  - id: id: bench_run_1\n", "bench_run_1")

    def test_groovy_and_yaml_artifacts_strip_markdown_fences(self) -> None:
        groovy = _artifact_text("```groovy\ndef x = 1\n```", "nifi")
        kestra = _artifact_text("```yaml\nid: bench_run_1\n```", "kestra")
        self.assertEqual(groovy, "def x = 1\n")
        self.assertEqual(kestra, "id: bench_run_1\n")

    def test_etl_and_join_prompts_include_task_semantics_without_fixture_records(self) -> None:
        etl = build_prompt(
            "t2_usgs_etl",
            get_task("t2_usgs_etl", self.benchmark),
            self.benchmark,
            "airflow",
            "http://recorder:8090",
            "etl-run",
        )
        self.assertIn("magnitude_band", etl)
        self.assertIn("minor", etl)
        self.assertIn("depth_km", etl)
        self.assertNotIn("272 km N of Saipan", etl)

        join = build_prompt(
            "t3_open_meteo_join",
            get_task("t3_open_meteo_join", self.benchmark),
            self.benchmark,
            "n8n",
            "http://recorder:8090",
            "join-run",
        )
        self.assertIn("inner-join on exact time", join)
        self.assertIn("/source/open-meteo/particulate", join)
        self.assertIn("/source/open-meteo/gases", join)
        self.assertIn("complete n8n workflow JSON", join)
        self.assertIn("n8n-nodes-base.httpRequest", join)
        self.assertIn("n8n-nodes-base.manualTrigger", join)
        self.assertNotIn("n8n-nodes-base.executeWorkflowTrigger", join)
        self.assertIn("parameters.jsCode", join)
        self.assertIn("saveDataErrorExecution", join)
        self.assertIn("preserve its node names, IDs, types, settings, parameters, and connections", join)
        self.assertNotIn("10.9", join)

    def test_n8n_prompt_skeleton_matches_source_count_and_contract_urls(self) -> None:
        task = get_task("t3_open_meteo_join", self.benchmark)
        prompt = build_prompt(
            "t3_open_meteo_join",
            task,
            self.benchmark,
            "n8n",
            "http://recorder:8090",
            "skeleton-check",
            attempt=2,
        )
        marker = "Canonical n8n workflow skeleton:\n"
        skeleton_text = prompt.split(marker, 1)[1].split(
            "\nReturn only the requested artifact", 1
        )[0]
        skeleton = json.loads(skeleton_text)
        self.assertEqual(skeleton["id"], "bench_skeleton-check")
        self.assertEqual(len(skeleton["nodes"]), 6)
        self.assertEqual(
            [node["name"] for node in skeleton["nodes"] if node["name"].startswith("Source ")],
            ["Source 1", "Source 2"],
        )
        # Both sources feed a Merge node, so the Code node waits for both before running.
        self.assertEqual(skeleton["connections"]["Source 2"]["main"], [[{"node": "Merge Sources", "type": "main", "index": 1}]])
        self.assertEqual(skeleton["connections"]["Merge Sources"]["main"], [[{"node": "Transform", "type": "main", "index": 0}]])
        source_urls = [
            node["parameters"]["url"]
            for node in skeleton["nodes"]
            if node["name"].startswith("Source ")
        ]
        self.assertTrue(all("attempt=2" in url for url in source_urls))
        sink = next(node for node in skeleton["nodes"] if node["name"] == "Sink")
        self.assertEqual(sink["parameters"]["jsonBody"], "={{ $json }}")
        self.assertEqual(
            skeleton["connections"]["Benchmark Trigger"]["main"],
            [[
                {"node": "Source 1", "type": "main", "index": 0},
                {"node": "Source 2", "type": "main", "index": 0},
            ]],
        )

    def test_n8n_artifact_strips_fence_and_requires_workflow_json(self) -> None:
        artifact = _artifact_text('```json\n{"nodes": [], "connections": {}}\n```', "n8n")
        self.assertEqual(json.loads(artifact)["nodes"], [])
        with self.assertRaises(ValueError):
            _artifact_text("not json", "n8n")

    def test_n8n_validation_reports_misplaced_http_url(self) -> None:
        workflow = {
            "id": "test",
            "nodes": [
                {
                    "id": "http",
                    "name": "Source",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [0, 0],
                    "url": "http://recorder/source",
                    "parameters": {},
                },
                {
                    "id": "trigger",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.executeWorkflowTrigger",
                    "typeVersion": 1.1,
                    "position": [0, 1],
                    "parameters": {},
                },
                {
                    "id": "code",
                    "name": "Transform",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [0, 2],
                    "parameters": {"jsCode": "return items;"},
                },
            ],
            "connections": {},
        }
        with self.assertRaisesRegex(PlatformError, "inside parameters.url"):
            _validate_n8n_workflow(workflow)

    def test_n8n_validation_reports_invalid_sink_expression(self) -> None:
        workflow = {
            "id": "test",
            "settings": {
                "executionOrder": "v1",
                "saveDataErrorExecution": "all",
                "saveDataSuccessExecution": "all",
                "saveManualExecutions": True,
            },
            "nodes": [
                {
                    "id": "trigger",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.executeWorkflowTrigger",
                    "typeVersion": 1.1,
                    "position": [0, 0],
                    "parameters": {},
                },
                {
                    "id": "source",
                    "name": "Source",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [0, 1],
                    "parameters": {"url": "http://recorder/source"},
                },
                {
                    "id": "code",
                    "name": "Transform",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [0, 2],
                    "parameters": {"mode": "runOnceForAllItems", "jsCode": "return items;"},
                },
                {
                    "id": "sink",
                    "name": "Sink",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [0, 3],
                    "parameters": {
                        "method": "POST",
                        "url": "http://recorder/sink",
                        "sendBody": True,
                        "specifyBody": "json",
                        "jsonBody": "{{= $json }}",
                    },
                },
            ],
            "connections": {},
        }
        with self.assertRaisesRegex(PlatformError, "parameters.jsonBody"):
            _validate_n8n_workflow(workflow)

    def test_n8n_validation_requires_connection_arrays_per_output(self) -> None:
        workflow = {
            "id": "test",
            "settings": {
                "executionOrder": "v1",
                "saveDataErrorExecution": "all",
                "saveDataSuccessExecution": "all",
                "saveManualExecutions": True,
            },
            "nodes": [
                {
                    "id": "trigger",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.executeWorkflowTrigger",
                    "typeVersion": 1.1,
                    "position": [0, 0],
                    "parameters": {},
                },
                {
                    "id": "source",
                    "name": "Source",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [0, 1],
                    "parameters": {"url": "http://recorder/source"},
                },
                {
                    "id": "code",
                    "name": "Transform",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [0, 2],
                    "parameters": {"mode": "runOnceForAllItems", "jsCode": "return items;"},
                },
            ],
            "connections": {
                "Trigger": {"main": [{"node": "Source", "type": "main", "index": 0}]}
            },
        }
        with self.assertRaisesRegex(PlatformError, "array of arrays"):
            _validate_n8n_workflow(workflow)

    def test_run_executes_attempt_once_and_saves_result(self) -> None:
        with TemporaryDirectory() as temp_dir:
            args = SimpleNamespace(
                task="t1_open_meteo_ingestion",
                platform="airflow",
                recorder_url="http://localhost:8090",
                source_url="http://recorder:8090",
                run_id="unit-run",
                output_dir=temp_dir,
                repairs=2,
                model="unit-model",
                ollama_url="http://localhost:11434",
                model_timeout=1,
                execution_timeout=1,
                input_cost_per_million_tokens=1.0,
                output_cost_per_million_tokens=2.0,
                temperature=0.0,
                seed=42,
                top_p=1.0,
                top_k=40,
                repeat_penalty=1.1,
                num_thread=0,
                num_ctx=8192,
                num_predict=4096,
                keep_alive="5m",
                think=False,
            )
            output = StringIO()
            with (
                patch("pipeline_bench.cli.warm_up"),
                patch(
                    "pipeline_bench.cli.generate_with_metrics",
                    return_value=("dag code", {
                        "generation_duration_seconds": 0.25,
                        "prompt_tokens": 120,
                        "output_tokens": 40,
                        "ollama_total_duration_seconds": 0.2,
                        "model_load_duration_seconds": 0.01,
                        "output_tokens_per_second": 2000.0,
                    }),
                ),
                patch(
                    "pipeline_bench.cli._collect_environment",
                    return_value={"ollama_model_digest": "sha256:test"},
                ),
                patch(
                    "pipeline_bench.cli._request_json",
                    return_value={
                        "source_request_count": 1,
                        "requested_source_ids": ["open_meteo_particulate"],
                        "sink_post_count": 1,
                        "sink_rows_accepted": 48,
                    },
                ),
                patch.object(
                    DockerResourceMonitor,
                    "start",
                    return_value=None,
                ),
                patch.object(
                    DockerResourceMonitor,
                    "stop",
                    return_value={
                        "sample_count": 2,
                        "mean_cpu_percent": 12.5,
                        "peak_cpu_percent": 20.0,
                        "peak_memory_bytes": 4_194_304,
                        "measurement_error": None,
                    },
                ),
                patch("pipeline_bench.cli.start_recorder") as start_recorder,
                patch("pipeline_bench.cli._reset_sink") as reset_sink,
                patch("pipeline_bench.cli.run_airflow") as run_airflow,
                patch("pipeline_bench.cli._score", return_value={"passed": True}),
                patch("pipeline_bench.cli.stop_services"),
                patch("pipeline_bench.cli.cleanup_staged_artifact"),
                redirect_stdout(output),
            ):
                status = _run(args)
            self.assertEqual(status, 0)
            start_recorder.assert_called_once()
            reset_sink.assert_called_once_with("http://localhost:8090", "unit-run", 0)
            run_airflow.assert_called_once()
            result = json.loads((Path(temp_dir) / "unit-run.json").read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["attempts"], 1)
            self.assertTrue(result["metrics"]["first_attempt_passed"])
            self.assertEqual(result["metrics"]["prompt_tokens"], 120)
            self.assertEqual(result["metrics"]["output_tokens"], 40)
            self.assertAlmostEqual(result["metrics"]["cost_estimate_usd"], 0.0002)
            self.assertEqual(result["metrics"]["peak_memory_bytes"], 4_194_304)
            self.assertEqual(result["metrics"]["source_coverage"], 1.0)
            self.assertEqual(result["configuration"]["ollama_generation_options"]["seed"], 42)
            self.assertEqual(result["configuration"]["ollama_generation_options"]["num_thread"], 0)
            self.assertEqual(result["configuration"]["ollama_keep_alive"], "5m")
            self.assertFalse(result["configuration"]["ollama_think"])
            self.assertEqual(result["environment"]["ollama_model_digest"], "sha256:test")

    def test_recorder_serves_sources_and_records_sink_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            state = RecorderState(Path(temp_dir) / "events.jsonl", self.benchmark)
            server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base_url = f"http://127.0.0.1:{server.server_port}"
            try:
                with urlopen(f"{base_url}/source/usgs/earthquakes") as response:
                    payload = json.loads(response.read())
                self.assertEqual(payload["type"], "FeatureCollection")
                with urlopen(f"{base_url}/source/usgs/earthquakes?run_id=test-run&attempt=1") as response:
                    self.assertEqual(json.loads(response.read())["type"], "FeatureCollection")
                request = Request(
                    f"{base_url}/sink/test-run?attempt=1",
                    data=json.dumps([{"id": "quake"}]).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    self.assertEqual(response.status, 202)
                with urlopen(f"{base_url}/sink/test-run?attempt=1") as response:
                    self.assertEqual(json.loads(response.read())["rows"], [{"id": "quake"}])
                with urlopen(f"{base_url}/sink/test-run?attempt=0") as response:
                    self.assertEqual(json.loads(response.read())["rows"], [])
                with urlopen(f"{base_url}/metrics/test-run?attempt=1") as response:
                    metrics = json.loads(response.read())
                self.assertEqual(metrics["source_request_count"], 1)
                self.assertEqual(metrics["requested_source_ids"], ["usgs_earthquakes"])
                self.assertEqual(metrics["sink_post_count"], 1)
                self.assertEqual(metrics["sink_rows_accepted"], 1)
                events = (Path(temp_dir) / "http-events.jsonl").read_text(encoding="utf-8")
                self.assertIn('"source_id":"usgs_earthquakes"', events)
                self.assertIn('"accepted_rows":1', events)
                with urlopen(
                    Request(f"{base_url}/sink/test-run?attempt=1", method="DELETE")
                ):
                    pass
                with urlopen(f"{base_url}/metrics/test-run?attempt=1") as response:
                    self.assertEqual(json.loads(response.read())["source_request_count"], 0)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
