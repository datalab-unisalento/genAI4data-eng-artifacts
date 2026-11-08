from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from .benchmark import expected_rows, get_task, list_tasks, load_benchmark
from .ollama import OllamaError, generate_with_metrics, warm_up
from .platforms import (
    ArtifactError,
    ExecutionError,
    DockerResourceMonitor,
    PlatformError,
    cleanup_staged_artifact,
    run_airflow,
    run_dagster,
    run_kestra,
    run_n8n,
    run_nifi,
    run_prefect,
    start_recorder,
    stop_services,
)
from .prompts import PROMPT_VERSION, build_prompt
from .recorder import serve
from .scoring import SCORER_VERSION, score_rows


ROOT = Path(__file__).resolve().parents[2]

PLATFORMS = ("airflow", "n8n", "dagster", "prefect", "nifi", "kestra")
PLATFORM_EXTENSIONS = {
    "airflow": "py",
    "n8n": "json",
    "dagster": "py",
    "prefect": "py",
    "nifi": "groovy",
    "kestra": "yaml",
}


def _command_metadata(command: list[str], timeout: int = 15) -> tuple[str | None, str | None]:
    try:
        result = subprocess.run(
            command, check=True, capture_output=True, text=True, timeout=timeout
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        return None, str(exc)
    return result.stdout.strip(), None


def _collect_environment(args: argparse.Namespace) -> dict[str, Any]:
    docker_version, docker_error = _command_metadata(
        ["docker", "version", "--format", "{{.Server.Version}}"]
    )
    compose_version, compose_error = _command_metadata(
        ["docker", "compose", "version", "--short"]
    )
    images, images_error = _command_metadata(
        ["docker", "compose", "-f", str(ROOT / "docker-compose.yml"), "config", "--images"]
    )

    model_digest = None
    ollama_version = None
    ollama_error = None
    try:
        with urllib.request.urlopen(f"{args.ollama_url.rstrip('/')}/api/version", timeout=5) as response:
            version_payload = json.loads(response.read())
        ollama_version = version_payload.get("version")
        with urllib.request.urlopen(f"{args.ollama_url.rstrip('/')}/api/tags", timeout=10) as response:
            tags_payload = json.loads(response.read())
        requested_model = args.model
        if ":" not in requested_model:
            requested_model = f"{requested_model}:latest"
        for model in tags_payload.get("models", []):
            if model.get("name") in {args.model, requested_model} or model.get("model") in {
                args.model, requested_model,
            }:
                model_digest = model.get("digest")
                break
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, AttributeError) as exc:
        ollama_error = str(exc)

    gpu_info = None
    if shutil.which("nvidia-smi"):
        gpu_info, _ = _command_metadata([
            "nvidia-smi",
            "--query-gpu=name,driver_version",
            "--format=csv,noheader",
        ])
    cpu_model = platform.processor() or None
    if not cpu_model and Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith(("model name", "hardware")):
                cpu_model = line.split(":", 1)[-1].strip()
                break
    memory_bytes = None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                memory_bytes = int(line.split()[1]) * 1024
                break
    image_names = images.splitlines() if images else []
    image_digests: dict[str, str] = {}
    image_digest_errors: dict[str, str] = {}
    for image in image_names:
        image_id, image_error = _command_metadata(
            ["docker", "image", "inspect", "--format", "{{.Id}}", image]
        )
        if image_id:
            image_digests[image] = image_id
        elif image_error:
            image_digest_errors[image] = image_error
    revision, revision_error = _command_metadata(["git", "rev-parse", "HEAD"])
    dirty_status, dirty_error = _command_metadata(["git", "status", "--porcelain"])
    source_digest = hashlib.sha256()
    hashed_files: list[Path] = []
    for directory in ("src", "scripts", "fixtures", "controls"):
        root = ROOT / directory
        if root.exists():
            hashed_files.extend(
                path for path in root.rglob("*")
                if path.is_file() and path.suffix in {".py", ".json", ".yml", ".yaml", ".toml"}
            )
    hashed_files.extend(
        ROOT / filename
        for filename in (
            "docker-compose.yml", "Dockerfile", "Dockerfile.python-runners", "pyproject.toml"
        )
        if (ROOT / filename).is_file()
    )
    for path in sorted(hashed_files):
        relative = path.relative_to(ROOT).as_posix()
        source_digest.update(relative.encode("utf-8"))
        source_digest.update(b"\0")
        source_digest.update(path.read_bytes())
    source_metadata = {
        "git_revision": revision,
        "git_worktree_dirty": bool(dirty_status),
        "source_tree_sha256": source_digest.hexdigest(),
    }
    if revision_error or dirty_error:
        source_metadata["collection_error"] = "; ".join(
            error for error in (revision_error, dirty_error) if error
        )
    return {
        "python_version": platform.python_version(),
        "operating_system": platform.platform(),
        "architecture": platform.machine(),
        "cpu_model": cpu_model,
        "logical_cpu_count": os.cpu_count(),
        "host_memory_bytes": memory_bytes,
        "gpu_devices": gpu_info.splitlines() if gpu_info else [],
        "docker_server_version": docker_version,
        "docker_compose_version": compose_version,
        "container_images": image_names,
        "container_image_ids": image_digests,
        "container_image_errors": image_digest_errors,
        "ollama_version": ollama_version,
        "ollama_model_digest": model_digest,
        "source": source_metadata,
        "environment_collection_errors": {
            key: value for key, value in {
                "docker_server": docker_error,
                "docker_compose": compose_error,
                "container_images": images_error,
                "ollama": ollama_error,
            }.items() if value
        },
    }


def _request_json(method: str, url: str, value: Any | None = None, timeout: int = 15) -> Any:
    data = None if value is None else json.dumps(value).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{method} {url} failed: {exc}") from exc


FAILURE_CATEGORIES = (
    "model_request_error",
    "invalid_artifact",
    "runtime_error",
    "platform_error",
    "harness_error",
    "incorrect_output",
)


def _classify_failure(stage: str, exc: BaseException) -> str:
    """Map a failed attempt to one failure category; see docs/experiment-design-icde2027.md."""
    if stage == "generation":
        return "model_request_error"
    if stage == "artifact_validation":
        return "invalid_artifact"
    if isinstance(exc, ArtifactError):
        return "invalid_artifact"
    if isinstance(exc, ExecutionError):
        return "runtime_error"
    if isinstance(exc, PlatformError):
        return "platform_error"
    return "harness_error"


def _artifact_text(text: str, platform: str) -> str:
    stripped = text.strip()
    match = re.fullmatch(r"```(?:python|json|groovy|yaml|yml)?\s*\n(.*?)\n```", stripped, re.DOTALL | re.IGNORECASE)
    if match:
        stripped = match.group(1).strip()
    if not stripped:
        raise ValueError("Model returned an empty artifact")
    if platform == "n8n":
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Model did not return valid n8n workflow JSON: {exc}") from exc
        if not isinstance(parsed, dict) or not isinstance(parsed.get("nodes"), list):
            raise ValueError("n8n artifact must be a workflow object with a 'nodes' array")
    return stripped + "\n"


def _reset_sink(recorder_url: str, run_id: str, attempt: int) -> None:
    request = urllib.request.Request(
        f"{recorder_url.rstrip('/')}/sink/{run_id}?attempt={attempt}",
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(request, timeout=10):
            return
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Could not reset sink for run {run_id}: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach recorder to reset sink: {exc}") from exc


def _score(
    recorder_url: str,
    run_id: str,
    attempt: int,
    task: dict[str, Any],
    benchmark: dict[str, Any],
) -> dict[str, Any]:
    value = _request_json("GET", f"{recorder_url.rstrip('/')}/sink/{run_id}?attempt={attempt}")
    rows = value.get("rows")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise RuntimeError("Recorder returned an invalid sink response")
    return score_rows(expected_rows(task, benchmark), rows)


def _run(args: argparse.Namespace) -> int:
    benchmark = load_benchmark()
    task = get_task(args.task, benchmark)
    if args.platform not in PLATFORMS:
        raise ValueError(f"Unsupported platform: {args.platform}")
    recorder_url = args.recorder_url.rstrip("/")
    run_id = args.run_id or uuid.uuid4().hex[:12]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", run_id):
        raise ValueError("run id may contain only letters, digits, underscores, and hyphens (max 64)")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    extension = PLATFORM_EXTENSIONS[args.platform]
    metadata_path = output_dir / f"{run_id}.json"
    failure_report: str | None = None
    previous_artifact: str | None = None
    history: list[dict[str, Any]] = []
    run_started = time.monotonic()
    options = options_from_args(args)
    environment = _collect_environment(args)

    for attempt in range(args.repairs + 1):
        prompt = build_prompt(
            args.task,
            task,
            benchmark,
            args.platform,
            args.source_url,
            run_id,
            attempt=attempt,
            previous_artifact=previous_artifact,
            failure_report=failure_report,
        )
        attempt_result: dict[str, Any] = {
            "attempt": attempt,
            "prompt": prompt,
            "generation_options": options,
        }
        passed = False
        resource_monitor = DockerResourceMonitor()
        execution_started: float | None = None
        artifact_path: Path | None = None
        stage = "generation"
        resource_monitor.start()
        generation_started = time.monotonic()
        try:
            warm_up(args.model, args.ollama_url, args.model_timeout, args.keep_alive)
            generated, generation_metrics = generate_with_metrics(
                prompt, args.model, args.ollama_url, args.model_timeout, options,
                args.keep_alive, args.think,
            )
            input_rate = args.input_cost_per_million_tokens
            output_rate = args.output_cost_per_million_tokens
            if (
                input_rate is not None
                and output_rate is not None
                and isinstance(generation_metrics.get("prompt_tokens"), int)
                and isinstance(generation_metrics.get("output_tokens"), int)
            ):
                generation_metrics["cost_estimate_usd"] = (
                    generation_metrics["prompt_tokens"] * input_rate
                    + generation_metrics["output_tokens"] * output_rate
                ) / 1_000_000
            else:
                generation_metrics["cost_estimate_usd"] = None
            attempt_result["generation_metrics"] = generation_metrics
            stage = "artifact_validation"
            artifact = _artifact_text(generated, args.platform)
            artifact_path = output_dir / f"{run_id}_attempt-{attempt}.{extension}"
            artifact_path.write_text(artifact, encoding="utf-8")
            attempt_result["artifact"] = str(artifact_path)
            previous_artifact = artifact

            stage = "platform_execution"
            execution_started = time.monotonic()
            start_recorder()
            _reset_sink(recorder_url, run_id, attempt)
            if args.platform == "airflow":
                run_airflow(artifact_path, run_id, args.execution_timeout)
            elif args.platform == "n8n":
                run_n8n(artifact_path, args.execution_timeout)
            elif args.platform == "dagster":
                run_dagster(artifact_path, run_id, args.execution_timeout)
            elif args.platform == "prefect":
                run_prefect(artifact_path, run_id, args.execution_timeout)
            elif args.platform == "nifi":
                run_nifi(artifact_path, run_id, args.execution_timeout)
            elif args.platform == "kestra":
                run_kestra(artifact_path, run_id, args.execution_timeout)
            evaluation = _score(recorder_url, run_id, attempt, task, benchmark)
            attempt_result["evaluation"] = evaluation
            passed = evaluation["passed"]
            if not passed:
                failure_report = json.dumps(evaluation, indent=2)
                attempt_result["failure_stage"] = "data_correctness"
                attempt_result["failure_category"] = "incorrect_output"
            else:
                attempt_result["failure_stage"] = None
                attempt_result["failure_category"] = None
        except (OllamaError, PlatformError, RuntimeError, ValueError, OSError) as exc:
            if stage == "generation":
                attempt_result["generation_metrics"] = {
                    "generation_duration_seconds": time.monotonic() - generation_started,
                    "prompt_tokens": None,
                    "output_tokens": None,
                    "cost_estimate_usd": None,
                }
            attempt_result["execution_error"] = str(exc)
            attempt_result["failure_stage"] = stage
            attempt_result["failure_category"] = _classify_failure(stage, exc)
            failure_report = str(exc)
        finally:
            attempt_result["execution_duration_seconds"] = (
                time.monotonic() - execution_started if execution_started is not None else 0
            )
            try:
                source_metrics = _request_json(
                    "GET",
                    f"{recorder_url}/metrics/{run_id}?attempt={attempt}",
                    timeout=5,
                )
                expected_sources = set(task["source_ids"])
                requested_sources = set(source_metrics.get("requested_source_ids", []))
                source_metrics["source_coverage"] = (
                    len(expected_sources & requested_sources) / len(expected_sources)
                    if expected_sources else 1.0
                )
                source_metrics["missing_source_ids"] = sorted(expected_sources - requested_sources)
                attempt_result["request_metrics"] = source_metrics
            except RuntimeError as exc:
                attempt_result["request_metrics_error"] = str(exc)
            attempt_result["container_resources"] = resource_monitor.stop()
            # Keep platform services warm for the next cell of the same platform only when this
            # cell is finished and healthy. Repair attempts and platform failures start clean.
            will_retry = not passed and attempt < args.repairs
            platform_failed = attempt_result.get("failure_category") in {"platform_error", "harness_error"}
            keep_running = getattr(args, "keep_services", False) and not will_retry and not platform_failed
            if not keep_running:
                try:
                    stop_services()
                except PlatformError as exc:
                    attempt_result["cleanup_error"] = str(exc)
            if artifact_path is not None:
                try:
                    cleanup_staged_artifact(args.platform, run_id, artifact_path)
                except (OSError, PlatformError) as exc:
                    attempt_result["artifact_cleanup_error"] = str(exc)
        history.append(attempt_result)
        if passed:
            result = _build_result(
                args, run_id, artifact_path, history, "passed",
                time.monotonic() - run_started, environment,
            )
            metadata_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(result, indent=2))
            return 0
        if attempt < args.repairs:
            print(f"Attempt {attempt + 1} failed; requesting repair {attempt + 1}/{args.repairs}.", file=sys.stderr)

    result = _build_result(
        args, run_id, artifact_path, history, "failed", time.monotonic() - run_started,
        environment,
    )
    metadata_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 1


def _build_result(
    args: argparse.Namespace,
    run_id: str,
    artifact_path: Path | None,
    history: list[dict[str, Any]],
    status: str,
    total_duration_seconds: float,
    environment: dict[str, Any],
) -> dict[str, Any]:
    generation_metrics = [
        entry["generation_metrics"] for entry in history
        if isinstance(entry.get("generation_metrics"), dict)
    ]
    resources = [
        entry["container_resources"] for entry in history
        if isinstance(entry.get("container_resources"), dict)
    ]
    f1_scores = [
        entry["evaluation"]["f1"]
        for entry in history
        if isinstance(entry.get("evaluation", {}).get("f1"), (int, float))
    ]

    def total_metric(name: str) -> int | float | None:
        values = [metrics.get(name) for metrics in generation_metrics]
        if not values or not all(isinstance(value, (int, float)) for value in values):
            return None
        return sum(values)

    resource_samples = sum(resource.get("sample_count", 0) for resource in resources)
    gpu_resources = [resource for resource in resources if resource.get("gpu_supported")]
    gpu_energy_values = [
        resource["estimated_gpu_energy_wh"]
        for resource in gpu_resources
        if isinstance(resource.get("estimated_gpu_energy_wh"), (int, float))
    ]
    cpu_samples = [
        (resource["mean_cpu_percent"], resource["sample_count"])
        for resource in resources
        if isinstance(resource.get("mean_cpu_percent"), (int, float))
        and resource.get("sample_count", 0) > 0
    ]
    metrics = {
        "first_attempt_passed": (
            history[0].get("evaluation", {}).get("passed") if history else None
        ),
        "first_attempt_f1": (
            history[0].get("evaluation", {}).get("f1") if history else None
        ),
        "best_f1": max(f1_scores, default=None),
        "generation_duration_seconds": sum(
            entry["generation_metrics"]["generation_duration_seconds"] for entry in history
        ),
        "execution_duration_seconds": sum(
            entry.get("execution_duration_seconds", 0) for entry in history
        ),
        "total_duration_seconds": total_duration_seconds,
        "prompt_tokens": total_metric("prompt_tokens"),
        "output_tokens": total_metric("output_tokens"),
        "cost_estimate_usd": total_metric("cost_estimate_usd"),
        "mean_cpu_percent": (
            sum(cpu * count for cpu, count in cpu_samples) / resource_samples
            if resource_samples else None
        ),
        "peak_cpu_percent": max(
            (resource["peak_cpu_percent"] for resource in resources
             if isinstance(resource.get("peak_cpu_percent"), (int, float))),
            default=None,
        ),
        "peak_memory_bytes": max(
            (resource["peak_memory_bytes"] for resource in resources
             if isinstance(resource.get("peak_memory_bytes"), int)),
            default=None,
        ),
        "resource_sample_count": resource_samples,
        "mean_gpu_utilization_percent": _weighted_resource_mean(
            resources, "mean_gpu_utilization_percent", "gpu_sample_count"
        ),
        "peak_gpu_memory_bytes": max(
            (resource["peak_gpu_memory_bytes"] for resource in resources
             if isinstance(resource.get("peak_gpu_memory_bytes"), int)),
            default=None,
        ),
        "estimated_gpu_energy_wh": (
            sum(gpu_energy_values)
            if gpu_resources and len(gpu_energy_values) == len(gpu_resources) else None
        ),
        "gpu_energy_measurement_complete": (
            bool(gpu_resources) and len(gpu_energy_values) == len(gpu_resources)
        ),
        "source_coverage": _mean_request_metric(history, "source_coverage"),
        "source_request_count": _sum_request_metric(history, "source_request_count"),
        "sink_post_count": _sum_request_metric(history, "sink_post_count"),
        "sink_rows_accepted": _sum_request_metric(history, "sink_rows_accepted"),
    }
    failure_categories: dict[str, int] = {}
    for entry in history:
        category = entry.get("failure_category")
        if isinstance(category, str):
            failure_categories[category] = failure_categories.get(category, 0) + 1
    return {
        "run_id": run_id,
        "task": args.task,
        "platform": args.platform,
        "model": args.model,
        "status": status,
        "attempts": len(history),
        "artifact_path": str(artifact_path) if artifact_path else None,
        "configuration": {
            "prompt_version": PROMPT_VERSION,
            "scorer_version": SCORER_VERSION,
            "repairs": args.repairs,
            "model_timeout_seconds": args.model_timeout,
            "execution_timeout_seconds": args.execution_timeout,
            "ollama_generation_options": options_from_args(args),
            "ollama_keep_alive": args.keep_alive,
            "ollama_think": args.think,
            "input_cost_per_million_tokens": args.input_cost_per_million_tokens,
            "output_cost_per_million_tokens": args.output_cost_per_million_tokens,
        },
        "environment": environment,
        "failure_categories": failure_categories,
        "metrics": metrics,
        "history": history,
        "recorded_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def options_from_args(args: argparse.Namespace) -> dict[str, int | float]:
    return {
        "temperature": args.temperature,
        "seed": args.seed,
        "top_p": args.top_p,
        "top_k": args.top_k,
        "repeat_penalty": args.repeat_penalty,
        "num_thread": args.num_thread,
        "num_ctx": args.num_ctx,
        "num_predict": args.num_predict,
    }


def _weighted_resource_mean(
    resources: list[dict[str, Any]], name: str, sample_count_key: str = "sample_count"
) -> float | None:
    values = [
        (resource[name], resource.get(sample_count_key, 0))
        for resource in resources
        if isinstance(resource.get(name), (int, float))
        and resource.get(sample_count_key, 0) > 0
    ]
    count = sum(samples for _, samples in values)
    return sum(value * samples for value, samples in values) / count if count else None


def _mean_request_metric(history: list[dict[str, Any]], name: str) -> float | None:
    values = [
        request_metrics[name]
        for entry in history
        if isinstance((request_metrics := entry.get("request_metrics")), dict)
        and isinstance(request_metrics.get(name), (int, float))
    ]
    return sum(values) / len(values) if values else None


def _sum_request_metric(history: list[dict[str, Any]], name: str) -> int:
    return sum(
        request_metrics[name]
        for entry in history
        if isinstance((request_metrics := entry.get("request_metrics")), dict)
        and isinstance(request_metrics.get(name), int)
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="pipeline-bench")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("serve", help="Run the fixture source and recording sink HTTP service")
    commands.add_parser("list", help="List configured benchmark tasks")
    generate_parser = commands.add_parser("run", help="Generate, execute, and score one benchmark cell")
    generate_parser.add_argument("--task", required=True)
    generate_parser.add_argument(
        "--platform", required=True, choices=list(PLATFORMS)
    )
    generate_parser.add_argument("--model", required=True, help="Ollama model identifier")
    generate_parser.add_argument("--ollama-url", default=os.environ.get("OLLAMA_URL", "http://localhost:11434"))
    generate_parser.add_argument("--recorder-url", default=os.environ.get("RECORDER_URL", "http://localhost:8090"))
    generate_parser.add_argument("--source-url", default=os.environ.get("SOURCE_URL", "http://recorder:8090"))
    generate_parser.add_argument("--run-id", default=None)
    generate_parser.add_argument("--output-dir", default=str(ROOT / "benchmark-results" / "runs"))
    generate_parser.add_argument("--repairs", type=int, default=2)
    generate_parser.add_argument("--model-timeout", type=int, default=300)
    generate_parser.add_argument("--execution-timeout", type=int, default=300)
    generate_parser.add_argument("--temperature", type=float, default=0.0)
    generate_parser.add_argument("--seed", type=int, default=42)
    generate_parser.add_argument("--top-p", type=float, default=1.0)
    generate_parser.add_argument("--top-k", type=int, default=40)
    generate_parser.add_argument("--repeat-penalty", type=float, default=1.1)
    generate_parser.add_argument("--num-thread", type=int, default=0)
    generate_parser.add_argument("--num-ctx", type=int, default=8192)
    generate_parser.add_argument("--num-predict", type=int, default=4096)
    generate_parser.add_argument("--keep-alive", default="30m")
    generate_parser.add_argument(
        "--keep-services", action="store_true",
        help="leave platform services running after a healthy cell (used by run_matrix for warm reuse)",
    )
    generate_parser.add_argument(
        "--think", action="store_true",
        help="Enable model reasoning output when supported (disabled by default for artifact-only generation)",
    )
    generate_parser.add_argument(
        "--input-cost-per-million-tokens",
        type=float,
        help="Optional input-token price in USD per million tokens for cost estimates",
    )
    generate_parser.add_argument(
        "--output-cost-per-million-tokens",
        type=float,
        help="Optional output-token price in USD per million tokens for cost estimates",
    )
    args = parser.parse_args()

    try:
        if args.command == "serve":
            serve()
        elif args.command == "list":
            benchmark = load_benchmark()
            print(json.dumps([
                {"id": task_id, "name": task["name"], "source_ids": task["source_ids"]}
                for task_id, task in list_tasks(benchmark)
            ], indent=2))
        elif args.command == "run":
            if args.repairs < 0 or args.repairs > 5:
                parser.error("--repairs must be between 0 and 5")
            rates = (
                args.input_cost_per_million_tokens,
                args.output_cost_per_million_tokens,
            )
            if (rates[0] is None) != (rates[1] is None):
                parser.error("both token cost rates must be provided together")
            if any(rate is not None and (not math.isfinite(rate) or rate < 0) for rate in rates):
                parser.error("token cost rates must be finite and non-negative")
            if not math.isfinite(args.temperature) or args.temperature < 0:
                parser.error("--temperature must be finite and non-negative")
            if not 0 <= args.top_p <= 1:
                parser.error("--top-p must be between 0 and 1")
            if args.top_k < 0 or args.num_thread < 0 or args.num_ctx < 1 or args.num_predict < 1:
                parser.error("--top-k and --num-thread must be non-negative; --num-ctx and --num-predict must be positive")
            if not args.keep_alive:
                parser.error("--keep-alive must not be empty")
            if not math.isfinite(args.repeat_penalty) or args.repeat_penalty <= 0:
                parser.error("--repeat-penalty must be finite and positive")
            sys.exit(_run(args))
    except (OllamaError, PlatformError, RuntimeError, ValueError) as exc:
        parser.exit(2, f"error: {exc}\n")


if __name__ == "__main__":
    main()
