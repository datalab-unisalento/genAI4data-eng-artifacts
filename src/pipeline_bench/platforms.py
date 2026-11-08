from __future__ import annotations

import base64
import http.client
import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker-compose.yml")]


class DockerResourceMonitor:
    def __init__(self, interval: float = 1.0) -> None:
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._cpu_samples: list[float] = []
        self._peak_memory_bytes = 0
        self._samples = 0
        self._gpu_utilization: list[float] = []
        self._gpu_samples = 0
        self._peak_gpu_memory_bytes = 0
        self._gpu_energy_wh = 0.0
        self._gpu_power_sample: tuple[float, float | None] | None = None
        self._gpu_power_samples = 0
        self._gpu_names: list[str] = []
        self._gpu_driver_version: str | None = None
        self._gpu_supported = shutil.which("nvidia-smi") is not None
        self._gpu_error: str | None = None
        self._error: str | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(30.0, self.interval * 2))
        return {
            "sample_count": self._samples,
            "mean_cpu_percent": (
                sum(self._cpu_samples) / len(self._cpu_samples) if self._cpu_samples else None
            ),
            "peak_cpu_percent": max(self._cpu_samples) if self._cpu_samples else None,
            "peak_memory_bytes": self._peak_memory_bytes or None,
            "gpu_supported": self._gpu_supported,
            "gpu_names": self._gpu_names,
            "gpu_driver_version": self._gpu_driver_version,
            "mean_gpu_utilization_percent": (
                sum(self._gpu_utilization) / len(self._gpu_utilization)
                if self._gpu_utilization else None
            ),
            "gpu_sample_count": self._gpu_samples,
            "peak_gpu_memory_bytes": self._peak_gpu_memory_bytes or None,
            "gpu_power_sample_count": self._gpu_power_samples,
            "estimated_gpu_energy_wh": self._gpu_energy_wh if self._gpu_power_samples > 1 else None,
            "measurement_error": self._error,
            "gpu_measurement_error": self._gpu_error,
        }

    def _sample_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._sample()
                self._error = None
            except (OSError, subprocess.SubprocessError, PlatformError, ValueError) as exc:
                self._error = str(exc)
            self._stop.wait(self.interval)

    def _sample(self) -> None:
        try:
            self._sample_gpu()
            self._gpu_error = None
        except (PlatformError, ValueError) as exc:
            self._gpu_error = str(exc)
        container_ids = _compose(
            "--profile", "airflow", "--profile", "n8n", "--profile", "dagster",
            "--profile", "prefect", "--profile", "kestra", "--profile", "nifi",
            "ps", "-q", timeout=10,
        ).split()
        if not container_ids:
            return
        try:
            result = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", *container_ids],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except FileNotFoundError as exc:
            raise PlatformError("Docker is required to collect container resource metrics") from exc
        except subprocess.CalledProcessError as exc:
            raise PlatformError(f"docker stats failed: {exc.stderr.strip() or exc.stdout.strip()}") from exc
        except subprocess.TimeoutExpired as exc:
            raise PlatformError("docker stats timed out while collecting resource metrics") from exc

        cpu_total = 0.0
        memory_total = 0
        found = False
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            stats = json.loads(line)
            cpu = stats.get("CPUPerc", "").rstrip("%")
            memory = stats.get("MemUsage", "").split("/", 1)[0].strip()
            cpu_total += float(cpu)
            memory_total += _parse_docker_bytes(memory)
            found = True
        if found:
            self._cpu_samples.append(cpu_total)
            self._peak_memory_bytes = max(self._peak_memory_bytes, memory_total)
            self._samples += 1

    def _sample_gpu(self) -> None:
        if not self._gpu_supported:
            return
        try:
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=name,driver_version,utilization.gpu,memory.used,power.draw",
                    "--format=csv,noheader,nounits",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise PlatformError(f"Unable to collect NVIDIA GPU metrics: {exc}") from exc

        names: list[str] = []
        drivers: list[str] = []
        utilization: list[float] = []
        memory_bytes = 0
        power_watts: float | None = 0.0
        for line in result.stdout.splitlines():
            fields = [field.strip() for field in line.split(",")]
            if len(fields) != 5:
                raise ValueError(f"Unexpected nvidia-smi output: {line!r}")
            name, driver, gpu_util, memory_mib, power = fields
            names.append(name)
            drivers.append(driver)
            utilization.append(float(gpu_util))
            memory_bytes += int(float(memory_mib) * 1_048_576)
            if power.lower() != "[n/a]":
                if power_watts is not None:
                    power_watts += float(power)
            else:
                power_watts = None
        if not utilization:
            return

        now = time.monotonic()
        if (
            self._gpu_power_sample is not None
            and self._gpu_power_sample[1] is not None
            and power_watts is not None
        ):
            previous_time, previous_power = self._gpu_power_sample
            elapsed = now - previous_time
            self._gpu_energy_wh += ((previous_power + power_watts) / 2) * elapsed / 3600
        self._gpu_power_sample = (now, power_watts)
        if power_watts is not None:
            self._gpu_power_samples += 1
        self._gpu_names = names
        self._gpu_driver_version = ",".join(sorted(set(drivers)))
        self._gpu_utilization.append(sum(utilization) / len(utilization))
        self._gpu_samples += 1
        self._peak_gpu_memory_bytes = max(self._peak_gpu_memory_bytes, memory_bytes)


def _parse_docker_bytes(value: str) -> int:
    units = {
        "B": 1,
        "kB": 1_000,
        "KB": 1_000,
        "KiB": 1_024,
        "MB": 1_000_000,
        "MiB": 1_048_576,
        "GB": 1_000_000_000,
        "GiB": 1_073_741_824,
        "TB": 1_000_000_000_000,
        "TiB": 1_099_511_627_776,
    }
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)", value.strip())
    if not match:
        raise ValueError(f"Unsupported Docker memory value: {value}")
    number, unit = match.groups()
    if unit == "":
        unit = "B"
    if unit not in units:
        raise ValueError(f"Unsupported Docker memory unit: {unit}")
    return int(float(number) * units[unit])


class PlatformError(RuntimeError):
    """Infrastructure or service failure, not caused by the generated artifact."""


class ArtifactError(PlatformError):
    """The generated artifact cannot be loaded, parsed, or validated by its platform."""


class ExecutionError(PlatformError):
    """The artifact loaded and ran, but the run failed during execution."""


_ARTIFACT_LOAD_MARKERS = (
    "SyntaxError", "ImportError", "ModuleNotFoundError", "NameError",
    "DagsterInvalidDefinitionError", "DagsterInvariantViolationError", "has no attribute",
    "is not defined", "invalid definition",
)


def _is_artifact_load_failure(message: str) -> bool:
    return any(marker in message for marker in _ARTIFACT_LOAD_MARKERS)


def _http_json(
    method: str,
    url: str,
    value: Any | None = None,
    *,
    username: str | None = None,
    password: str | None = None,
    timeout: int = 10,
) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if username is not None and password is not None:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    data = None if value is None else json.dumps(value).encode()
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
            return response.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise PlatformError(f"{method} {url} returned HTTP {exc.code}: {body}") from exc
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise PlatformError(f"{method} {url} failed: {exc}") from exc


def _compose(*args: str, timeout: int = 60) -> str:
    try:
        result = subprocess.run(
            [*COMPOSE, *args],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            # The recorder runs as the host user so it can write the logs it owns.
            env={**os.environ, "HOST_UID": str(os.getuid()), "HOST_GID": str(os.getgid())},
        )
    except FileNotFoundError as exc:
        raise PlatformError("Docker Compose is required to execute a benchmark cell") from exc
    except subprocess.CalledProcessError as exc:
        raise PlatformError(
            f"docker compose {' '.join(args)} failed: {exc.stderr.strip() or exc.stdout.strip()}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise PlatformError(f"docker compose {' '.join(args)} timed out") from exc
    return result.stdout.strip()


def _wait_http(url: str, timeout: int = 180, username: str | None = None, password: str | None = None) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        try:
            _http_json("GET", url, username=username, password=password, timeout=3)
            return
        except PlatformError as exc:
            last_error = str(exc)
            time.sleep(2)
    raise PlatformError(f"Service did not become ready at {url}: {last_error}")


def start_recorder() -> None:
    _compose("up", "-d", "--build", "recorder", timeout=600)
    _wait_http("http://localhost:8090/health")
    # Fail before any cell runs if the recorder cannot write its logs.
    try:
        _compose("exec", "-T", "recorder", "sh", "-c", "touch /data/.preflight && rm -f /data/.preflight")
    except PlatformError as exc:
        raise PlatformError(f"Recorder cannot write to its log directory: {exc}") from exc


def _service_ip(service: str, profile: str) -> str:
    container_id = _compose("--profile", profile, "ps", "-q", service).strip()
    if not container_id:
        raise PlatformError(f"{service} container is not running")
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container_id],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise PlatformError(f"Unable to discover {service} container address: {exc}") from exc
    address = result.stdout.strip()
    if not re.fullmatch(r"[0-9]{1,3}(?:\.[0-9]{1,3}){3}", address):
        raise PlatformError(f"{service} container returned an invalid network address: {address!r}")
    return address


def _airflow_api_base() -> str:
    return f"http://{_service_ip('airflow', 'airflow')}:8080/api/v1"


def _airflow_run_details(
    airflow_url: str,
    dag_path: str,
    run_path: str,
    username: str,
    password: str,
) -> dict[str, Any]:
    details: dict[str, Any] = {}
    try:
        _, dag_run = _http_json(
            "GET",
            f"{airflow_url}/dags/{dag_path}/dagRuns/{run_path}",
            username=username,
            password=password,
        )
        details["dag_run"] = dag_run
        _, task_response = _http_json(
            "GET",
            f"{airflow_url}/dags/{dag_path}/dagRuns/{run_path}/taskInstances",
            username=username,
            password=password,
        )
        task_instances = task_response.get("task_instances", [])
        for task in task_instances:
            task_id = task.get("task_id")
            try_number = task.get("try_number")
            if task_id and try_number and task.get("state") in {"failed", "running"}:
                try:
                    _, logs = _http_json(
                        "GET",
                        f"{airflow_url}/dags/{dag_path}/dagRuns/{run_path}/taskInstances/"
                        f"{urllib.parse.quote(task_id, safe='')}/logs/{try_number}",
                        username=username,
                        password=password,
                    )
                    task["log_excerpt"] = str(logs.get("content", logs))[-8000:]
                except PlatformError as exc:
                    task["log_error"] = str(exc)
        details["task_instances"] = task_instances
    except PlatformError as exc:
        details["diagnostic_error"] = str(exc)
    return details


def stop_services() -> None:
    _compose(
        "--profile", "airflow", "--profile", "n8n", "--profile", "dagster", "--profile", "prefect",
        "--profile", "kestra", "--profile", "nifi", "down", "--remove-orphans", timeout=180,
    )


def cleanup_staged_artifact(platform: str, run_id: str, generated_path: Path) -> None:
    if platform == "airflow":
        staged = ROOT / "artifacts" / "airflow" / f"bench_{run_id.replace('-', '_')}.py"
    elif platform in {"dagster", "prefect"}:
        staged = ROOT / "artifacts" / "python" / f"{platform}_{run_id.replace('-', '_')}.py"
    else:
        if platform != "n8n":
            return
        workflow = _load_n8n_workflow(generated_path)
        workflow_id = workflow.get("id")
        if not isinstance(workflow_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", workflow_id):
            return
        staged = ROOT / "artifacts" / "n8n" / f"{workflow_id}.json"
    staged.unlink(missing_ok=True)


def run_airflow(artifact_path: Path, run_id: str, timeout: int = 300) -> None:
    artifact_dir = ROOT / "artifacts" / "airflow"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    staged = artifact_dir / f"bench_{run_id.replace('-', '_')}.py"
    staged.write_text(artifact_path.read_text(encoding="utf-8"), encoding="utf-8")
    username = os.environ.get("AIRFLOW_USERNAME", "bench")
    password = os.environ.get("AIRFLOW_PASSWORD", "bench-local-only")
    _compose("--profile", "airflow", "up", "-d", "recorder", "airflow", timeout=900)
    _wait_http("http://localhost:8090/health")
    airflow_url = _airflow_api_base()
    _wait_http(f"{airflow_url}/health")
    try:
        _compose(
            "--profile", "airflow", "exec", "-T", "airflow", "airflow", "users", "create",
            "--username", username,
            "--firstname", "Benchmark",
            "--lastname", "Runner",
            "--role", "Admin",
            "--email", f"{username}@localhost",
            "--password", password,
            timeout=60,
        )
    except PlatformError as exc:
        # Services are kept warm across cells, so the user usually exists already.
        if "already exist" not in str(exc).lower():
            raise
    dag_id = f"bench_{run_id.replace('-', '_')}"
    dag_path = urllib.parse.quote(dag_id, safe="")
    dag_deadline = time.monotonic() + 120
    while True:
        try:
            _http_json("GET", f"{airflow_url}/dags/{dag_path}", username=username, password=password)
            break
        except PlatformError as exc:
            if time.monotonic() >= dag_deadline:
                diagnostics = ""
                try:
                    _, errors = _http_json(
                        "GET",
                        f"{airflow_url}/importErrors",
                        username=username,
                        password=password,
                    )
                    import_errors = errors.get("import_errors", [])
                    if import_errors:
                        diagnostics = f"; DAG import errors: {import_errors}"
                except PlatformError as diagnostic_error:
                    diagnostics = f"; unable to read DAG import errors: {diagnostic_error}"
                raise ArtifactError(f"Airflow did not load DAG {dag_id}: {exc}{diagnostics}") from exc
            time.sleep(2)
    dag_run_id = f"manual__{run_id.replace('-', '_')}"
    _http_json(
        "PATCH",
        f"{airflow_url}/dags/{dag_path}",
        {"is_paused": False},
        username=username,
        password=password,
    )
    _http_json(
        "POST",
        f"{airflow_url}/dags/{dag_path}/dagRuns",
        {"dag_run_id": dag_run_id, "conf": {}},
        username=username,
        password=password,
    )
    run_path = urllib.parse.quote(dag_run_id, safe="")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _, result = _http_json(
            "GET",
            f"{airflow_url}/dags/{dag_path}/dagRuns/{run_path}",
            username=username,
            password=password,
        )
        state = result.get("state")
        if state == "success":
            return
        if state in {"failed", "upstream_failed"}:
            details = _airflow_run_details(airflow_url, dag_path, run_path, username, password)
            raise ExecutionError(f"Airflow DAG run ended in state {state}: {details}")
        time.sleep(2)
    details = _airflow_run_details(airflow_url, dag_path, run_path, username, password)
    raise PlatformError(f"Airflow DAG run exceeded {timeout} seconds: {details}")


def run_n8n(artifact_path: Path, timeout: int = 300) -> None:
    workflow = _load_n8n_workflow(artifact_path)
    _validate_n8n_workflow(workflow)
    workflow_id = workflow.get("id")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", workflow_id):
        raise PlatformError("n8n workflow id may contain only letters, digits, underscores, and hyphens")
    artifact_dir = ROOT / "artifacts" / "n8n"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    staged = artifact_dir / f"{workflow_id}.json"
    staged.write_text(json.dumps([workflow]), encoding="utf-8")
    _compose("--profile", "n8n", "up", "-d", "recorder", "n8n", timeout=600)
    _wait_http("http://localhost:8090/health")
    n8n_url = f"http://{_service_ip('n8n', 'n8n')}:5678"
    _wait_http(f"{n8n_url}/healthz", timeout=180)
    try:
        _compose("exec", "-T", "n8n", "n8n", "import:workflow", f"--input=/benchmark/{staged.name}", timeout=timeout)
    except PlatformError as exc:
        raise ArtifactError(str(exc)) from exc
    try:
        _compose("exec", "-T", "n8n", "n8n", "execute", "--id", workflow_id, timeout=timeout)
    except PlatformError as exc:
        message = str(exc)
        if "has issues" in message or "WorkflowHasIssuesError" in message or _is_artifact_load_failure(message):
            raise ArtifactError(message) from exc
        raise ExecutionError(message) from exc


def run_dagster(artifact_path: Path, run_id: str, timeout: int = 300) -> None:
    staged_name = f"dagster_{run_id.replace('-', '_')}.py"
    artifact_dir = ROOT / "artifacts" / "python"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / staged_name).write_text(artifact_path.read_text(encoding="utf-8"), encoding="utf-8")
    job_name = f"bench_{run_id.replace('-', '_')}"
    try:
        _compose(
            "--profile", "dagster", "run", "--rm", "-T", "--no-deps", "python-runner",
            "sh", "-c",
            f"mkdir -p \"$DAGSTER_HOME\" && exec dagster job execute -f "
            f"/benchmark/{staged_name} -j {job_name}",
            timeout=timeout + 900,
        )
    except PlatformError as exc:
        raise (ArtifactError if _is_artifact_load_failure(str(exc)) else ExecutionError)(str(exc)) from exc


def run_prefect(artifact_path: Path, run_id: str, timeout: int = 300) -> None:
    staged_name = f"prefect_{run_id.replace('-', '_')}.py"
    artifact_dir = ROOT / "artifacts" / "python"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / staged_name).write_text(artifact_path.read_text(encoding="utf-8"), encoding="utf-8")
    try:
        _compose(
            "--profile", "prefect", "run", "--rm", "-T", "--no-deps", "python-runner",
            "python", f"/benchmark/{staged_name}",
            timeout=timeout + 900,
        )
    except PlatformError as exc:
        raise (ArtifactError if _is_artifact_load_failure(str(exc)) else ExecutionError)(str(exc)) from exc


NIFI_PROCESSOR_TYPE = "org.apache.nifi.processors.groovyx.ExecuteGroovyScript"
NIFI_PORT = 8443
NIFI_SNI_NAME = "localhost"  # NiFi rejects TLS requests whose SNI does not match its certificate.
KESTRA_PORT = 8080
KESTRA_NAMESPACE = "bench"


class _SniHTTPSConnection(http.client.HTTPSConnection):
    """Connect to a container IP while presenting the certificate's expected SNI name."""

    def __init__(self, address: str, port: int, sni_name: str, context: ssl.SSLContext) -> None:
        super().__init__(address, port, timeout=30, context=context)
        self._sni_name = sni_name

    def connect(self) -> None:
        sock = socket.create_connection((self.host, self.port), self.timeout)
        self.sock = self._context.wrap_socket(sock, server_hostname=self._sni_name)


def _nifi_request(
    address: str,
    method: str,
    path: str,
    *,
    token: str | None = None,
    body: Any | None = None,
    form: dict[str, str] | None = None,
) -> Any:
    # The NiFi certificate is self-signed and issued for localhost; verification is disabled
    # only for this loopback-to-container link on the benchmark's internal network.
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    headers = {"Host": f"{NIFI_SNI_NAME}:{NIFI_PORT}"}
    data: bytes | None = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    connection = _SniHTTPSConnection(address, NIFI_PORT, NIFI_SNI_NAME, context)
    try:
        connection.request(method, f"/nifi-api{path}", body=data, headers=headers)
        response = connection.getresponse()
        payload = response.read()
    except (OSError, http.client.HTTPException) as exc:
        raise PlatformError(f"NiFi {method} {path} failed: {exc}") from exc
    finally:
        connection.close()
    if response.status >= 400:
        raise PlatformError(
            f"NiFi {method} {path} returned HTTP {response.status}: "
            f"{payload.decode(errors='replace')[:2000]}"
        )
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except ValueError:
        return payload.decode(errors="replace")


def _wait_nifi_token(address: str, username: str, password: str, timeout: int = 300) -> str:
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        try:
            token = _nifi_request(
                address, "POST", "/access/token", form={"username": username, "password": password}
            )
            if isinstance(token, str) and token:
                return token
            last_error = f"unexpected token response: {token!r}"[:500]
        except PlatformError as exc:
            last_error = str(exc)
        time.sleep(3)
    raise PlatformError(f"NiFi did not become ready: {last_error}")


def _nifi_processor_bundle(address: str, token: str) -> dict[str, str]:
    types = _nifi_request(address, "GET", "/flow/processor-types", token=token)
    for processor_type in types.get("processorTypes", []):
        if processor_type.get("type") == NIFI_PROCESSOR_TYPE:
            return processor_type["bundle"]
    raise PlatformError(f"NiFi does not provide processor type {NIFI_PROCESSOR_TYPE}")


def run_nifi(artifact_path: Path, run_id: str, timeout: int = 300) -> None:
    script = artifact_path.read_text(encoding="utf-8")
    username = os.environ.get("NIFI_USERNAME", "bench")
    password = os.environ.get("NIFI_PASSWORD", "bench-local-only-123")
    _compose("--profile", "nifi", "up", "-d", "recorder", "nifi", timeout=900)
    _wait_http("http://localhost:8090/health")
    address = _service_ip("nifi", "nifi")
    token = _wait_nifi_token(address, username, password)
    root_id = _nifi_request(address, "GET", "/flow/process-groups/root", token=token)["processGroupFlow"]["id"]
    # Services are kept warm across cells: remove processors left by earlier cells.
    for leftover in _nifi_request(address, "GET", f"/process-groups/{root_id}/processors", token=token).get("processors", []):
        if leftover["component"]["name"].startswith("bench_") and leftover["component"]["state"] == "STOPPED":
            _nifi_request(
                address, "DELETE",
                f"/processors/{leftover['id']}?version={leftover['revision']['version']}",
                token=token,
            )
    bundle = _nifi_processor_bundle(address, token)
    created = _nifi_request(
        address, "POST", f"/process-groups/{root_id}/processors", token=token,
        body={
            "revision": {"version": 0},
            "component": {
                "type": NIFI_PROCESSOR_TYPE,
                "bundle": bundle,
                "name": f"bench_{run_id.replace('-', '_')}",
                "position": {"x": 0, "y": 0},
            },
        },
    )
    processor_id = created["id"]
    entity = _nifi_request(address, "GET", f"/processors/{processor_id}", token=token)
    component = entity["component"]
    component["config"]["properties"]["Script Body"] = script
    component["config"]["autoTerminatedRelationships"] = ["success", "failure"]
    component["relationships"] = [
        {"name": relationship["name"], "description": relationship.get("description"),
         "autoTerminate": True, "retry": False}
        for relationship in component["relationships"]
    ]
    entity = _nifi_request(
        address, "PUT", f"/processors/{processor_id}", token=token,
        body={"revision": entity["revision"], "component": component},
    )

    deadline = time.monotonic() + timeout
    while True:
        entity = _nifi_request(address, "GET", f"/processors/{processor_id}", token=token)
        if entity["component"].get("validationStatus") == "VALID":
            break
        if time.monotonic() >= deadline:
            raise ArtifactError(
                f"NiFi processor did not validate: {entity['component'].get('validationErrors')}"
            )
        time.sleep(1)

    tasks_before = entity["status"]["aggregateSnapshot"].get("tasks", 0)
    _nifi_request(
        address, "PUT", f"/processors/{processor_id}/run-status", token=token,
        body={"revision": entity["revision"], "state": "RUN_ONCE"},
    )
    deadline = time.monotonic() + timeout
    while True:
        entity = _nifi_request(address, "GET", f"/processors/{processor_id}", token=token)
        finished = (
            entity["status"]["aggregateSnapshot"].get("tasks", 0) > tasks_before
            and entity["component"].get("state") == "STOPPED"
            and not entity["status"].get("activeThreadCount")
        )
        if finished:
            break
        if time.monotonic() >= deadline:
            raise PlatformError(f"NiFi processor run exceeded {timeout} seconds")
        time.sleep(2)
    errors = [
        item["bulletin"]["message"]
        for item in entity.get("bulletins", [])
        if item.get("bulletin", {}).get("level") == "ERROR"
    ]
    if errors:
        raise ExecutionError(f"NiFi processor failed: {'; '.join(errors)}"[:8000])


def _validate_kestra_flow(flow_yaml: str, flow_id: str) -> None:
    ids = re.findall(r"^id:\s*(\S+)\s*$", flow_yaml, re.MULTILINE)
    namespaces = re.findall(r"^namespace:\s*(\S+)\s*$", flow_yaml, re.MULTILINE)
    if not ids or ids[0] != flow_id:
        raise ArtifactError(f"Kestra flow must declare top-level id {flow_id!r}")
    if not namespaces or namespaces[0] != KESTRA_NAMESPACE:
        raise ArtifactError(f"Kestra flow must declare top-level namespace {KESTRA_NAMESPACE!r}")


def run_kestra(artifact_path: Path, run_id: str, timeout: int = 300) -> None:
    flow_id = f"bench_{run_id.replace('-', '_')}"
    flow_yaml = artifact_path.read_text(encoding="utf-8")
    _validate_kestra_flow(flow_yaml, flow_id)
    username = os.environ.get("KESTRA_USERNAME", "bench@example.com")
    password = os.environ.get("KESTRA_PASSWORD", "Bench-local-only-123")
    _compose("--profile", "kestra", "up", "-d", "recorder", "kestra", timeout=900)
    _wait_http("http://localhost:8090/health")
    kestra_url = f"http://{_service_ip('kestra', 'kestra')}:{KESTRA_PORT}"
    _wait_http(f"{kestra_url}/api/v1/configs", timeout=240)
    flow_body = flow_yaml.encode("utf-8")
    flow_url = f"{kestra_url}/api/v1/main/flows/{KESTRA_NAMESPACE}/{flow_id}"
    try:
        _http_yaml("POST", f"{kestra_url}/api/v1/main/flows", flow_body, username, password)
    except PlatformError as exc:
        # A 422 is either a duplicate id (update the existing flow) or an invalid flow (report it).
        if "HTTP 422" not in str(exc) or not _kestra_flow_exists(flow_url, username, password):
            raise
        _http_yaml("PUT", flow_url, flow_body, username, password)

    execution = _kestra_trigger_execution(kestra_url, flow_id, username, password)
    execution_path = f"{kestra_url}/api/v1/main/executions/{execution['id']}"
    deadline = time.monotonic() + timeout
    while True:
        _, current = _http_json("GET", execution_path, username=username, password=password)
        state = current.get("state", {}).get("current")
        if state == "SUCCESS":
            return
        if state in {"FAILED", "KILLED", "CANCELLED", "WARNING"}:
            details = _kestra_execution_details(kestra_url, execution["id"], username, password)
            raise ExecutionError(f"Kestra execution ended in state {state}: {details}")
        if time.monotonic() >= deadline:
            raise PlatformError(f"Kestra execution exceeded {timeout} seconds in state {state}")
        time.sleep(2)


def _kestra_flow_exists(flow_url: str, username: str, password: str) -> bool:
    try:
        _http_json("GET", flow_url, username=username, password=password)
    except PlatformError as exc:
        if "HTTP 404" in str(exc):
            return False
        raise
    return True


def _kestra_trigger_execution(kestra_url: str, flow_id: str, username: str, password: str) -> dict[str, Any]:
    # Kestra accepts execution triggers only as multipart/form-data; an empty body means no inputs.
    boundary = "pipeline-bench-empty-inputs"
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = urllib.request.Request(
        f"{kestra_url}/api/v1/main/executions/{KESTRA_NAMESPACE}/{flow_id}",
        data=f"--{boundary}--\r\n".encode(),
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Authorization": f"Basic {token}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise PlatformError(f"POST {request.full_url} returned HTTP {exc.code}: {exc.read().decode(errors='replace')[:2000]}") from exc
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise PlatformError(f"POST {request.full_url} failed: {exc}") from exc


def _http_yaml(method: str, url: str, body: bytes, username: str, password: str) -> None:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = urllib.request.Request(
        url, data=body, method=method,
        headers={"Content-Type": "application/x-yaml", "Authorization": f"Basic {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            return
    except urllib.error.HTTPError as exc:
        message = f"{method} {url} returned HTTP {exc.code}: {exc.read().decode(errors='replace')[:2000]}"
        # Kestra answers 422 only when it rejects the flow definition itself.
        if exc.code == 422:
            raise ArtifactError(message) from exc
        raise PlatformError(message) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PlatformError(f"{method} {url} failed: {exc}") from exc


def _kestra_execution_details(
    kestra_url: str, execution_id: str, username: str, password: str
) -> str:
    try:
        _, logs = _http_json(
            "GET",
            f"{kestra_url}/api/v1/main/logs/{execution_id}",
            username=username, password=password,
        )
        return json.dumps(logs)[-8000:]
    except PlatformError as exc:
        return f"unable to read execution logs: {exc}"


def _load_n8n_workflow(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"Generated n8n artifact is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ArtifactError("Generated n8n artifact must be a JSON object")
    return value


def _validate_n8n_workflow(workflow: dict[str, Any]) -> None:
    workflow_id = workflow.get("id")
    if not isinstance(workflow_id, str) or not workflow_id:
        raise ArtifactError("n8n workflow JSON must contain a non-empty top-level string 'id'")
    nodes = workflow.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ArtifactError("n8n workflow JSON must contain a non-empty 'nodes' array")
    node_names: set[str] = set()
    node_types: set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            raise ArtifactError("Each n8n node must be a JSON object")
        name, node_type = node.get("name"), node.get("type")
        if not isinstance(name, str) or not name:
            raise ArtifactError("Each n8n node must have a non-empty 'name'")
        if not isinstance(node_type, str):
            raise ArtifactError(f"n8n node {name!r} must have a string 'type'")
        if not isinstance(node.get("position"), list) or len(node["position"]) != 2:
            raise ArtifactError(f"n8n node {name!r} must have a two-number 'position' array")
        parameters = node.get("parameters")
        if not isinstance(parameters, dict):
            raise ArtifactError(f"n8n node {name!r} must have a 'parameters' object")
        node_names.add(name)
        node_types.add(node_type)
        if node_type == "n8n-nodes-base.httpRequest":
            if not isinstance(parameters.get("url"), str) or not parameters["url"]:
                raise ArtifactError(
                    f"n8n HTTP Request node {name!r} must set its URL inside parameters.url"
                )
        elif node_type == "n8n-nodes-base.code":
            if not isinstance(parameters.get("jsCode"), str) or not parameters["jsCode"]:
                raise ArtifactError(
                    f"n8n Code node {name!r} must put its JavaScript in parameters.jsCode"
                )
            if parameters.get("mode") != "runOnceForAllItems":
                raise ArtifactError(
                    f"n8n Code node {name!r} must set parameters.mode to 'runOnceForAllItems'"
                )
        if (
            node_type == "n8n-nodes-base.httpRequest"
            and parameters.get("method") == "POST"
            and (
                parameters.get("sendBody") is not True
                or parameters.get("specifyBody") != "json"
                or parameters.get("jsonBody") != "={{ $json }}"
            )
        ):
            raise ArtifactError(
                f"n8n sink node {name!r} must set parameters.sendBody=true, "
                "parameters.specifyBody='json', and parameters.jsonBody='={{ $json }}'; "
                f"received jsonBody={parameters.get('jsonBody')!r}"
            )
    if not node_types & {"n8n-nodes-base.manualTrigger", "n8n-nodes-base.executeWorkflowTrigger"}:
        raise ArtifactError(
            "n8n workflow must include an n8n-nodes-base.manualTrigger node"
        )
    if "n8n-nodes-base.code" not in node_types:
        raise ArtifactError("n8n workflow must include an n8n-nodes-base.code node")
    connections = workflow.get("connections")
    if not isinstance(connections, dict):
        raise ArtifactError("n8n workflow must contain a connections object")
    for source_name, outputs in connections.items():
        if source_name not in node_names or not isinstance(outputs, dict):
            raise ArtifactError(
                f"n8n connections must use existing node names; invalid source {source_name!r}"
            )
        for output_type, groups in outputs.items():
            if not isinstance(groups, list) or any(not isinstance(group, list) for group in groups):
                raise ArtifactError(
                    f"n8n connection {source_name!r}.{output_type} must be an array of arrays "
                    "of links, e.g. [[{\"node\":\"NEXT_NODE\",\"type\":\"main\",\"index\":0}]]"
                )
            for group in groups:
                for link in group:
                    if not isinstance(link, dict) or link.get("node") not in node_names:
                        raise ArtifactError(
                            f"n8n connection {source_name!r}.{output_type} has an invalid "
                            "destination node"
                        )
    settings = workflow.get("settings")
    required_settings = {
        "executionOrder": "v1",
        "saveDataErrorExecution": "all",
        "saveDataSuccessExecution": "all",
        "saveManualExecutions": True,
    }
    if not isinstance(settings, dict) or any(
        settings.get(key) != value for key, value in required_settings.items()
    ):
        raise ArtifactError(
            "n8n workflow settings must explicitly set executionOrder, "
            "saveDataErrorExecution, saveDataSuccessExecution, and saveManualExecutions "
            "to the required benchmark values"
        )
