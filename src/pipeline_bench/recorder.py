from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .benchmark import load_benchmark


class RecorderState:
    def __init__(self, path: Path, benchmark: dict[str, Any], http_log_path: Path | None = None):
        self.path = path
        self.http_log_path = http_log_path or path.with_name("http-events.jsonl")
        self.benchmark = benchmark
        self.lock = threading.Lock()
        self.source_ids = {source["path"]: source_id for source_id, source in benchmark["sources"].items()}
        self.sinks: dict[tuple[str, int], list[dict[str, Any]]] = {}
        self.http_events: list[dict[str, Any]] = []
        self._load()
        self._load_http_events()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            for line in self.path.read_text(encoding="utf-8").splitlines():
                event = json.loads(line)
                self.sinks.setdefault((event["run_id"], event.get("attempt", 0)), []).extend(event["rows"])
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            raise RuntimeError(f"Sink event log is invalid: {self.path}")

    def _load_http_events(self) -> None:
        if not self.http_log_path.exists():
            return
        try:
            self.http_events = [
                json.loads(line)
                for line in self.http_log_path.read_text(encoding="utf-8").splitlines()
            ]
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"HTTP event log is invalid: {self.http_log_path}") from exc

    def append(self, run_id: str, rows: list[dict[str, Any]], attempt: int = 0) -> None:
        event = {"run_id": run_id, "attempt": attempt, "rows": rows}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock:
            with self.path.open("a", encoding="utf-8") as log:
                log.write(json.dumps(event, separators=(",", ":")) + "\n")
            self.sinks.setdefault((run_id, attempt), []).extend(rows)
            self._record_http({
                "run_id": run_id,
                "attempt": attempt,
                "method": "POST",
                "path": "sink",
                "accepted_rows": len(rows),
            })

    def record_source(self, path: str, run_id: str, attempt: int) -> None:
        with self.lock:
            self._record_http({
                "run_id": run_id,
                "attempt": attempt,
                "method": "GET",
                "path": path,
                "source_id": self.source_ids.get(path, "unknown"),
            })

    def _record_http(self, event: dict[str, Any]) -> None:
        self.http_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.http_log_path.open("a", encoding="utf-8") as log:
            log.write(json.dumps(event, separators=(",", ":")) + "\n")
        self.http_events.append(event)

    def get_metrics(self, run_id: str, attempt: int = 0) -> dict[str, Any]:
        with self.lock:
            events = [
                event for event in self.http_events
                if event.get("run_id") == run_id and event.get("attempt", 0) == attempt
            ]
        source_ids = sorted({
            event["source_id"] for event in events
            if event.get("method") == "GET" and isinstance(event.get("source_id"), str)
        })
        sink_events = [
            event for event in events
            if event.get("method") == "POST" and event.get("path") == "sink"
        ]
        return {
            "source_request_count": sum(
                event.get("method") == "GET" and event.get("source_id") != "unknown"
                for event in events
            ),
            "requested_source_ids": source_ids,
            "sink_post_count": len(sink_events),
            "sink_rows_accepted": sum(event.get("accepted_rows", 0) for event in sink_events),
        }

    def get_rows(self, run_id: str, attempt: int = 0) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.sinks.get((run_id, attempt), []))

    def clear(self, run_id: str, attempt: int = 0) -> None:
        with self.lock:
            self.sinks.pop((run_id, attempt), None)
            retained = []
            if self.path.exists():
                for line in self.path.read_text(encoding="utf-8").splitlines():
                    event = json.loads(line)
                    if (event.get("run_id"), event.get("attempt", 0)) != (run_id, attempt):
                        retained.append(line)
            self.path.write_text("".join(f"{line}\n" for line in retained), encoding="utf-8")
            retained_http = [
                event for event in self.http_events
                if (event.get("run_id"), event.get("attempt", 0)) != (run_id, attempt)
            ]
            self.http_log_path.write_text(
                "".join(f"{json.dumps(event, separators=(',', ':'))}\n" for event in retained_http),
                encoding="utf-8",
            )
            self.http_events = retained_http


def make_handler(state: RecorderState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "PipelineBenchRecorder/0.1"

        def _send_json(self, status: int, value: Any) -> None:
            body = json.dumps(value).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send_json(200, {"status": "ok"})
                return
            if parsed.path.startswith("/metrics/"):
                run_id = unquote(parsed.path.removeprefix("/metrics/"))
                try:
                    attempt = _attempt(parse_qs(parsed.query))
                except ValueError as exc:
                    self._send_json(400, {"error": str(exc)})
                    return
                self._send_json(200, {
                    "run_id": run_id,
                    "attempt": attempt,
                    **state.get_metrics(run_id, attempt),
                })
                return
            if parsed.path.startswith("/source/"):
                source_path = parsed.path
                for source in state.benchmark["sources"].values():
                    if source["path"] == source_path:
                        query = parse_qs(parsed.query)
                        run_id = query.get("run_id", [""])[0]
                        try:
                            attempt = _attempt(query)
                        except ValueError as exc:
                            self._send_json(400, {"error": str(exc)})
                            return
                        state.record_source(source_path, run_id, attempt)
                        self._send_json(200, source["payload"])
                        return
                self._send_json(404, {"error": "unknown fixture source"})
                return
            if parsed.path.startswith("/sink/"):
                run_id = unquote(parsed.path.removeprefix("/sink/"))
                try:
                    attempt = _attempt(parse_qs(parsed.query))
                except ValueError as exc:
                    self._send_json(400, {"error": str(exc)})
                    return
                self._send_json(200, {"run_id": run_id, "attempt": attempt, "rows": state.get_rows(run_id, attempt)})
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path.startswith("/sink/"):
                run_id = unquote(parsed.path.removeprefix("/sink/"))
                if not run_id or "/" in run_id:
                    self._send_json(400, {"error": "invalid run id"})
                    return
                try:
                    attempt = _attempt(parse_qs(parsed.query))
                    length = int(self.headers.get("Content-Length", "0"))
                    if length <= 0 or length > 2_000_000:
                        raise ValueError("body size must be between 1 and 2000000 bytes")
                    payload = json.loads(self.rfile.read(length))
                    rows = payload if isinstance(payload, list) else payload.get("records")
                    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                        raise ValueError("expected a JSON array of records or an object with a records array")
                except (ValueError, json.JSONDecodeError) as exc:
                    self._send_json(400, {"error": str(exc)})
                    return
                state.append(run_id, rows, attempt)
                self._send_json(202, {"accepted": len(rows), "run_id": run_id})
                return
            self._send_json(404, {"error": "not found"})

        def do_DELETE(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path.startswith("/sink/"):
                run_id = unquote(parsed.path.removeprefix("/sink/"))
                try:
                    attempt = _attempt(parse_qs(parsed.query))
                except ValueError as exc:
                    self._send_json(400, {"error": str(exc)})
                    return
                state.clear(run_id, attempt)
                self._send_json(200, {"cleared": True, "run_id": run_id, "attempt": attempt})
                return
            self._send_json(404, {"error": "not found"})

        def log_message(self, fmt: str, *args: Any) -> None:
            print(f"{self.address_string()} - {fmt % args}")

    return Handler


def serve(host: str = "0.0.0.0", port: int = 8090) -> None:
    benchmark = load_benchmark()
    log_path = Path(os.environ.get("BENCH_SINK_LOG", "benchmark-results/recorder/sink-events.jsonl"))
    http_log_path = Path(os.environ.get("BENCH_HTTP_LOG", "benchmark-results/recorder/http-events.jsonl"))
    server = ThreadingHTTPServer((host, port), make_handler(RecorderState(log_path, benchmark, http_log_path)))
    print(f"Fixture source and sink service listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _attempt(query: dict[str, list[str]]) -> int:
    try:
        attempt = int(query.get("attempt", ["0"])[0])
    except ValueError as exc:
        raise ValueError("attempt must be a non-negative integer") from exc
    if attempt < 0:
        raise ValueError("attempt must be a non-negative integer")
    return attempt
