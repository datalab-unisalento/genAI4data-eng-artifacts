from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any


class OllamaError(RuntimeError):
    pass


def generate(
    prompt: str,
    model: str,
    base_url: str = "http://localhost:11434",
    timeout: int = 300,
) -> str:
    text, _ = generate_with_metrics(prompt, model, base_url, timeout)
    return text


WARM_UP_OPTIONS: dict[str, int | float] = {
    "temperature": 0.0,
    "seed": 42,
    "num_ctx": 8192,
    "num_predict": 1,
}


def warm_up(model: str, base_url: str, timeout: int, keep_alive: str | int) -> None:
    """Make one throwaway call so the measured generation is never the first call after a model load.

    Output of the first call after a cold load differed from later calls under identical
    settings (scripts/check_determinism.py), so each measured generation is preceded by this call.
    """
    generate_with_metrics("Reply with OK.", model, base_url, timeout, WARM_UP_OPTIONS, keep_alive, False)


def generate_with_metrics(
    prompt: str,
    model: str,
    base_url: str = "http://localhost:11434",
    timeout: int = 300,
    options: dict[str, int | float] | None = None,
    keep_alive: str | int = "5m",
    think: bool = False,
) -> tuple[str, dict[str, Any]]:
    request_body: dict[str, Any] = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "keep_alive": keep_alive,
        "think": think,
    }
    if options:
        request_body["options"] = options
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/generate",
        data=json.dumps(request_body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            result: dict[str, Any] = json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise OllamaError(f"Ollama generation request failed: {exc}") from exc
    text = result.get("response")
    if not isinstance(text, str) or not text.strip():
        reason = result.get("done_reason", "unknown")
        generated_tokens = result.get("eval_count", "unknown")
        raise OllamaError(
            "Ollama returned no artifact text "
            f"(done_reason={reason}, generated_tokens={generated_tokens}); "
            "the output limit may have been consumed by reasoning"
        )
    prompt_tokens = result.get("prompt_eval_count")
    output_tokens = result.get("eval_count")
    output_duration = result.get("eval_duration")
    metrics = {
        "generation_duration_seconds": time.monotonic() - started,
        "ollama_total_duration_seconds": _nanoseconds_to_seconds(result.get("total_duration")),
        "model_load_duration_seconds": _nanoseconds_to_seconds(result.get("load_duration")),
        "prompt_tokens": prompt_tokens if isinstance(prompt_tokens, int) else None,
        "output_tokens": output_tokens if isinstance(output_tokens, int) else None,
        "output_tokens_per_second": (
            output_tokens / (output_duration / 1_000_000_000)
            if isinstance(output_tokens, int) and isinstance(output_duration, int) and output_duration > 0
            else None
        ),
    }
    return text, metrics


def _nanoseconds_to_seconds(value: Any) -> float | None:
    return value / 1_000_000_000 if isinstance(value, int) else None
