"""Active OpenRouter health probing for dynamic routing."""

from __future__ import annotations

import base64
import time
from typing import Any

import httpx

from admin.backend.services.openrouter_catalog import list_free_models
from admin.backend.services.openrouter_picker import (
    TASK_FAMILY_TEXT_BASIC,
    fetch_openrouter_runtime_state,
    record_call_result,
    reserve_model_slot,
)
from shared.config import get_settings
from shared.redis_client import get_client

_TINY_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9s8xR9wAAAAASUVORK5CYII="
)
_MAX_MODELS_PER_RUN = 4
_CURSOR_KEY = "or:health:probe_cursor"


def _health_profile(model: dict[str, Any]) -> str:
    if model.get("supports_vision"):
        return "vision"
    return "text"


def _probe_payload(model_id: str, profile: str) -> dict[str, Any]:
    settings = get_settings()
    if profile == "vision":
        b64 = base64.b64encode(_TINY_PNG_BYTES).decode("ascii")
        return {
            "model": model_id,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Return the word ok."},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{b64}"},
                        },
                    ],
                }
            ],
            "temperature": 0,
            "max_tokens": int(settings.openrouter_health_probe_max_tokens or 1),
        }
    return {
        "model": model_id,
        "messages": [{"role": "user", "content": "Reply with ok."}],
        "temperature": 0,
        "max_tokens": int(settings.openrouter_health_probe_max_tokens or 1),
    }


async def _probe_model(client: httpx.AsyncClient, model: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    model_id = str(model["id"])
    profile = _health_profile(model)
    allowed, reason = await reserve_model_slot(model_id, service_name="admin")
    if not allowed:
        return {
            "model_id": model_id,
            "profile": profile,
            "status": "skipped",
            "reason": reason,
        }

    payload = _probe_payload(model_id, profile)
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "HTTP-Referer": settings.openrouter_referrer,
        "X-Title": "Frontier Intelligence",
        "Content-Type": "application/json",
    }
    started_at = time.monotonic()
    try:
        response = await client.post(
            f"{settings.openrouter_base_url.rstrip('/')}/chat/completions",
            headers=headers,
            json=payload,
        )
        latency_ms = (time.monotonic() - started_at) * 1000.0
        if response.status_code >= 400:
            reset_raw = response.headers.get("X-RateLimit-Reset")
            try:
                reset_at = float(reset_raw) if reset_raw else None
            except (TypeError, ValueError):
                reset_at = None
            await record_call_result(
                model_id,
                success=False,
                latency_ms=latency_ms,
                status_code=response.status_code,
                or_reset_at=reset_at,
                probe_profile=profile,
                service_name="admin",
            )
            return {
                "model_id": model_id,
                "profile": profile,
                "status": "error",
                "status_code": response.status_code,
            }

        await record_call_result(
            model_id,
            success=True,
            latency_ms=latency_ms,
            status_code=200,
            probe_profile=profile,
            service_name="admin",
        )
        return {
            "model_id": model_id,
            "profile": profile,
            "status": "ok",
        }
    except Exception as exc:
        latency_ms = (time.monotonic() - started_at) * 1000.0
        await record_call_result(
            model_id,
            success=False,
            latency_ms=latency_ms,
            status_code=None,
            probe_profile=profile,
            service_name="admin",
        )
        return {
            "model_id": model_id,
            "profile": profile,
            "status": "error",
            "error": str(exc),
        }


async def probe_openrouter_health() -> dict[str, Any]:
    settings = get_settings()
    if not settings.openrouter_api_key:
        return {
            "status": "missing_api_key",
            "probed": 0,
            "models": [],
            "task_family": TASK_FAMILY_TEXT_BASIC,
        }

    state = await fetch_openrouter_runtime_state(service_name="admin")
    catalog = {item["model_id"]: item for item in state.get("models", [])}
    candidates = await list_free_models()
    candidates = [model for model in candidates if model["id"] in catalog]
    candidates.sort(
        key=lambda model: (
            float(catalog[model["id"]].get("last_check") or 0.0),
            int(not model.get("supports_vision")),
        )
    )
    if not candidates:
        return {
            "status": "ok",
            "fetched_at": time.time(),
            "probed": 0,
            "ok": 0,
            "skipped": 0,
            "models": [],
        }
    batch_size = max(
        1,
        min(
            int(getattr(settings, "openrouter_health_probe_batch_size", _MAX_MODELS_PER_RUN) or _MAX_MODELS_PER_RUN),
            len(candidates),
        ),
    )
    redis = get_client()
    cursor_raw = await redis.get(_CURSOR_KEY)
    try:
        cursor = int(cursor_raw or 0)
    except (TypeError, ValueError):
        cursor = 0
    start = cursor % len(candidates)
    selected = [candidates[(start + idx) % len(candidates)] for idx in range(batch_size)]
    next_cursor = (start + batch_size) % len(candidates)
    await redis.set(_CURSOR_KEY, str(next_cursor), ex=86400)

    timeout = httpx.Timeout(
        float(settings.openrouter_health_probe_timeout_sec or 20.0),
        connect=10.0,
    )
    async with httpx.AsyncClient(timeout=timeout) as client:
        results = []
        for model in selected:
            results.append(await _probe_model(client, model))

    ok_count = sum(1 for item in results if item.get("status") == "ok")
    skipped_count = sum(1 for item in results if item.get("status") == "skipped")
    return {
        "status": "ok",
        "fetched_at": time.time(),
        "probed": len(results),
        "ok": ok_count,
        "skipped": skipped_count,
        "models": results,
    }


async def fetch_openrouter_health_snapshot() -> dict[str, Any]:
    state = await fetch_openrouter_runtime_state(service_name="admin")
    return {
        "status": state.get("status", "ok"),
        "fetched_at": state.get("fetched_at"),
        "usable_model_count": state.get("usable_model_count", 0),
        "quarantined_model_count": state.get("quarantined_model_count", 0),
        "near_cap_model_count": state.get("near_cap_model_count", 0),
        "models": state.get("models", []),
    }
