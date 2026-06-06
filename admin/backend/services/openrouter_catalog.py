"""OpenRouter free-model catalog snapshot for dynamic routing.

Periodically fetches GET /api/v1/models, filters models with zero pricing
(``:free`` family), normalizes capabilities, and stores the snapshot in Redis.

Consumed by ``openrouter_picker`` to decide which model serves each task.

Pattern mirrors ``wormsoft_limits.py``: async lock, fall back to last good
snapshot on transient failures, expose Prometheus metrics.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from shared.config import get_settings
from shared.metrics import set_openrouter_catalog_snapshot
from shared.redis_client import get_client

CACHE_KEY = "or:catalog:snapshot"
FETCHED_AT_KEY = "or:catalog:fetched_at"

_CACHE_LOCK = asyncio.Lock()
_LAST_PAYLOAD: dict[str, Any] | None = None


def _is_free(pricing: dict[str, Any]) -> bool:
    """Free model = zero prompt AND zero completion price.

    Pricing fields are strings in OR catalog to avoid float precision issues.
    """
    try:
        prompt = float(pricing.get("prompt", "0") or "0")
        completion = float(pricing.get("completion", "0") or "0")
    except (TypeError, ValueError):
        return False
    return prompt == 0.0 and completion == 0.0


def _normalize_model(raw: dict[str, Any]) -> dict[str, Any] | None:
    model_id = str(raw.get("id") or "").strip()
    if not model_id:
        return None
    if model_id == "openrouter/free":
        return None
    pricing = raw.get("pricing") or {}
    if not _is_free(pricing):
        return None

    architecture = raw.get("architecture") or {}
    input_mods = architecture.get("input_modalities") or []
    output_mods = architecture.get("output_modalities") or []

    supported = raw.get("supported_parameters") or []

    top_provider = raw.get("top_provider") or {}
    ctx = (
        raw.get("context_length")
        or top_provider.get("context_length")
        or 0
    )

    return {
        "id": model_id,
        "name": str(raw.get("name") or model_id),
        "context_length": int(ctx or 0),
        "input_modalities": list(input_mods),
        "output_modalities": list(output_mods),
        "supports_vision": "image" in input_mods,
        "supports_structured": any(
            p in supported for p in ("structured_outputs", "json_mode", "tools")
        ),
        "supports_tools": "tools" in supported,
        "supported_parameters": list(supported),
        "max_completion_tokens": top_provider.get("max_completion_tokens"),
        "pricing": pricing,
        "is_moderated": top_provider.get("is_moderated", False),
    }


async def _store(payload: dict[str, Any]) -> None:
    global _LAST_PAYLOAD
    async with _CACHE_LOCK:
        _LAST_PAYLOAD = dict(payload)
    client = get_client()
    await client.set(CACHE_KEY, json.dumps(payload), ex=3600)
    await client.set(FETCHED_AT_KEY, str(payload.get("fetched_at", time.time())))


async def _load_cached() -> dict[str, Any] | None:
    global _LAST_PAYLOAD
    async with _CACHE_LOCK:
        if _LAST_PAYLOAD:
            return dict(_LAST_PAYLOAD)
    client = get_client()
    raw = await client.get(CACHE_KEY)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    async with _CACHE_LOCK:
        _LAST_PAYLOAD = dict(data)
    return dict(data)


async def fetch_openrouter_catalog() -> dict[str, Any]:
    """Fetch full /models, keep only free entries, persist snapshot."""
    settings = get_settings()
    api_key = getattr(settings, "openrouter_api_key", "") or ""
    models_url = f"{settings.openrouter_base_url.rstrip('/')}/models"
    headers: dict[str, str] = {
        "HTTP-Referer": getattr(settings, "openrouter_referrer", "https://frontier-intelligence.local"),
        "X-Title": "Frontier Intelligence",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    proxy = (getattr(settings, "xray_probe_proxy_url", "") or "").strip() or None
    try:
        async with httpx.AsyncClient(timeout=20.0, proxy=proxy) as client:
            resp = await client.get(models_url, headers=headers)
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:  # noqa: BLE001 — surface as snapshot status
        cached = await _load_cached()
        if cached:
            cached.update(
                {
                    "status": "stale_request_error",
                    "stale": True,
                    "error": str(exc),
                }
            )
            set_openrouter_catalog_snapshot("admin", cached)
            return cached
        payload = {
            "status": "request_error",
            "error": str(exc),
            "models": [],
            "model_count": 0,
            "fetched_at": time.time(),
        }
        set_openrouter_catalog_snapshot("admin", payload)
        return payload

    raw_models = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(raw_models, list):
        raw_models = []

    free_models: list[dict[str, Any]] = []
    for raw in raw_models:
        if not isinstance(raw, dict):
            continue
        norm = _normalize_model(raw)
        if norm:
            free_models.append(norm)

    snapshot = {
        "status": "ok",
        "fetched_at": time.time(),
        "model_count": len(free_models),
        "models": free_models,
    }
    await _store(snapshot)
    set_openrouter_catalog_snapshot("admin", snapshot)
    return snapshot


async def list_free_models(*, capability: str | None = None) -> list[dict[str, Any]]:
    """Return cached free-model list, optionally filtered by capability.

    capability values: ``vision``, ``structured``, ``tools``, ``long_context``.
    """
    cached = await _load_cached()
    if not cached or not cached.get("models"):
        cached = await fetch_openrouter_catalog()
    models = list(cached.get("models") or [])
    if not capability:
        return models
    if capability == "vision":
        return [m for m in models if m.get("supports_vision")]
    if capability == "structured":
        return [m for m in models if m.get("supports_structured")]
    if capability == "tools":
        return [m for m in models if m.get("supports_tools")]
    if capability == "long_context":
        return [m for m in models if (m.get("context_length") or 0) >= 32000]
    return models
