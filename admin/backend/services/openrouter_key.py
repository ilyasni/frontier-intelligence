"""OpenRouter key snapshot for admin observability and budget reconciliation."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from shared.config import get_settings
from shared.metrics import set_openrouter_key_snapshot
from shared.redis_client import get_client

_CACHE_LOCK = asyncio.Lock()
_CACHE_KEY = "admin:openrouter_key:last_ok"
_LAST_PAYLOAD: dict[str, Any] | None = None


def _expected_free_requests_daily_limit(is_free_tier: bool) -> int:
    return 50 if is_free_tier else 1000


def _normalize_key_payload(payload: Any) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        data = {}

    def _number(name: str) -> float:
        value = data.get(name, 0.0)
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    is_free_tier = bool(data.get("is_free_tier"))
    return {
        "available": bool(data),
        "status": "ok" if data else "invalid_response",
        "label": str(data.get("label") or ""),
        "limit": data.get("limit"),
        "limit_remaining": _number("limit_remaining"),
        "limit_reset": str(data.get("limit_reset") or ""),
        "usage": _number("usage"),
        "usage_daily": _number("usage_daily"),
        "usage_weekly": _number("usage_weekly"),
        "usage_monthly": _number("usage_monthly"),
        "byok_usage": _number("byok_usage"),
        "byok_usage_daily": _number("byok_usage_daily"),
        "byok_usage_weekly": _number("byok_usage_weekly"),
        "byok_usage_monthly": _number("byok_usage_monthly"),
        "is_free_tier": is_free_tier,
        "expires_at": str(data.get("expires_at") or ""),
        "free_model_rpm_limit": 20,
        "free_model_daily_limit": _expected_free_requests_daily_limit(is_free_tier),
        "fetched_at": time.time(),
    }


async def _store_snapshot(payload: dict[str, Any]) -> None:
    global _LAST_PAYLOAD
    async with _CACHE_LOCK:
        _LAST_PAYLOAD = dict(payload)
    client = get_client()
    await client.set(_CACHE_KEY, json.dumps(payload))


async def _load_cached_snapshot() -> dict[str, Any] | None:
    global _LAST_PAYLOAD
    async with _CACHE_LOCK:
        if _LAST_PAYLOAD:
            return dict(_LAST_PAYLOAD)
    client = get_client()
    raw = await client.get(_CACHE_KEY)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    async with _CACHE_LOCK:
        _LAST_PAYLOAD = dict(payload)
    return dict(payload)


async def fetch_openrouter_key() -> dict[str, Any]:
    settings = get_settings()
    if not settings.openrouter_api_key:
        payload = {
            "available": False,
            "status": "missing_api_key",
            "is_free_tier": True,
            "limit_remaining": 0.0,
            "usage_daily": 0.0,
            "byok_usage_daily": 0.0,
            "free_model_rpm_limit": 20,
            "free_model_daily_limit": 50,
            "fetched_at": time.time(),
        }
        set_openrouter_key_snapshot("admin", payload)
        return payload

    key_url = f"{settings.openrouter_base_url.rstrip('/')}/key"
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "HTTP-Referer": settings.openrouter_referrer,
        "X-Title": "Frontier Intelligence",
    }

    proxy = (getattr(settings, "xray_probe_proxy_url", "") or "").strip() or None
    try:
        async with httpx.AsyncClient(timeout=20.0, proxy=proxy) as client:
            response = await client.get(key_url, headers=headers)
            response.raise_for_status()
            payload = _normalize_key_payload(response.json())
    except Exception as exc:
        cached = await _load_cached_snapshot()
        if cached:
            cached.update(
                {
                    "available": True,
                    "status": "stale_request_error",
                    "stale": True,
                    "error": str(exc),
                }
            )
            set_openrouter_key_snapshot("admin", cached)
            return cached
        payload = {
            "available": False,
            "status": "request_error",
            "error": str(exc),
            "is_free_tier": True,
            "limit_remaining": 0.0,
            "usage_daily": 0.0,
            "byok_usage_daily": 0.0,
            "free_model_rpm_limit": 20,
            "free_model_daily_limit": 50,
            "fetched_at": time.time(),
        }
        set_openrouter_key_snapshot("admin", payload)
        return payload

    await _store_snapshot(payload)
    set_openrouter_key_snapshot("admin", payload)
    return payload
