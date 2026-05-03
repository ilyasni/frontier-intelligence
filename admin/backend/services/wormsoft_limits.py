"""Wormsoft limits and pricing snapshot for admin settings and alerting."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from urllib.parse import urlsplit

import httpx
import redis.asyncio as aioredis

from shared.config import get_settings
from shared.metrics import set_wormsoft_limits_snapshot

_CACHE_LOCK = asyncio.Lock()
_CACHE_KEY = "admin:wormsoft_limits:last_ok"
_LAST_PAYLOAD: dict[str, Any] | None = None


def _wormsoft_origin() -> str:
    settings = get_settings()
    base_url = str(settings.wormsoft_api_base or "").strip()
    if not base_url:
        return ""
    parts = urlsplit(base_url)
    if not parts.scheme or not parts.netloc:
        return ""
    return f"{parts.scheme}://{parts.netloc}"


def _normalize_plan(plan_id: str, raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    amount = int(raw.get("amount") or 0)
    seconds = int(raw.get("seconds") or 0)
    price = float(raw.get("price") or 0.0)
    period_days = int(raw.get("periodDays") or 0)
    return {
        "id": str(plan_id),
        "amount": amount,
        "seconds": seconds,
        "window_hours": round(seconds / 3600.0, 2) if seconds else 0.0,
        "price": price,
        "period_days": period_days,
    }


def _normalize_plans(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    plans: list[dict[str, Any]] = []
    for plan_id, raw in payload.items():
        item = _normalize_plan(str(plan_id), raw)
        if item:
            plans.append(item)
    plans.sort(key=lambda item: (float(item.get("price") or 0.0), str(item.get("id") or "")))
    return plans


def _normalize_pricing(payload: Any) -> dict[str, dict[str, float]]:
    if not isinstance(payload, dict):
        return {}
    pricing: dict[str, dict[str, float]] = {}
    for model_id, raw in payload.items():
        if not isinstance(raw, dict):
            continue
        normalized: dict[str, float] = {}
        for key in ("input", "output", "cache"):
            value = raw.get(key)
            if value is None:
                continue
            normalized[key] = float(value)
        if normalized:
            pricing[str(model_id)] = normalized
    return dict(sorted(pricing.items(), key=lambda item: item[0]))


async def _store_snapshot(payload: dict[str, Any]) -> None:
    global _LAST_PAYLOAD
    async with _CACHE_LOCK:
        _LAST_PAYLOAD = dict(payload)
    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(_CACHE_KEY, json.dumps(payload))
    finally:
        await client.aclose()


async def _load_cached_snapshot() -> dict[str, Any] | None:
    global _LAST_PAYLOAD
    async with _CACHE_LOCK:
        if _LAST_PAYLOAD:
            return dict(_LAST_PAYLOAD)
    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        raw = await client.get(_CACHE_KEY)
    finally:
        await client.aclose()
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


async def fetch_wormsoft_limits() -> dict[str, Any]:
    settings = get_settings()
    key_present = bool(settings.wormsoft_api_key)
    origin = _wormsoft_origin()
    if not origin:
        return {
            "available": False,
            "status": "missing_base_url",
            "key_present": key_present,
            "plans": [],
            "pricing": {},
        }
    if not key_present:
        return {
            "available": False,
            "status": "missing_api_key",
            "key_present": False,
            "plans": [],
            "pricing": {},
        }

    headers = {"Authorization": f"Bearer {settings.wormsoft_api_key}"}
    urls = {
        "plans": f"{origin}/api/user-connector/subscription-limits",
        "pricing": f"{origin}/api/money/token-pricing",
    }
    errors: dict[str, str] = {}
    raw_plans: Any = None
    raw_pricing: Any = None
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            plan_resp, pricing_resp = await asyncio.gather(
                client.get(urls["plans"], headers=headers),
                client.get(urls["pricing"], headers=headers),
                return_exceptions=True,
            )
    except Exception as exc:
        cached = await _load_cached_snapshot()
        if cached:
            cached.update(
                {
                    "available": True,
                    "status": "stale_request_error",
                    "stale": True,
                    "key_present": True,
                    "error": str(exc),
                }
            )
            return cached
        return {
            "available": False,
            "status": "request_error",
            "key_present": True,
            "plans": [],
            "pricing": {},
            "error": str(exc),
        }

    if isinstance(plan_resp, Exception):
        errors["plans"] = str(plan_resp)
    else:
        try:
            plan_resp.raise_for_status()
            raw_plans = plan_resp.json()
        except Exception as exc:
            errors["plans"] = str(exc)

    if isinstance(pricing_resp, Exception):
        errors["pricing"] = str(pricing_resp)
    else:
        try:
            pricing_resp.raise_for_status()
            raw_pricing = pricing_resp.json()
        except Exception as exc:
            errors["pricing"] = str(exc)

    plans = _normalize_plans(raw_plans)
    pricing = _normalize_pricing(raw_pricing)
    available = bool(plans or pricing)
    status = "ok" if not errors and available else ("partial_request_error" if available else "request_error")
    payload = {
        "available": available,
        "status": status,
        "key_present": True,
        "base_origin": origin,
        "plans": plans,
        "pricing": pricing,
        "plan_count": len(plans),
        "pricing_model_count": len(pricing),
        "fetched_at": time.time(),
        "errors": errors,
        "alerting_note": (
            "Wormsoft does not publish an account-level remaining-credit endpoint here; "
            "monitor 429 bursts and fallback spikes for live limit pressure."
        ),
    }
    if available:
        await _store_snapshot(payload)
        set_wormsoft_limits_snapshot("admin", payload)
        return payload

    cached = await _load_cached_snapshot()
    if cached:
        cached.update(
            {
                "available": True,
                "status": f"stale_{status}",
                "stale": True,
                "key_present": True,
                "errors": errors,
            }
        )
        return cached

    set_wormsoft_limits_snapshot("admin", payload)
    return payload
