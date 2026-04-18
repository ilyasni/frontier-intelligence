"""Direct GigaChat balance lookup for package-token tariffs."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from uuid import uuid4

import httpx
import redis.asyncio as aioredis

from shared.config import Settings, get_settings
from shared.metrics import note_gigachat_balance_refresh, set_gigachat_balance
from admin.backend.services.telegram_alerts import send_telegram_alert_message

_TOKEN_LOCK = asyncio.Lock()
_TOKEN_VALUE = ""
_TOKEN_EXPIRES_AT = 0.0
_ALERT_LOCK = asyncio.Lock()
_LAST_LOW_BALANCE_SIGNATURE = ""
_BALANCE_CACHE_LOCK = asyncio.Lock()
_LAST_BALANCE_ITEMS: list[dict[str, Any]] = []
_LAST_BALANCE_FETCHED_AT = 0.0
_LAST_BALANCE_TOKEN_EXPIRES_AT: float | None = None
_BALANCE_CACHE_KEY = "admin:gigachat_balance:last_ok"


def _normalize_auth_header(credentials: str) -> str:
    credentials = credentials.strip()
    if not credentials:
        return ""
    if credentials.lower().startswith("basic "):
        return credentials
    return f"Basic {credentials}"


def _normalize_expires_at(raw_expires_at: Any) -> float:
    try:
        value = float(raw_expires_at or 0)
    except (TypeError, ValueError):
        return 0.0
    if value > 10_000_000_000:
        return value / 1000.0
    return value


async def _get_access_token(settings: Settings) -> tuple[str, float]:
    global _TOKEN_VALUE, _TOKEN_EXPIRES_AT

    now = time.time()
    if _TOKEN_VALUE and _TOKEN_EXPIRES_AT - now > 60:
        return _TOKEN_VALUE, _TOKEN_EXPIRES_AT

    async with _TOKEN_LOCK:
        now = time.time()
        if _TOKEN_VALUE and _TOKEN_EXPIRES_AT - now > 60:
            return _TOKEN_VALUE, _TOKEN_EXPIRES_AT

        auth_header = _normalize_auth_header(settings.gigachat_credentials)
        if not auth_header:
            _TOKEN_VALUE = ""
            _TOKEN_EXPIRES_AT = 0.0
            return "", 0.0

        async with httpx.AsyncClient(
            timeout=30,
            verify=settings.gigachat_verify_ssl_certs,
            follow_redirects=True,
        ) as client:
            resp = await client.post(
                settings.gigachat_auth_url,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "RqUID": str(uuid4()),
                    "Authorization": auth_header,
                },
                data={"scope": settings.gigachat_scope},
            )
            if resp.status_code != 200:
                _TOKEN_VALUE = ""
                _TOKEN_EXPIRES_AT = 0.0
                raise RuntimeError(f"gigachat_oauth_failed status={resp.status_code}")

            payload = resp.json()
            _TOKEN_VALUE = str(payload.get("access_token") or "")
            _TOKEN_EXPIRES_AT = _normalize_expires_at(payload.get("expires_at"))
            return _TOKEN_VALUE, _TOKEN_EXPIRES_AT


async def fetch_gigachat_balance() -> dict[str, Any]:
    """Return remaining GigaChat tokens by model when package balance API is available."""
    settings = get_settings()
    if not settings.gigachat_credentials:
        return {
            "available": False,
            "status": "missing_credentials",
            "balance": [],
            "token_expires_at": None,
        }

    try:
        token, expires_at = await _get_access_token(settings)
    except Exception as exc:
        return {
            "available": False,
            "status": "oauth_error",
            "error": str(exc),
            "balance": [],
            "token_expires_at": None,
        }

    if not token:
        return {
            "available": False,
            "status": "missing_token",
            "balance": [],
            "token_expires_at": None,
        }

    url = f"{settings.gigachat_base_url.rstrip('/')}/balance"
    try:
        async with httpx.AsyncClient(
            timeout=30,
            verify=settings.gigachat_verify_ssl_certs,
        ) as client:
            resp = await client.get(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {token}",
                    "X-Request-ID": str(uuid4()),
                },
            )
            if resp.status_code == 403:
                return await _balance_error_payload(
                    available=False,
                    status="permission_denied",
                    http_status=resp.status_code,
                    balance=[],
                    token_expires_at=expires_at,
                    note="GET /balance works only for package-token tariffs; pay-as-you-go returns 403.",
                )
            if resp.status_code != 200:
                return await _balance_error_payload(
                    available=False,
                    status="balance_error",
                    http_status=resp.status_code,
                    error=resp.text[:500],
                    balance=[],
                    token_expires_at=expires_at,
                )
            payload = resp.json()
            items = [
                {
                    "usage": str(item.get("usage") or ""),
                    "value": int(item.get("value") or 0),
                }
                for item in (payload.get("balance") or [])
                if str(item.get("usage") or "").strip()
            ]
            for item in items:
                set_gigachat_balance("admin", item["usage"], item["value"])
            note_gigachat_balance_refresh("admin", time.time())
            await _store_balance_snapshot(items, expires_at)
            await _notify_low_balance_if_needed(settings, items)
            return {
                "available": True,
                "status": "ok",
                "balance": items,
                "alert_threshold": settings.gigachat_balance_alert_threshold,
                "fetched_at": _LAST_BALANCE_FETCHED_AT,
                "token_expires_at": expires_at,
            }
    except Exception as exc:
        return await _balance_error_payload(
            available=False,
            status="request_error",
            error=str(exc),
            balance=[],
            token_expires_at=expires_at,
        )


async def _store_balance_snapshot(
    items: list[dict[str, Any]],
    token_expires_at: float | None,
) -> None:
    global _LAST_BALANCE_FETCHED_AT, _LAST_BALANCE_ITEMS, _LAST_BALANCE_TOKEN_EXPIRES_AT

    async with _BALANCE_CACHE_LOCK:
        _LAST_BALANCE_ITEMS = [dict(item) for item in items]
        _LAST_BALANCE_FETCHED_AT = time.time()
        _LAST_BALANCE_TOKEN_EXPIRES_AT = token_expires_at

    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(
            _BALANCE_CACHE_KEY,
            json.dumps(
                {
                    "balance": items,
                    "fetched_at": _LAST_BALANCE_FETCHED_AT,
                    "token_expires_at": token_expires_at,
                }
            ),
        )
    finally:
        await client.aclose()


async def _balance_error_payload(**payload: Any) -> dict[str, Any]:
    """Serve the last successful balance snapshot when /balance is temporarily rate-limited."""
    settings = get_settings()

    async with _BALANCE_CACHE_LOCK:
        cached_items = [dict(item) for item in _LAST_BALANCE_ITEMS]
        fetched_at = _LAST_BALANCE_FETCHED_AT
        cached_token_expires_at = _LAST_BALANCE_TOKEN_EXPIRES_AT

    if not cached_items:
        redis_snapshot = await _load_balance_snapshot_from_redis()
        cached_items = redis_snapshot["balance"]
        fetched_at = redis_snapshot["fetched_at"]
        cached_token_expires_at = redis_snapshot["token_expires_at"]

    if not cached_items:
        payload["alert_threshold"] = settings.gigachat_balance_alert_threshold
        payload.setdefault("fetched_at", None)
        return payload

    payload.update(
        {
            "available": True,
            "status": f"stale_{payload.get('status') or 'balance_error'}",
            "balance": cached_items,
            "alert_threshold": settings.gigachat_balance_alert_threshold,
            "fetched_at": fetched_at,
            "token_expires_at": payload.get("token_expires_at") or cached_token_expires_at,
            "stale": True,
        }
    )
    return payload


async def _load_balance_snapshot_from_redis() -> dict[str, Any]:
    global _LAST_BALANCE_FETCHED_AT, _LAST_BALANCE_ITEMS, _LAST_BALANCE_TOKEN_EXPIRES_AT

    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        raw_snapshot = await client.get(_BALANCE_CACHE_KEY)
    finally:
        await client.aclose()

    if not raw_snapshot:
        return {
            "balance": [],
            "fetched_at": None,
            "token_expires_at": None,
        }

    try:
        snapshot = json.loads(raw_snapshot)
    except json.JSONDecodeError:
        return {
            "balance": [],
            "fetched_at": None,
            "token_expires_at": None,
        }

    items = [
        {
            "usage": str(item.get("usage") or ""),
            "value": int(item.get("value") or 0),
        }
        for item in (snapshot.get("balance") or [])
        if str(item.get("usage") or "").strip()
    ]
    fetched_at = float(snapshot.get("fetched_at") or 0) or None
    token_expires_at = _normalize_expires_at(snapshot.get("token_expires_at"))

    async with _BALANCE_CACHE_LOCK:
        _LAST_BALANCE_ITEMS = [dict(item) for item in items]
        _LAST_BALANCE_FETCHED_AT = fetched_at or 0.0
        _LAST_BALANCE_TOKEN_EXPIRES_AT = token_expires_at

    return {
        "balance": items,
        "fetched_at": fetched_at,
        "token_expires_at": token_expires_at,
    }


async def _notify_low_balance_if_needed(
    settings: Settings,
    items: list[dict[str, Any]],
) -> None:
    global _LAST_LOW_BALANCE_SIGNATURE

    if not settings.telegram_bot_token or not settings.telegram_alert_chat_id:
        return

    low_items = [
        item
        for item in items
        if int(item.get("value") or 0) < settings.gigachat_balance_alert_threshold
    ]
    signature = "|".join(
        f"{item['usage']}={int(item.get('value') or 0)}"
        for item in sorted(low_items, key=lambda row: str(row.get("usage") or ""))
    )

    async with _ALERT_LOCK:
        if signature == _LAST_LOW_BALANCE_SIGNATURE:
            return
        _LAST_LOW_BALANCE_SIGNATURE = signature

    if not low_items:
        return

    lines = [
        "Frontier alert: low GigaChat token balance",
        f"threshold: {settings.gigachat_balance_alert_threshold}",
    ]
    lines.extend(
        f"{item['usage']}: {int(item.get('value') or 0)}"
        for item in sorted(low_items, key=lambda row: str(row.get("usage") or ""))
    )

    await send_telegram_alert_message("\n".join(lines))
