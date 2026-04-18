from __future__ import annotations

import base64
import asyncio
import hashlib
import logging
from typing import Any

import redis.asyncio as aioredis
from fastapi import APIRouter, HTTPException, Request

from admin.backend.services.telegram_alerts import (
    format_alertmanager_message,
    send_telegram_alert_message,
    telegram_alerts_enabled,
)
from shared.config import get_settings

router = APIRouter()
_ALERTMANAGER_BASIC_AUTH_USERNAME = "alertmanager"
_ALERT_DEDUPE_TTL_SECONDS = 1800
logger = logging.getLogger(__name__)


def _parse_basic_auth_password(header_value: str) -> str | None:
    if not header_value.lower().startswith("basic "):
        return None
    encoded = header_value[6:].strip()
    if not encoded:
        return None
    try:
        decoded = base64.b64decode(encoded).decode("utf-8")
    except Exception:
        return None
    username, _, password = decoded.partition(":")
    if username != _ALERTMANAGER_BASIC_AUTH_USERNAME:
        return None
    return password


def _assert_alertmanager_token(request: Request) -> None:
    settings = get_settings()
    expected = settings.alertmanager_webhook_token.strip()
    if not expected:
        return
    basic_password = _parse_basic_auth_password(
        request.headers.get("authorization", "").strip()
    )
    provided = basic_password or (
        request.headers.get("x-alertmanager-token")
        or request.query_params.get("token")
        or ""
    ).strip()
    if provided != expected:
        raise HTTPException(status_code=403, detail="invalid_alertmanager_token")


def _alert_delivery_key(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "firing").strip().lower()
    group_key = str(payload.get("groupKey") or "").strip()
    if group_key:
        digest_source = f"{status}|{group_key}"
    else:
        common_labels = payload.get("commonLabels") or {}
        alert_fingerprints = ",".join(
            sorted(
                str((alert.get("fingerprint") or "")).strip()
                for alert in (payload.get("alerts") or [])
                if str((alert.get("fingerprint") or "")).strip()
            )
        )
        digest_source = "|".join(
            [
                status,
                str(common_labels.get("alertname") or ""),
                str(common_labels.get("severity") or ""),
                str(common_labels.get("service") or common_labels.get("job") or ""),
                alert_fingerprints,
            ]
        )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
    return f"admin:alertmanager:delivery:{digest}"


async def _claim_alert_delivery(payload: dict[str, Any]) -> bool:
    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        return bool(
            await client.set(
                _alert_delivery_key(payload),
                "1",
                ex=_ALERT_DEDUPE_TTL_SECONDS,
                nx=True,
            )
        )
    finally:
        await client.aclose()


async def _deliver_alert_message(payload: dict[str, Any], message: str) -> None:
    attempts = 3
    delay_seconds = 2.0
    for attempt in range(1, attempts + 1):
        try:
            await send_telegram_alert_message(message)
            return
        except Exception:
            logger.exception(
                "alertmanager_telegram_delivery_failed attempt=%s/%s alertname=%s status=%s",
                attempt,
                attempts,
                (payload.get("commonLabels") or {}).get("alertname"),
                payload.get("status"),
            )
            if attempt >= attempts:
                return
            await asyncio.sleep(delay_seconds)
            delay_seconds *= 2


@router.get("/alertmanager/health")
async def alertmanager_health() -> dict[str, Any]:
    settings = get_settings()
    return {
        "status": "ok",
        "telegram_enabled": telegram_alerts_enabled(),
        "alertmanager_token_configured": bool(settings.alertmanager_webhook_token.strip()),
        "alertmanager_basic_auth_username": _ALERTMANAGER_BASIC_AUTH_USERNAME,
        "proxy_configured": bool(settings.telegram_alert_proxy_url.strip()),
        "chat_id_configured": bool(settings.telegram_alert_chat_id.strip()),
    }


@router.post("/alertmanager/webhook")
async def alertmanager_webhook(request: Request) -> dict[str, Any]:
    _assert_alertmanager_token(request)
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid_payload")
    alerts = payload.get("alerts") or []
    if not isinstance(alerts, list):
        raise HTTPException(status_code=400, detail="invalid_alerts")
    if not alerts:
        return {
            "status": "ignored",
            "delivered": False,
            "reason": "empty_alert_group",
            "alerts": 0,
            "receiver": payload.get("receiver"),
        }
    if not await _claim_alert_delivery(payload):
        return {
            "status": "ignored",
            "delivered": False,
            "reason": "duplicate_alert_group",
            "alerts": len(alerts),
            "receiver": payload.get("receiver"),
        }
    message = format_alertmanager_message(payload)
    asyncio.create_task(_deliver_alert_message(payload, message))
    return {
        "status": "accepted",
        "delivered": False,
        "alerts": len(alerts),
        "receiver": payload.get("receiver"),
    }
