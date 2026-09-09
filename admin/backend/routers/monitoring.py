from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
from typing import Any

import redis.asyncio as aioredis
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from admin.backend.services.ntfy_alerts import (
    format_alertmanager_message,
    ntfy_alerts_enabled,
    send_ntfy_alert_message,
)
from admin.backend.services.xray_health import (
    get_xray_health_history,
    get_xray_health_snapshot,
    run_xray_health_check,
)
from admin.backend.services.xray_runtime import (
    failover_to_next_profile,
    get_remediation_history,
    get_runtime_state,
    rollback_profile,
    switch_profile,
)
from shared.config import get_settings

router = APIRouter()
_ALERTMANAGER_BASIC_AUTH_USERNAME = "alertmanager"
_ALERT_PENDING_TTL_SECONDS = 120
_ALERT_DEDUPE_TTL_SECONDS = 1800
logger = logging.getLogger(__name__)


class XraySwitchRequest(BaseModel):
    profile_name: str = Field(..., min_length=1)
    reason: str = Field("manual_switch", min_length=1)


class XrayRollbackRequest(BaseModel):
    reason: str = Field("manual_rollback", min_length=1)


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
        # Раньше здесь стоял `return`, то есть при пустой переменной единственный
        # контроль доступа к этому эндпоинту молча выключался, а эндпоинт продолжал
        # принимать и рассылать произвольные уведомления. Отказ громче тихого допуска:
        # ошибку конфигурации видно в логах Alertmanager как провал доставки.
        logger.error(
            "ALERTMANAGER_WEBHOOK_TOKEN пуст — вебхук отклоняется. "
            "Сгенерировать: scripts/server-ensure-alertmanager-token.sh"
        )
        raise HTTPException(
            status_code=503, detail="alertmanager_webhook_token_not_configured"
        )
    basic_password = _parse_basic_auth_password(
        request.headers.get("authorization", "").strip()
    )
    # query_params остаётся ради совместимости со старым конфигом, но Alertmanager
    # с 04.08.2026 шлёт токен через Basic-auth: uvicorn логирует полный путь запроса,
    # и токен в query string ложился открытым текстом в docker-логи admin.
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
        key = _alert_delivery_key(payload)
        if await client.set(key, "pending", ex=_ALERT_PENDING_TTL_SECONDS, nx=True):
            return True
        if await client.get(key) == "delivered":
            return False
        raise HTTPException(status_code=503, detail="alertmanager_delivery_pending")
    finally:
        await client.aclose()


async def _mark_alert_delivered(payload: dict[str, Any]) -> bool:
    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        # Один SET атомарно сохраняет подтверждение доставки вместе с новым TTL.
        return bool(
            await client.set(
                _alert_delivery_key(payload),
                "delivered",
                ex=_ALERT_DEDUPE_TTL_SECONDS,
            )
        )
    finally:
        await client.aclose()


async def _release_alert_delivery(payload: dict[str, Any]) -> None:
    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.delete(_alert_delivery_key(payload))
    finally:
        await client.aclose()


async def _deliver_alert_message(payload: dict[str, Any], message: str) -> bool:
    attempts = 3
    delay_seconds = 2.0
    for attempt in range(1, attempts + 1):
        try:
            delivered = await send_ntfy_alert_message(
                message,
                title="Frontier Alertmanager",
                priority="high",
                tags="warning,frontier",
            )
        except Exception:
            delivered = False
        if delivered:
            return True
        logger.warning(
            "alertmanager_ntfy_delivery_failed attempt=%s/%s",
            attempt,
            attempts,
        )
        if attempt < attempts:
            await asyncio.sleep(delay_seconds)
            delay_seconds *= 2
    return False


@router.get("/alertmanager/health")
async def alertmanager_health() -> dict[str, Any]:
    settings = get_settings()
    return {
        "status": "ok",
        "ntfy_enabled": ntfy_alerts_enabled(),
        "alertmanager_token_configured": bool(settings.alertmanager_webhook_token.strip()),
        "alertmanager_basic_auth_username": _ALERTMANAGER_BASIC_AUTH_USERNAME,
        "ntfy_url_configured": bool(settings.ntfy_url.strip()),
        "ntfy_credential_file_configured": bool(
            settings.ntfy_credential_file.strip()
        ),
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
    if not await _deliver_alert_message(payload, message):
        try:
            await _release_alert_delivery(payload)
        except Exception:
            logger.warning("alertmanager_delivery_claim_release_failed")
        raise HTTPException(status_code=503, detail="alertmanager_ntfy_delivery_failed")
    try:
        marked_delivered = await _mark_alert_delivered(payload)
    except Exception:
        marked_delivered = False
    if not marked_delivered:
        logger.warning("alertmanager_delivery_confirmation_failed")
        raise HTTPException(status_code=503, detail="alertmanager_delivery_confirmation_failed")
    return {
        "status": "accepted",
        "delivered": True,
        "alerts": len(alerts),
        "receiver": payload.get("receiver"),
    }


@router.get("/xray/health")
async def xray_health_snapshot() -> dict[str, Any]:
    return await get_xray_health_snapshot()


@router.post("/xray/health/run")
async def run_xray_health() -> dict[str, Any]:
    return await run_xray_health_check()


@router.get("/xray/health/history")
async def xray_health_history(limit: int = 20) -> dict[str, Any]:
    return {"history": await get_xray_health_history(limit=limit)}


@router.get("/xray/profiles")
async def xray_profiles() -> dict[str, Any]:
    return get_runtime_state()


@router.get("/xray/remediation/history")
async def xray_remediation_history(limit: int = 20) -> dict[str, Any]:
    return {"history": await get_remediation_history(limit=limit)}


@router.post("/xray/remediate/failover")
async def xray_failover(request: Request) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if request.headers.get("content-length") not in {None, "", "0"}:
        try:
            parsed = await request.json()
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}
    reason = str(payload.get("reason") or payload.get("event") or "xray_failover").strip()
    return await failover_to_next_profile(
        reason=reason,
        trigger="auto_failover" if payload else "manual_failover",
        metadata=payload,
    )


@router.post("/xray/remediate/switch")
async def xray_switch(payload: XraySwitchRequest) -> dict[str, Any]:
    try:
        return await switch_profile(
            target_name=payload.profile_name,
            reason=payload.reason,
            trigger="manual_switch",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/xray/remediate/rollback")
async def xray_rollback(payload: XrayRollbackRequest | None = None) -> dict[str, Any]:
    reason = payload.reason if payload else "manual_rollback"
    return await rollback_profile(reason=reason, trigger="manual_rollback")
