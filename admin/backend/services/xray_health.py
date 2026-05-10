from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import redis.asyncio as aioredis

from admin.backend.services.telegram_alerts import (
    send_telegram_alert_message,
    telegram_alerts_enabled,
)
from shared.config import get_settings

logger = logging.getLogger(__name__)

_XRAY_STREAK_KEY = "admin:xray:degradation_streak"
_XRAY_LAST_ALERT_KEY = "admin:xray:last_alert_ts"
_XRAY_LAST_REMEDIATE_KEY = "admin:xray:last_remediate_ts"
_XRAY_LAST_RESULT_KEY = "admin:xray:last_result"
_XRAY_HISTORY_KEY = "admin:xray:history"
_XRAY_HISTORY_MAX_ITEMS = 100


@dataclass(frozen=True)
class ProbeResult:
    url: str
    ok: bool
    status_code: int | None
    error: str | None


def _probe_targets() -> list[str]:
    settings = get_settings()
    return [target.strip() for target in settings.xray_probe_targets if target.strip()]


async def _probe_once(url: str, *, proxy: str | None) -> ProbeResult:
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, connect=8.0),
            follow_redirects=True,
            proxy=proxy,
        ) as client:
            response = await client.get(url)
        return ProbeResult(url=url, ok=response.status_code < 500, status_code=response.status_code, error=None)
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(url=url, ok=False, status_code=None, error=str(exc))


async def _send_xray_alert(streak: int, failed: list[ProbeResult], total: int) -> None:
    if not telegram_alerts_enabled():
        return
    failed_bits = []
    for item in failed[:6]:
        status = str(item.status_code) if item.status_code is not None else "ERR"
        detail = (item.error or "").strip()
        if detail:
            failed_bits.append(f"- {item.url} [{status}] {detail[:120]}")
        else:
            failed_bits.append(f"- {item.url} [{status}]")
    text = "\n".join(
        [
            "Frontier XRAY degraded",
            f"failed probes: {len(failed)}/{total}",
            f"consecutive degraded checks: {streak}",
            *failed_bits,
            "action: verify xray upstream and restart xray+ingest if needed",
        ]
    )
    try:
        await send_telegram_alert_message(text)
    except Exception:  # noqa: BLE001
        logger.exception("xray_degradation_alert_delivery_failed")


async def _trigger_remediation_webhook(streak: int, failed: list[ProbeResult], total: int) -> dict[str, Any]:
    settings = get_settings()
    if not settings.xray_auto_remediation_webhook_url.strip():
        return {"triggered": False, "reason": "webhook_not_configured"}
    payload = {
        "event": "xray_degradation",
        "streak": streak,
        "failed": len(failed),
        "total": total,
        "targets": [
            {
                "url": item.url,
                "ok": item.ok,
                "status_code": item.status_code,
                "error": item.error,
            }
            for item in failed
        ],
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                settings.xray_auto_remediation_webhook_url,
                json=payload,
                headers={"content-type": "application/json"},
            )
        return {
            "triggered": resp.status_code < 300,
            "status_code": resp.status_code,
            "response_text": resp.text[:400],
        }
    except Exception as exc:  # noqa: BLE001
        return {"triggered": False, "reason": "request_failed", "error": str(exc)}


async def run_xray_health_check() -> dict[str, Any]:
    settings = get_settings()
    targets = _probe_targets()
    if not targets:
        return {"status": "disabled", "reason": "no_probe_targets"}

    proxy = settings.xray_probe_proxy_url.strip() or None
    results = [await _probe_once(url, proxy=proxy) for url in targets]
    failed = [item for item in results if not item.ok]
    failure_ratio = len(failed) / max(len(results), 1)
    is_degraded = failure_ratio >= settings.xray_degradation_failure_ratio

    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        streak = int(await redis.get(_XRAY_STREAK_KEY) or 0)
        if is_degraded:
            streak += 1
            await redis.set(_XRAY_STREAK_KEY, str(streak), ex=7 * 24 * 3600)
        else:
            streak = 0
            await redis.set(_XRAY_STREAK_KEY, "0", ex=7 * 24 * 3600)

        alert_sent = False
        remediation: dict[str, Any] | None = None

        if is_degraded and streak >= settings.xray_degradation_consecutive_threshold:
            # Ограничение частоты алертов.
            can_alert = True
            last_alert = await redis.get(_XRAY_LAST_ALERT_KEY)
            if last_alert is not None:
                can_alert = False
            if can_alert:
                await _send_xray_alert(streak, failed, len(results))
                alert_sent = True
                await redis.set(_XRAY_LAST_ALERT_KEY, "1", ex=settings.xray_alert_cooldown_seconds)

            # Опциональная авторемедиация через внешний webhook.
            if settings.xray_auto_remediation_enabled:
                last_remediate = await redis.get(_XRAY_LAST_REMEDIATE_KEY)
                if last_remediate is None:
                    remediation = await _trigger_remediation_webhook(streak, failed, len(results))
                    await redis.set(
                        _XRAY_LAST_REMEDIATE_KEY,
                        "1",
                        ex=settings.xray_auto_remediation_cooldown_seconds,
                    )

        payload = {
            "status": "degraded" if is_degraded else "ok",
            "proxy": proxy,
            "targets_total": len(results),
            "targets_failed": len(failed),
            "failure_ratio": round(failure_ratio, 4),
            "streak": streak,
            "alert_sent": alert_sent,
            "remediation": remediation,
            "results": [
                {
                    "url": item.url,
                    "ok": item.ok,
                    "status_code": item.status_code,
                    "error": item.error,
                }
                for item in results
            ],
        }
        history_record = {
            "status": payload["status"],
            "failure_ratio": payload["failure_ratio"],
            "targets_failed": payload["targets_failed"],
            "targets_total": payload["targets_total"],
            "streak": payload["streak"],
            "alert_sent": payload["alert_sent"],
            "checked_at": datetime.now(UTC).isoformat(),
        }
        await redis.set(_XRAY_LAST_RESULT_KEY, json.dumps(payload, ensure_ascii=False), ex=7 * 24 * 3600)
        await redis.lpush(_XRAY_HISTORY_KEY, json.dumps(history_record, ensure_ascii=False))
        await redis.ltrim(_XRAY_HISTORY_KEY, 0, _XRAY_HISTORY_MAX_ITEMS - 1)
        logger.info("xray_health_check %s", json.dumps(payload, ensure_ascii=False))
        return payload
    finally:
        await redis.aclose()


async def get_xray_health_snapshot() -> dict[str, Any]:
    settings = get_settings()
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        raw = await redis.get(_XRAY_LAST_RESULT_KEY)
        if not raw:
            return {
                "status": "unknown",
                "reason": "no_snapshot",
                "targets_total": 0,
                "targets_failed": 0,
                "failure_ratio": 0.0,
                "streak": int(await redis.get(_XRAY_STREAK_KEY) or 0),
                "results": [],
            }
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {
                "status": "unknown",
                "reason": "invalid_snapshot_payload",
                "targets_total": 0,
                "targets_failed": 0,
                "failure_ratio": 0.0,
                "streak": int(await redis.get(_XRAY_STREAK_KEY) or 0),
                "results": [],
            }
        payload["streak"] = int(await redis.get(_XRAY_STREAK_KEY) or payload.get("streak") or 0)
        return payload
    finally:
        await redis.aclose()


async def get_xray_health_history(*, limit: int = 20) -> list[dict[str, Any]]:
    settings = get_settings()
    count = max(1, min(int(limit or 20), 100))
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        items = await redis.lrange(_XRAY_HISTORY_KEY, 0, count - 1)
        history: list[dict[str, Any]] = []
        for raw in items:
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                history.append(value)
        return history
    finally:
        await redis.aclose()
