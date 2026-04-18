from __future__ import annotations

from typing import Any

import httpx

from shared.config import get_settings


def telegram_alerts_enabled() -> bool:
    settings = get_settings()
    return bool(settings.telegram_bot_token and settings.telegram_alert_chat_id)


async def send_telegram_alert_message(text: str) -> bool:
    settings = get_settings()
    if not telegram_alerts_enabled():
        return False
    async with httpx.AsyncClient(
        timeout=30,
        proxy=settings.telegram_alert_proxy_url or None,
    ) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
            json={
                "chat_id": settings.telegram_alert_chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
        )
        response.raise_for_status()
    return True


def format_alertmanager_message(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "firing").upper()
    common_labels = payload.get("commonLabels") or {}
    common_annotations = payload.get("commonAnnotations") or {}
    alerts = payload.get("alerts") or []

    severity = str(common_labels.get("severity") or "unknown").upper()
    alertname = str(common_labels.get("alertname") or "FrontierAlert")
    service = str(common_labels.get("service") or common_labels.get("job") or "frontier")

    lines = [
        f"Frontier {status}: {alertname}",
        f"severity: {severity}",
        f"service: {service}",
        f"alerts: {len(alerts)}",
    ]

    summary = str(common_annotations.get("summary") or "").strip()
    description = str(common_annotations.get("description") or "").strip()
    runbook_url = str(common_annotations.get("runbook_url") or "").strip()

    if summary:
        lines.append(f"summary: {summary}")
    if description:
        lines.append(f"description: {description}")

    for alert in alerts[:5]:
        labels = alert.get("labels") or {}
        item_bits = []
        for key in ("job", "instance", "stream", "group", "job_name", "workspace_id", "usage"):
            value = str(labels.get(key) or "").strip()
            if value:
                item_bits.append(f"{key}={value}")
        if item_bits:
            lines.append(f"- {' '.join(item_bits)}")

    truncated = int(payload.get("truncatedAlerts") or 0)
    if truncated > 0:
        lines.append(f"truncated_alerts: {truncated}")
    if runbook_url:
        lines.append(f"runbook: {runbook_url}")
    return "\n".join(lines)
