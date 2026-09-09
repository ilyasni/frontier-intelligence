from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from shared.config import get_settings

_MAX_MESSAGE_BYTES = 4096
_MAX_RECEIPT_BYTES = 16384
_RESERVED_TOPICS = {
    "account",
    "admin",
    "app",
    "docs",
    "file",
    "health",
    "metrics",
    "settings",
    "static",
    "v1",
}
_TOPIC_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
_CREDENTIAL_RE = re.compile(r"[A-Za-z0-9._~-]+")


class NtfyDeliveryError(RuntimeError):
    """Safe-to-log delivery failure without credentials or response content."""


def ntfy_alerts_enabled() -> bool:
    settings = get_settings()
    return bool(
        str(settings.ntfy_url).strip()
        and str(settings.ntfy_credential_file).strip()
    )


def _validate_url(url: str, *, _allow_http_loopback: bool = False) -> str:
    try:
        parts = urlsplit(url)
        topic = parts.path.removeprefix("/")
        authority_suffix = parts.netloc.rsplit("]", 1)[-1]
        valid_transport = (
            parts.scheme == "https" and ":" not in authority_suffix
        ) or (
            _allow_http_loopback
            and parts.scheme == "http"
            and parts.hostname in {"127.0.0.1", "::1", "localhost"}
        )
        if (
            not valid_transport
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or "?" in url
            or "#" in url
            or _TOPIC_RE.fullmatch(topic) is None
            or topic in _RESERVED_TOPICS
            or any(char.isspace() for char in url)
            or parts.netloc.endswith(":")
            or parts.port == 0
        ):
            raise ValueError
    except ValueError:
        raise NtfyDeliveryError(
            "Invalid ntfy URL: expected HTTPS with exactly one topic"
        ) from None
    return topic


def _read_credential(path_value: str) -> str:
    try:
        credential = Path(path_value).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        raise NtfyDeliveryError("ntfy credential file is unavailable") from None
    if not credential or _CREDENTIAL_RE.fullmatch(credential) is None:
        raise NtfyDeliveryError("ntfy credential file has invalid content")
    return credential


def _utf8_prefix(text: str, max_bytes: int) -> str:
    if max_bytes <= 0:
        return ""
    return text.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")


def truncate_ntfy_message(
    text: str,
    suffix: str = "",
    max_bytes: int = _MAX_MESSAGE_BYTES,
) -> str:
    """Bound text by encoded size without splitting a UTF-8 code point."""
    if max_bytes < 0:
        raise ValueError("ntfy max_bytes must be non-negative")
    suffix_bytes = suffix.encode("utf-8")
    if len(suffix_bytes) > max_bytes:
        raise ValueError("ntfy truncation suffix exceeds max_bytes")
    if max_bytes == 0:
        return ""
    if len(text.encode("utf-8")) <= max_bytes:
        return text
    return _utf8_prefix(text, max_bytes - len(suffix_bytes)) + suffix


async def send_ntfy_alert_message(
    text: str,
    *,
    title: str | None = None,
    priority: str | None = None,
    tags: str | None = None,
) -> bool:
    return await _send_ntfy_alert_message(
        text, title=title, priority=priority, tags=tags
    )


async def _send_ntfy_alert_message(
    text: str,
    *,
    title: str | None = None,
    priority: str | None = None,
    tags: str | None = None,
    _allow_http_loopback: bool = False,
) -> bool:
    """HTTP loopback разрешается явно только для локальных transport-тестов."""
    settings = get_settings()
    url = str(settings.ntfy_url)
    topic = _validate_url(url, _allow_http_loopback=_allow_http_loopback)
    body = text.encode("utf-8")
    if not body or len(body) > _MAX_MESSAGE_BYTES:
        raise NtfyDeliveryError("ntfy message must be 1-4096 UTF-8 bytes")
    credential = _read_credential(str(settings.ntfy_credential_file))
    headers = {
        "Authorization": f"Bearer {credential}",
        "Content-Type": "text/plain; charset=utf-8",
    }
    if title:
        headers["Title"] = title
    if priority:
        headers["Priority"] = priority
    if tags:
        headers["Tags"] = tags

    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=False) as client:
            response = await client.post(url, content=body, headers=headers)
        if response.status_code != 200:
            raise NtfyDeliveryError(
                f"ntfy rejected publication: HTTP {response.status_code}"
            )
        raw = response.content
        receipt = json.loads(raw) if len(raw) <= _MAX_RECEIPT_BYTES else None
    except NtfyDeliveryError:
        raise
    except httpx.HTTPError:
        raise NtfyDeliveryError("ntfy connection failed") from None
    except (ValueError, UnicodeError):
        raise NtfyDeliveryError("ntfy returned an invalid receipt") from None

    if (
        not isinstance(receipt, dict)
        or not isinstance(receipt.get("id"), str)
        or not receipt["id"].strip()
        or receipt.get("event") != "message"
        or receipt.get("topic") != topic
    ):
        raise NtfyDeliveryError("ntfy receipt does not match the expected topic")
    return True


def format_alertmanager_message(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "firing").upper()
    common_labels = payload.get("commonLabels") or {}
    common_annotations = payload.get("commonAnnotations") or {}
    alerts = payload.get("alerts") or []

    severity = str(common_labels.get("severity") or "unknown").upper()
    alertname = str(common_labels.get("alertname") or "FrontierAlert")
    service = str(
        common_labels.get("service") or common_labels.get("job") or "frontier"
    )
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
        for key in (
            "job",
            "instance",
            "stream",
            "group",
            "job_name",
            "workspace_id",
            "usage",
        ):
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
    return truncate_ntfy_message(
        "\n".join(lines),
        suffix="\n… message truncated",
    )
