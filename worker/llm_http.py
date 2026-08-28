"""Shared HTTP defaults for LLM provider clients.

Wormsoft uses its own richer per-provider timeout settings (see
:class:`worker.wormsoft_client.WormsoftTextClient`); the remaining OpenAI-compatible
clients (GigaChat proxy, Polza, OpenRouter) share this single default so the timeout
lives in one place instead of being duplicated as a magic literal.
"""
from __future__ import annotations

import httpx

from shared.config import get_settings

# Total request timeout / connect timeout for general LLM HTTP clients.
DEFAULT_LLM_HTTP_TIMEOUT = httpx.Timeout(90.0, connect=15.0)


def resolve_openrouter_proxy() -> str | None:
    """Proxy URL for OpenRouter calls, or ``None`` when proxying is disabled.

    openrouter.ai rejects our egress IP at the edge: a direct request returns
    HTTP 403 ``{"error": "Access denied by security policy."}`` before it ever
    reaches the API, so every worker call fell through to the next provider
    (measured 27.08.2026: 193 requests over the whole retention window, zero
    successes). The admin side never hit this because its ``/key``, ``/credits``
    and health-probe clients already go through the xray tunnel; the worker
    inference clients were the only ones left going out directly. Same setting,
    same tunnel — clearing ``XRAY_PROBE_PROXY_URL`` disables proxying everywhere.
    """
    settings = get_settings()
    return (getattr(settings, "xray_probe_proxy_url", "") or "").strip() or None
