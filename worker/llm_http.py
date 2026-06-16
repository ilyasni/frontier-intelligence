"""Shared HTTP defaults for LLM provider clients.

Wormsoft uses its own richer per-provider timeout settings (see
:class:`worker.wormsoft_client.WormsoftTextClient`); the remaining OpenAI-compatible
clients (GigaChat proxy, Polza, OpenRouter) share this single default so the timeout
lives in one place instead of being duplicated as a magic literal.
"""
from __future__ import annotations

import httpx

# Total request timeout / connect timeout for general LLM HTTP clients.
DEFAULT_LLM_HTTP_TIMEOUT = httpx.Timeout(90.0, connect=15.0)
