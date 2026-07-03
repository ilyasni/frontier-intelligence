"""Юнит-тесты guard'ов MCP: SSRF-защита enqueue-URL и allowlist workspace (mcp/guards.py)."""
from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from mcp import guards


def _run_ssrf(url: str) -> None:
    asyncio.run(guards.assert_public_http_url(url))


@pytest.mark.unit
@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/x",
        "http://[::1]:8080/x",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.5/x",
        "http://192.168.1.1/x",
        "http://0.0.0.0/x",
        "http://redis:6379/",
        "http://qdrant:6333/",
        "http://neo4j:7474/",
        "http://admin:8101/",
        "http://localhost/",
    ],
)
def test_ssrf_blocks_private_and_internal(url: str) -> None:
    with pytest.raises(HTTPException) as exc:
        _run_ssrf(url)
    assert exc.value.status_code == 400
    assert "private/internal" in exc.value.detail


@pytest.mark.unit
@pytest.mark.parametrize("url", ["ftp://example.com/x", "not-a-url", "file:///etc/passwd"])
def test_ssrf_blocks_non_http_scheme(url: str) -> None:
    with pytest.raises(HTTPException) as exc:
        _run_ssrf(url)
    assert exc.value.status_code == 400


@pytest.mark.unit
def test_ssrf_allows_public_host() -> None:
    # example.com резолвится в публичный IP — не должен блокироваться.
    _run_ssrf("https://example.com/")


@pytest.mark.unit
def test_workspace_allowlist_contains_config_slugs() -> None:
    allow = guards.get_workspace_allowlist()
    assert {"disruption", "ai_trends", "ai_research", "ai_products_media", "design"} <= allow


@pytest.mark.unit
@pytest.mark.parametrize("ws", ["disruption", "ai_trends", "design", None, ""])
def test_workspace_known_or_optional_passes(ws: str | None) -> None:
    guards.assert_known_workspace(ws)  # не должно бросать


@pytest.mark.unit
@pytest.mark.parametrize("ws", ["bogus_ws", "'; DROP TABLE workspaces; --", "DISRUPTION"])
def test_workspace_unknown_rejected(ws: str) -> None:
    with pytest.raises(HTTPException) as exc:
        guards.assert_known_workspace(ws)
    assert exc.value.status_code == 400
    assert exc.value.detail == "unknown workspace"
