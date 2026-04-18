"""Unit tests for ingest.account_rotator Telegram proxy resolution."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.account_rotator import (
    MtProxySettings,
    TelegramAccount,
    AccountRotator,
    build_resolved_proxy,
    env_telegram_proxy_configured,
    socks5_proxy_dict,
    _parse_socks5_dsn,
)
from telethon.network.connection.tcpmtproxy import ConnectionTcpMTProxyRandomizedIntermediate


@pytest.mark.unit
def test_parse_socks5_dsn_four_parts():
    assert _parse_socks5_dsn("1.2.3.4:1080:user:pass") == ("1.2.3.4", 1080, "user", "pass")


@pytest.mark.unit
def test_parse_socks5_dsn_password_with_colon():
    assert _parse_socks5_dsn("h:1:u:p:art") == ("h", 1, "u", "p:art")


@pytest.mark.unit
def test_socks5_proxy_dict_minimal():
    d = socks5_proxy_dict("10.0.0.1", 1080)
    assert d == {"proxy_type": "socks5", "addr": "10.0.0.1", "port": 1080, "rdns": True}


@pytest.mark.unit
def test_socks5_proxy_dict_auth():
    d = socks5_proxy_dict("h", 1, "a", "b")
    assert d["username"] == "a"
    assert d["password"] == "b"


@pytest.mark.unit
def test_build_resolved_proxy_source_socks5():
    cfg = {"host": "proxy.example", "port": 9999, "user": "u", "password": "p"}
    r = build_resolved_proxy(cfg)
    assert r == socks5_proxy_dict("proxy.example", 9999, "u", "p")


@pytest.mark.unit
def test_build_resolved_proxy_source_mtproto():
    cfg = {"type": "mtproto", "host": "mt.example", "port": 2002, "secret": "abc"}
    r = build_resolved_proxy(cfg)
    assert r == MtProxySettings(host="mt.example", port=2002, secret="abc")


@pytest.mark.unit
def test_env_telegram_proxy_configured_socks5(monkeypatch):
    for k in (
        "MTPROXY_HOST",
        "MTPROXY_SECRET",
        "TG_PROXY_HOST",
        "WG_SOCKS_HOST",
        "TG_SOCKS5",
        "TG_PROXY_DSN",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TG_SOCKS5", "127.0.0.1:1080")
    assert env_telegram_proxy_configured() is True


@pytest.mark.unit
def test_env_telegram_proxy_configured_mtproxy(monkeypatch):
    for k in (
        "MTPROXY_HOST",
        "MTPROXY_SECRET",
        "TG_PROXY_HOST",
        "WG_SOCKS_HOST",
        "TG_SOCKS5",
        "TG_PROXY_DSN",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("MTPROXY_HOST", "m")
    monkeypatch.setenv("MTPROXY_SECRET", "s")
    assert env_telegram_proxy_configured() is True


@pytest.mark.unit
def test_env_telegram_proxy_configured_false(monkeypatch):
    for k in (
        "MTPROXY_HOST",
        "MTPROXY_SECRET",
        "TG_PROXY_HOST",
        "TG_PROXY_PORT",
        "WG_SOCKS_HOST",
        "TG_SOCKS5",
        "TG_PROXY_DSN",
    ):
        monkeypatch.delenv(k, raising=False)
    assert env_telegram_proxy_configured() is False


@pytest.mark.asyncio
async def test_get_client_passes_mtproxy_to_telegram_client(monkeypatch):
    for k in (
        "MTPROXY_HOST",
        "MTPROXY_SECRET",
        "TG_PROXY_HOST",
        "WG_SOCKS_HOST",
        "TG_SOCKS5",
        "TG_PROXY_DSN",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("MTPROXY_HOST", "mt.example")
    monkeypatch.setenv("MTPROXY_PORT", "443")
    monkeypatch.setenv("MTPROXY_SECRET", "0123456789abcdef0123456789abcdef")

    mock_instance = MagicMock()
    mock_instance.is_connected.return_value = False
    mock_instance.connect = AsyncMock()

    with patch("ingest.account_rotator.TelegramClient") as TC:
        TC.return_value = mock_instance
        rotator = AccountRotator([TelegramAccount(1, "hash", "/tmp/x")])
        await rotator.get_client(0, None)

        TC.assert_called_once()
        kw = TC.call_args.kwargs
        assert kw["connection"] is ConnectionTcpMTProxyRandomizedIntermediate
        assert kw["proxy"] == ("mt.example", 443, "0123456789abcdef0123456789abcdef")


@pytest.mark.asyncio
async def test_get_client_passes_socks5_dict(monkeypatch):
    for k in (
        "MTPROXY_HOST",
        "MTPROXY_SECRET",
        "TG_PROXY_HOST",
        "WG_SOCKS_HOST",
        "TG_SOCKS5",
        "TG_PROXY_DSN",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TG_SOCKS5", "127.0.0.1:1080:u:p")

    mock_instance = MagicMock()
    mock_instance.is_connected.return_value = False
    mock_instance.connect = AsyncMock()

    with patch("ingest.account_rotator.TelegramClient") as TC:
        TC.return_value = mock_instance
        rotator = AccountRotator([TelegramAccount(1, "hash", "/tmp/x")])
        await rotator.get_client(0, None)

        kw = TC.call_args.kwargs
        assert kw["proxy"] == socks5_proxy_dict("127.0.0.1", 1080, "u", "p")
        assert "connection" not in kw


@pytest.mark.asyncio
async def test_get_client_recreates_stale_client(monkeypatch):
    for k in (
        "MTPROXY_HOST",
        "MTPROXY_SECRET",
        "TG_PROXY_HOST",
        "WG_SOCKS_HOST",
        "TG_SOCKS5",
        "TG_PROXY_DSN",
    ):
        monkeypatch.delenv(k, raising=False)

    stale = MagicMock()
    stale.is_connected.return_value = False
    stale.disconnect = AsyncMock()

    fresh = MagicMock()
    fresh.is_connected.return_value = False
    fresh.connect = AsyncMock()

    account = TelegramAccount(1, "hash", "/tmp/x")
    account.client = stale

    with patch("ingest.account_rotator.TelegramClient", return_value=fresh) as tc:
        rotator = AccountRotator([account])
        client = await rotator.get_client(0, None)

    assert client is fresh
    stale.disconnect.assert_not_awaited()
    fresh.connect.assert_awaited_once()
    tc.assert_called_once()


@pytest.mark.asyncio
async def test_reset_client_disconnects_and_clears_reference():
    client = MagicMock()
    client.is_connected.return_value = True
    client.disconnect = AsyncMock()

    account = TelegramAccount(1, "hash", "/tmp/x")
    account.client = client
    rotator = AccountRotator([account])

    reset = await rotator.reset_client(0, "test reset")

    assert reset is True
    client.disconnect.assert_awaited_once()
    assert account.client is None
