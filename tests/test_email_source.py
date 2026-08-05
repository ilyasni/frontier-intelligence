"""Юнит-тесты email-коннектора. Живой ящик не трогаем: IMAP замокан целиком."""
from __future__ import annotations

import asyncio
import email
import logging
from email.message import Message
from typing import Any

import pytest

from ingest.sources import email_source as email_source_module
from ingest.sources.email_source import (
    EmailAuthConfigError,
    EmailSource,
    resolve_imap_password,
)
from shared.events.posts_parsed_v1 import PostParsedEvent
from shared.source_definitions import normalize_source_extra

pytestmark = pytest.mark.unit


# Тема намеренно в RFC 2047 (=?utf-8?B?...?=): именно так приходят почти все
# нелатинские заголовки, и именно так ловится «сырой» разбор темы.
RAW_EMAIL = (
    b"Subject: =?utf-8?B?0KHQuNCz0L3QsNC7INC/0YDQvtGI0ZHQuw==?=\r\n"
    b"From: Alerts <alerts@example.com>\r\n"
    b"Message-ID: <msg-1@example.com>\r\n"
    b"Date: Mon, 04 Aug 2026 10:00:00 +0000\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"\r\n"
    b"Body text with a link https://example.com/report\r\n"
)
EXPECTED_SUBJECT = "Сигнал прошёл"


def _settings() -> Any:
    return email_source_module.get_settings()


def _config(**fetch_overrides: Any) -> dict[str, Any]:
    fetch: dict[str, Any] = {
        "host": "imap.example.com",
        "username": "bot@example.com",
        "mailbox": "INBOX",
        "search": "ALL",
        "max_items_per_run": 5,
    }
    fetch.update(fetch_overrides)
    return {"source_type": "email", "fetch": fetch}


class _FakeRedis:
    def __init__(self) -> None:
        self.pushed: list[dict[str, Any]] = []

    async def xadd(self, stream: str, payload: dict[str, Any]) -> str:
        self.pushed.append(payload)
        return "1-1"


class _FakeRuntimeStore:
    def __init__(self) -> None:
        self.upsert_calls: list[dict[str, Any]] = []
        self.finish_calls: list[dict[str, Any]] = []

    async def load_checkpoint(self, source_id: str) -> dict[str, Any]:
        return {}

    async def start_run(self, source_id: str) -> str:
        return "run-1"

    async def upsert_checkpoint(self, **kwargs: Any) -> None:
        self.upsert_calls.append(kwargs)

    async def finish_run(self, run_id: str, **kwargs: Any) -> None:
        self.finish_calls.append(kwargs)


def _source(
    *,
    redis: Any = None,
    runtime_store: Any = None,
    **fetch_overrides: Any,
) -> EmailSource:
    return EmailSource(
        source_id="email-alerts",
        workspace_id="disruption",
        config=_config(**fetch_overrides),
        redis=redis or _FakeRedis(),
        runtime_store=runtime_store,
    )


# --- пароль ------------------------------------------------------------------


def test_password_comes_from_settings_not_from_source_config(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "from-settings", False)
    monkeypatch.setattr(_settings(), "imap_passwords", "", False)

    resolved = resolve_imap_password("email-alerts", {"password": "from-config"})

    assert resolved == "from-settings"


def test_password_key_selects_entry_from_settings_map(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "default-pw", False)
    monkeypatch.setattr(
        _settings(),
        "imap_passwords",
        '{"alerts": "alerts-pw", "digest": "digest-pw"}',
        False,
    )

    assert resolve_imap_password("s", {"auth_ref": "digest"}) == "digest-pw"


def test_missing_password_raises_explicit_error(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "", False)
    monkeypatch.setattr(_settings(), "imap_passwords", "", False)

    with pytest.raises(EmailAuthConfigError) as excinfo:
        resolve_imap_password("email-alerts", {})

    assert "IMAP_PASSWORD" in str(excinfo.value)
    assert "email-alerts" in str(excinfo.value)


def test_unknown_password_key_raises_instead_of_falling_back(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "default-pw", False)
    monkeypatch.setattr(_settings(), "imap_passwords", '{"alerts": "x"}', False)

    with pytest.raises(EmailAuthConfigError):
        resolve_imap_password("s", {"auth_ref": "missing"})


def test_broken_passwords_json_raises_named_error(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_passwords", "{not json", False)

    with pytest.raises(EmailAuthConfigError) as excinfo:
        resolve_imap_password("s", {"auth_ref": "alerts"})

    assert "IMAP_PASSWORDS" in str(excinfo.value)


def test_config_password_is_stripped_from_normalized_extra() -> None:
    normalized = normalize_source_extra(
        "email",
        {"fetch": {"host": "imap.example.com", "username": "u", "password": "secret"}},
    )

    assert "password" not in normalized["fetch"]
    assert "auth_ref" in normalized["fetch"]


# --- асинхронность -----------------------------------------------------------


def test_imap_session_runs_in_thread(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "pw", False)
    calls: list[dict[str, Any]] = []

    async def fake_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        calls.append(kwargs)
        assert func is email_source_module._collect_messages_blocking
        return [email.message_from_bytes(RAW_EMAIL)], False

    monkeypatch.setattr(email_source_module.asyncio, "to_thread", fake_to_thread)

    src = _source()
    messages = asyncio.run(src.fetch_index())

    assert len(calls) == 1, "IMAP-session must be handed to to_thread as a whole"
    assert calls[0]["host"] == "imap.example.com"
    assert calls[0]["password"] == "pw"
    assert len(messages) == 1
    assert src._partial_fetch is False


def test_imap_session_is_a_single_thread_hop(monkeypatch) -> None:
    """Обёртка вокруг отдельных imaplib-вызовов дала бы гонку — её быть не должно."""
    monkeypatch.setattr(_settings(), "imap_password", "pw", False)
    hops = 0
    real_to_thread = asyncio.to_thread

    class _FakeIMAP:
        def __init__(self, host: str, port: int, timeout: int | None = None) -> None:
            self.host = host

        def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
            return "OK", [b""]

        def select(self, mailbox: str) -> tuple[str, list[bytes]]:
            return "OK", [b"1"]

        def search(self, charset: Any, query: str) -> tuple[str, list[bytes]]:
            return "OK", [b"1"]

        def fetch(self, msg_id: bytes, spec: str) -> tuple[str, list[Any]]:
            return "OK", [(b"1 (RFC822", RAW_EMAIL)]

        def close(self) -> None:
            return None

        def logout(self) -> None:
            return None

    async def counting_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal hops
        hops += 1
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(email_source_module.imaplib, "IMAP4_SSL", _FakeIMAP)
    monkeypatch.setattr(email_source_module.asyncio, "to_thread", counting_to_thread)

    messages = asyncio.run(_source().fetch_index())

    assert hops == 1
    assert len(messages) == 1


def test_partial_fetch_marks_source_and_keeps_messages(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "pw", False)

    async def fake_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return [email.message_from_bytes(RAW_EMAIL)], True

    monkeypatch.setattr(email_source_module.asyncio, "to_thread", fake_to_thread)

    src = _source()
    messages = asyncio.run(src.fetch_index())

    assert len(messages) == 1
    assert src._partial_fetch is True


def test_fetch_without_host_fails_loudly(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "pw", False)

    with pytest.raises(ValueError):
        asyncio.run(_source(host="").fetch_index())


# --- логирование ошибок разбора ----------------------------------------------


class _ExplodingMessage(Message):
    def is_multipart(self) -> bool:
        raise ValueError("broken MIME structure")


def test_parse_error_is_logged_and_not_swallowed(caplog) -> None:
    broken = _ExplodingMessage()
    broken["Message-ID"] = "<broken@example.com>"

    with caplog.at_level(logging.WARNING, logger=email_source_module.__name__):
        item = asyncio.run(_source().normalize_item(broken))

    assert item is None
    messages = [record.getMessage() for record in caplog.records]
    assert any("failed to parse email" in text for text in messages)
    assert any("broken@example.com" in text for text in messages)


def test_logout_failure_is_logged_not_passed(monkeypatch, caplog) -> None:
    class _LogoutBoom:
        def __init__(self, host: str, port: int, timeout: int | None = None) -> None:
            pass

        def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
            return "OK", [b""]

        def select(self, mailbox: str) -> tuple[str, list[bytes]]:
            return "OK", [b"1"]

        def search(self, charset: Any, query: str) -> tuple[str, list[bytes]]:
            return "OK", [b""]

        def close(self) -> None:
            raise RuntimeError("not selected")

        def logout(self) -> None:
            raise RuntimeError("connection already dead")

    monkeypatch.setattr(email_source_module.imaplib, "IMAP4_SSL", _LogoutBoom)

    with caplog.at_level(logging.WARNING, logger=email_source_module.__name__):
        messages, partial = email_source_module._collect_messages_blocking(
            source_id="email-alerts",
            host="h",
            port=993,
            use_ssl=True,
            username="u",
            password="p",
            mailbox="INBOX",
            search_query="ALL",
            max_items=5,
            timeout=5,
        )

    assert messages == []
    assert partial is False
    assert any("IMAP logout failed" in record.getMessage() for record in caplog.records)


def test_broken_single_message_marks_partial_and_logs(monkeypatch, caplog) -> None:
    class _FlakyFetch:
        def __init__(self, host: str, port: int, timeout: int | None = None) -> None:
            pass

        def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
            return "OK", [b""]

        def select(self, mailbox: str) -> tuple[str, list[bytes]]:
            return "OK", [b"2"]

        def search(self, charset: Any, query: str) -> tuple[str, list[bytes]]:
            return "OK", [b"1 2"]

        def fetch(self, msg_id: bytes, spec: str) -> tuple[str, list[Any]]:
            if msg_id == b"1":
                return "NO", []
            return "OK", [(b"2 (RFC822", RAW_EMAIL)]

        def close(self) -> None:
            return None

        def logout(self) -> None:
            return None

    monkeypatch.setattr(email_source_module.imaplib, "IMAP4_SSL", _FlakyFetch)

    with caplog.at_level(logging.WARNING, logger=email_source_module.__name__):
        messages, partial = email_source_module._collect_messages_blocking(
            source_id="email-alerts",
            host="h",
            port=993,
            use_ssl=True,
            username="u",
            password="p",
            mailbox="INBOX",
            search_query="ALL",
            max_items=5,
            timeout=5,
        )

    assert len(messages) == 1
    assert partial is True
    assert any("IMAP fetch failed" in record.getMessage() for record in caplog.records)


# --- нормализация в PostParsedEvent ------------------------------------------


def test_message_normalizes_to_valid_event() -> None:
    src = _source()
    item = asyncio.run(src.normalize_item(email.message_from_bytes(RAW_EMAIL)))

    assert item is not None
    assert item.external_id == "<msg-1@example.com>"
    assert item.title == EXPECTED_SUBJECT
    assert "Body text with a link" in item.content
    assert item.linked_urls == ["https://example.com/report"]
    assert item.author == "Alerts <alerts@example.com>"
    assert item.published_at is not None and item.published_at.year == 2026

    event = src.to_event(item)
    assert isinstance(event, PostParsedEvent)
    assert event.workspace_id == "disruption"
    assert event.source_id == "email-alerts"
    assert event.external_id == "<msg-1@example.com>"
    assert event.content == item.content
    assert event.extra["connector"] == "email"
    assert event.extra["title"] == EXPECTED_SUBJECT


def test_html_only_message_yields_content() -> None:
    raw = (
        b"Subject: HTML alert\r\n"
        b"Message-ID: <html-1@example.com>\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body><p>Quarterly <b>signal</b></p>"
        b'<a href="https://example.com/x">more</a></body></html>\r\n'
    )
    item = asyncio.run(_source().normalize_item(email.message_from_bytes(raw)))

    assert item is not None
    assert "Quarterly" in item.content
    assert "https://example.com/x" in item.linked_urls


# --- чекпоинт ----------------------------------------------------------------


def test_checkpoint_advances_only_on_success(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "pw", False)

    async def ok_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return [email.message_from_bytes(RAW_EMAIL)], False

    monkeypatch.setattr(email_source_module.asyncio, "to_thread", ok_to_thread)

    store = _FakeRuntimeStore()
    pushed = asyncio.run(_source(redis=_FakeRedis(), runtime_store=store).run())

    assert pushed == 1
    assert len(store.upsert_calls) == 1
    cursor = store.upsert_calls[0]["cursor_json"]
    assert "<msg-1@example.com>" in cursor["seen_external_ids"]


def test_checkpoint_held_back_on_partial_fetch(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "pw", False)

    async def partial_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return [email.message_from_bytes(RAW_EMAIL)], True

    monkeypatch.setattr(email_source_module.asyncio, "to_thread", partial_to_thread)

    store = _FakeRuntimeStore()
    pushed = asyncio.run(_source(redis=_FakeRedis(), runtime_store=store).run())

    # Письмо опубликовано (at-least-once), но чекпоинт не двигается: пропущенное
    # перечитается следующим прогоном, дубль отсечёт dedup ниже по конвейеру.
    assert pushed == 1
    assert len(store.upsert_calls) == 1
    assert "cursor_json" not in store.upsert_calls[0]
    assert store.upsert_calls[0]["last_success_at"] is None
    assert store.finish_calls[0]["status"] == "error"


def test_checkpoint_not_advanced_when_auth_fails(monkeypatch) -> None:
    monkeypatch.setattr(_settings(), "imap_password", "", False)
    monkeypatch.setattr(_settings(), "imap_passwords", "", False)

    store = _FakeRuntimeStore()
    pushed = asyncio.run(_source(redis=_FakeRedis(), runtime_store=store).run())

    assert pushed == 0
    assert "cursor_json" not in store.upsert_calls[0]
    assert "IMAP_PASSWORD" in store.upsert_calls[0]["last_error"]
