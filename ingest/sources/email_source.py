"""IMAP-based alert/email source.

Три инварианта, ради которых этот файл выглядит именно так:

1. Пароль берётся из ``Settings`` (``IMAP_PASSWORD`` / ``IMAP_PASSWORDS``), а не из
   конфига источника: тот едет из git-трекаемого ``config/sources.yml`` и лежит
   в PostgreSQL открытым текстом. Источник несёт только имя ключа.
2. Вся IMAP-сессия целиком уходит в один ``asyncio.to_thread``. ``imaplib``
   держит состояние соединения и не потокобезопасен — обёртка вокруг отдельных
   вызовов дала бы гонку, а без обёртки блокируется event-loop всего ingest.
3. Сорванная выборка помечает ``_partial_fetch``: чекпоинт не двигается, и
   следующий прогон перечитает пропущенное (at-least-once, см. sources/base.py).
"""
from __future__ import annotations

import asyncio
import email
import imaplib
import json
import logging
from collections.abc import Mapping
from email.header import decode_header, make_header
from email.message import Message
from typing import Any

from ingest.sources.base import (
    NormalizedSourceItem,
    StructuredSource,
    build_external_id,
    compact_whitespace,
    detect_language,
    html_fragment_to_text,
    parse_datetime,
)
from shared.config import get_settings
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls

logger = logging.getLogger(__name__)

_DEFAULT_MAX_ITEMS = 50
_DEFAULT_PORT = 993
_DEFAULT_TIMEOUT_SEC = 20


class EmailAuthConfigError(RuntimeError):
    """Пароль ящика не удалось получить из Settings — источник запускать нечем."""


def resolve_imap_password(source_id: str, fetch_cfg: Mapping[str, Any]) -> str:
    """Достать пароль ящика из Settings по имени ключа из конфига источника.

    Пустой ``auth_ref`` означает «единственный ящик» и берёт ``IMAP_PASSWORD``.
    Непустой — ищет ключ в JSON-карте ``IMAP_PASSWORDS``.
    """
    if fetch_cfg.get("password"):
        # Сюда попадать не должно (normalize_source_extra вырезает ключ), но если
        # значение всё же дожило до рантайма — оно не используется, и об этом
        # надо сказать вслух, а не молча аутентифицироваться неизвестно чем.
        logger.warning(
            "[%s] extra.fetch.password ignored: IMAP password is read from Settings "
            "(IMAP_PASSWORD / IMAP_PASSWORDS), remove the value from the source config",
            source_id,
        )

    settings = get_settings()
    key = str(fetch_cfg.get("auth_ref") or "").strip()
    if not key:
        password = settings.imap_password or ""
        if not password:
            raise EmailAuthConfigError(
                f"[{source_id}] IMAP password is not configured: set IMAP_PASSWORD in the "
                "server .env, or point extra.fetch.auth_ref at a key of IMAP_PASSWORDS"
            )
        return password

    raw_map = (settings.imap_passwords or "").strip()
    if not raw_map:
        raise EmailAuthConfigError(
            f"[{source_id}] extra.fetch.auth_ref={key!r} is set, but IMAP_PASSWORDS "
            "is empty in the server .env"
        )
    try:
        parsed = json.loads(raw_map)
    except json.JSONDecodeError as exc:
        raise EmailAuthConfigError(
            f"[{source_id}] IMAP_PASSWORDS is not valid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise EmailAuthConfigError(
            f"[{source_id}] IMAP_PASSWORDS must be a JSON object {{key: password}}, "
            f"got {type(parsed).__name__}"
        )
    password = str(parsed.get(key) or "")
    if not password:
        raise EmailAuthConfigError(
            f"[{source_id}] no password for key {key!r} in IMAP_PASSWORDS "
            "(keys are configured in the server .env, never in the source config)"
        )
    return password


def _collect_messages_blocking(
    *,
    source_id: str,
    host: str,
    port: int,
    use_ssl: bool,
    username: str,
    password: str,
    mailbox: str,
    search_query: str,
    max_items: int,
    timeout: int,
) -> tuple[list[Message], bool]:
    """Синхронная IMAP-сессия целиком. Возвращает (сообщения, была ли потеря).

    Вызывается ТОЛЬКО из ``asyncio.to_thread``: всё тело блокирующее.
    """
    if use_ssl:
        client: imaplib.IMAP4 = imaplib.IMAP4_SSL(host, port, timeout=timeout)
    else:
        client = imaplib.IMAP4(host, port, timeout=timeout)

    messages: list[Message] = []
    partial = False
    try:
        client.login(username, password)
        status, _ = client.select(mailbox)
        if status != "OK":
            raise RuntimeError(f"IMAP SELECT {mailbox!r} returned {status!r}")
        status, data = client.search(None, search_query)
        if status != "OK":
            raise RuntimeError(f"IMAP SEARCH {search_query!r} returned {status!r}")

        raw_ids = (data[0] if data else b"") or b""
        if isinstance(raw_ids, str):
            raw_ids = raw_ids.encode("ascii", errors="ignore")
        ids = list(reversed(raw_ids.split()))

        for msg_id in ids[:max_items]:
            try:
                status, msg_data = client.fetch(msg_id, "(RFC822)")
                if status != "OK" or not msg_data or not isinstance(msg_data[0], tuple):
                    raise RuntimeError(f"IMAP FETCH returned {status!r}")
                messages.append(email.message_from_bytes(msg_data[0][1]))
            except Exception as exc:
                # Потеря одного письма не должна ронять всю выборку, но и
                # проглатывать её нельзя: помечаем прогон неполным, чтобы
                # чекпоинт не уехал вперёд пропущенного.
                partial = True
                logger.warning(
                    "[%s] IMAP fetch failed for uid %r: %s", source_id, msg_id, exc
                )
        return messages, partial
    finally:
        try:
            client.close()
        except Exception as exc:
            # Нормально, если mailbox не был выбран, — поэтому debug, но не pass.
            logger.debug("[%s] IMAP close failed: %s", source_id, exc)
        try:
            client.logout()
        except Exception as exc:
            logger.warning("[%s] IMAP logout failed: %s", source_id, exc)


def _decode_header_value(value: Any) -> str:
    """RFC 2047 (``=?utf-8?B?...?=``) → человекочитаемая строка."""
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    try:
        decoded = str(make_header(decode_header(value)))
    except Exception as exc:
        logger.debug("header decode failed (%s), using raw value: %s", exc, value[:120])
        decoded = value
    return compact_whitespace(decoded)


def _decode_payload(part: Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="ignore")
    except LookupError:
        return payload.decode("utf-8", errors="ignore")


class EmailSource(StructuredSource):
    """IMAP-коннектор: письмо → PostParsedEvent."""

    async def fetch_index(self) -> list[Any]:
        return await self._fetch_messages()

    async def _fetch_messages(self) -> list[Message]:
        fetch_cfg = self.config.get("fetch") or {}
        host = str(fetch_cfg.get("host") or "").strip()
        username = str(fetch_cfg.get("username") or "").strip()
        if not host or not username:
            raise ValueError(
                f"[{self.source_id}] email source requires extra.fetch.host "
                "and extra.fetch.username"
            )
        password = resolve_imap_password(self.source_id, fetch_cfg)

        try:
            max_items = int(fetch_cfg.get("max_items_per_run") or _DEFAULT_MAX_ITEMS)
        except (TypeError, ValueError):
            max_items = _DEFAULT_MAX_ITEMS

        messages, partial = await asyncio.to_thread(
            _collect_messages_blocking,
            source_id=self.source_id,
            host=host,
            port=int(fetch_cfg.get("port") or _DEFAULT_PORT),
            use_ssl=bool(fetch_cfg.get("use_ssl", True)),
            username=username,
            password=password,
            mailbox=str(fetch_cfg.get("mailbox") or "INBOX"),
            search_query=str(fetch_cfg.get("search") or "ALL"),
            max_items=max(1, max_items),
            timeout=int(fetch_cfg.get("timeout_sec") or _DEFAULT_TIMEOUT_SEC),
        )
        if partial:
            # Читает sources/base.py: чекпоинт не будет сохранён, пропуск
            # перечитается на следующем прогоне.
            self._partial_fetch = True
        return messages

    async def normalize_item(self, raw_item: Message) -> NormalizedSourceItem | None:
        try:
            return self._normalize_message(raw_item)
        except Exception as exc:
            message_id = ""
            getter = getattr(raw_item, "get", None)
            if callable(getter):
                try:
                    message_id = str(getter("Message-ID") or "")
                except Exception:  # noqa: BLE001 — заголовок тоже может быть битым
                    message_id = ""
            logger.warning(
                "[%s] failed to parse email message_id=%s: %s",
                self.source_id,
                message_id or "<unknown>",
                exc,
                exc_info=True,
            )
            return None

    def _normalize_message(self, raw_item: Message) -> NormalizedSourceItem | None:
        subject = _decode_header_value(raw_item.get("Subject"))
        message_id = compact_whitespace(raw_item.get("Message-ID"))
        author = _decode_header_value(raw_item.get("From"))
        published_at = parse_datetime(raw_item.get("Date"))

        text_parts: list[str] = []
        html_parts: list[str] = []
        if raw_item.is_multipart():
            for part in raw_item.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    text_parts.append(_decode_payload(part))
                elif content_type == "text/html":
                    html_parts.append(_decode_payload(part))
        else:
            body = _decode_payload(raw_item)
            if raw_item.get_content_type() == "text/html":
                html_parts.append(body)
            else:
                text_parts.append(body)

        html_links: list[str] = []
        if text_parts:
            content = compact_whitespace("\n".join(text_parts))
        else:
            # Алерт-рассылки сплошь и рядом приходят только в HTML — без этой
            # ветки такое письмо давало пустой content и падало в to_event().
            html_text_parts: list[str] = []
            for chunk in html_parts:
                text, urls = html_fragment_to_text(chunk)
                if text:
                    html_text_parts.append(text)
                html_links.extend(urls)
            content = compact_whitespace("\n".join(html_text_parts))

        links = finalize_linked_urls(extract_urls_from_plain_text(content) + html_links)

        if not content and not subject:
            logger.warning(
                "[%s] email message_id=%s has neither body nor subject — skipped",
                self.source_id,
                message_id or "<unknown>",
            )
            return None

        summary = ""
        if (self.config.get("parse") or {}).get("summary_from_email", True):
            summary = content[:500]

        return NormalizedSourceItem(
            external_id=build_external_id(
                guid=message_id,
                title=subject,
                published_at=published_at,
            ),
            url=links[0] if links else None,
            title=subject or "Email alert",
            content=content or subject,
            summary=summary,
            author=author or None,
            published_at=published_at,
            tags=[],
            linked_urls=links,
            lang=detect_language(subject, content),
            raw_payload={"subject": subject, "message_id": message_id, "from": author},
            extra={"connector": "email"},
        )
