"""IMAP-based alert/email source."""
from __future__ import annotations

import email
import imaplib
import logging
from email.message import Message
from typing import Any

from ingest.sources.base import (
    NormalizedSourceItem,
    StructuredSource,
    build_external_id,
    compact_whitespace,
    detect_language,
    parse_datetime,
)
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls

logger = logging.getLogger(__name__)


class EmailSource(StructuredSource):
    async def fetch_index(self) -> list[Any]:
        return await self._fetch_messages()

    async def _fetch_messages(self) -> list[Message]:
        fetch_cfg = self.config.get("fetch") or {}
        mailbox = str(fetch_cfg.get("mailbox") or "INBOX")
        search_query = str(fetch_cfg.get("search") or "ALL")
        use_ssl = bool(fetch_cfg.get("use_ssl", True))
        host = str(fetch_cfg.get("host") or "")
        port = int(fetch_cfg.get("port") or 993)
        username = str(fetch_cfg.get("username") or "")
        password = str(fetch_cfg.get("password") or "")

        client = imaplib.IMAP4_SSL(host, port) if use_ssl else imaplib.IMAP4(host, port)
        try:
            client.login(username, password)
            client.select(mailbox)
            _, data = client.search(None, search_query)
            ids = list(reversed((data[0] or b"").split()))
            max_items = int(self.config.get("fetch", {}).get("max_items_per_run") or 50)
            messages: list[Message] = []
            for msg_id in ids[:max_items]:
                _, msg_data = client.fetch(msg_id, "(RFC822)")
                raw_bytes = msg_data[0][1]
                messages.append(email.message_from_bytes(raw_bytes))
            return messages
        finally:
            try:
                client.logout()
            except Exception:
                pass

    async def normalize_item(self, raw_item: Message) -> NormalizedSourceItem | None:
        subject = compact_whitespace(raw_item.get("Subject"))
        message_id = compact_whitespace(raw_item.get("Message-ID"))
        author = compact_whitespace(raw_item.get("From"))
        published_at = parse_datetime(raw_item.get("Date"))

        body_parts: list[str] = []
        if raw_item.is_multipart():
            for part in raw_item.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True) or b""
                    body_parts.append(
                        payload.decode(part.get_content_charset() or "utf-8", errors="ignore")
                    )
        else:
            payload = raw_item.get_payload(decode=True) or b""
            body_parts.append(
                payload.decode(raw_item.get_content_charset() or "utf-8", errors="ignore")
            )

        content = compact_whitespace("\n".join(body_parts))
        links = finalize_linked_urls(extract_urls_from_plain_text(content))
        return NormalizedSourceItem(
            external_id=build_external_id(
                guid=message_id,
                title=subject,
                published_at=published_at,
            ),
            url=links[0] if links else None,
            title=subject or "Email alert",
            content=content or subject,
            summary=content[:500],
            author=author or None,
            published_at=published_at,
            tags=[],
            linked_urls=links,
            lang=detect_language(subject, content),
            raw_payload={"subject": subject, "message_id": message_id, "from": author},
            extra={"connector": "email"},
        )
