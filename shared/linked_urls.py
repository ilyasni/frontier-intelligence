"""Извлечение внешних URL для crawl4ai (не permalink Telegram)."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from telethon.tl.types import Message

# Максимум ссылок в одном событии (защита от спама)
MAX_LINKED_URLS = 10

# Хосты, которые не считаем «внешним контентом» для crawl
_BLOCKED_HOSTS = frozenset(
    {
        "t.me",
        "telegram.me",
        "telegram.dog",
        "www.telegram.me",
        "twitter.com",
        "www.twitter.com",
        "x.com",
        "www.x.com",
    }
)

# «Голые» URL в тексте
_URL_IN_TEXT = re.compile(r"https?://[^\s\]\)\"'<>]+", re.IGNORECASE)


def _host_blocked(url: str) -> bool:
    try:
        p = urlparse(url.strip())
        host = (p.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host in _BLOCKED_HOSTS
    except Exception:
        return True


def finalize_linked_urls(urls: list[str]) -> list[str]:
    """Дедуп, фильтр Telegram/Twitter, лимит, нормализация пробелов."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in urls:
        u = (raw or "").strip()
        if not u or not u.startswith(("http://", "https://")):
            continue
        if _host_blocked(u):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= MAX_LINKED_URLS:
            break
    return out


def extract_urls_from_plain_text(text: str) -> list[str]:
    """Fallback: regex по тексту поста."""
    if not text:
        return []
    return _URL_IN_TEXT.findall(text)


def extract_urls_from_telethon_message(msg: Message) -> list[str]:
    """URL из entities Telethon (приоритетнее regex)."""
    from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

    if not getattr(msg, "entities", None) or not (msg.message or ""):
        return []
    out: list[str] = []
    try:
        pairs = msg.get_entities_text()
    except Exception:
        return out
    for entity, fragment in pairs:
        if isinstance(entity, MessageEntityTextUrl):
            if entity.url and entity.url.startswith(("http://", "https://")):
                out.append(entity.url.strip())
        elif isinstance(entity, MessageEntityUrl):
            u = (fragment or "").strip()
            if u.startswith(("http://", "https://")):
                out.append(u)
    return out


def build_linked_urls_for_telegram_messages(messages: list[Message], combined_text: str) -> list[str]:
    """Альбом или одно сообщение: entities со всех сообщений + regex по объединённому тексту."""
    collected: list[str] = []
    for m in messages:
        collected.extend(extract_urls_from_telethon_message(m))
    collected.extend(extract_urls_from_plain_text(combined_text))
    return finalize_linked_urls(collected)
