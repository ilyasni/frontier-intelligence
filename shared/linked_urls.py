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

# Хвосты, которые прилипают к URL, выдранному из прозы, и делают краул
# гарантированно бесполезным. Замер 2026-08-05: среди реально отказавших
# ссылок нашлись `github.com/CPS-research-group/ink_bwts.`,
# `github.com/Xia12121/LoCA}{here}.` — это URL из абстрактов arXiv вместе
# с точкой конца предложения и остатками TeX-разметки. Каждая такая ссылка
# даёт 404 и в метрике неотличима от настоящей мёртвой ссылки.
#
# Скобки чистятся ТОЛЬКО непарные: в адресах Wikipedia круглые скобки
# значащие (`.../Foo_(bar)`), и обрезать их вслепую значит сломать рабочие
# ссылки ради починки битых.
_TRAILING_JUNK = ".,;:!?'\"«»…"
_TRAILING_BRACKETS = {")": "(", "]": "[", "}": "{"}

# Символы, недопустимые в URL по RFC 3986. Их появление означает, что регулярка
# захватила соседний текст, а не адрес, — поэтому обрезаем ПО ПЕРВОМУ такому
# символу, а не с конца. Иначе `LoCA}{here}.` не вычистить: снятие хвоста
# по одному символу останавливается на букве `e` внутри `{here}`.
_ILLEGAL_IN_URL = '{}|\\^`" <>'


def strip_url_tail(url: str) -> str:
    """Снять с URL прилипший текст: пунктуацию, непарные скобки, TeX-остатки."""
    value = (url or "").strip()

    # Сначала обрезаем по первому недопустимому символу — он гарантированно
    # означает конец адреса и начало окружающей прозы или разметки.
    for index, char in enumerate(value):
        if char in _ILLEGAL_IN_URL:
            value = value[:index]
            break

    # Затем снимаем хвостовую пунктуацию и скобки, оставшиеся незакрытыми.
    # Парные скобки не трогаем: в адресах Wikipedia они значащие.
    while value:
        last = value[-1]
        if last in _TRAILING_JUNK:
            value = value[:-1]
            continue
        opening = _TRAILING_BRACKETS.get(last)
        if opening and value.count(opening) < value.count(last):
            value = value[:-1]
            continue
        break
    return value


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
        u = strip_url_tail(raw)
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
