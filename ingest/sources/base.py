"""Abstract source primitives and a generic connector framework."""
from __future__ import annotations

import abc
import hashlib
import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup

from ingest.source_runtime import SourceRuntimeStore
from shared.events.posts_parsed_v1 import PostParsedEvent
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls
from shared.redis_client import RedisClient
from shared.source_definitions import normalize_source_extra

logger = logging.getLogger(__name__)

_TRACKED_QUERY_PREFIXES = ("utm_", "rss", "ref", "source", "fbclid", "gclid")
_DEFAULT_USER_AGENT = "frontier-intelligence-ingest/1.0 (+https://frontier-intelligence.local)"
_RETRIABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


@dataclass
class NormalizedSourceItem:
    external_id: str
    url: str | None
    title: str
    content: str
    summary: str = ""
    author: str | None = None
    published_at: datetime | None = None
    tags: list[str] = field(default_factory=list)
    linked_urls: list[str] = field(default_factory=list)
    lang: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)


class AbstractSource(abc.ABC):
    """Collect posts and emit PostParsedEvent to Redis Stream."""

    source_id: str
    workspace_id: str
    stream_name: str = "stream:posts:parsed"

    def __init__(
        self,
        source_id: str,
        workspace_id: str,
        config: dict[str, Any],
        redis: RedisClient,
        runtime_store: SourceRuntimeStore | None = None,
    ):
        self.source_id = source_id
        self.workspace_id = workspace_id
        self.config = config
        self.redis = redis
        self.runtime_store = runtime_store
        self._fetched_count = 0
        self._emitted_count = 0
        self._checkpoint: dict[str, Any] = {}
        self._checkpoint_updates: dict[str, Any] = {}

    @abc.abstractmethod
    async def fetch(self) -> list[PostParsedEvent]:
        """Fetch new posts from the source and return events."""
        ...

    async def load_runtime_state(self) -> None:
        if not self.runtime_store:
            return
        self._checkpoint = await self.runtime_store.load_checkpoint(self.source_id)

    def checkpoint_cursor(self) -> dict[str, Any]:
        raw = self._checkpoint.get("cursor_json") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                raw = {}
        return raw if isinstance(raw, dict) else {}

    async def update_runtime_state(self, *, error: str = "", success: bool = False) -> None:
        if not self.runtime_store:
            return
        kwargs = {
            "source_id": self.source_id,
            "cursor_json": self._checkpoint_updates.get("cursor_json"),
            "etag": self._checkpoint_updates.get("etag"),
            "last_modified": self._checkpoint_updates.get("last_modified"),
            "last_seen_published_at": self._checkpoint_updates.get("last_seen_published_at"),
            "last_error": error[:4000] if error else None,
            "last_success_at": datetime.now(UTC) if success else None,
        }
        await self.runtime_store.upsert_checkpoint(**kwargs)

    async def emit_to_stream(self, events: list[PostParsedEvent]) -> int:
        pushed = 0
        for event in events:
            try:
                await self.redis.xadd(
                    self.stream_name,
                    event.model_dump(mode="json", exclude_none=True),
                )
                pushed += 1
            except Exception as exc:
                logger.error("Failed to push event %s: %s", event.external_id, exc)
        self._emitted_count = pushed
        return pushed

    async def run(self) -> int:
        run_id = None
        if self.runtime_store:
            run_id = await self.runtime_store.start_run(self.source_id)
        try:
            await self.load_runtime_state()
            events = await self.fetch()
            if events:
                pushed = await self.emit_to_stream(events)
                logger.info("[%s] pushed %d/%d events", self.source_id, pushed, len(events))
                await self.update_runtime_state(success=True)
                if run_id:
                    await self.runtime_store.finish_run(
                        run_id,
                        status="success",
                        fetched_count=self._fetched_count,
                        emitted_count=pushed,
                    )
                return pushed
            logger.info("[%s] fetch returned 0 events", self.source_id)
            await self.update_runtime_state(success=True)
            if run_id:
                await self.runtime_store.finish_run(
                    run_id,
                    status="success",
                    fetched_count=self._fetched_count,
                    emitted_count=0,
                )
            return 0
        except Exception as exc:
            logger.exception("[%s] run failed: %s", self.source_id, exc)
            await self.update_runtime_state(error=str(exc), success=False)
            if run_id:
                await self.runtime_store.finish_run(
                    run_id,
                    status="error",
                    fetched_count=self._fetched_count,
                    emitted_count=self._emitted_count,
                    error_text=str(exc),
                )
            return 0


class StructuredSource(AbstractSource):
    """Base class for RSS/Web/API/Email style connectors."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = normalize_source_extra(self.config.get("source_type", ""), self.config)
        self._seen_ids: list[str] = []

    async def fetch(self) -> list[PostParsedEvent]:
        checkpoint = self.checkpoint_cursor()
        self._seen_ids = list(checkpoint.get("seen_external_ids") or [])
        raw_items = await self.fetch_index()
        self._fetched_count = len(raw_items)
        events: list[PostParsedEvent] = []
        newest_published_at: datetime | None = None
        new_seen = list(self._seen_ids)

        normalized_items: list[NormalizedSourceItem] = []
        for raw in raw_items:
            item = await self.normalize_item(raw)
            if not item:
                continue
            if self.is_duplicate(item):
                continue
            if not self.matches_filters(item):
                continue
            item = await self.hydrate_item(item)
            normalized_items.append(item)

        normalized_items.sort(key=lambda x: x.published_at or datetime.min.replace(tzinfo=UTC))

        for item in normalized_items:
            events.append(self.to_event(item))
            if item.external_id not in new_seen:
                new_seen.append(item.external_id)
            if item.published_at and (
                newest_published_at is None or item.published_at > newest_published_at
            ):
                newest_published_at = item.published_at

        self._checkpoint_updates["cursor_json"] = {
            **checkpoint,
            **(self._checkpoint_updates.get("cursor_json") or {}),
            "seen_external_ids": new_seen[-500:],
        }
        if newest_published_at:
            self._checkpoint_updates["last_seen_published_at"] = newest_published_at
        return events

    @abc.abstractmethod
    async def fetch_index(self) -> list[Any]:
        ...

    @abc.abstractmethod
    async def normalize_item(self, raw_item: Any) -> NormalizedSourceItem | None:
        ...

    async def hydrate_item(self, item: NormalizedSourceItem) -> NormalizedSourceItem:
        if not self.config.get("parse", {}).get("full_content") or not item.url:
            return item
        article_selector = (self.config.get("parse") or {}).get("article_selector") or ""
        try:
            hydrated = await fetch_url_content(
                item.url,
                timeout=self.request_timeout(),
                article_selector=article_selector,
                source_config=self.config,
            )
        except Exception as exc:
            logger.warning("[%s] hydration failed for %s: %s", self.source_id, item.url, exc)
            item.extra["hydration_error"] = str(exc)
            return item
        if hydrated.get("content"):
            item.content = hydrated["content"]
            item.summary = hydrated.get("summary") or item.summary
            item.title = hydrated.get("title") or item.title
            item.linked_urls = finalize_linked_urls(
                item.linked_urls + hydrated.get("linked_urls", [])
            )
            item.extra["hydrated"] = True
        return item

    def to_event(self, item: NormalizedSourceItem) -> PostParsedEvent:
        content = item.content or item.summary or item.title or ""
        if not content:
            raise ValueError(f"Normalized item for {self.source_id} has no content")
        extra = {
            "title": item.title,
            "summary": item.summary,
            "lang": item.lang,
            "raw_payload": item.raw_payload,
            **(item.extra or {}),
        }
        return PostParsedEvent(
            workspace_id=self.workspace_id,
            source_id=self.source_id,
            external_id=item.external_id,
            content=content,
            linked_urls=finalize_linked_urls(item.linked_urls),
            published_at=item.published_at,
            url=item.url,
            author=item.author,
            tags=item.tags,
            extra=extra,
        )

    def matches_filters(self, item: NormalizedSourceItem) -> bool:
        filters = self.config.get("filters") or {}
        include_keywords = [
            str(x).lower() for x in filters.get("include_keywords") or [] if str(x).strip()
        ]
        exclude_keywords = [
            str(x).lower() for x in filters.get("exclude_keywords") or [] if str(x).strip()
        ]
        lang_allow = [str(x).lower() for x in filters.get("lang_allow") or [] if str(x).strip()]
        haystack = " ".join([item.title, item.summary, item.content]).lower()
        if include_keywords and not any(keyword in haystack for keyword in include_keywords):
            return False
        if exclude_keywords and any(keyword in haystack for keyword in exclude_keywords):
            return False
        if lang_allow and item.lang and item.lang.lower() not in lang_allow:
            return False
        return True

    def is_duplicate(self, item: NormalizedSourceItem) -> bool:
        return item.external_id in self._seen_ids

    def request_timeout(self) -> int:
        fetch_cfg = self.config.get("fetch") or {}
        return int(fetch_cfg.get("timeout_sec") or 20)


def canonicalize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return url.strip()
    query = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if not any(k.lower().startswith(prefix) for prefix in _TRACKED_QUERY_PREFIXES)
    ]
    clean = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        query=urlencode(query, doseq=True),
        fragment="",
    )
    return urlunparse(clean)


def build_external_id(
    *,
    guid: str | None = None,
    url: str | None = None,
    title: str = "",
    published_at: datetime | None = None,
) -> str:
    if guid:
        return guid.strip()
    canonical_url = canonicalize_url(url)
    if canonical_url:
        return canonical_url
    raw = f"{title}|{published_at.isoformat() if published_at else ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def detect_language(*parts: str | None) -> str:
    text = " ".join(part or "" for part in parts).strip()
    if not text:
        return "unknown"
    cyrillic = sum(1 for char in text[:300] if "\u0400" <= char <= "\u04FF")
    return "ru" if cyrillic else "en"


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, tuple):
        try:
            return datetime(*value[:6], tzinfo=UTC)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for parser in (
            lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")),
            parsedate_to_datetime,
        ):
            try:
                dt = parser(text)
                return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
            except Exception:
                continue
    return None


def html_to_text(html: str, *, article_selector: str = "") -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    root = soup.select_one(article_selector) if article_selector else soup
    if root is None:
        root = soup
    for tag in root(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()
    title = ""
    if soup.title and soup.title.get_text(strip=True):
        title = soup.title.get_text(strip=True)
    h1 = root.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)
    paragraphs = [
        el.get_text(" ", strip=True)
        for el in root.find_all(["p", "li", "blockquote", "h2", "h3"])
    ]
    paragraphs = [p for p in paragraphs if p]
    content = "\n\n".join(paragraphs)
    summary = paragraphs[0][:500] if paragraphs else ""
    urls = [a.get("href", "").strip() for a in root.find_all("a", href=True)]
    return {
        "title": title,
        "content": content[:20000],
        "summary": summary,
        "linked_urls": finalize_linked_urls(urls + extract_urls_from_plain_text(content)),
    }


def html_fragment_to_text(html: str | None) -> tuple[str, list[str]]:
    if not html:
        return "", []
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    urls = [a.get("href", "").strip() for a in soup.find_all("a", href=True)]
    text = soup.get_text(separator=" ", strip=True)
    text = compact_whitespace(text)
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    return text, finalize_linked_urls(urls)


def build_http_headers(source_config: Mapping[str, Any] | None = None) -> dict[str, str]:
    fetch_cfg = source_config.get("fetch") if isinstance(source_config, Mapping) else {}
    extra_headers = fetch_cfg.get("headers") if isinstance(fetch_cfg, Mapping) else {}
    headers = {
        "User-Agent": _DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if isinstance(extra_headers, Mapping):
        headers.update(
            {
                str(key): str(value)
                for key, value in extra_headers.items()
                if str(key).strip() and value is not None
            }
        )
    return headers


def build_httpx_client(
    *,
    source_config: Mapping[str, Any] | None = None,
    timeout: int = 20,
    follow_redirects: bool = True,
) -> httpx.AsyncClient:
    proxy_config = source_config.get("proxy_config") if isinstance(source_config, Mapping) else {}
    proxy = None
    if isinstance(proxy_config, Mapping):
        proxy_host = str(proxy_config.get("host") or "").strip()
        proxy_type = str(proxy_config.get("type") or "http").strip().lower()
        if proxy_host and proxy_type in {"http", "https", "socks5"}:
            auth = ""
            if proxy_config.get("user"):
                auth = str(proxy_config["user"])
                if proxy_config.get("password"):
                    auth += f":{proxy_config['password']}"
                auth += "@"
            if proxy_type == "socks5":
                scheme = "socks5"
            elif proxy_type == "https":
                scheme = "https"
            else:
                scheme = "http"
            default_port = 1080 if scheme == "socks5" else (443 if scheme == "https" else 8080)
            port = int(proxy_config.get("port") or default_port)
            proxy = f"{scheme}://{auth}{proxy_host}:{port}"
    return httpx.AsyncClient(
        headers=build_http_headers(source_config),
        timeout=httpx.Timeout(float(timeout), connect=float(timeout), read=float(timeout)),
        follow_redirects=follow_redirects,
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        proxy=proxy,
    )


async def http_get_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    attempts: int = 3,
    allow_status_codes: set[int] | None = None,
) -> httpx.Response:
    last_error: Exception | None = None
    merged_headers = dict(headers or {})
    allowed = allow_status_codes or set()
    for attempt in range(1, attempts + 1):
        try:
            response = await client.get(url, headers=merged_headers or None, params=params)
            status_code = int(getattr(response, "status_code", 200) or 200)
            if status_code in allowed:
                return response
            if status_code in _RETRIABLE_STATUS_CODES and attempt < attempts:
                continue
            response.raise_for_status()
            return response
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            last_error = exc
            if attempt >= attempts:
                raise
        except httpx.HTTPStatusError as exc:
            last_error = exc
            if exc.response.status_code not in _RETRIABLE_STATUS_CODES or attempt >= attempts:
                raise
    if last_error:
        raise last_error
    raise RuntimeError(f"GET {url} failed without response")


async def fetch_url_content(
    url: str,
    *,
    timeout: int = 20,
    article_selector: str = "",
    source_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    async with build_httpx_client(
        source_config=source_config,
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        response = await http_get_with_retries(client, url)
        data = html_to_text(response.text, article_selector=article_selector)
        data["url"] = canonicalize_url(str(response.url))
        return data


def absolute_url(base_url: str, maybe_relative: str) -> str:
    return urljoin(base_url, maybe_relative)


def compact_whitespace(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def dig_path(payload: Any, path: str) -> Any:
    if not path:
        return payload
    current = payload
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except Exception:
                return None
        else:
            return None
    return current
