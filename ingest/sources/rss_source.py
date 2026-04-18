"""RSS/Atom connector with filtering, dedupe, and optional full-content hydration."""
from __future__ import annotations

import logging
from typing import Any

from bs4 import BeautifulSoup

from ingest.sources.base import (
    NormalizedSourceItem,
    StructuredSource,
    absolute_url,
    build_external_id,
    build_httpx_client,
    canonicalize_url,
    compact_whitespace,
    detect_language,
    html_fragment_to_text,
    http_get_with_retries,
    parse_datetime,
)
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls

logger = logging.getLogger(__name__)


class RSSSource(StructuredSource):
    @staticmethod
    def _looks_like_feed_response(response) -> bool:
        content_type = str(response.headers.get("Content-Type", "")).lower()
        if any(token in content_type for token in ("xml", "rss", "atom")):
            return True
        body_start = response.text[:500].lower()
        return any(token in body_start for token in ("<rss", "<feed", "<rdf:rdf"))

    @staticmethod
    def _extract_alternate_feed_url(html: str, base_url: str) -> str | None:
        soup = BeautifulSoup(html, "lxml")
        for link in soup.select("head link[rel]"):
            rel_values = [str(value).strip().lower() for value in link.get("rel", [])]
            if "alternate" not in rel_values:
                continue
            link_type = str(link.get("type", "")).strip().lower()
            if link_type not in {"application/rss+xml", "application/atom+xml"}:
                continue
            href = str(link.get("href", "")).strip()
            if href:
                return canonicalize_url(absolute_url(base_url, href))
        return None

    async def fetch_index(self) -> list[Any]:
        url = self.config.get("url")
        if not url:
            return []
        checkpoint = self.checkpoint_cursor()
        request_url = checkpoint.get("resolved_feed_url") or url

        headers = {}
        if self.config.get("fetch", {}).get("use_conditional_get", True):
            etag = self._checkpoint.get("etag")
            last_modified = self._checkpoint.get("last_modified")
            if etag:
                headers["If-None-Match"] = etag
            if last_modified:
                headers["If-Modified-Since"] = last_modified

        async with build_httpx_client(
            source_config=self.config,
            timeout=self.request_timeout(),
            follow_redirects=True,
        ) as client:
            response = await http_get_with_retries(
                client,
                request_url,
                headers=headers,
                allow_status_codes={304},
            )
            if response.status_code == 304:
                logger.info("[%s] feed not modified", self.source_id)
                return []
            feed_response = response
            if not self._looks_like_feed_response(response):
                alternate_feed_url = self._extract_alternate_feed_url(
                    response.text,
                    str(response.url),
                )
                if not alternate_feed_url:
                    logger.warning("[%s] no alternate feed discovered for %s", self.source_id, url)
                    return []
                feed_response = await http_get_with_retries(client, alternate_feed_url)
                self._checkpoint_updates["cursor_json"] = {
                    **checkpoint,
                    "resolved_feed_url": alternate_feed_url,
                }
            feed_response.raise_for_status()
            import feedparser
            parsed = feedparser.parse(feed_response.content)
            self._checkpoint_updates["etag"] = feed_response.headers.get("ETag")
            self._checkpoint_updates["last_modified"] = feed_response.headers.get("Last-Modified")

        max_items = int(self.config.get("fetch", {}).get("max_items_per_run") or 50)
        return list(parsed.entries or [])[:max_items]

    async def normalize_item(self, raw_item: Any) -> NormalizedSourceItem | None:
        title = compact_whitespace(getattr(raw_item, "title", None) or raw_item.get("title"))
        link = canonicalize_url(getattr(raw_item, "link", None) or raw_item.get("link"))
        guid = getattr(raw_item, "id", None) or raw_item.get("id") or raw_item.get("guid")
        raw_summary = (
            getattr(raw_item, "summary", None)
            or raw_item.get("summary")
            or getattr(raw_item, "description", None)
            or raw_item.get("description")
        )
        summary, summary_urls = html_fragment_to_text(raw_summary)
        content_parts = []
        content_urls: list[str] = []
        raw_content = getattr(raw_item, "content", None) or raw_item.get("content") or []
        for piece in raw_content:
            if isinstance(piece, dict):
                part_text, part_urls = html_fragment_to_text(piece.get("value"))
                if part_text:
                    content_parts.append(part_text)
                content_urls.extend(part_urls)
        content = "\n\n".join(part for part in content_parts if part) or summary or title
        published_at = parse_datetime(
            getattr(raw_item, "published_parsed", None)
            or raw_item.get("published_parsed")
            or getattr(raw_item, "published", None)
            or raw_item.get("published")
            or getattr(raw_item, "updated_parsed", None)
            or raw_item.get("updated_parsed")
        )
        author = compact_whitespace(getattr(raw_item, "author", None) or raw_item.get("author"))
        tags = [
            compact_whitespace(tag.get("term"))
            for tag in raw_item.get("tags", [])
            if tag.get("term")
        ]
        linked_urls = finalize_linked_urls(
            [link]
            + summary_urls
            + content_urls
            + extract_urls_from_plain_text(summary)
            + extract_urls_from_plain_text(content)
        )

        if not any([title, summary, content, link]):
            return None

        return NormalizedSourceItem(
            external_id=build_external_id(
                guid=str(guid) if guid else None,
                url=link,
                title=title,
                published_at=published_at,
            ),
            url=link,
            title=title or summary[:120] or link or "Untitled",
            content=content,
            summary=summary,
            author=author or None,
            published_at=published_at,
            tags=[tag for tag in tags if tag],
            linked_urls=linked_urls,
            lang=detect_language(title, summary, content),
            raw_payload=dict(raw_item),
            extra={"feed_type": "rss"},
        )
