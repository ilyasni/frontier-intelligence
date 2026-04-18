"""Website listing connector driven by selectors in source.extra.parse."""
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
    fetch_url_content,
    http_get_with_retries,
    parse_datetime,
)
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls

logger = logging.getLogger(__name__)


class WebSource(StructuredSource):
    async def fetch_index(self) -> list[Any]:
        url = self.config.get("url")
        parse_cfg = self.config.get("parse") or {}
        listing_selector = parse_cfg.get("listing_selector") or "article, .post, .item"
        async with build_httpx_client(
            source_config=self.config,
            timeout=self.request_timeout(),
            follow_redirects=True,
        ) as client:
            response = await http_get_with_retries(client, url)
        soup = BeautifulSoup(response.text, "lxml")
        items = soup.select(listing_selector)
        max_items = int(self.config.get("fetch", {}).get("max_items_per_run") or 20)
        return items[:max_items]

    async def normalize_item(self, raw_item: Any) -> NormalizedSourceItem | None:
        parse_cfg = self.config.get("parse") or {}
        base_url = self.config.get("url") or ""
        full_content = bool(parse_cfg.get("full_content", True))
        link_selector = parse_cfg.get("link_selector") or "a[href]"
        title_selector = parse_cfg.get("title_selector") or "h1, h2, h3, a[href]"
        date_selector = parse_cfg.get("date_selector") or "time"
        article_selector = parse_cfg.get("article_selector") or ""

        link_el = raw_item.select_one(link_selector)
        title_el = raw_item.select_one(title_selector)
        date_el = raw_item.select_one(date_selector)

        href = link_el.get("href", "").strip() if link_el else ""
        if not href:
            return None
        url = canonicalize_url(absolute_url(base_url, href))
        title = compact_whitespace(title_el.get_text(" ", strip=True) if title_el else "") or href
        published_at = parse_datetime(
            (date_el.get("datetime") if date_el and date_el.has_attr("datetime") else None)
            or (date_el.get_text(" ", strip=True) if date_el else None)
        )
        listing_summary = compact_whitespace(raw_item.get_text(" ", strip=True))
        hydration_error = None
        hydrated: dict[str, Any] = {}
        if full_content:
            try:
                hydrated = await fetch_url_content(
                    url,
                    timeout=self.request_timeout(),
                    article_selector=article_selector,
                    source_config=self.config,
                )
            except Exception as exc:
                hydration_error = str(exc)
                logger.warning("[%s] listing hydration failed for %s: %s", self.source_id, url, exc)
                hydrated = {}
        if full_content:
            summary = hydrated.get("summary") or title or listing_summary or url or href
        else:
            summary = hydrated.get("summary") or listing_summary or title or url or href
        content = hydrated.get("content") if full_content else None
        content = content or summary
        linked_urls = finalize_linked_urls(
            hydrated.get("linked_urls", []) + extract_urls_from_plain_text(summary)
        )

        return NormalizedSourceItem(
            external_id=build_external_id(url=url, title=title, published_at=published_at),
            url=url,
            title=title,
            content=content,
            summary=summary,
            author=None,
            published_at=published_at,
            tags=[],
            linked_urls=linked_urls,
            lang=detect_language(title, summary, content),
            raw_payload={"href": href, "title": title},
            extra={
                "selector_source": "web",
                **({"hydration_error": hydration_error} if hydration_error else {}),
            },
        )

    async def hydrate_item(self, item: NormalizedSourceItem) -> NormalizedSourceItem:
        # WebSource already handles optional article hydration in normalize_item().
        return item
