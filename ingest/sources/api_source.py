"""Structured JSON/CSV API connector with cursor support."""
from __future__ import annotations

import csv
import io
import logging
from typing import Any

import httpx

from ingest.sources.base import (
    NormalizedSourceItem,
    StructuredSource,
    build_external_id,
    build_httpx_client,
    canonicalize_url,
    compact_whitespace,
    detect_language,
    dig_path,
    ensure_list,
    http_get_with_retries,
    parse_datetime,
)
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls

logger = logging.getLogger(__name__)


class APISource(StructuredSource):
    async def _expand_items(
        self,
        client: httpx.AsyncClient,
        items: list[Any],
        field_map: dict[str, Any],
    ) -> list[Any]:
        item_url_template = str(
            (self.config.get("fetch") or {}).get("item_url_template") or ""
        ).strip()
        if not item_url_template or not items:
            return items
        if isinstance(items[0], dict) and field_map.get("title"):
            return items

        expanded: list[Any] = []
        max_items = int(self.config.get("fetch", {}).get("max_items_per_run") or 50)
        for raw_id in items[:max_items]:
            detail_url = item_url_template.format(id=raw_id)
            response = await client.get(detail_url)
            response.raise_for_status()
            expanded.append(response.json())
        return expanded

    def _apply_auth(
        self,
        headers: dict[str, str],
        params: dict[str, str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        auth = self.config.get("fetch", {}).get("auth") or {}
        mode = auth.get("mode", "none")
        if mode == "header_token" and auth.get("token"):
            headers[str(auth.get("header", "Authorization"))] = str(auth["token"])
        elif mode == "basic" and auth.get("username"):
            token = httpx.BasicAuth(str(auth["username"]), str(auth.get("password", "")))
            headers["_basic_auth_obj"] = token
        elif mode == "query_key" and auth.get("param") and auth.get("token"):
            params[str(auth["param"])] = str(auth["token"])
        return headers, params

    async def fetch_index(self) -> list[Any]:
        url = self.config.get("url")
        fetch_cfg = self.config.get("fetch") or {}
        parse_cfg = self.config.get("parse") or {}
        field_map = parse_cfg.get("field_map") or {}
        cursor = self.checkpoint_cursor()
        next_cursor_path = str(field_map.get("next_cursor", "") or "").strip()
        headers: dict[str, str] = {}
        params: dict[str, str] = {}
        headers, params = self._apply_auth(headers, params)

        if next_cursor_path and cursor.get("next_cursor"):
            params[str(fetch_cfg.get("cursor_param", "cursor"))] = str(cursor["next_cursor"])
        elif cursor.get("page"):
            params[str(fetch_cfg.get("page_param", "page"))] = str(cursor["page"])

        basic_auth = headers.pop("_basic_auth_obj", None)
        async with build_httpx_client(
            source_config=self.config,
            timeout=self.request_timeout(),
            follow_redirects=True,
        ) as client:
            if basic_auth is not None:
                client.auth = basic_auth
            response = await http_get_with_retries(client, url, headers=headers, params=params)
            response_format = str(parse_cfg.get("format") or "json").lower()
            if response_format == "csv":
                items = list(csv.DictReader(io.StringIO(response.text)))
                payload: Any = {"items": items}
            else:
                payload = response.json()
                items = (
                    dig_path(payload, field_map.get("items_path", "")) if field_map else payload
                )
                items = ensure_list(items)
                items = await self._expand_items(client, items, field_map)

        next_cursor = dig_path(payload, next_cursor_path) if next_cursor_path else None
        page_mode = bool(fetch_cfg.get("page_param"))
        next_page = int(cursor.get("page") or 1) + 1 if items and page_mode else None
        self._checkpoint_updates["cursor_json"] = {
            **cursor,
            "next_cursor": next_cursor,
            "page": next_page or cursor.get("page") or 1,
        }
        max_items = int(fetch_cfg.get("max_items_per_run") or 50)
        return list(items)[:max_items]

    async def normalize_item(self, raw_item: Any) -> NormalizedSourceItem | None:
        field_map = (self.config.get("parse") or {}).get("field_map") or {}
        url = canonicalize_url(
            str(dig_path(raw_item, field_map.get("url", "url")) or "").strip() or None
        )
        title = compact_whitespace(
            str(dig_path(raw_item, field_map.get("title", "title")) or "")
        )
        content = compact_whitespace(
            str(dig_path(raw_item, field_map.get("content", "content")) or "")
        )
        summary = compact_whitespace(
            str(dig_path(raw_item, field_map.get("summary", "summary")) or "")
        )
        author = compact_whitespace(
            str(dig_path(raw_item, field_map.get("author", "author")) or "")
        )
        published_at = parse_datetime(
            dig_path(raw_item, field_map.get("published_at", "published_at"))
        )
        tags = ensure_list(dig_path(raw_item, field_map.get("tags", "tags")))
        linked_urls = ensure_list(dig_path(raw_item, field_map.get("linked_urls", "linked_urls")))
        guid = dig_path(raw_item, field_map.get("id", "id"))
        merged_content = content or summary or title
        if not merged_content and not url:
            return None

        return NormalizedSourceItem(
            external_id=build_external_id(
                guid=str(guid) if guid else None,
                url=url,
                title=title,
                published_at=published_at,
            ),
            url=url,
            title=title or summary[:120] or "Untitled API item",
            content=merged_content,
            summary=summary,
            author=author or None,
            published_at=published_at,
            tags=[compact_whitespace(str(tag)) for tag in tags if compact_whitespace(str(tag))],
            linked_urls=finalize_linked_urls(
                linked_urls + extract_urls_from_plain_text(merged_content)
            ),
            lang=detect_language(title, summary, merged_content),
            raw_payload=raw_item if isinstance(raw_item, dict) else {"value": raw_item},
            extra={"connector": "api"},
        )
