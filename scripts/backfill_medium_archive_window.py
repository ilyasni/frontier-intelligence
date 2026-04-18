"""Backfill Medium tag archive pages into stream:posts:parsed for a time window."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import asyncpg
from bs4 import BeautifulSoup

sys.path.insert(0, "/app")
sys.path.insert(0, os.getcwd())

from ingest.sources.base import (
    build_external_id,
    build_httpx_client,
    canonicalize_url,
    compact_whitespace,
    detect_language,
    fetch_url_content,
    http_get_with_retries,
    parse_datetime,
)
from shared.events.posts_parsed_v1 import PostParsedEvent
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls
from shared.redis_client import RedisClient

STREAM_NAME = "stream:posts:parsed"


@dataclass(slots=True)
class SourceRecord:
    id: str
    workspace_id: str
    url: str
    proxy_config: dict[str, Any]
    extra: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--workspace-id", default="disruption")
    parser.add_argument(
        "--source-id",
        action="append",
        required=True,
        help="Repeatable source id, e.g. rss_medium_design",
    )
    parser.add_argument("--start", required=True, help="UTC ISO timestamp")
    parser.add_argument("--end", required=True, help="UTC ISO timestamp")
    parser.add_argument("--max-links-per-day", type=int, default=80)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def require_url(value: str, name: str) -> str:
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def parse_ts(value: str, name: str) -> datetime:
    parsed = parse_datetime(value)
    if parsed is None:
        raise SystemExit(f"Invalid {name}: {value}")
    return parsed.astimezone(UTC)


def coerce_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    return dict(value)


def iter_dates(start: datetime, end: datetime) -> list[date]:
    cursor = start.date()
    stop = end.date()
    out: list[date] = []
    while cursor <= stop:
        out.append(cursor)
        cursor += timedelta(days=1)
    return out


async def load_sources(
    conn: asyncpg.Connection,
    *,
    workspace_id: str,
    source_ids: list[str],
) -> list[SourceRecord]:
    rows = await conn.fetch(
        """
        SELECT id, workspace_id, url, proxy_config, extra
        FROM sources
        WHERE workspace_id = $1
          AND id = ANY($2::text[])
        ORDER BY id
        """,
        workspace_id,
        source_ids,
    )
    if len(rows) != len(set(source_ids)):
        found = {str(row["id"]) for row in rows}
        missing = sorted(set(source_ids) - found)
        raise SystemExit(f"Source(s) not found: {', '.join(missing)}")
    return [
        SourceRecord(
            id=str(row["id"]),
            workspace_id=str(row["workspace_id"]),
            url=str(row["url"] or ""),
            proxy_config=coerce_json_dict(row["proxy_config"]),
            extra=coerce_json_dict(row["extra"]),
        )
        for row in rows
    ]


def medium_tag_slug(source: SourceRecord) -> str:
    parsed = urlparse(source.url)
    parts = [part for part in parsed.path.split("/") if part]
    try:
        tag_index = parts.index("tag")
    except ValueError as exc:
        raise SystemExit(f"Source URL does not look like a Medium tag feed: {source.id} -> {source.url}") from exc
    if tag_index + 1 >= len(parts):
        raise SystemExit(f"Cannot derive Medium tag slug from {source.id} -> {source.url}")
    return parts[tag_index + 1]


def archive_day_url(slug: str, day: date) -> str:
    return f"https://medium.com/tag/{slug}/archive/{day.year:04d}/{day.month:02d}/{day.day:02d}"


def extract_archive_links(html: str, base_url: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = compact_whitespace(anchor.get("href", ""))
        if not href:
            continue
        href = canonicalize_url(href) or href
        if href.startswith("/"):
            href = canonicalize_url(f"https://medium.com{href}") or href
        if not href.startswith("https://medium.com/"):
            continue
        if "/tag/" in href or "/archive" in href or href.endswith("/followers"):
            continue
        if "/m/" in href or "/me/" in href or "/about" in href:
            continue
        parsed = urlparse(href)
        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) < 2:
            continue
        if parsed.netloc != "medium.com":
            continue
        if href in seen:
            continue
        seen.add(href)
        links.append(href)
        if len(links) >= limit:
            break
    return links


def parse_ld_json_datetime(payload: Any) -> datetime | None:
    if isinstance(payload, list):
        for item in payload:
            parsed = parse_ld_json_datetime(item)
            if parsed is not None:
                return parsed
        return None
    if not isinstance(payload, dict):
        return None
    direct = parse_datetime(payload.get("datePublished") or payload.get("dateCreated"))
    if direct is not None:
        return direct
    for key in ("@graph", "mainEntity", "itemListElement"):
        nested = payload.get(key)
        parsed = parse_ld_json_datetime(nested)
        if parsed is not None:
            return parsed
    return None


async def fetch_medium_article(
    *,
    source: SourceRecord,
    article_url: str,
) -> PostParsedEvent | None:
    config = {
        "source_type": "rss",
        "url": source.url,
        "proxy_config": source.proxy_config,
        "fetch": source.extra.get("fetch") or {},
        "parse": source.extra.get("parse") or {},
    }
    async with build_httpx_client(source_config=config, timeout=30, follow_redirects=True) as client:
        response = await http_get_with_retries(client, article_url)
        html = response.text
    soup = BeautifulSoup(html, "lxml")
    published_at = None
    meta_published = soup.select_one("meta[property='article:published_time']")
    if meta_published is not None:
        published_at = parse_datetime(meta_published.get("content"))
    if published_at is None:
        for script in soup.select("script[type='application/ld+json']"):
            raw = script.string or script.get_text(" ", strip=True)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            published_at = parse_ld_json_datetime(payload)
            if published_at is not None:
                break
    hydrated = await fetch_url_content(article_url, timeout=30, source_config=config)
    title = compact_whitespace(
        str(
            hydrated.get("title")
            or (soup.select_one("meta[property='og:title']") or {}).get("content", "")
        )
    )
    summary = compact_whitespace(
        str(
            (soup.select_one("meta[name='description']") or {}).get("content", "")
            or hydrated.get("summary")
        )
    )
    content = compact_whitespace(str(hydrated.get("content") or summary or title))
    if not content:
        return None
    linked_urls = finalize_linked_urls(
        [article_url]
        + list(hydrated.get("linked_urls", []))
        + extract_urls_from_plain_text(content)
    )
    return PostParsedEvent(
        workspace_id=source.workspace_id,
        source_id=source.id,
        external_id=build_external_id(url=article_url, title=title, published_at=published_at),
        content=content,
        linked_urls=linked_urls,
        published_at=published_at,
        url=article_url,
        author=None,
        extra={
            "title": title or article_url,
            "summary": summary,
            "lang": detect_language(title, summary, content),
            "connector": "rss",
            "backfill_provider": "medium_archive_day",
        },
    )


def matches_filters(event: PostParsedEvent, filters: dict[str, Any]) -> bool:
    include_keywords = [
        str(x).lower() for x in (filters.get("include_keywords") or []) if str(x).strip()
    ]
    exclude_keywords = [
        str(x).lower() for x in (filters.get("exclude_keywords") or []) if str(x).strip()
    ]
    lang_allow = [str(x).lower() for x in (filters.get("lang_allow") or []) if str(x).strip()]
    haystack = " ".join(
        [str(event.extra.get("title") or ""), str(event.extra.get("summary") or ""), event.content]
    ).lower()
    if include_keywords and not any(keyword in haystack for keyword in include_keywords):
        return False
    if exclude_keywords and any(keyword in haystack for keyword in exclude_keywords):
        return False
    lang = str(event.extra.get("lang") or "").lower()
    if lang_allow and lang and lang not in lang_allow:
        return False
    return True


async def existing_external_ids(
    conn: asyncpg.Connection,
    *,
    source_id: str,
    external_ids: list[str],
) -> set[str]:
    if not external_ids:
        return set()
    rows = await conn.fetch(
        """
        SELECT external_id
        FROM posts
        WHERE source_id = $1
          AND external_id = ANY($2::text[])
        """,
        source_id,
        external_ids,
    )
    return {str(row["external_id"]) for row in rows}


async def emit_events(redis_url: str, events: list[PostParsedEvent], dry_run: bool) -> int:
    if dry_run or not events:
        return len(events)
    client = RedisClient(redis_url)
    await client.connect()
    try:
        pushed = 0
        for event in events:
            await client.xadd(STREAM_NAME, event.model_dump(mode="json", exclude_none=True))
            pushed += 1
        return pushed
    finally:
        await client.disconnect()


async def backfill_source(
    *,
    conn: asyncpg.Connection,
    source: SourceRecord,
    start: datetime,
    end: datetime,
    max_links_per_day: int,
    redis_url: str,
    dry_run: bool,
) -> tuple[int, int, int]:
    slug = medium_tag_slug(source)
    config = {
        "source_type": "rss",
        "url": source.url,
        "proxy_config": source.proxy_config,
        "fetch": source.extra.get("fetch") or {},
    }
    article_urls: list[str] = []
    async with build_httpx_client(source_config=config, timeout=30, follow_redirects=True) as client:
        for day in iter_dates(start, end):
            response = await http_get_with_retries(client, archive_day_url(slug, day))
            article_urls.extend(extract_archive_links(response.text, source.url, max_links_per_day))
    deduped_urls = list(dict.fromkeys(article_urls))
    events: list[PostParsedEvent] = []
    for article_url in deduped_urls:
        event = await fetch_medium_article(source=source, article_url=article_url)
        if event is None or event.published_at is None:
            continue
        if not (start <= event.published_at.astimezone(UTC) < end):
            continue
        if not matches_filters(event, dict(source.extra.get("filters") or {})):
            continue
        events.append(event)
    unique_events = list({event.external_id: event for event in events}.values())
    existing = await existing_external_ids(
        conn,
        source_id=source.id,
        external_ids=[event.external_id for event in unique_events],
    )
    new_events = [event for event in unique_events if event.external_id not in existing]
    pushed = await emit_events(redis_url, new_events, dry_run)
    return len(deduped_urls), len(unique_events), pushed


async def main_async() -> int:
    args = parse_args()
    database_url = require_url(args.database_url, "DATABASE_URL")
    redis_url = require_url(args.redis_url, "REDIS_URL")
    start = parse_ts(args.start, "start")
    end = parse_ts(args.end, "end")
    if end <= start:
        raise SystemExit("--end must be later than --start")

    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        sources = await load_sources(
            conn,
            workspace_id=args.workspace_id,
            source_ids=list(dict.fromkeys(args.source_id)),
        )
        for source in sources:
            scanned_urls, normalized, pushed = await backfill_source(
                conn=conn,
                source=source,
                start=start,
                end=end,
                max_links_per_day=args.max_links_per_day,
                redis_url=redis_url,
                dry_run=args.dry_run,
            )
            print(
                "provider=medium_archive_day "
                f"source_id={source.id} window_start={start.isoformat()} window_end={end.isoformat()} "
                f"candidate_urls={scanned_urls} normalized={normalized} pushed={pushed} dry_run={args.dry_run}"
            )
    finally:
        await conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
