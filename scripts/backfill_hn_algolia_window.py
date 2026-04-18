"""Backfill a Hacker News time window into stream:posts:parsed via Algolia history API.

This is an approximation tool for historical recovery. Algolia exposes HN story
history by time window, but does not preserve membership in the live
topstories/newstories/beststories lists. By default we target the broadest live
source, `api_hn_newstories`, though another HN source ID can be supplied.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import asyncpg
import httpx

sys.path.insert(0, "/app")
sys.path.insert(0, os.getcwd())

from ingest.sources.base import compact_whitespace, detect_language, parse_datetime
from shared.events.posts_parsed_v1 import PostParsedEvent
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls
from shared.redis_client import RedisClient

ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"
STREAM_NAME = "stream:posts:parsed"


@dataclass(slots=True)
class SourceRecord:
    id: str
    workspace_id: str
    extra: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--workspace-id", default="disruption")
    parser.add_argument("--source-id", default="api_hn_newstories")
    parser.add_argument("--start", required=True, help="UTC ISO timestamp, e.g. 2026-04-08T04:10:00+00:00")
    parser.add_argument("--end", required=True, help="UTC ISO timestamp, e.g. 2026-04-09T18:40:00+00:00")
    parser.add_argument("--hits-per-page", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _require_url(value: str, name: str) -> str:
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def _parse_ts(value: str, name: str) -> datetime:
    parsed = parse_datetime(value)
    if parsed is None:
        raise SystemExit(f"Invalid {name}: {value}")
    return parsed.astimezone(UTC)


def _coerce_json_dict(value: Any) -> dict[str, Any]:
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


async def load_source(conn: asyncpg.Connection, source_id: str, workspace_id: str) -> SourceRecord:
    row = await conn.fetchrow(
        """
        SELECT id, workspace_id, extra
        FROM sources
        WHERE id = $1 AND workspace_id = $2
        """,
        source_id,
        workspace_id,
    )
    if row is None:
        raise SystemExit(f"Source not found: {workspace_id}/{source_id}")
    return SourceRecord(
        id=str(row["id"]),
        workspace_id=str(row["workspace_id"]),
        extra=_coerce_json_dict(row["extra"]),
    )


def matches_filters(item: PostParsedEvent, filters: dict[str, Any]) -> bool:
    include_keywords = [
        str(x).lower() for x in (filters.get("include_keywords") or []) if str(x).strip()
    ]
    exclude_keywords = [
        str(x).lower() for x in (filters.get("exclude_keywords") or []) if str(x).strip()
    ]
    lang_allow = [str(x).lower() for x in (filters.get("lang_allow") or []) if str(x).strip()]
    haystack = " ".join(
        [
            item.extra.get("title", ""),
            item.extra.get("summary", ""),
            item.content,
        ]
    ).lower()
    if include_keywords and not any(keyword in haystack for keyword in include_keywords):
        return False
    if exclude_keywords and any(keyword in haystack for keyword in exclude_keywords):
        return False
    lang = str(item.extra.get("lang") or "").lower()
    if lang_allow and lang and lang not in lang_allow:
        return False
    return True


async def fetch_hn_hits(
    *,
    start: datetime,
    end: datetime,
    hits_per_page: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    hits: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for page in range(max_pages):
            response = await client.get(
                ALGOLIA_URL,
                params={
                    "tags": "story",
                    "numericFilters": f"created_at_i>{start_ts},created_at_i<{end_ts}",
                    "hitsPerPage": str(hits_per_page),
                    "page": str(page),
                },
            )
            response.raise_for_status()
            payload = response.json()
            page_hits = payload.get("hits") or []
            if not page_hits:
                break
            for hit in page_hits:
                if isinstance(hit, dict):
                    hits.append(hit)
            if page + 1 >= int(payload.get("nbPages") or 0):
                break
    return hits


def normalize_hit(hit: dict[str, Any], source: SourceRecord) -> PostParsedEvent | None:
    object_id = str(hit.get("story_id") or hit.get("objectID") or "").strip()
    if not object_id.isdigit():
        return None
    title = compact_whitespace(str(hit.get("title") or hit.get("story_title") or ""))
    summary = compact_whitespace(str(hit.get("story_text") or hit.get("comment_text") or ""))
    url = compact_whitespace(
        str(hit.get("url") or f"https://news.ycombinator.com/item?id={object_id}")
    )
    published_at = parse_datetime(hit.get("created_at") or hit.get("created_at_i"))
    content = summary or title
    if not content:
        return None
    linked_urls = finalize_linked_urls([url] + extract_urls_from_plain_text(content))
    event = PostParsedEvent(
        workspace_id=source.workspace_id,
        source_id=source.id,
        external_id=object_id,
        content=content,
        linked_urls=linked_urls,
        published_at=published_at,
        url=url,
        author=compact_whitespace(str(hit.get("author") or "")) or None,
        extra={
            "title": title or f"Hacker News story {object_id}",
            "summary": summary,
            "lang": detect_language(title, summary, content),
            "raw_payload": hit,
            "connector": "api",
            "backfill_provider": "algolia_hn_history",
        },
    )
    if not matches_filters(event, dict(source.extra.get("filters") or {})):
        return None
    return event


async def existing_external_ids(
    conn: asyncpg.Connection,
    *,
    source_id: str,
    external_ids: Iterable[str],
) -> set[str]:
    ids = [item for item in external_ids if item]
    if not ids:
        return set()
    rows = await conn.fetch(
        """
        SELECT external_id
        FROM posts
        WHERE source_id = $1
          AND external_id = ANY($2::text[])
        """,
        source_id,
        ids,
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


async def main_async() -> int:
    args = parse_args()
    database_url = _require_url(args.database_url, "DATABASE_URL")
    redis_url = _require_url(args.redis_url, "REDIS_URL")
    start = _parse_ts(args.start, "start")
    end = _parse_ts(args.end, "end")
    if end <= start:
        raise SystemExit("--end must be later than --start")

    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        source = await load_source(conn, args.source_id, args.workspace_id)
        raw_hits = await fetch_hn_hits(
            start=start,
            end=end,
            hits_per_page=args.hits_per_page,
            max_pages=args.max_pages,
        )
        events = [event for hit in raw_hits if (event := normalize_hit(hit, source)) is not None]
        seen: dict[str, PostParsedEvent] = {}
        for event in events:
            seen[event.external_id] = event
        deduped = list(seen.values())
        existing = await existing_external_ids(
            conn,
            source_id=source.id,
            external_ids=[event.external_id for event in deduped],
        )
        new_events = [event for event in deduped if event.external_id not in existing]
        pushed = await emit_events(redis_url, new_events, args.dry_run)
    finally:
        await conn.close()

    print(
        "provider=algolia_hn_history "
        f"source_id={args.source_id} window_start={start.isoformat()} window_end={end.isoformat()} "
        f"hits={len(raw_hits)} normalized={len(events)} unique={len(deduped)} "
        f"new={len(new_events)} pushed={pushed} dry_run={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
