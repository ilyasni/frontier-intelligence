"""Backfill stored post content from HTML-ish feed fragments into plain text."""
from __future__ import annotations

import argparse
import asyncio
import html
import os
import sys

import asyncpg

sys.path.insert(0, "/app")
sys.path.insert(0, os.getcwd())

from ingest.sources.base import compact_whitespace, html_fragment_to_text


def normalize_post_content(content: str | None) -> str:
    raw = content or ""
    text, _ = html_fragment_to_text(raw)
    if not text:
        text = compact_whitespace(html.unescape(raw))
    return compact_whitespace(html.unescape(text))


async def backfill_posts(
    *,
    database_url: str,
    workspace_id: str,
    source_like: str,
    limit: int,
    dry_run: bool,
) -> tuple[int, int]:
    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        rows = await conn.fetch(
            """
            SELECT id, content
            FROM posts
            WHERE workspace_id = $1
              AND source_id LIKE $2
              AND (
                    content ~ '<[^>]+>'
                    OR content LIKE '%&amp;%'
                    OR content LIKE '%&lt;%'
                    OR content LIKE '%&gt;%'
              )
            ORDER BY created_at ASC
            LIMIT $3
            """,
            workspace_id,
            source_like,
            limit,
        )
        changed = 0
        for row in rows:
            normalized = normalize_post_content(row["content"])
            if normalized and normalized != (row["content"] or ""):
                changed += 1
                if not dry_run:
                    await conn.execute(
                        """
                        UPDATE posts
                        SET content = $2,
                            updated_at = NOW()
                        WHERE id = $1
                        """,
                        row["id"],
                        normalized,
                    )
        return len(rows), changed
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--workspace-id", default="disruption")
    parser.add_argument("--source-like", default="rss_medium_%")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


async def _main() -> int:
    args = parse_args()
    if not args.database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2
    scanned, changed = await backfill_posts(
        database_url=args.database_url,
        workspace_id=args.workspace_id,
        source_like=args.source_like,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    print(
        f"scanned={scanned} changed={changed} dry_run={args.dry_run} "
        f"workspace={args.workspace_id} source_like={args.source_like}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
