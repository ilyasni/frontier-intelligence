from __future__ import annotations

import argparse
import asyncio
import os

import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from shared.reindex import STREAM_POSTS_REINDEX, build_post_reindex_event
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS


async def enqueue_reindex_events(
    *,
    database_url: str,
    redis_url: str,
    kinds: list[str],
    workspace: str | None,
    limit: int,
    dry_run: bool,
) -> dict[str, int]:
    engine = create_async_engine(database_url, **ASYNC_ENGINE_POOL_KWARGS)
    redis = aioredis.from_url(redis_url, decode_responses=True)
    try:
        params = {"kinds": kinds, "workspace": workspace, "limit": limit}
        async with AsyncSession(engine) as session:
            result = await session.execute(
                text(
                    """
                    SELECT DISTINCT p.id AS post_id, p.workspace_id
                    FROM posts p
                    JOIN indexing_status i ON i.post_id = p.id
                    JOIN post_enrichments pe ON pe.post_id = p.id
                    WHERE i.embedding_status = 'done'
                      AND pe.kind = ANY(:kinds)
                      AND (
                        CAST(:workspace AS text) IS NULL
                        OR p.workspace_id = CAST(:workspace AS text)
                      )
                    ORDER BY p.id
                    LIMIT :limit
                    """
                ),
                params,
            )
            rows = [dict(row) for row in result.mappings().all()]

        queued = 0
        for row in rows:
            if dry_run:
                continue
            await redis.xadd(
                STREAM_POSTS_REINDEX,
                build_post_reindex_event(
                    post_id=row["post_id"],
                    workspace_id=row["workspace_id"],
                    reason="backfill:" + ",".join(kinds),
                    source="enqueue_reindex_enriched_posts",
                ),
                maxlen=100_000,
                approximate=True,
            )
            queued += 1
        return {"matched": len(rows), "queued": queued}
    finally:
        await redis.aclose()
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enqueue reindex events for posts with crawl/vision enrichments."
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://redis:6379"))
    parser.add_argument("--kind", action="append", choices=["crawl", "vision"], dest="kinds")
    parser.add_argument("--workspace", default=None)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")
    kinds = args.kinds or ["crawl", "vision"]
    result = asyncio.run(
        enqueue_reindex_events(
            database_url=args.database_url,
            redis_url=args.redis_url,
            kinds=kinds,
            workspace=args.workspace,
            limit=max(1, args.limit),
            dry_run=args.dry_run,
        )
    )
    print(result)


if __name__ == "__main__":
    main()
