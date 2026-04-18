"""
Пакетный reprocess постов с embedding_status='done' — пересоздаёт индекс в Qdrant
со sparse-вектором после включения fastembed (hybrid).

Требует DATABASE_URL (как у остальных скриптов) и доступный Admin API.

Примеры (из корня репо): задать ADMIN_API_BASE, затем запуск с --dry-run или --limit.
  Локально: http://127.0.0.1:8101. В Docker-сети: http://admin:8101.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from shared.config import get_settings
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS

log = structlog.get_logger()


async def fetch_post_ids(
    engine,
    *,
    limit: int,
    workspace_id: str | None,
) -> list[str]:
    async with AsyncSession(engine) as session:
        if workspace_id:
            q = text("""
                SELECT i.post_id
                FROM indexing_status i
                JOIN posts p ON p.id = i.post_id
                WHERE i.embedding_status = 'done' AND p.workspace_id = :ws
                ORDER BY i.updated_at DESC
                LIMIT :lim
            """)
            params = {"ws": workspace_id, "lim": limit}
        else:
            q = text("""
                SELECT post_id
                FROM indexing_status
                WHERE embedding_status = 'done'
                ORDER BY updated_at DESC
                LIMIT :lim
            """)
            params = {"lim": limit}
        result = await session.execute(q, params)
        return [str(r[0]) for r in result.fetchall()]


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reprocess posts with embedding_status=done (hybrid sparse).",
    )
    parser.add_argument("--limit", type=int, default=100, help="Максимум постов за запуск")
    parser.add_argument("--workspace-id", type=str, default=None, help="Фильтр по workspace_id")
    parser.add_argument("--dry-run", action="store_true", help="Только список post_id, без HTTP")
    parser.add_argument(
        "--admin-base",
        type=str,
        default=os.environ.get("ADMIN_API_BASE", "http://127.0.0.1:8101"),
        help="База Admin API (env ADMIN_API_BASE)",
    )
    parser.add_argument("--delay-ms", type=int, default=200, help="Пауза между POST (мс)")
    args = parser.parse_args()

    settings = get_settings()
    engine = create_async_engine(settings.database_url, pool_size=2, **ASYNC_ENGINE_POOL_KWARGS)
    try:
        ids = await fetch_post_ids(engine, limit=args.limit, workspace_id=args.workspace_id)
    finally:
        await engine.dispose()

    log.info("reprocess_done_for_sparse candidates", count=len(ids), dry_run=args.dry_run)
    if not ids:
        return

    if args.dry_run:
        for pid in ids:
            print(pid)
        return

    base = args.admin_base.rstrip("/")
    timeout = httpx.Timeout(120.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for i, pid in enumerate(ids):
            url = f"{base}/api/pipeline/reprocess/{pid}"
            try:
                r = await client.post(url)
                r.raise_for_status()
                log.info("reprocess ok", post_id=pid[:16], index=i + 1, total=len(ids))
            except httpx.HTTPError as exc:
                log.error("reprocess failed", post_id=pid[:16], error=str(exc))
            if args.delay_ms > 0 and i < len(ids) - 1:
                await asyncio.sleep(args.delay_ms / 1000.0)


if __name__ == "__main__":
    asyncio.run(main())
