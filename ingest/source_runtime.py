"""Database-backed runtime state for source checkpoints and execution history."""
from __future__ import annotations

import datetime as dt
import json
import logging
import uuid
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


class SourceRuntimeStore:
    def __init__(self, database_url: str):
        self._database_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
        self._pool: asyncpg.Pool | None = None

    async def _get_pool(self) -> asyncpg.Pool:
        # Один пул на процесс: connect-на-каждый-вызов исчерпывал max_connections.
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._database_url,
                min_size=1,
                max_size=5,
            )
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def load_checkpoint(self, source_id: str) -> dict[str, Any]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM source_checkpoints WHERE source_id = $1",
                source_id,
            )
            return dict(row) if row else {}

    async def upsert_checkpoint(
        self,
        source_id: str,
        *,
        cursor_json: dict[str, Any] | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
        last_seen_published_at: dt.datetime | None = None,
        last_success_at: dt.datetime | None = None,
        last_error: str | None = None,
    ) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO source_checkpoints (
                    source_id, cursor_json, etag, last_modified,
                    last_seen_published_at, last_success_at, last_error, updated_at
                )
                VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT (source_id) DO UPDATE SET
                    cursor_json = COALESCE(EXCLUDED.cursor_json, source_checkpoints.cursor_json),
                    etag = COALESCE(EXCLUDED.etag, source_checkpoints.etag),
                    last_modified = COALESCE(
                        EXCLUDED.last_modified,
                        source_checkpoints.last_modified
                    ),
                    last_seen_published_at = COALESCE(
                        EXCLUDED.last_seen_published_at,
                        source_checkpoints.last_seen_published_at
                    ),
                    last_success_at = COALESCE(
                        EXCLUDED.last_success_at,
                        source_checkpoints.last_success_at
                    ),
                    last_error = EXCLUDED.last_error,
                    updated_at = NOW()
                """,
                source_id,
                json.dumps(cursor_json) if cursor_json is not None else None,
                etag,
                last_modified,
                last_seen_published_at,
                last_success_at,
                last_error,
            )

    async def start_run(self, source_id: str) -> str:
        # Времена прогона пишутся clock_timestamp(), а не NOW(). Сейчас разницы нет:
        # asyncpg-вызовы ниже идут в autocommit, каждый оператор — своя транзакция,
        # и NOW() совпал бы с моментом выполнения. Но эта правильность держится на
        # неявном свойстве вызывающего кода: стоит обернуть пару операторов в одну
        # транзакцию — и NOW() молча замрёт на её начале. Ровно так длительность
        # cluster_runs оказалась нулевой (worker/services/semantic_clustering.py:_finish_run).
        run_id = str(uuid.uuid4())
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE source_runs
                SET finished_at = clock_timestamp(),
                    status = 'error',
                    error_text = COALESCE(
                        NULLIF(error_text, ''),
                        'Superseded by a newer source run'
                    )
                WHERE source_id = $1
                  AND status = 'running'
                """,
                source_id,
            )
            await conn.execute(
                """
                INSERT INTO source_runs (
                    id, source_id, started_at, status, fetched_count, emitted_count
                )
                VALUES ($1, $2, clock_timestamp(), 'running', 0, 0)
                """,
                run_id,
                source_id,
            )
        return run_id

    async def cleanup_stale_runs(self, *, max_age_minutes: int = 180) -> str:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            return await conn.execute(
                """
                UPDATE source_runs
                SET finished_at = clock_timestamp(),
                    status = 'error',
                    error_text = COALESCE(
                        NULLIF(error_text, ''),
                        'Marked stale after exceeding runtime threshold'
                    )
                WHERE status = 'running'
                  AND started_at < NOW() - make_interval(mins => $1::int)
                """,
                max_age_minutes,
            )

    async def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        fetched_count: int,
        emitted_count: int,
        error_text: str = "",
    ) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE source_runs
                SET finished_at = clock_timestamp(),
                    status = $2,
                    fetched_count = $3,
                    emitted_count = $4,
                    error_text = NULLIF($5, '')
                WHERE id = $1
                """,
                run_id,
                status,
                fetched_count,
                emitted_count,
                error_text[:4000],
            )
