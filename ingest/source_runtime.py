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

    async def _connect(self):
        return await asyncpg.connect(self._database_url)

    async def load_checkpoint(self, source_id: str) -> dict[str, Any]:
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                "SELECT * FROM source_checkpoints WHERE source_id = $1",
                source_id,
            )
            return dict(row) if row else {}
        finally:
            await conn.close()

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
        conn = await self._connect()
        try:
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
        finally:
            await conn.close()

    async def start_run(self, source_id: str) -> str:
        run_id = str(uuid.uuid4())
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE source_runs
                SET finished_at = NOW(),
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
                VALUES ($1, $2, NOW(), 'running', 0, 0)
                """,
                run_id,
                source_id,
            )
        finally:
            await conn.close()
        return run_id

    async def cleanup_stale_runs(self, *, max_age_minutes: int = 180) -> str:
        conn = await self._connect()
        try:
            return await conn.execute(
                """
                UPDATE source_runs
                SET finished_at = NOW(),
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
        finally:
            await conn.close()

    async def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        fetched_count: int,
        emitted_count: int,
        error_text: str = "",
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE source_runs
                SET finished_at = NOW(),
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
        finally:
            await conn.close()
