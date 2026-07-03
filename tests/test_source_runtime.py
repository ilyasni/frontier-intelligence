from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.source_runtime import SourceRuntimeStore


def _pool_for(conn) -> MagicMock:
    """Build a mock asyncpg pool whose acquire() yields the given connection."""

    @asynccontextmanager
    async def _acquire():
        yield conn

    pool = MagicMock()
    pool.acquire = MagicMock(side_effect=_acquire)
    pool.close = AsyncMock()
    return pool


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_run_closes_prior_running_rows_for_same_source():
    conn = AsyncMock()
    conn.execute = AsyncMock()
    pool = _pool_for(conn)

    with patch("ingest.source_runtime.asyncpg.create_pool", new=AsyncMock(return_value=pool)):
        store = SourceRuntimeStore("postgresql+asyncpg://user:pass@db/app")
        run_id = await store.start_run("rss_medium_future")

    assert run_id
    assert conn.execute.await_count == 2
    first_sql, first_source_id = conn.execute.await_args_list[0].args
    second_sql, second_run_id, second_source_id = conn.execute.await_args_list[1].args
    assert "UPDATE source_runs" in first_sql
    assert first_source_id == "rss_medium_future"
    assert "INSERT INTO source_runs" in second_sql
    assert second_run_id == run_id
    assert second_source_id == "rss_medium_future"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cleanup_stale_runs_marks_old_running_rows_as_error():
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value="UPDATE 4")
    pool = _pool_for(conn)

    with patch("ingest.source_runtime.asyncpg.create_pool", new=AsyncMock(return_value=pool)):
        store = SourceRuntimeStore("postgresql+asyncpg://user:pass@db/app")
        result = await store.cleanup_stale_runs(max_age_minutes=90)

    sql, age_minutes = conn.execute.await_args.args
    assert "UPDATE source_runs" in sql
    assert "status = 'error'" in sql
    assert age_minutes == 90
    assert result == "UPDATE 4"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pool_created_once_and_closed():
    conn = AsyncMock()
    conn.execute = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    pool = _pool_for(conn)
    create_pool = AsyncMock(return_value=pool)

    with patch("ingest.source_runtime.asyncpg.create_pool", new=create_pool):
        store = SourceRuntimeStore("postgresql+asyncpg://user:pass@db/app")
        await store.load_checkpoint("s1")
        await store.start_run("s1")
        await store.close()

    # A single pool is shared across calls, not one connection per call.
    assert create_pool.await_count == 1
    pool.close.assert_awaited_once()
