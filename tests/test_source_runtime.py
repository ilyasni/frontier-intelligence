from unittest.mock import AsyncMock, patch

import pytest

from ingest.source_runtime import SourceRuntimeStore


@pytest.mark.unit
@pytest.mark.asyncio
async def test_start_run_closes_prior_running_rows_for_same_source():
    conn = AsyncMock()
    conn.execute = AsyncMock()
    conn.close = AsyncMock()

    with patch("ingest.source_runtime.asyncpg.connect", new=AsyncMock(return_value=conn)):
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
    conn.close = AsyncMock()

    with patch("ingest.source_runtime.asyncpg.connect", new=AsyncMock(return_value=conn)):
        store = SourceRuntimeStore("postgresql+asyncpg://user:pass@db/app")
        result = await store.cleanup_stale_runs(max_age_minutes=90)

    sql, age_minutes = conn.execute.await_args.args
    assert "UPDATE source_runs" in sql
    assert "status = 'error'" in sql
    assert age_minutes == 90
    assert result == "UPDATE 4"
