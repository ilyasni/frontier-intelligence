"""Unit: shared.db_stale_retry — retry после OperationalError и dispose пула."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine

from shared.db_stale_retry import is_pool_stale_error, run_twice_on_stale_pool


@pytest.mark.unit
def test_is_pool_stale_error_operational() -> None:
    exc = OperationalError("statement", {}, Exception("connection closed"))
    assert is_pool_stale_error(exc) is True


@pytest.mark.unit
def test_is_pool_stale_error_value() -> None:
    assert is_pool_stale_error(ValueError("x")) is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_twice_on_stale_pool_retries() -> None:
    engine = MagicMock(spec=AsyncEngine)
    engine.dispose = AsyncMock()

    call_count = {"n": 0}

    async def op() -> None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise OperationalError("statement", {}, Exception("connection closed"))

    retries: list[str] = []

    await run_twice_on_stale_pool(
        engine,
        op,
        sleep_s=0.01,
        on_retry=lambda **kw: retries.append(kw.get("error_type", "")),
        post_id="abc",
    )

    assert call_count["n"] == 2
    engine.dispose.assert_awaited_once()
    assert retries == ["OperationalError"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_twice_on_stale_pool_no_retry_on_value_error() -> None:
    engine = MagicMock(spec=AsyncEngine)
    engine.dispose = AsyncMock()

    async def op() -> None:
        raise ValueError("not db")

    with pytest.raises(ValueError, match="not db"):
        await run_twice_on_stale_pool(engine, op, sleep_s=0.01)

    engine.dispose.assert_not_awaited()
