"""Shared database helpers for admin routers."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from shared.config import get_settings
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS

_engine: AsyncEngine | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is not None:
        return _engine
    settings = get_settings()
    _engine = create_async_engine(
        settings.database_url,
        pool_size=3,
        max_overflow=2,
        pool_timeout=10,
        **ASYNC_ENGINE_POOL_KWARGS,
    )
    return _engine
