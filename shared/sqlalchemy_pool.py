"""Общие параметры пула для async SQLAlchemy (PostgreSQL + asyncpg).

pool_pre_ping — не выдавать из пула уже закрытые сервером соединения.
pool_recycle — обновлять соединения до типичного server-side idle timeout.
"""

from __future__ import annotations

# Секунды; для managed Postgres часто idle ~10m–1h — 3600 — безопасный компромисс
ASYNC_POOL_RECYCLE_SECONDS: int = 3600

ASYNC_ENGINE_POOL_KWARGS: dict[str, object] = {
    "pool_pre_ping": True,
    "pool_recycle": ASYNC_POOL_RECYCLE_SECONDS,
}
