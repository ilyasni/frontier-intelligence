"""
Повтор async-операции с БД после dispose пула при «мёртвом» соединении
(idle timeout, рестарт Postgres). Используется crawl4ai и др. сервисами.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine


def is_pool_stale_error(exc: BaseException) -> bool:
    """Признак того, что соединение из пула больше не пригодно."""
    if isinstance(exc, (InterfaceError, OperationalError)):
        return True
    if isinstance(exc, DBAPIError):
        msg = str(exc).lower()
        if "connection" in msg and "closed" in msg:
            return True
    return False


async def run_twice_on_stale_pool(
    engine: AsyncEngine,
    op: Callable[[], Awaitable[None]],
    *,
    sleep_s: float = 1.5,
    on_retry: Callable[..., Any] | None = None,
    **retry_context: Any,
) -> None:
    """
    Выполняет op(); при is_pool_stale_error — await engine.dispose(), пауза, второй вызов op().
    """
    for attempt in range(2):
        try:
            await op()
            return
        except Exception as exc:
            if attempt == 0 and is_pool_stale_error(exc):
                if on_retry is not None:
                    on_retry(error_type=type(exc).__name__, **retry_context)
                await engine.dispose()
                await asyncio.sleep(sleep_s)
                continue
            raise
