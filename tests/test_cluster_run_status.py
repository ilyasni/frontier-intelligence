"""Регрессия на бесшумный отказ аналитики (инцидент 2026-07-31..08-02).

Обработчик ошибки писал UPDATE cluster_runs в уже аборченной транзакции, падал
сам на InFailedSQLTransactionError, подменял исходное исключение и оставлял
прогон навсегда в status='running'. За 54 часа: 93 осиротевшие строки, ни одной
'error', APScheduler рапортует "executed successfully".
"""

from __future__ import annotations

import asyncio

import pytest

from worker.services import semantic_clustering as sc

pytestmark = pytest.mark.unit


class _AbortedSession:
    """Сессия с аборченной транзакцией: execute падает, пока не сделан rollback."""

    def __init__(self, *, rollback_raises: bool = False) -> None:
        self.aborted = True
        self.rollback_raises = rollback_raises
        self.executed: list[tuple[str, dict]] = []
        self.commits = 0
        self.rollbacks = 0

    async def execute(self, stmt, params=None):
        if self.aborted:
            raise RuntimeError("current transaction is aborted, commands ignored until rollback")
        self.executed.append((str(stmt), params or {}))

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1
        if self.rollback_raises:
            raise RuntimeError("rollback failed too")
        self.aborted = False


def test_mark_run_failed_rolls_back_before_writing_status() -> None:
    """Без rollback запись статуса в аборченной транзакции невозможна в принципе."""
    session = _AbortedSession()

    asyncio.run(
        sc._mark_run_failed(session, "cluster-run:test", "disruption", ValueError("boom"))
    )

    assert session.rollbacks == 1, "rollback обязан предшествовать записи статуса"
    assert session.commits == 1
    assert len(session.executed) == 1

    sql, params = session.executed[0]
    assert "UPDATE cluster_runs" in sql
    assert params["status"] == "error"
    assert params["id"] == "cluster-run:test"
    # Тип и текст первопричины должны попасть в summary — иначе причину придётся
    # искать в логах вручную.
    assert "ValueError" in params["summary"]
    assert "boom" in params["summary"]


def test_mark_run_failed_never_masks_original_exception() -> None:
    """Сбой самой recovery-записи не должен подменять первопричину.

    Вызывающий код делает `raise` исходного исключения сразу после этого хелпера,
    поэтому хелпер обязан проглотить собственную ошибку.
    """
    session = _AbortedSession(rollback_raises=True)

    # Не должно поднять исключение наружу.
    asyncio.run(
        sc._mark_run_failed(session, "cluster-run:test", "disruption", ValueError("original"))
    )

    assert session.rollbacks == 1
    assert session.executed == []
    assert session.commits == 0
