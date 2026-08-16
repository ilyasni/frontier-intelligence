"""Регрессия: длительность прогона обязана быть длительностью, а не нулём.

16.08.2026: прогон signal-analysis у `disruption` шёл ~13 минут, а cluster_runs
показывал `finished_at - started_at = 00:00:00.009907`. То же у пяти последних
успешных прогонов: 0.0099 / 0.0134 / 0.0117 / 0.0090 / 0.0065 с.

Причина — NOW(). В PostgreSQL это синоним transaction_timestamp(): момент НАЧАЛА
транзакции, неизменный до её конца. Прогон делает всю работу внутри одной длинной
транзакции, поэтому оба конца замера попадали в её начало.

Тест проверяет свойство, а не текст SQL. Сессия-заглушка ниже воспроизводит ровно
одну особенность настоящего PostgreSQL — NOW() замирает на время транзакции,
clock_timestamp() идёт вместе с часами, — и прогон, внутри которого была пауза,
обязан дать длительность не меньше этой паузы. На NOW() оба основных теста дают
ноль, то есть падают.
"""

from __future__ import annotations

import asyncio
import re

import pytest

from worker.services import semantic_clustering as sc

pytestmark = pytest.mark.unit


def _split_top_level(chunk: str) -> list[str]:
    """Разбить список по запятым нулевого уровня вложенности.

    Нужно из-за CAST(:summary AS jsonb) и подобных: наивный split(',') разорвал бы
    их пополам.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in chunk:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _assignments(sql: str) -> tuple[str, dict[str, str]]:
    """Вернуть (таблица, {колонка: выражение}) для INSERT или UPDATE."""
    insert = re.search(
        r"INSERT\s+INTO\s+(?P<table>\w+)\s*\((?P<cols>.*?)\)\s*VALUES\s*\((?P<vals>.*)\)\s*$",
        sql.strip(),
        re.S | re.I,
    )
    if insert:
        cols = _split_top_level(insert.group("cols"))
        vals = _split_top_level(insert.group("vals"))
        assert len(cols) == len(vals), f"колонок {len(cols)}, значений {len(vals)}"
        return insert.group("table"), dict(zip(cols, vals, strict=True))

    update = re.search(
        r"UPDATE\s+(?P<table>\w+)\s+SET\s+(?P<sets>.*?)\s+WHERE\s",
        sql.strip(),
        re.S | re.I,
    )
    if update:
        pairs: dict[str, str] = {}
        for item in _split_top_level(update.group("sets")):
            column, _, expression = item.partition("=")
            pairs[column.strip()] = expression.strip()
        return update.group("table"), pairs

    return "", {}


class _Clock:
    """Часы, которые двигает тест, а не планировщик ОС: замер обязан быть детерминированным."""

    def __init__(self, start: float = 1_755_000_000.0) -> None:
        self.now = start

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _PgSession:
    """Сессия, воспроизводящая семантику времени в PostgreSQL.

    NOW() = transaction_timestamp() — момент, когда открылась транзакция; она
    открывается первым же оператором после commit/rollback и держится до
    следующего commit. clock_timestamp() — показания часов на момент оператора.
    """

    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.tx_start: float | None = None
        self.rows: dict[str, dict[str, object]] = {}

    def _resolve(self, expression: str, params: dict) -> object:
        token = expression.strip().lower()
        if token in {"now()", "transaction_timestamp()"}:
            assert self.tx_start is not None
            return self.tx_start
        if token == "clock_timestamp()":
            return self.clock.now
        if expression.strip().startswith(":"):
            return params.get(expression.strip()[1:])
        return expression.strip()

    async def execute(self, stmt, params=None):
        params = params or {}
        if self.tx_start is None:
            self.tx_start = self.clock.now
        table, pairs = _assignments(str(stmt))
        if table != "cluster_runs":
            return None
        values = {col: self._resolve(expr, params) for col, expr in pairs.items()}
        row_id = values.get("id") or params.get("id")
        assert isinstance(row_id, str)
        self.rows.setdefault(row_id, {}).update(values)
        return None

    async def commit(self) -> None:
        self.tx_start = None

    async def rollback(self) -> None:
        self.tx_start = None

    def duration(self, run_id: str) -> float:
        row = self.rows[run_id]
        return float(row["finished_at"]) - float(row["started_at"])  # type: ignore[arg-type]


def test_signal_analysis_duration_survives_a_long_transaction() -> None:
    """Форма stage='signal-analysis': вся работа в одной транзакции после commit'а."""
    clock = _Clock()
    session = _PgSession(clock)

    async def scenario() -> str:
        # Первая транзакция открывается на чтении настроек воркспейса.
        await session.execute("SELECT 1 FROM workspaces WHERE id = :id", {"id": "disruption"})
        run_id = await sc._create_run(session, "disruption", {}, stage="signal-analysis")
        await session.commit()

        # Вторая транзакция открывается первым запросом рабочей фазы
        # (_load_semantic_state) и держится до самого конца прогона.
        await session.execute("SELECT 1 FROM semantic_clusters", {})
        clock.advance(13 * 60)  # ~13 минут реальной работы, замеренных 16.08.2026
        await sc._finish_run(session, run_id, "success", {"semantic_clusters_loaded": 7}, {})
        await session.commit()
        return run_id

    run_id = asyncio.run(scenario())

    assert session.rows[run_id]["status"] == "success"
    assert session.duration(run_id) >= 13 * 60, (
        "длительность схлопнулась в ноль: finished_at взят на начало транзакции, "
        "а не на момент записи"
    )


def test_full_stage_measures_signal_phase_too() -> None:
    """Форма stage='full': промежуточный commit давал правдоподобную, но неполную цифру.

    Здесь опаснее всего: на NOW() замер показывал только фазу семантической
    кластеризации (00:09:28 у disruption 16.08.2026) — ровно без фазы
    signal-analysis, той самой, что падала по OOM. Цифра не выглядела нелепой,
    поэтому и не вызывала подозрений.
    """
    clock = _Clock()
    session = _PgSession(clock)
    semantic_phase = 9 * 60 + 28
    signal_phase = 4 * 60

    async def scenario() -> str:
        await session.execute("SELECT 1 FROM workspaces", {})
        run_id = await sc._create_run(session, "disruption", {}, stage="full")
        await session.commit()

        await session.execute("SELECT 1 FROM posts", {})
        clock.advance(semantic_phase)
        await session.commit()  # commit после _replace_signal_series

        await session.execute("SELECT 1 FROM semantic_clusters", {})
        clock.advance(signal_phase)
        await sc._finish_run(session, run_id, "success", {}, {})
        await session.commit()
        return run_id

    run_id = asyncio.run(scenario())

    assert (
        session.duration(run_id) >= semantic_phase + signal_phase
    ), "в замер попала только фаза до промежуточного commit'а"


def test_failed_run_duration_covers_work_before_the_failure() -> None:
    """Строка с ошибкой тоже обязана нести настоящую длительность.

    Этот случай NOW() не ломал (rollback в _mark_run_failed закрывает транзакцию,
    и следующий оператор открывает новую), но инвариант держится на порядке
    вызовов внутри хелпера. Тест фиксирует результат, а не порядок.
    """
    clock = _Clock()
    session = _PgSession(clock)

    async def scenario() -> str:
        await session.execute("SELECT 1 FROM workspaces", {})
        run_id = await sc._create_run(session, "disruption", {}, stage="signal-analysis")
        await session.commit()

        await session.execute("SELECT 1 FROM semantic_clusters", {})
        clock.advance(8 * 60)  # работа до падения
        await sc._mark_run_failed(session, run_id, "disruption", MemoryError("OOM"))
        return run_id

    run_id = asyncio.run(scenario())

    assert session.rows[run_id]["status"] == "error"
    assert session.duration(run_id) >= 8 * 60


def test_stub_reproduces_postgres_now_semantics() -> None:
    """Проверка самой заглушки: без этого тесты выше ничего не доказывают.

    Если бы NOW() в заглушке шёл вместе с часами, они проходили бы на любом коде.
    """
    clock = _Clock()
    session = _PgSession(clock)

    async def scenario() -> None:
        await session.execute(
            "INSERT INTO cluster_runs (id, started_at, finished_at) "
            "VALUES (:id, NOW(), clock_timestamp())",
            {"id": "probe"},
        )
        clock.advance(100.0)
        await session.execute(
            "UPDATE cluster_runs SET started_at = NOW(), finished_at = clock_timestamp() "
            "WHERE id = :id",
            {"id": "probe"},
        )

    asyncio.run(scenario())

    row = session.rows["probe"]
    assert row["started_at"] == 1_755_000_000.0, "NOW() обязан замереть на начале транзакции"
    assert row["finished_at"] == 1_755_000_100.0, "clock_timestamp() обязан идти с часами"
