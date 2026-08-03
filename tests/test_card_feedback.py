"""Тесты редакторского контура (mcp/tools/editorial.py): запись разметки и экспорт карточек.

Проверяем инварианты, ради которых таблица card_feedback вообще заведена: ровно одна
выбранная карточка на батч, повтор батча без дублей, радиус поражения в одну таблицу,
цифра на карточке прослеживается до колонки БД, и разметка читается после того, как
строка-источник исчезла. БД мокается — реальный Postgres не нужен.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from mcp import guards
from mcp.tools import editorial as ed
from mcp.tools.editorial import (
    CardInput,
    ExportInboxCardsRequest,
    ListCardFeedbackRequest,
    RecordCardFeedbackRequest,
)

pytestmark = pytest.mark.unit


def _declare_workspaces(monkeypatch, *slugs: str) -> None:
    """Объявить, какие воркспейсы «заведены», для assert_known_workspace.

    Без этого тест меряет не код, а содержимое файла: guards читает
    config/workspaces.yml и берёт ПЕРВЫЙ найденный путь — в образе это запечённая
    копия /app/config/workspaces.yml, и репозиторный кандидат до неё не доходит,
    как и _FALLBACK_WORKSPACE_SLUGS. Копия в admin-образе от 26.06 знает пять
    воркспейсов и не знает auto_hmi, поэтому тесты про auto_hmi падали на «unknown
    workspace» — на состоянии, которого они не контролируют. Проверяемое поведение
    не ослабляется: guards.assert_known_workspace вызывается настоящий, подменён
    только источник списка.
    """
    monkeypatch.setattr(guards, "get_workspace_allowlist", lambda: frozenset(slugs))


class _Result:
    """Мок результата session.execute: поддерживает mappings().all(), first() и rowcount."""

    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


# Пред-проверка существования воркспейса ходит в workspaces отдельным SELECT — узнаём её
# по тексту запроса, чтобы она не съедала результаты, заготовленные под вставки.
_WS_PROBE = "FROM workspaces"
_CARD_INSERT = "INSERT INTO card_feedback"


class _Session:
    """Мок AsyncSession как async-context-manager. Отдаёт результаты по порядку execute.

    workspace_exists управляет ответом pre-flight SELECT по workspaces: False — воркспейс
    есть в config/workspaces.yml, но в БД не забутстраплен (живой случай auto_hmi).
    """

    def __init__(self, results, workspace_exists: bool = True):
        self._results = list(results)
        self._workspace_exists = workspace_exists
        self.executed: list[tuple[str, dict]] = []
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, statement, params=None):
        sql = str(statement)
        self.executed.append((sql, dict(params or {})))
        if _WS_PROBE in sql:
            return _Result(rows=[{"?column?": 1}] if self._workspace_exists else [])
        return self._results.pop(0) if self._results else _Result()

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True


class _ConflictingSession(_Session):
    """БД поднимает IntegrityError на N-й вставке; по умолчанию — на своём индексе chosen."""

    def __init__(self, fail_at: int = 1, constraint: str = "uq_card_feedback_batch_chosen"):
        super().__init__([])
        self._fail_at = fail_at
        self._constraint = constraint
        self.rolled_back = False

    async def execute(self, statement, params=None):
        result = await super().execute(statement, params)
        if _CARD_INSERT not in str(statement):  # pre-flight по workspaces не ломаем
            return result
        if len(_inserts(self)) >= self._fail_at:
            raise IntegrityError(
                _CARD_INSERT,
                {},
                Exception(f'violates constraint "{self._constraint}"'),
            )
        return _Result(rowcount=1)


def _inserts(session) -> list[tuple[str, dict]]:
    """Только вставки разметки: pre-flight SELECT по workspaces сюда не попадает."""
    return [(sql, params) for sql, params in session.executed if _CARD_INSERT in sql]


def _rows_by_entity(session) -> dict[str, dict]:
    """Параметры вставок, разложенные по entity_id — так читаемее, чем индексы."""
    return {params["entity_id"]: params for _sql, params in _inserts(session)}


def _patch_session(monkeypatch, session):
    monkeypatch.setattr(ed, "get_engine", lambda: object())
    monkeypatch.setattr(ed, "AsyncSession", lambda _engine: session)
    return session


def _cards(count=3):
    return [
        {
            "entity_kind": "emerging",
            "entity_id": f"sig-{index}",
            "title": f"Сигнал {index}",
            "metric_name": "signal_score",
            "metric_value": 0.5 + index / 100,
            "relevance": None,
            "own_stake": None,
            "stake_quadrant": None,
            "question": "",
        }
        for index in range(count)
    ]


def _request(**overrides):
    payload = {
        "workspace": "disruption",
        "batch_id": "cards-20260803-abcd1234",
        "cards": _cards(),
        "chosen_entity_id": "sig-1",
        "reason": "остальные — пересказ чужого релиза",
    }
    payload.update(overrides)
    return RecordCardFeedbackRequest(**payload)


# ── record_card_feedback: валидация входа ────────────────────────────────────


async def test_record_rejects_chosen_not_in_cards(monkeypatch):
    s = _patch_session(monkeypatch, _Session([]))
    with pytest.raises(HTTPException) as ei:
        await ed.record_card_feedback(_request(chosen_entity_id="sig-999"))
    assert ei.value.status_code == 400
    assert "exactly one card" in ei.value.detail
    assert s.executed == []  # ни одной записи в БД


async def test_record_rejects_unknown_workspace(monkeypatch):
    s = _patch_session(monkeypatch, _Session([]))
    with pytest.raises(HTTPException) as ei:
        await ed.record_card_feedback(_request(workspace="bogus_ws"))
    assert ei.value.status_code == 400
    assert ei.value.detail == "unknown workspace"
    assert s.executed == []


async def test_record_rejects_unknown_entity_kind(monkeypatch):
    s = _patch_session(monkeypatch, _Session([]))
    cards = _cards()
    cards[0]["entity_kind"] = "galaxy"
    with pytest.raises(HTTPException) as ei:
        await ed.record_card_feedback(_request(cards=cards))
    assert ei.value.status_code == 400
    assert "entity_kind" in ei.value.detail
    assert s.executed == []


async def test_record_rejects_duplicate_card_in_batch(monkeypatch):
    s = _patch_session(monkeypatch, _Session([]))
    cards = _cards()
    cards[2]["entity_id"] = cards[0]["entity_id"]
    with pytest.raises(HTTPException) as ei:
        await ed.record_card_feedback(_request(cards=cards))
    assert ei.value.status_code == 400
    assert "duplicate card" in ei.value.detail
    assert s.executed == []


@pytest.mark.parametrize("count", [0, 1, 11])
def test_record_rejects_batch_size_outside_2_10(count: int):
    with pytest.raises(ValidationError):
        _request(cards=_cards(count))


@pytest.mark.parametrize("count", [2, 5, 10])
def test_record_accepts_batch_size_inside_2_10(count: int):
    assert len(_request(cards=_cards(count)).cards) == count


# ── record_card_feedback: запись ─────────────────────────────────────────────


async def test_record_writes_exactly_one_chosen_row(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    out = await ed.record_card_feedback(_request())
    assert out["status"] == "ok"
    assert out["cards_recorded"] == 3
    assert out["inserted"] == 3
    assert out["skipped_existing"] == 0
    assert s.committed
    verdicts = [params["verdict"] for _sql, params in _inserts(s)]
    assert verdicts.count("chosen") == 1
    assert verdicts.count("passed") == 2
    chosen = [params for _sql, params in _inserts(s) if params["verdict"] == "chosen"]
    assert chosen[0]["entity_id"] == "sig-1"


async def test_record_is_single_transaction_over_card_feedback_only(monkeypatch):
    """Радиус поражения: пишем только в свою таблицу, всё — одной транзакцией.

    workspaces фигурирует ровно один раз и только на чтение (pre-flight SELECT) — писать
    туда этот инструмент по-прежнему не имеет права.
    """
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    await ed.record_card_feedback(_request())
    assert len(_inserts(s)) == 3
    for sql, _params in _inserts(s):
        for forbidden in ("workspaces", "threshold_proposals", "relevance_decisions"):
            assert forbidden not in sql
    probes = [sql for sql, _params in s.executed if _CARD_INSERT not in sql]
    assert len(probes) == 1
    assert probes[0].strip().upper().startswith("SELECT")  # только чтение
    assert len(s.executed) == 4  # 1 pre-flight + 3 вставки, больше ничего
    assert s.committed  # ровно один commit в конце


async def test_record_replay_of_same_batch_creates_no_duplicates(monkeypatch):
    """ON CONFLICT DO NOTHING: повтор батча — no-op, id детерминирован ключом UNIQUE."""
    first = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    out_first = await ed.record_card_feedback(_request())
    ids_first = [params["id"] for _sql, params in _inserts(first)]

    replay = _patch_session(monkeypatch, _Session([_Result(rowcount=0) for _ in range(3)]))
    out_replay = await ed.record_card_feedback(_request())
    ids_replay = [params["id"] for _sql, params in _inserts(replay)]

    assert out_first["inserted"] == 3
    assert out_replay["inserted"] == 0
    assert out_replay["skipped_existing"] == 3
    assert ids_first == ids_replay
    sql = _inserts(replay)[0][0]
    assert "ON CONFLICT (batch_id, entity_kind, entity_id) DO NOTHING" in sql


async def test_record_accepts_the_chosen_less_half_of_a_cross_workspace_batch(monkeypatch):
    """batch_id сквозной, workspace в вызове один — половина без выбора обязана записаться.

    Иначе пятёрка из двух воркспейсов записывается только двумя вызовами со своим
    chosen в каждом, то есть двумя chosen-строками на один batch_id.
    """
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(2)]))
    out = await ed.record_card_feedback(_request(cards=_cards(2), chosen_entity_id=None))
    assert out["status"] == "ok"
    assert out["chosen_entity_id"] is None
    assert [params["verdict"] for _sql, params in _inserts(s)] == ["passed", "passed"]


async def test_record_rejects_a_second_chosen_on_the_same_batch(monkeypatch):
    """Инвариант «ровно одна chosen на батч» держится по batch_id, а не внутри вызова.

    Таблица append-only: вторая chosen-строка неисправима, поэтому вставка
    откатывается целиком, а вызывающий получает 409 с указанием, что делать.
    """
    s = _patch_session(monkeypatch, _ConflictingSession(fail_at=2))
    with pytest.raises(HTTPException) as ei:
        await ed.record_card_feedback(_request())
    assert ei.value.status_code == 409
    assert "already has a chosen card" in ei.value.detail
    assert s.rolled_back
    assert not s.committed


async def test_record_does_not_relabel_an_unrelated_integrity_error(monkeypatch):
    """FK на workspaces — другая причина, свой текст.

    Пересказ любого IntegrityError как «у батча уже есть chosen» увёл бы диагностику
    в сторону от единственного действия, которое чинит этот случай. Пред-проверка
    существования воркспейса эту ветку не подменяет: гонку (воркспейс удалили между
    SELECT и вставкой) по-прежнему ловит и поднимает наверх именно IntegrityError.
    """
    s = _patch_session(
        monkeypatch, _ConflictingSession(fail_at=1, constraint="card_feedback_workspace_id_fkey")
    )
    with pytest.raises(IntegrityError):
        await ed.record_card_feedback(_request())
    assert s.rolled_back
    assert not s.committed


async def test_record_stores_snapshot_and_both_numbers(monkeypatch):
    """Снимок и *_at_pick обязательны: строка обязана пережить источник."""
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(2)]))
    cards = _cards(2)
    cards[1]["relevance"] = 0.71
    cards[1]["own_stake"] = 0.42
    await ed.record_card_feedback(_request(cards=cards, chosen_entity_id="sig-1"))
    chosen = _rows_by_entity(s)["sig-1"]
    assert chosen["relevance"] == 0.71
    assert chosen["own_stake"] == 0.42
    assert "Сигнал 1" in chosen["snapshot"]
    assert "signal_score" in chosen["snapshot"]


async def test_record_binds_injection_shaped_id_as_param(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(2)]))
    cards = _cards(2)
    cards[0]["entity_id"] = "' OR '1'='1"
    await ed.record_card_feedback(_request(cards=cards, chosen_entity_id="' OR '1'='1"))
    sql, params = _inserts(s)[0]
    assert params["entity_id"] == "' OR '1'='1"
    assert "OR '1'='1" not in sql


# ── record_card_feedback: смысл reason ───────────────────────────────────────


async def test_batch_reason_never_lands_on_the_chosen_row(monkeypatch):
    """Общий reason — «почему остальные прошли мимо», и на chosen он означал бы обратное.

    Таблица append-only: строка «остальные — пересказ чужого релиза» на выбранной карточке
    неисправима, и калибровочный SELECT reason WHERE verdict='chosen' читает именно её.
    """
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    await ed.record_card_feedback(_request())
    rows = _rows_by_entity(s)
    assert rows["sig-1"]["verdict"] == "chosen"
    assert rows["sig-1"]["reason"] is None
    assert rows["sig-0"]["reason"] == "остальные — пересказ чужого релиза"
    assert rows["sig-2"]["reason"] == "остальные — пересказ чужого релиза"


async def test_per_card_reason_is_the_only_way_to_annotate_the_chosen_row(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    cards = _cards()
    cards[1]["reason"] = "берём: есть свой стенд, будет чем проверить"
    await ed.record_card_feedback(_request(cards=cards, chosen_entity_id="sig-1"))
    rows = _rows_by_entity(s)
    assert rows["sig-1"]["reason"] == "берём: есть свой стенд, будет чем проверить"
    assert rows["sig-0"]["reason"] == "остальные — пересказ чужого релиза"


async def test_per_card_reason_beats_the_batch_reason_on_a_passed_row(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    cards = _cards()
    cards[0]["reason"] = "мимо: это тред 2023 года"
    await ed.record_card_feedback(_request(cards=cards))
    rows = _rows_by_entity(s)
    assert rows["sig-0"]["reason"] == "мимо: это тред 2023 года"
    assert rows["sig-2"]["reason"] == "остальные — пересказ чужого релиза"


async def test_blank_per_card_reason_does_not_become_a_row_reason(monkeypatch):
    """Пробелы — это не объяснение: chosen остаётся NULL, passed берёт общий reason."""
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    cards = _cards()
    cards[1]["reason"] = "   "
    cards[0]["reason"] = ""
    await ed.record_card_feedback(_request(cards=cards, chosen_entity_id="sig-1"))
    rows = _rows_by_entity(s)
    assert rows["sig-1"]["reason"] is None
    assert rows["sig-0"]["reason"] == "остальные — пересказ чужого релиза"


# ── record_card_feedback: воркспейс есть в конфиге, но не в БД ───────────────


async def test_record_rejects_a_workspace_that_was_never_bootstrapped(monkeypatch):
    """auto_hmi живёт в config/workspaces.yml, но строки в workspaces нет.

    assert_known_workspace такой воркспейс пропускает, а FK card_feedback.workspace_id — нет.
    Без пред-проверки автор получает через шлюз голое «Client error '500'», а набранная
    руками пятёрка теряется. Нужен 400 с названием ровно того вызова, который это чинит.

    Первое условие теста — «auto_hmi есть в конфиге» — объявлено явно: иначе проверяемая
    развилка вообще не достигается, а тест краснеет на unknown workspace из чужого файла.
    """
    _declare_workspaces(monkeypatch, "disruption", "auto_hmi")
    s = _patch_session(monkeypatch, _Session([], workspace_exists=False))
    with pytest.raises(HTTPException) as ei:
        await ed.record_card_feedback(_request(workspace="auto_hmi"))
    assert ei.value.status_code == 400
    assert "auto_hmi" in ei.value.detail
    assert "/api/workspaces/bootstrap?workspace_id=auto_hmi" in ei.value.detail
    assert _inserts(s) == []  # ни одной строки разметки не записано
    assert not s.committed


async def test_record_probes_the_workspace_before_touching_card_feedback(monkeypatch):
    """Пред-проверка должна стоять ПЕРЕД вставками, иначе она ничего не спасает."""
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1) for _ in range(3)]))
    await ed.record_card_feedback(_request())
    assert _WS_PROBE in s.executed[0][0]
    assert s.executed[0][1]["ws"] == "disruption"
    assert all(_CARD_INSERT in sql for sql, _params in s.executed[1:])


# ── list_card_feedback ───────────────────────────────────────────────────────


async def test_list_survives_deleted_source_row(monkeypatch):
    """Источника нет — заголовок и цифра всё равно приходят из снимка."""
    row = {
        "id": "cardfb:deadbeef",
        "workspace_id": "disruption",
        "batch_id": "cards-20260803-abcd1234",
        "entity_kind": "missing",
        "entity_id": "missing-удалён",
        "verdict": "chosen",
        "reason": "теперь этой строки в missing_signals нет",
        "relevance_at_pick": 0.61,
        "own_stake_at_pick": 0.33,
        "card_snapshot": {
            "title": "Автомобильный HMI",
            "metric_name": "gap_score",
            "metric_value": 0.87,
        },
        "reviewed_by": "author",
        "decided_at": datetime(2026, 8, 3, tzinfo=timezone.utc),
        "created_at": datetime(2026, 8, 3, tzinfo=timezone.utc),
    }
    s = _patch_session(monkeypatch, _Session([_Result(rows=[row])]))
    out = await ed.list_card_feedback(ListCardFeedbackRequest(workspace="disruption"))
    assert out["count"] == 1
    item = out["feedback"][0]
    assert item["title"] == "Автомобильный HMI"
    assert item["metric_name"] == "gap_score"
    assert item["metric_value"] == 0.87
    assert item["relevance_at_pick"] == 0.61
    assert item["own_stake_at_pick"] == 0.33
    assert "workspace_id = :ws" in s.executed[0][0]
    assert s.executed[0][1]["ws"] == "disruption"


async def test_list_parses_snapshot_delivered_as_text(monkeypatch):
    row = {
        "id": "cardfb:1",
        "card_snapshot": (
            '{"title": "Из строки", "metric_name": "burst_score", "metric_value": 2.5}'
        ),
    }
    _patch_session(monkeypatch, _Session([_Result(rows=[row])]))
    out = await ed.list_card_feedback(ListCardFeedbackRequest())
    assert out["feedback"][0]["title"] == "Из строки"
    assert out["feedback"][0]["metric_value"] == 2.5


async def test_list_lifts_the_batch_reason_onto_the_chosen_row(monkeypatch):
    """У chosen-строки reason пустой по построению — контекст даёт batch_reason.

    Иначе читающий chosen-строку остаётся вообще без объяснения, ради которого разметка
    и ведётся. Поле названо «batch», чтобы его нельзя было принять за оценку выбранной.
    """
    row = {
        "id": "cardfb:1",
        "batch_id": "cards-20260803-abcd1234",
        "verdict": "chosen",
        "reason": None,
        "batch_reason": "остальные — пересказ чужого релиза",
        "relevance_at_pick": None,
        "own_stake_at_pick": None,
        "card_snapshot": {},
    }
    s = _patch_session(monkeypatch, _Session([_Result(rows=[row])]))
    out = await ed.list_card_feedback(ListCardFeedbackRequest())
    item = out["feedback"][0]
    assert item["reason"] is None
    assert item["batch_reason"] == "остальные — пересказ чужого релиза"
    sql = s.executed[0][0]
    assert "AS batch_reason" in sql
    assert "PARTITION BY batch_id" in sql
    assert "verdict = 'passed'" in sql


def _sqlite_batch_reasons(sql: str, params: dict, rows: list[tuple]) -> dict[str, str | None]:
    """Выполнить оконное выражение из боевого SQL на sqlite и вернуть batch_reason по entity_id.

    Проверка текста запроса поймать подмену reason не может: она видит подстроки, а не то,
    что окно реально поднимет. Синтаксис MAX(...) FILTER (WHERE ...) OVER (PARTITION BY ...)
    и оператор ->> у sqlite и Postgres здесь совпадают, поэтому берём ровно ту строку SQL,
    которую собрал list_card_feedback, и прогоняем её на живой таблице.
    """
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE card_feedback (
                id TEXT PRIMARY KEY, workspace_id TEXT, batch_id TEXT, entity_kind TEXT,
                entity_id TEXT, verdict TEXT, reason TEXT, relevance_at_pick REAL,
                own_stake_at_pick REAL, card_snapshot TEXT, reviewed_by TEXT,
                decided_at TEXT, created_at TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO card_feedback (id, workspace_id, batch_id, entity_id, verdict, "
            "reason, card_snapshot) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        cur = conn.execute(sql, params)
        columns = [c[0] for c in cur.description]
        selected = [dict(zip(columns, r)) for r in cur.fetchall()]
        return {r["entity_id"]: r["batch_reason"] for r in selected}
    finally:
        conn.close()


def _fb_row(entity_id: str, verdict: str, reason: str | None, snap_reason: str | None) -> tuple:
    """Строка card_feedback одного батча: значимы verdict, reason и reason внутри снимка."""
    snapshot = json.dumps({"reason": snap_reason}, ensure_ascii=False)
    return (f"fb:{entity_id}", "disruption", "b1", entity_id, verdict, reason, snapshot)


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 38),
    reason="оператор ->> появился в sqlite 3.38; на Postgres он есть всегда",
)
async def test_batch_reason_never_picks_up_a_per_card_reason(monkeypatch):
    """batch_reason обязан быть общим reason батча, а не MAX() по всем passed-строкам.

    На записи персональный reason карточки перебивает общий, поэтому у батча со смешанной
    разметкой passed-строки хранят разное. MAX(reason) без фильтра поднимал бы фразу про
    ОДНУ отбракованную карточку («это ретвит апрельского треда» про sig-0) и показывал бы
    её на всех строках батча — включая chosen — как объяснение батча. Это та же подмена
    reason, что закрыта на записи.
    """
    s = _patch_session(monkeypatch, _Session([_Result(rows=[])]))
    await ed.list_card_feedback(ListCardFeedbackRequest(workspace="disruption"))
    sql, params = s.executed[0]
    per_card = "это ретвит апрельского треда"
    shared = "все три — пересказ чужого релиза"
    rows = [
        _fb_row("sig-1", "chosen", None, None),
        _fb_row("sig-0", "passed", per_card, per_card),  # персональный reason карточки
        _fb_row("sig-2", "passed", shared, None),  # общий reason батча
    ]
    got = _sqlite_batch_reasons(sql, params, rows)
    assert got == {"sig-0": shared, "sig-1": shared, "sig-2": shared}


async def test_list_counts_how_many_rows_actually_carry_both_numbers(monkeypatch):
    """Спека C стоит на «60 парах с обоими числами» — полупустые не должны копиться молча."""
    rows = [
        {"id": "a", "relevance_at_pick": 0.7, "own_stake_at_pick": 0.4, "card_snapshot": {}},
        {"id": "b", "relevance_at_pick": 0.6, "own_stake_at_pick": None, "card_snapshot": {}},
        {"id": "c", "relevance_at_pick": None, "own_stake_at_pick": None, "card_snapshot": {}},
    ]
    _patch_session(monkeypatch, _Session([_Result(rows=rows)]))
    out = await ed.list_card_feedback(ListCardFeedbackRequest())
    assert out["count"] == 3
    assert out["axes_filled"] == {
        "relevance_at_pick": 2,
        "own_stake_at_pick": 1,
        "both": 1,
    }


async def test_list_rejects_unknown_workspace(monkeypatch):
    s = _patch_session(monkeypatch, _Session([]))
    with pytest.raises(HTTPException) as ei:
        await ed.list_card_feedback(ListCardFeedbackRequest(workspace="bogus_ws"))
    assert ei.value.status_code == 400
    assert s.executed == []


# ── export_inbox_cards ───────────────────────────────────────────────────────


def _emerging_rows(count=2):
    now = datetime.now(timezone.utc)
    return [
        {
            "id": f"emerging-{index}",
            "workspace_id": "disruption",
            "title": f"Тема {index}",
            "signal_score": 0.6 + index / 10,
            "source_count": 4,
            "detected_at": now - timedelta(days=index),
        }
        for index in range(count)
    ]


def _patch_emerging(monkeypatch, rows):
    calls: list = []

    async def _fake(req):
        calls.append(req)
        return {"signals": rows}

    monkeypatch.setattr(ed, "list_emerging_signals", _fake)
    return calls


async def test_export_metric_is_traceable_to_a_db_column(monkeypatch):
    rows = _emerging_rows(2)
    _patch_emerging(monkeypatch, rows)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", limit=2, source="emerging")
    )
    assert out["count"] == 2
    card = out["cards"][0]
    assert card["metric_name"] == "signal_score"
    assert card["metric_value"] == rows[0]["signal_score"]
    assert card["entity_id"] == rows[0]["id"]
    assert card["entity_kind"] == "emerging"


async def test_export_falls_back_to_source_count_never_invents(monkeypatch):
    rows = _emerging_rows(1)
    rows[0]["signal_score"] = None
    _patch_emerging(monkeypatch, rows)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", limit=2, source="emerging")
    )
    assert out["cards"][0]["metric_name"] == "source_count"
    assert out["cards"][0]["metric_value"] == 4.0

    rows[0]["source_count"] = None
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", limit=2, source="emerging")
    )
    assert out["cards"][0]["metric_name"] is None
    assert out["cards"][0]["metric_value"] is None


async def test_export_card_shape_matches_record_input(monkeypatch):
    """Выдачу экспорта можно отдать в record_card_feedback без перекладывания."""
    _patch_emerging(monkeypatch, _emerging_rows(3))
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", limit=3, source="emerging")
    )
    for card in out["cards"]:
        assert set(card) == set(CardInput.model_fields)
        CardInput(**card)
    assert out["cards"][0]["question"] == ""  # вопрос пишет локальный агент, не сервер
    assert out["cards"][0]["own_stake"] is None
    assert out["cards"][0]["stake_quadrant"] is None
    RecordCardFeedbackRequest(
        workspace="disruption",
        batch_id=out["batch_id"],
        cards=out["cards"],
        chosen_entity_id=out["cards"][0]["entity_id"],
    )


async def test_export_mints_new_batch_id_every_call(monkeypatch):
    _patch_emerging(monkeypatch, _emerging_rows(2))
    req = ExportInboxCardsRequest(workspace="disruption", limit=2)
    first = await ed.export_inbox_cards(req)
    second = await ed.export_inbox_cards(req)
    assert first["batch_id"] != second["batch_id"]
    assert first["target_path"] == "content/inbox.md"
    assert first["batch_id"] in first["markdown"]


async def test_export_filters_by_days_back(monkeypatch):
    rows = _emerging_rows(2)
    rows[1]["detected_at"] = datetime.now(timezone.utc) - timedelta(days=90)
    _patch_emerging(monkeypatch, rows)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", limit=5, source="emerging", days_back=7)
    )
    assert [card["entity_id"] for card in out["cards"]] == ["emerging-0"]


async def test_export_rejects_unknown_workspace(monkeypatch):
    calls = _patch_emerging(monkeypatch, _emerging_rows(2))
    with pytest.raises(HTTPException) as ei:
        await ed.export_inbox_cards(ExportInboxCardsRequest(workspace="bogus_ws"))
    assert ei.value.status_code == 400
    assert calls == []


async def test_export_trend_uses_burst_score_and_russian_title(monkeypatch):
    now = datetime.now(timezone.utc)
    row = {
        "id": "trend-1",
        "title": "Software defined vehicle",
        "title_ru": "Программно-определяемый автомобиль",
        "burst_score": 3.25,
        "detected_at": now,
    }

    async def _fake(req):
        assert req.kind == "trend"
        return {"kind": "trend", "trends": [row]}

    monkeypatch.setattr(ed, "list_clusters", _fake)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", limit=2, source="trend")
    )
    card = out["cards"][0]
    assert card["entity_kind"] == "trend"
    assert card["title"] == "Программно-определяемый автомобиль"
    assert card["metric_name"] == "burst_score"
    assert card["metric_value"] == 3.25


async def test_export_missing_uses_gap_score_and_topic(monkeypatch):
    row = {
        "id": "missing-1",
        "topic": "automotive HMI design",
        "gap_score": 0.87,
        "updated_at": datetime.now(timezone.utc),
    }

    async def _fake(req):
        return {"signals": [row]}

    monkeypatch.setattr(ed, "list_missing_signals", _fake)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", limit=2, source="missing")
    )
    card = out["cards"][0]
    assert card["entity_kind"] == "missing"
    assert card["title"] == "automotive HMI design"
    assert card["metric_name"] == "gap_score"
    assert card["metric_value"] == 0.87


async def test_export_empty_result_is_honest(monkeypatch):
    _patch_emerging(monkeypatch, [])
    out = await ed.export_inbox_cards(ExportInboxCardsRequest(workspace="disruption"))
    assert out["count"] == 0
    assert out["cards"] == []
    assert "Сигналов за окно нет" in out["markdown"]


# ── export_inbox_cards: почему пусто ─────────────────────────────────────────


async def test_export_empty_missing_keeps_the_last_run_diagnostic(monkeypatch):
    """«Разрывов на этой неделе нет» обязано отличаться от «анализ сломан».

    list_missing_signals отдаёт last_run именно на пустой выдаче; выбросить его здесь —
    значит вернуть ту же неразличимость, ради которой его и добавили.
    """
    last_run = {
        "id": "run-42",
        "workspace_id": "disruption",
        "status": "error",
        "started_at": datetime(2026, 8, 2, tzinfo=timezone.utc),
        "finished_at": None,
        "summary": {"topics_generated": 0},
    }

    async def _fake(req):
        return {"signals": [], "last_run": last_run}

    monkeypatch.setattr(ed, "list_missing_signals", _fake)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", source="missing")
    )
    assert out["count"] == 0
    assert out["last_run"] == last_run
    assert out["last_run_supported"] is True
    assert "status=error" in out["markdown"]


async def test_export_empty_missing_without_any_run_says_the_analysis_never_ran(monkeypatch):
    """auto_hmi объявляем явно: воркспейс молодой, и в запечённой копии конфига его нет."""
    _declare_workspaces(monkeypatch, "disruption", "auto_hmi")

    async def _fake(req):
        return {"signals": []}

    monkeypatch.setattr(ed, "list_missing_signals", _fake)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="auto_hmi", source="missing")
    )
    assert out["last_run"] is None
    assert out["last_run_supported"] is True
    assert "ни разу не отрабатывал" in out["markdown"]


async def test_export_empty_emerging_admits_it_cannot_tell(monkeypatch):
    """list_emerging_signals диагностики не отдаёт — врать про прогон нельзя, молчать тоже."""
    _patch_emerging(monkeypatch, [])
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", source="emerging")
    )
    assert out["last_run"] is None
    assert out["last_run_supported"] is False
    assert "не различить" in out["markdown"]


async def test_export_blames_the_window_not_the_run_when_rows_existed(monkeypatch):
    """Строки были, окно их срезало — валить пустоту на прогон анализа было бы неправдой."""
    rows = _emerging_rows(2)
    for row in rows:
        row["detected_at"] = datetime.now(timezone.utc) - timedelta(days=90)
    _patch_emerging(monkeypatch, rows)
    out = await ed.export_inbox_cards(
        ExportInboxCardsRequest(workspace="disruption", source="emerging", days_back=7)
    )
    assert out["count"] == 0
    assert out["rows_before_window"] == 2
    assert "старше окна days_back" in out["markdown"]
    assert "не различить" not in out["markdown"]


async def test_export_non_empty_result_carries_no_run_diagnostic(monkeypatch):
    """Диагностика — ответ на «почему пусто»; при непустой выдаче вопроса нет."""
    _patch_emerging(monkeypatch, _emerging_rows(2))
    out = await ed.export_inbox_cards(ExportInboxCardsRequest(workspace="disruption", limit=2))
    assert out["count"] == 2
    assert out["last_run"] is None
    assert "не различить" not in out["markdown"]


# ── export_inbox_cards: какие оси у карточки вообще есть ─────────────────────


async def test_export_states_which_numbers_this_card_kind_can_have(monkeypatch):
    """relevance_at_pick и own_stake_at_pick уйдут NULL — это должно быть сказано вслух.

    Иначе автор набирает 60 полупустых пар, считая, что собирает полные.
    """
    _patch_emerging(monkeypatch, _emerging_rows(2))
    out = await ed.export_inbox_cards(ExportInboxCardsRequest(workspace="disruption", limit=2))
    assert out["axes"] == {"relevance_at_pick": None, "own_stake_at_pick": None}
    assert "relevance_at_pick" in out["axes_note"]
    assert "own_stake_at_pick" in out["axes_note"]
    assert out["axes_note"] in out["markdown"]


@pytest.mark.parametrize("source", ["emerging", "trend", "missing"])
def test_no_source_kind_claims_a_relevance_column(source: str):
    """Колонки релевантности нет ни у одной из трёх таблиц-источников — проверено по init.sql."""
    assert ed.RELEVANCE_COLUMN_BY_SOURCE[source] is None


async def test_export_never_passes_the_metric_off_as_relevance(monkeypatch):
    """Шкалы разные, и metric_value внутри одного батча отличается от карточки к карточке."""
    rows = _emerging_rows(2)
    _patch_emerging(monkeypatch, rows)
    out = await ed.export_inbox_cards(ExportInboxCardsRequest(workspace="disruption", limit=2))
    values = {card["metric_value"] for card in out["cards"]}
    assert len(values) == 2  # одно число на батч не натянуть
    for card in out["cards"]:
        assert card["relevance"] is None
        assert card["own_stake"] is None
        assert card["metric_value"] is not None
