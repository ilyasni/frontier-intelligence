import asyncio

from worker.services import relevance_audit as ra
from worker.services.relevance_audit import (
    _current_relevance_threshold,
    propose_relevance_threshold,
    run_relevance_audit_metrics,
)


def _audited(fn_scores, correct_n):
    rows = [{"score": s, "verdict": "false_negative"} for s in fn_scores]
    rows += [{"score": 0.1, "verdict": "correct_reject"} for _ in range(correct_n)]
    return rows


def test_insufficient_audited_returns_none() -> None:
    assert propose_relevance_threshold(0.6, _audited([0.55, 0.58], 3)) is None


def test_insufficient_false_negatives_returns_none() -> None:
    # 20 audited but only 2 FN (< REL_MIN_FN)
    assert propose_relevance_threshold(0.6, _audited([0.55, 0.58], 18)) is None


def test_low_fn_rate_returns_none() -> None:
    # 5 FN out of 100 -> rate 0.05 < 0.15
    assert propose_relevance_threshold(0.6, _audited([0.55] * 5, 95)) is None


def test_proposes_lower_threshold_to_recover_false_negatives() -> None:
    fns = [0.45, 0.50, 0.52, 0.55, 0.57, 0.58]
    prop = propose_relevance_threshold(0.6, _audited(fns, 14))
    assert prop is not None
    assert prop["threshold_key"] == "relevance_threshold"
    assert prop["direction"] == "lower"
    assert prop["proposed_value"] == 0.5
    assert prop["evidence"]["recovered_at_proposed"] == 5
    assert prop["evidence"]["n_false_negative"] == 6


def test_step_cap_bounds_large_drop() -> None:
    # all FN very low -> candidate would crash down, but step capped to 0.2 below current
    fns = [0.05] * 10
    prop = propose_relevance_threshold(0.6, _audited(fns, 20))
    assert prop is not None
    assert prop["proposed_value"] >= 0.4  # 0.6 - 0.2 step cap


# ── propose_relevance_threshold: дополнительные граничные случаи ───────────────

def test_propose_min_audited_exact_boundary() -> None:
    # ровно 15 отсуженных (10 correct + 5 FN), fn_rate = 5/15 ≈ 0.333 > 0.15 → предложение есть
    rows = _audited([0.40, 0.45, 0.48, 0.50, 0.52], 10)
    assert len(rows) == 15
    prop = propose_relevance_threshold(0.6, rows)
    assert prop is not None
    assert prop["evidence"]["n_audited"] == 15
    assert prop["evidence"]["n_false_negative"] == 5


def test_propose_min_fn_exact_boundary() -> None:
    # FN ровно 5 (REL_MIN_FN) → есть; держим n большим, чтобы границу проверял именно min_fn,
    # но при этом fn_rate оставался выше порога.
    fns = [0.40, 0.45, 0.48, 0.50, 0.52]
    assert propose_relevance_threshold(0.6, _audited(fns, 10)) is not None
    # FN = 4 (< REL_MIN_FN) → None
    assert propose_relevance_threshold(0.6, _audited([0.40, 0.45, 0.48, 0.50], 11)) is None


def test_propose_fn_rate_exact_boundary() -> None:
    # ровно 0.15 (15 FN из 100) → проходит (>= не строгое: условие отсева `< fn_rate`)
    fns = [0.40] * 15
    prop = propose_relevance_threshold(0.6, _audited(fns, 85))
    assert prop is not None
    assert prop["evidence"]["fn_rate"] == 0.15
    # 0.14 (14 FN из 100) < 0.15 → None
    assert propose_relevance_threshold(0.6, _audited([0.40] * 14, 86)) is None


def test_propose_only_lowers_clamps_to_current() -> None:
    # высокие FN-scores дают percentile выше текущего порога → обрезается до current,
    # значит изменение нулевое и предложения нет (proposed не может быть выше current).
    fns = [0.80, 0.82, 0.85, 0.88, 0.90, 0.92]
    assert propose_relevance_threshold(0.6, _audited(fns, 14)) is None


def test_propose_step_cap_floor_at_current_minus_max_step() -> None:
    # очень низкие FN, current=0.6 → кандидат уехал бы вниз, но шаг ограничен REL_MAX_STEP=0.2
    fns = [0.01] * 12
    prop = propose_relevance_threshold(0.6, _audited(fns, 18))
    assert prop is not None
    assert prop["proposed_value"] == 0.4  # 0.6 - 0.2


def test_propose_min_change_too_small_returns_none() -> None:
    # FN-scores у самого порога: кандидат ≈ current, изменение < REL_MIN_CHANGE (0.02) → None
    fns = [0.595, 0.596, 0.598, 0.599, 0.599, 0.600]
    assert propose_relevance_threshold(0.6, _audited(fns, 14)) is None


def test_propose_empty_audited_returns_none() -> None:
    assert propose_relevance_threshold(0.6, []) is None


def test_propose_all_correct_reject_returns_none() -> None:
    # FN = 0 (только correct_reject) → None
    assert propose_relevance_threshold(0.6, _audited([], 30)) is None


def test_propose_recovered_count_in_evidence() -> None:
    # candidate = percentile([...], 20). FN >= candidate засчитываются как recovered.
    fns = [0.45, 0.50, 0.52, 0.55, 0.57, 0.58]
    prop = propose_relevance_threshold(0.6, _audited(fns, 14))
    assert prop is not None
    candidate = prop["proposed_value"]
    expected_recovered = sum(1 for s in fns if s >= candidate)
    assert prop["evidence"]["recovered_at_proposed"] == expected_recovered


# ── orchestration helpers: фейковые AsyncSession в стиле репо ──────────────────

class _FakeResult:
    def __init__(self, row=None, rows=None) -> None:
        self._row = row
        self._rows = rows or []

    def mappings(self):
        return self

    def first(self):
        return self._row

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, responses, statements) -> None:
        self._responses = responses
        self._statements = statements
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        self._statements.append((str(statement), params or {}))
        if self._responses:
            return self._responses.pop(0)
        return _FakeResult()

    async def commit(self):
        self.committed = True
        return None


def _patch_session_factory(monkeypatch, session):
    import shared.db as shared_db

    monkeypatch.setattr(shared_db, "get_session_factory", lambda: (lambda: session))
    # set_relevance_audit_metric импортируется в модуль relevance_audit — мокаем там
    monkeypatch.setattr(ra, "set_relevance_audit_metric", lambda *a, **kw: None)


# ── run_relevance_audit_metrics: оркестрация ──────────────────────────────────

def test_run_metrics_counts_passthrough(monkeypatch) -> None:
    # audited < 15 → ветка предложения не выполняется, проверяем только счётчики
    statements: list = []
    counts = _FakeResult(row={
        "rejected": 120,
        "audited": 10,
        "false_negatives": 3,
        "correct": 7,
    })
    session = _FakeSession([counts], statements)
    _patch_session_factory(monkeypatch, session)

    result = asyncio.run(run_relevance_audit_metrics("disruption"))

    assert result["workspace_id"] == "disruption"
    assert result["status"] == "ok"
    assert result["rejected_30d"] == 120
    assert result["audited_30d"] == 10
    assert result["false_negatives_30d"] == 3
    assert result["false_negative_rate"] == round(3 / 10, 4)
    assert result["proposal"] is None


def test_run_metrics_proposes_and_upserts_when_sufficient(monkeypatch) -> None:
    # audited >= 15, высокий fn_rate, низкие FN-scores → proposal + upsert вызван
    statements: list = []
    fns = [0.40, 0.45, 0.48, 0.50, 0.52]
    audited_rows = [{"score": s, "verdict": "false_negative"} for s in fns]
    audited_rows += [{"score": 0.1, "verdict": "correct_reject"} for _ in range(15)]
    responses = [
        _FakeResult(row={"rejected": 200, "audited": 20, "false_negatives": 5, "correct": 15}),
        _FakeResult(rows=audited_rows),          # выборка отсуженных
        _FakeResult(row={"thr": "0.6"}),          # _current_relevance_threshold
        _FakeResult(),                             # _upsert_relevance_proposal INSERT
    ]
    session = _FakeSession(responses, statements)
    _patch_session_factory(monkeypatch, session)

    result = asyncio.run(run_relevance_audit_metrics("disruption"))

    assert result["audited_30d"] == 20
    assert result["false_negatives_30d"] == 5
    assert result["proposal"] is not None
    assert result["proposal"]["threshold_key"] == "relevance_threshold"
    assert result["proposal"]["direction"] == "lower"
    # upsert выполнен (INSERT INTO threshold_proposals) и коммит был
    assert any("INSERT INTO threshold_proposals" in s for s, _ in statements)
    assert session.committed is True


def test_run_metrics_no_proposal_when_insufficient_audited(monkeypatch) -> None:
    # audited = 14 (< REL_MIN_AUDITED) → ветка предложения не запускается, upsert не вызван
    statements: list = []
    counts = _FakeResult(row={
        "rejected": 90,
        "audited": 14,
        "false_negatives": 8,
        "correct": 6,
    })
    session = _FakeSession([counts], statements)
    _patch_session_factory(monkeypatch, session)

    result = asyncio.run(run_relevance_audit_metrics("disruption"))

    assert result["audited_30d"] == 14
    assert result["proposal"] is None
    assert not any("INSERT INTO threshold_proposals" in s for s, _ in statements)
    # одно-единственное обращение к БД (только counts), коммита не было
    assert len(statements) == 1
    assert session.committed is False


# ── _current_relevance_threshold ──────────────────────────────────────────────

def test_current_threshold_default_on_none_row(monkeypatch) -> None:
    # row is None → default 0.6
    session = _FakeSession([_FakeResult(row=None)], [])
    assert asyncio.run(_current_relevance_threshold(session, "disruption")) == 0.6


def test_current_threshold_default_on_null_value(monkeypatch) -> None:
    # thr is None (NULL) → default 0.6
    session = _FakeSession([_FakeResult(row={"thr": None})], [])
    assert asyncio.run(_current_relevance_threshold(session, "disruption")) == 0.6


def test_current_threshold_parses_valid_value(monkeypatch) -> None:
    session = _FakeSession([_FakeResult(row={"thr": "0.75"})], [])
    assert asyncio.run(_current_relevance_threshold(session, "disruption")) == 0.75


def test_current_threshold_default_on_invalid_value(monkeypatch) -> None:
    # нечисловая строка → ValueError перехвачен → default 0.6
    session = _FakeSession([_FakeResult(row={"thr": "invalid"})], [])
    assert asyncio.run(_current_relevance_threshold(session, "disruption")) == 0.6
