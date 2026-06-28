import asyncio

from worker.chains.novelty_judge_chain import (
    NoveltyJudgeChain,
    build_prompt,
    normalize_verdict,
)


def test_normalize_verdict_clamps_and_coerces() -> None:
    v = normalize_verdict({"novelty_score": 1.5, "out_of_distribution": "yes", "reasoning": "x" * 300})
    assert v["novelty_score"] == 1.0
    assert v["out_of_distribution"] is True
    assert len(v["reasoning"]) <= 200

    v2 = normalize_verdict({})
    assert v2["novelty_score"] == 0.0
    assert v2["out_of_distribution"] is False

    v3 = normalize_verdict({"novelty_score": "bad", "out_of_distribution": 0})
    assert v3["novelty_score"] == 0.0
    assert v3["out_of_distribution"] is False


def test_build_prompt_includes_title_and_concepts() -> None:
    p = build_prompt(
        {"title": "Solid-state battery breakthrough", "keywords": ["battery", "solid-state"], "source_count": 2}
    )
    assert "Solid-state battery" in p
    assert "battery" in p


class _Resp:
    def __init__(self, content: str, model: str = "model-x") -> None:
        self.content = content
        self.actual_model = model


class _Client:
    def __init__(self, available: bool, content: str | None = None, raise_exc: bool = False) -> None:
        self.is_available = available
        self._content = content
        self._raise = raise_exc

    async def chat(self, **kwargs):
        if self._raise:
            raise RuntimeError("boom")
        return _Resp(self._content or "{}")


def _run(coro):
    return asyncio.run(coro)


def test_chain_uses_wormsoft_when_available() -> None:
    w = _Client(True, '{"novelty_score":0.8,"out_of_distribution":true,"reasoning":"novel"}')
    p = _Client(True, '{"novelty_score":0.1}')
    chain = NoveltyJudgeChain(w, p, model="deepseek-ai/deepseek-v4-pro", fallback_model="deepseek/deepseek-v3.2")
    v = _run(chain.run({"title": "x", "keywords": []}))
    assert v is not None
    assert v["_provider"] == "wormsoft"
    assert v["novelty_score"] == 0.8


def test_chain_falls_back_to_polza_on_wormsoft_error() -> None:
    w = _Client(True, raise_exc=True)
    p = _Client(True, '{"novelty_score":0.5,"out_of_distribution":false,"reasoning":"meh"}')
    chain = NoveltyJudgeChain(w, p, model="m1", fallback_model="m2")
    v = _run(chain.run({"title": "x", "keywords": []}))
    assert v is not None
    assert v["_provider"] == "polza"
    assert v["novelty_score"] == 0.5


def test_chain_returns_none_when_both_unavailable() -> None:
    chain = NoveltyJudgeChain(_Client(False), _Client(False), model="m1", fallback_model="m2")
    assert _run(chain.run({"title": "x", "keywords": []})) is None


# ── Контур B RSI: дополнительные юнит-тесты ──────────────────────────────────


class _CapturingClient:
    """Клиент, захватывающий kwargs последнего вызова chat() (для регресса бюджета)."""

    def __init__(
        self,
        available: bool,
        content: str | None = None,
        raise_exc: bool = False,
    ) -> None:
        self.is_available = available
        self._content = content
        self._raise = raise_exc
        self.last_kwargs: dict | None = None

    async def chat(self, **kwargs):
        self.last_kwargs = kwargs
        if self._raise:
            raise RuntimeError("boom")
        return _Resp(self._content or "{}")


def test_chain_passes_token_budget_as_max_tokens_wormsoft() -> None:
    # Регресс реального бага: token_budget должен прокидываться как max_tokens.
    # Был баг max_tokens=400 → reasoning-модель съедала бюджет → пустой content.
    w = _CapturingClient(True, '{"novelty_score":0.7,"out_of_distribution":true,"reasoning":"ok"}')
    p = _CapturingClient(True, '{"novelty_score":0.1}')
    chain = NoveltyJudgeChain(w, p, model="m1", fallback_model="m2", token_budget=1200)
    v = _run(chain.run({"title": "x", "keywords": []}))
    assert v is not None and v["_provider"] == "wormsoft"
    assert w.last_kwargs is not None
    assert w.last_kwargs["max_tokens"] == 1200


def test_chain_passes_token_budget_as_max_tokens_polza_fallback() -> None:
    # Тот же контракт для polza-fallback (wormsoft недоступен → идём в polza).
    w = _CapturingClient(False)
    p = _CapturingClient(True, '{"novelty_score":0.5,"out_of_distribution":false,"reasoning":"meh"}')
    chain = NoveltyJudgeChain(w, p, model="m1", fallback_model="m2", token_budget=900)
    v = _run(chain.run({"title": "x", "keywords": []}))
    assert v is not None and v["_provider"] == "polza"
    assert p.last_kwargs is not None
    assert p.last_kwargs["max_tokens"] == 900


def test_chain_empty_content_wormsoft_falls_back_to_polza() -> None:
    # Пустой/без-JSON content у wormsoft (reasoning съел бюджет, объекта нет) →
    # ValueError в парсинге → fallback на polza.
    w = _CapturingClient(True, content="Размышляю... бюджет кончился, JSON не вернул")
    p = _CapturingClient(True, '{"novelty_score":0.4,"out_of_distribution":false,"reasoning":"x"}')
    chain = NoveltyJudgeChain(w, p, model="m1", fallback_model="m2")
    v = _run(chain.run({"title": "x", "keywords": []}))
    assert v is not None
    assert v["_provider"] == "polza"
    assert v["novelty_score"] == 0.4


def test_chain_returns_none_when_both_providers_raise() -> None:
    w = _CapturingClient(True, raise_exc=True)
    p = _CapturingClient(True, raise_exc=True)
    chain = NoveltyJudgeChain(w, p, model="m1", fallback_model="m2")
    assert _run(chain.run({"title": "x", "keywords": []})) is None


def test_normalize_verdict_clamps_negative_to_zero() -> None:
    v = normalize_verdict({"novelty_score": -0.5})
    assert v["novelty_score"] == 0.0


def test_normalize_verdict_clamps_above_one() -> None:
    v = normalize_verdict({"novelty_score": 2.0})
    assert v["novelty_score"] == 1.0


def test_normalize_verdict_drops_extra_fields_to_exactly_three_keys() -> None:
    v = normalize_verdict(
        {
            "novelty_score": 0.5,
            "out_of_distribution": True,
            "reasoning": "ok",
            "garbage": 123,
            "_provider": "should-not-survive",
        }
    )
    assert set(v.keys()) == {"novelty_score", "out_of_distribution", "reasoning"}


def test_normalize_verdict_invalid_and_none() -> None:
    v = normalize_verdict({"novelty_score": None})
    assert v["novelty_score"] == 0.0
    v2 = normalize_verdict({"novelty_score": "not-a-number"})
    assert v2["novelty_score"] == 0.0


# ── run_novelty_judge (service) ──────────────────────────────────────────────

from worker.services import novelty_judge as nj_module  # noqa: E402


class _Settings:
    novelty_judge_enabled = True
    novelty_judge_max_per_run = 10
    novelty_judge_threshold = 0.6
    novelty_judge_model = "m1"
    novelty_judge_fallback_model = "m2"
    novelty_judge_token_budget = 1200


class _FakeResult:
    def __init__(self, rows=None) -> None:
        self._rows = rows or []

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows, *, raise_on_update: bool = False) -> None:
        self._rows = rows
        self._raise_on_update = raise_on_update
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement, params=None):
        sql = str(statement)
        if "SELECT" in sql:
            return _FakeResult(rows=self._rows)
        # UPDATE
        if self._raise_on_update:
            raise RuntimeError("update boom")
        return _FakeResult()

    async def commit(self):
        self.committed = True
        return None


class _RecordingClient:
    """Клиент-заглушка с флагом закрытия для проверки finally."""

    def __init__(self, *args, **kwargs) -> None:
        self.is_available = False
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _patch_service(monkeypatch, *, settings, rows, chain_run, session=None):
    monkeypatch.setattr(nj_module, "get_settings", lambda: settings)
    monkeypatch.setattr(nj_module, "note_novelty_judge", lambda *a, **k: None)

    clients = {}

    def _make_wormsoft(*a, **k):
        c = _RecordingClient()
        clients["wormsoft"] = c
        return c

    def _make_polza(*a, **k):
        c = _RecordingClient()
        clients["polza"] = c
        return c

    monkeypatch.setattr(nj_module, "WormsoftTextClient", _make_wormsoft)
    monkeypatch.setattr(nj_module, "PolzaTextClient", _make_polza)

    class _Chain:
        def __init__(self, *a, **k) -> None:
            pass

        async def run(self, candidate):
            return chain_run(candidate)

    monkeypatch.setattr(nj_module, "NoveltyJudgeChain", _Chain)

    sess = session if session is not None else _FakeSession(rows)

    def _factory():
        return lambda: sess

    import shared.db as shared_db

    monkeypatch.setattr(shared_db, "get_session_factory", _factory)
    return clients, sess


def test_run_novelty_judge_disabled_short_circuits(monkeypatch) -> None:
    settings = _Settings()
    settings.novelty_judge_enabled = False
    calls = {"n": 0}

    def _chain_run(_cand):
        calls["n"] += 1
        return None

    _patch_service(monkeypatch, settings=settings, rows=[], chain_run=_chain_run)
    out = _run(nj_module.run_novelty_judge("disruption"))
    assert out["status"] == "disabled"
    assert out["judged"] == 0
    assert calls["n"] == 0


def test_run_novelty_judge_counters(monkeypatch) -> None:
    settings = _Settings()  # threshold 0.6
    rows = [
        {"id": 1, "title": "a", "keywords": []},
        {"id": 2, "title": "b", "keywords": []},
        {"id": 3, "title": "c", "keywords": []},
    ]
    verdicts = {
        1: {"novelty_score": 0.9, "out_of_distribution": True, "reasoning": "high"},
        2: {"novelty_score": 0.2, "out_of_distribution": False, "reasoning": "low"},
        3: None,
    }

    def _chain_run(cand):
        return verdicts[cand["id"]]

    _patch_service(monkeypatch, settings=settings, rows=rows, chain_run=_chain_run)
    out = _run(nj_module.run_novelty_judge("disruption"))
    assert out["status"] == "ok"
    assert out["judged"] == 2  # вердикты 1 и 2; 3 — None
    assert out["underrated"] == 1  # только 0.9 >= 0.6
    assert out["failed"] == 1  # None


def test_run_novelty_judge_underrated_boundary_inclusive(monkeypatch) -> None:
    settings = _Settings()  # threshold 0.6, код использует >=
    rows = [{"id": 1, "title": "a", "keywords": []}]

    def _chain_run(_cand):
        return {"novelty_score": 0.6, "out_of_distribution": False, "reasoning": "edge"}

    _patch_service(monkeypatch, settings=settings, rows=rows, chain_run=_chain_run)
    out = _run(nj_module.run_novelty_judge("disruption"))
    assert out["judged"] == 1
    assert out["underrated"] == 1  # ровно == threshold → underrated (>=)


def test_run_novelty_judge_underrated_boundary_below(monkeypatch) -> None:
    settings = _Settings()  # threshold 0.6
    rows = [{"id": 1, "title": "a", "keywords": []}]

    def _chain_run(_cand):
        return {"novelty_score": 0.5999, "out_of_distribution": False, "reasoning": "just below"}

    _patch_service(monkeypatch, settings=settings, rows=rows, chain_run=_chain_run)
    out = _run(nj_module.run_novelty_judge("disruption"))
    assert out["judged"] == 1
    assert out["underrated"] == 0  # чуть ниже threshold → не underrated


def test_run_novelty_judge_closes_clients_on_exception(monkeypatch) -> None:
    settings = _Settings()
    rows = [{"id": 1, "title": "a", "keywords": []}]
    sess = _FakeSession(rows, raise_on_update=True)

    def _chain_run(_cand):
        return {"novelty_score": 0.9, "out_of_distribution": True, "reasoning": "x"}

    clients, _ = _patch_service(
        monkeypatch, settings=settings, rows=rows, chain_run=_chain_run, session=sess
    )

    import pytest

    with pytest.raises(RuntimeError):
        _run(nj_module.run_novelty_judge("disruption"))

    # finally должен закрыть оба клиента несмотря на исключение в цикле.
    assert clients["wormsoft"].closed is True
    assert clients["polza"].closed is True
