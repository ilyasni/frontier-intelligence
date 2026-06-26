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
