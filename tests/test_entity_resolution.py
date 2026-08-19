from types import SimpleNamespace

import pytest

from worker.services.entity_resolution import (
    _fold,
    acronym_of,
    find_acronym_candidates,
)


def test_acronym_of() -> None:
    assert acronym_of("Human-Machine Interface") == "hmi"
    assert acronym_of("Large Language Model") == "llm"
    assert acronym_of("AI") is None          # single word
    assert acronym_of("") is None
    assert acronym_of("the End of X") == "ex"  # stopwords the/of skipped
    assert acronym_of("retrieval augmented generation") == "rag"


def test_find_acronym_candidates() -> None:
    concepts = [
        {"name": "HMI", "norm": "hmi", "mentions": 5},
        {"name": "Human-Machine Interface", "norm": "humanmachineinterface", "mentions": 3},
        {"name": "Large Language Model", "norm": "largelanguagemodel", "mentions": 10},
        {"name": "LLM", "norm": "llm", "mentions": 20},
        {"name": "Random Thing", "norm": "randomthing", "mentions": 1},  # acronym 'rt' absent
    ]
    cands = find_acronym_candidates(concepts)
    keys = {(c["acronym_norm"], c["expansion_norm"]) for c in cands}
    assert ("hmi", "humanmachineinterface") in keys
    assert ("llm", "largelanguagemodel") in keys
    assert len(cands) == 2
    # sorted by combined mentions desc: LLM pair (30) before HMI pair (8)
    assert cands[0]["expansion_norm"] == "largelanguagemodel"


def test_no_self_pair() -> None:
    # a concept whose own norm equals its acronym must not pair with itself
    concepts = [{"name": "A B", "norm": "ab", "mentions": 1}]
    assert find_acronym_candidates(concepts) == []


# ── acronym_of: дополнительные кейсы (кириллица, стоп-слова, разделители, цифры) ──


def test_acronym_of_cyrillic() -> None:
    # Первые буквы значимых слов, lower-кириллица.
    assert acronym_of("Большая Языковая Модель") == "бям"


def test_acronym_of_russian_stopwords() -> None:
    # Русские стоп-слова (и/в/на/...) не дают буквы в акроним.
    assert acronym_of("Анализ и Прогноз") == "ап"


def test_acronym_of_separators() -> None:
    # Разделители _ / . и схлопывание пробелов. Буквы выбраны не-стоп-слова
    # (англ. "a" — артикль-стоп-слово, потому здесь x/y/z/w).
    assert acronym_of("X_Y/Z.W") == "xyzw"
    assert acronym_of("X   Y") == "xy"


def test_acronym_of_digit_leading_word() -> None:
    # У слова "3D" первый символ "3" не isalpha → буквы не даёт; остаются P и T.
    assert acronym_of("3D Printing Tech") == "pt"


def test_acronym_of_none_and_single_significant_word() -> None:
    assert acronym_of(None) is None  # type: ignore[arg-type]
    assert acronym_of("") is None
    assert acronym_of("Interface") is None  # одно значимое слово → None


# ── find_acronym_candidates: кросс-алфавит, регресс, дедуп ──


def test_find_acronym_candidates_cross_alphabet_fold() -> None:
    # ФИКС #4: латинская расшифровка + кириллический акроним-узел.
    # acronym_of("Large Language Model") == "llm"; точного norm "llm" нет,
    # но _fold("ллм") == "llm" — folded fallback связывает пару.
    concepts = [
        {"name": "Large Language Model", "norm": "large language model", "mentions": 10},
        {"name": "ЛЛМ", "norm": "ллм", "mentions": 7},
    ]
    cands = find_acronym_candidates(concepts)
    assert len(cands) == 1
    pair = cands[0]
    assert pair["acronym_norm"] == "ллм"
    assert pair["expansion_norm"] == "large language model"
    assert pair["mentions"] == 17


def test_find_acronym_candidates_exact_match_regression() -> None:
    # Точное совпадение нормы по-прежнему работает (без folded fallback).
    concepts = [
        {"name": "HMI", "norm": "hmi", "mentions": 5},
        {"name": "Human-Machine Interface", "norm": "humanmachineinterface", "mentions": 3},
    ]
    cands = find_acronym_candidates(concepts)
    keys = {(c["acronym_norm"], c["expansion_norm"]) for c in cands}
    assert ("hmi", "humanmachineinterface") in keys
    assert len(cands) == 1


def test_find_acronym_candidates_dedup_seen() -> None:
    # Две расшифровки с ОДИНАКОВОЙ нормой дают одну и ту же пару — дубль не повторяется.
    concepts = [
        {"name": "HMI", "norm": "hmi", "mentions": 5},
        {"name": "Human-Machine Interface", "norm": "humanmachineinterface", "mentions": 3},
        {"name": "Human Machine Interface", "norm": "humanmachineinterface", "mentions": 2},
    ]
    cands = find_acronym_candidates(concepts)
    keys = [(c["acronym_norm"], c["expansion_norm"]) for c in cands]
    assert keys.count(("hmi", "humanmachineinterface")) == 1
    assert len(cands) == 1


# ── _fold: кросс-алфавитное сворачивание ──


def test_fold_cyrillic_and_latin() -> None:
    assert _fold("ллм") == "llm"
    assert _fold("llm") == "llm"
    # Смешанная строка проходит без ошибок (кириллица транслит, латиница как есть).
    assert _fold("AIпром") == "aiprom"
    assert _fold("") == ""


# ── EntityEquivalenceChain.run / normalize_verdict (мок wormsoft/polza) ──


class _FakeLLMClient:
    """Минимальный двойник wormsoft/polza text-клиента."""

    def __init__(self, *, available: bool, content: str | None = None, exc: Exception | None = None) -> None:
        self.is_available = available
        self._content = content
        self._exc = exc
        self.called = False

    async def chat(self, **kwargs) -> SimpleNamespace:  # noqa: ANN003
        self.called = True
        if self._exc is not None:
            raise self._exc
        return SimpleNamespace(content=self._content)


class _FakeRouter:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def chat(self, **kwargs) -> SimpleNamespace:  # noqa: ANN003
        self.calls.append(kwargs)
        return SimpleNamespace(
            content='{"equivalent": true, "canonical": "b", "confidence": 0.8}'
        )


def _make_chain(wormsoft: _FakeLLMClient, polza: _FakeLLMClient):
    from worker.chains.entity_equivalence_chain import EntityEquivalenceChain

    return EntityEquivalenceChain(
        wormsoft, polza, model="judge-model", fallback_model="fallback-model", token_budget=500
    )


@pytest.mark.asyncio
async def test_chain_run_wormsoft_success() -> None:
    wormsoft = _FakeLLMClient(
        available=True,
        content='{"equivalent": true, "canonical": "b", "confidence": 0.9, "reasoning": "ok"}',
    )
    polza = _FakeLLMClient(available=True, content="{}")
    chain = _make_chain(wormsoft, polza)

    verdict = await chain.run("LLM", "Large Language Model")

    assert verdict is not None
    assert verdict["_provider"] == "wormsoft"
    assert verdict["equivalent"] is True
    assert verdict["confidence"] == 0.9
    assert wormsoft.called is True
    assert polza.called is False  # fallback не вызывается при успехе


@pytest.mark.asyncio
async def test_chain_routes_entity_judge_through_control_plane() -> None:
    from worker.chains.entity_equivalence_chain import EntityEquivalenceChain

    router = _FakeRouter()
    chain = EntityEquivalenceChain(
        None,
        None,
        model="deepseek-ai/deepseek-v4-pro",
        fallback_model="deepseek/deepseek-v3.2",
        router_client=router,
    )

    verdict = await chain.run("LLM", "Large Language Model")

    assert verdict is not None and verdict["_provider"] == "wormsoft"
    assert router.calls[0]["provider_override"] == "wormsoft"
    assert router.calls[0]["model_override"] == "deepseek-ai/deepseek-v4-pro"


@pytest.mark.asyncio
async def test_chain_run_falls_back_to_polza() -> None:
    # wormsoft бросает (или вернул мусор) → fallback на polza.
    wormsoft = _FakeLLMClient(available=True, exc=ValueError("boom"))
    polza = _FakeLLMClient(
        available=True,
        content='{"equivalent": true, "canonical": "a", "confidence": 0.5}',
    )
    chain = _make_chain(wormsoft, polza)

    verdict = await chain.run("HMI", "Human-Machine Interface")

    assert verdict is not None
    assert verdict["_provider"] == "polza"
    assert verdict["canonical"] == "a"
    assert wormsoft.called is True
    assert polza.called is True


@pytest.mark.asyncio
async def test_chain_run_invalid_json_falls_back() -> None:
    # wormsoft вернул невалидный JSON → parse бросает → fallback polza.
    wormsoft = _FakeLLMClient(available=True, content="not json at all")
    polza = _FakeLLMClient(
        available=True,
        content='{"equivalent": false, "canonical": "b", "confidence": 0.1}',
    )
    chain = _make_chain(wormsoft, polza)

    verdict = await chain.run("a", "b")

    assert verdict is not None
    assert verdict["_provider"] == "polza"


@pytest.mark.asyncio
async def test_chain_run_both_unavailable_returns_none() -> None:
    chain = _make_chain(
        _FakeLLMClient(available=False), _FakeLLMClient(available=False)
    )
    assert await chain.run("a", "b") is None


def test_normalize_verdict_clamps_and_fallbacks() -> None:
    from worker.chains.entity_equivalence_chain import normalize_verdict

    # canonical вне {a,b} → fallback "b"; confidence > 1 → clamp до 1.0.
    v1 = normalize_verdict({"equivalent": True, "canonical": "X", "confidence": 5})
    assert v1["canonical"] == "b"
    assert v1["confidence"] == 1.0
    assert v1["equivalent"] is True

    # confidence < 0 → clamp до 0.0; canonical "A" → нижний регистр "a".
    v2 = normalize_verdict({"equivalent": 1, "canonical": "A", "confidence": -1})
    assert v2["canonical"] == "a"
    assert v2["confidence"] == 0.0
    assert v2["equivalent"] is True

    # Невалидный/отсутствующий confidence → 0.0, equivalent по умолчанию False.
    v3 = normalize_verdict({"confidence": "not-a-number"})
    assert v3["confidence"] == 0.0
    assert v3["equivalent"] is False
    assert v3["canonical"] == "b"
