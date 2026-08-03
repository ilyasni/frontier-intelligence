"""Парсинг JSON из ответов LLM."""
import json

import pytest

from worker.llm_json import (
    extract_balanced_json_object,
    parse_llm_json_object,
    repair_truncated_json_object,
    strip_code_fences,
)


def test_strip_code_fences_json() -> None:
    raw = 'Вот ответ:\n```json\n{"score": 0.5}\n```\n'
    assert strip_code_fences(raw).strip().startswith('{"score"')


def test_extract_balanced_nested() -> None:
    s = 'prefix {"a": {"b": 1}, "c": "}"} tail'
    blob = extract_balanced_json_object(s)
    assert blob is not None
    assert parse_llm_json_object(s)["a"]["b"] == 1


def test_parse_trailing_comma() -> None:
    raw = '{"score": 1.0, "category": "x", }'
    d = parse_llm_json_object(raw)
    assert d["score"] == 1.0


def test_parse_no_json_raises() -> None:
    with pytest.raises(ValueError, match="no JSON"):
        parse_llm_json_object("только текст без скобок")


# --- Обрыв ответа по лимиту токенов (регрессия 2026-08-02) ------------------
#
# missing_signals с max_tokens=700 при потребности в 725 получал ответ без
# закрывающих скобок и фенса. Строгий extract_balanced_json_object возвращал
# None, весь ответ терялся, workspace молча уезжал на шаблонные темы.

TRUNCATED = (
    '```json\n'
    '{\n  "topics": [\n'
    '    {"topic": "AI Energy", "query": "power consumption", "category": "technology"},\n'
    '    {"topic": "Agent Safety", "query": "guardrails", "category": "science"},\n'
    '    {"topic": "Legal Liability", "query": "\\"AI agent liab'
)


def test_strict_extractor_still_rejects_truncated() -> None:
    """Контракт строгого извлечения не меняем — оно по-прежнему возвращает None."""
    assert extract_balanced_json_object(TRUNCATED) is None


def test_repair_recovers_complete_prefix() -> None:
    """Незавершённый хвост отбрасывается, завершённые элементы сохраняются."""
    blob = repair_truncated_json_object(TRUNCATED)
    assert blob is not None

    data = json.loads(blob)
    topics = data["topics"]
    assert len(topics) == 2, "должны выжить только полностью завершённые элементы"
    assert topics[0]["topic"] == "AI Energy"
    assert topics[1]["topic"] == "Agent Safety"


def test_parse_llm_json_object_uses_repair_transparently() -> None:
    """Публичный парсер спасает оборванный ответ вместо ValueError."""
    data = parse_llm_json_object(TRUNCATED)
    assert [t["topic"] for t in data["topics"]] == ["AI Energy", "Agent Safety"]


def test_repair_handles_truncation_inside_string() -> None:
    """Обрыв прямо посреди строкового значения не должен давать битый JSON."""
    raw = '{"a": 1, "b": "незакрытая строка обрывается здесь'
    blob = repair_truncated_json_object(raw)
    assert blob is not None
    assert json.loads(blob) == {"a": 1}


def test_repair_returns_none_when_nothing_complete() -> None:
    """Если не завершён ни один элемент, спасать нечего."""
    assert repair_truncated_json_object('{"topics": [{"topic": "обрыв') is None


def test_repair_does_not_touch_valid_json() -> None:
    """Валидный ответ идёт строгим путём, до ремонта дело не доходит."""
    raw = '{"topics": [{"topic": "ok"}]}'
    assert extract_balanced_json_object(raw) is not None
    assert parse_llm_json_object(raw)["topics"][0]["topic"] == "ok"
