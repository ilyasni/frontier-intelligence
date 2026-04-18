"""Парсинг JSON из ответов LLM."""
import pytest

from worker.llm_json import extract_balanced_json_object, parse_llm_json_object, strip_code_fences


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
