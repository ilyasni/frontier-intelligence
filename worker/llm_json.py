"""Извлечение JSON-объекта из ответа LLM (обход типичных отклонений от инструкции)."""
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def strip_code_fences(text: str) -> str:
    """Убирает обёртку ``` / ```json … ``` если модель всё же вернула markdown."""
    t = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", t, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return t


def extract_balanced_json_object(text: str) -> str | None:
    """
    Первый сбалансированный JSON-объект {...} с учётом строк в двойных кавычках.
    Надёжнее жадного regex \\{.*\\}, который ломается на преамбуле и fence-блоках.
    """
    s = strip_code_fences(text)
    start = s.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(s)):
        ch = s[i]
        if escape:
            escape = False
            continue
        if in_string:
            if ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[start : i + 1]
    return None


def parse_llm_json_object(text: str) -> dict[str, Any]:
    """
    Парсит первый JSON-объект из ответа.
    Raises:
        ValueError: нет объекта или JSON невалиден после мягкой правки.
    """
    blob = extract_balanced_json_object(text)
    if not blob:
        raise ValueError("no JSON object in response")
    try:
        out = json.loads(blob)
    except json.JSONDecodeError:
        # Хвостовые запятые — частая ошибка моделей
        blob2 = re.sub(r",\s*}", "}", blob)
        blob2 = re.sub(r",\s*]", "]", blob2)
        out = json.loads(blob2)
    if not isinstance(out, dict):
        raise ValueError(f"expected JSON object, got {type(out).__name__}")
    return out
