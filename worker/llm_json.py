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


def _close_open_containers(fragment: str) -> str | None:
    """Дописать закрывающие скобки к фрагменту. None — если это невозможно."""
    stack: list[str] = []
    in_string = False
    escape = False
    for ch in fragment:
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
        elif ch == "{":
            stack.append("}")
        elif ch == "[":
            stack.append("]")
        elif ch in "}]":
            if not stack:
                return None
            stack.pop()
    if in_string or not stack:
        return None
    return fragment + "".join(reversed(stack))


def _cut_candidates(body: str) -> tuple[list[int], list[int]]:
    """Позиции обрыва: (после закрытых контейнеров, на запятых), от поздних к ранним."""
    closes: list[int] = []
    commas: list[int] = []
    in_string = False
    escape = False
    for i, ch in enumerate(body):
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
        elif ch in "}]":
            closes.append(i + 1)
        elif ch == ",":
            commas.append(i)
    return list(reversed(closes)), list(reversed(commas))


def repair_truncated_json_object(text: str) -> str | None:
    """Спасти объект из ответа, оборванного на середине (обычно по max_tokens).

    Модель, упёршаяся в лимит, не закрывает ни скобки, ни markdown-фенс, поэтому
    строгий extract_balanced_json_object возвращает None и весь ответ теряется.
    Отбрасываем незавершённый хвост до последней границы элемента и закрываем
    стек — полезная часть выборки сохраняется.

    Реальный случай: missing_signals с max_tokens=700 при потребности в 725
    молча деградировал к заглушке на двух workspace.
    """
    s = strip_code_fences(text)
    start = s.find("{")
    if start < 0:
        return None
    body = s[start:]
    closes, commas = _cut_candidates(body)
    # Сначала режем по закрытым контейнерам: так выживают только те элементы,
    # которые модель успела дописать целиком. Обрыв по запятой оставил бы
    # полузаполненный объект ({"topic": ...} без query/category), который дальше
    # по пайплайну неотличим от полноценного. Запятые — запасной вариант для
    # плоских объектов, где закрывать нечего.
    for cut in (*closes, *commas):
        candidate = body[:cut].rstrip().rstrip(",").rstrip()
        closed = _close_open_containers(candidate)
        if closed is None:
            continue
        try:
            parsed = json.loads(closed)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return closed
    return None


def parse_llm_json_object(text: str) -> dict[str, Any]:
    """
    Парсит первый JSON-объект из ответа.
    Raises:
        ValueError: нет объекта или JSON невалиден после мягкой правки.
    """
    blob = extract_balanced_json_object(text)
    if not blob:
        # Ответ мог быть оборван по лимиту токенов — пробуем спасти префикс,
        # иначе теряется вся выборка целиком.
        blob = repair_truncated_json_object(text)
        if blob:
            logger.warning(
                "llm_json_truncated_response_repaired chars=%d recovered=%d — "
                "ответ модели оборван (вероятно max_tokens), спасён префикс",
                len(text or ""),
                len(blob),
            )
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
