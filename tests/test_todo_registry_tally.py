"""
Сводная таблица реестра обязана сходиться с его телом.

Заведён 2026-08-06. Повод: шапка `docs/TODO-UNFINISHED.md` объявляла
«Сделано 27 · Частично 5 · Отложено 2 · Открыто 27» — это 61 пункт при заявленных
в той же строке 57 — и перечисляла в списке «сделано» 23 номера вместо 27, без
26 и 28, которые в теле помечены ✅.

Почему это стоит теста, а не аккуратности. Весь реестр ведётся против одного
класса — «механизм существует, выглядит рабочим и не может сработать». Инструмент
учёта этого класса сам разъехался с реальностью и никем не проверялся: чтобы
заметить, надо было сложить четыре числа руками. Ровно та же болезнь, только
в документе, а не в коде.

Проверка идёт от ТЕЛА к ШАПКЕ, а не наоборот: источник правды — пометка в
заголовке пункта, шапка обязана ей соответствовать.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REGISTRY = Path(__file__).resolve().parents[1] / "docs" / "TODO-UNFINISHED.md"

# Пометка в заголовке пункта → статус. Порядок проверки важен: «✅ СДЕЛАНО» может
# соседствовать с зачёркнутым исходным заголовком, а «🔒 ОТЛОЖЕНО» — со словом
# СДЕЛАНО в тексте ниже, поэтому смотрим только на строку заголовка.
_MARKERS: tuple[tuple[str, str], ...] = (
    ("✅", "done"),
    ("🟡", "partial"),
    ("♻️", "regressed"),
    ("🔒", "deferred"),
    ("ОТЛОЖЕНО", "deferred"),
)

_ITEM_HEADER = re.compile(r"^### (\d+)\. (.*)$", re.MULTILINE)
# Строка сводной таблицы: | ✅ **Сделано** | 26 | 1, 2, 4, ... |
_SUMMARY_ROW = re.compile(r"^\|\s*(✅|🟡|🔒|♻️|⬜)[^|]*\|\s*(\d+)\s*\|", re.MULTILINE)
_TOTAL_DECLARED = re.compile(r"Всего пунктов \*\*(\d+)\*\*")

_SUMMARY_STATUS: dict[str, str] = {
    "✅": "done",
    "🟡": "partial",
    "🔒": "deferred",
    "♻️": "regressed",
    "⬜": "open",
}


@lru_cache(maxsize=1)
def _registry_text() -> str:
    return REGISTRY.read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def _body_statuses() -> dict[int, str]:
    """Номер пункта → статус по пометке в его заголовке.

    Пункты вида `### 10-bis.` в разбор не попадают намеренно: `-bis` — это
    сохранённая исходная формулировка, а не отдельный пункт.
    """
    statuses: dict[int, str] = {}
    for match in _ITEM_HEADER.finditer(_registry_text()):
        number = int(match.group(1))
        if number in statuses:  # первый заголовок побеждает; -bis идёт следом
            continue
        title = match.group(2)
        statuses[number] = next(
            (status for marker, status in _MARKERS if marker in title), "open"
        )
    return statuses


@lru_cache(maxsize=1)
def _summary_counts() -> dict[str, int]:
    """Разбирает ровно одну таблицу — ту, что идёт после заголовка с колонкой «Пунктов».

    Границы берём по строкам, а не поиском `---`: разделитель самой таблицы
    (`|---|---|---|`) содержит ту же подстроку, и срез по ней обрывался бы до
    первой строки данных, оставляя проверку вакуумной.
    """
    lines = _registry_text().splitlines()
    start = next(i for i, line in enumerate(lines) if "| Пунктов" in line)
    counts: dict[str, int] = {}
    for line in lines[start + 1 :]:
        if not line.startswith("|"):
            break
        match = _SUMMARY_ROW.match(line)
        if match:
            counts[_SUMMARY_STATUS[match.group(1)]] = int(match.group(2))
    return counts


def test_registry_body_is_parseable_at_all() -> None:
    """Страховка от вакуумной проверки: разбор обязан что-то найти.

    Без неё смена формата заголовков превратила бы все проверки ниже в
    сравнение двух пустых множеств — приём, уже принятый в соседних контрактах
    этого репозитория.
    """
    body = _body_statuses()
    assert len(body) >= 50, f"разобрано всего {len(body)} пунктов — сломался разбор заголовков"
    assert _summary_counts(), "сводная таблица не разобралась ни одной строкой"


def test_summary_counts_match_the_body() -> None:
    """Каждое число в шапке равно числу пунктов с этой пометкой в теле."""
    body = _body_statuses()
    actual: dict[str, int] = {}
    for status in body.values():
        actual[status] = actual.get(status, 0) + 1

    declared = _summary_counts()
    mismatch = {
        status: {"в шапке": count, "в теле": actual.get(status, 0)}
        for status, count in declared.items()
        if actual.get(status, 0) != count
    }
    assert not mismatch, (
        f"сводная таблица разошлась с телом реестра: {mismatch}. "
        "Источник правды — пометка в заголовке пункта; правь шапку."
    )


def test_declared_total_equals_the_sum_of_the_table() -> None:
    """«Всего пунктов N» обязано быть суммой строк таблицы, а не отдельным числом.

    Именно здесь и разъехалось: 27 + 5 + 2 + 27 = 61 при заявленных 57, и никто
    не складывал.
    """
    match = _TOTAL_DECLARED.search(_registry_text())
    assert match, "в шапке нет строки «Всего пунктов **N**»"
    declared_total = int(match.group(1))
    table_total = sum(_summary_counts().values())

    assert declared_total == table_total, (
        f"заявлено пунктов {declared_total}, а таблица суммируется в {table_total}"
    )
    assert declared_total == len(_body_statuses()), (
        f"заявлено пунктов {declared_total}, а в теле их {len(_body_statuses())}"
    )


def test_numbering_has_no_gaps() -> None:
    """Пункты нумеруются подряд: пропуск означает потерянную запись."""
    numbers = sorted(_body_statuses())
    expected = list(range(1, len(numbers) + 1))
    assert numbers == expected, (
        f"нумерация с дырами: не хватает {sorted(set(expected) - set(numbers))}, "
        f"лишние {sorted(set(numbers) - set(expected))}"
    )
