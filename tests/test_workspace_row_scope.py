"""
Скоуп воркспейса у инструментов, читающих строку по голому id (пункт 11 реестра).

Четыре читающих инструмента выбирали строку `WHERE id = :id` вообще без фильтра
по воркспейсу, а `get_signal_timeline` звал `assert_known_workspace` — то есть
проверял сам слаг — и следом тянул кластер тем же голым запросом. При этом
`workspace_id` в проекте объявлен обязательным полем в каждой таблице: изоляция
существовала в схеме и не существовала на поверхности доступа.

Отказ отдаётся как **404, а не 403**: 403 подтвердил бы, что объект с таким id
существует, и превратил бы гвард в оракул для перебора id. Клиент получает ровно
тот же ответ, что и для несуществующего идентификатора.

`workspace=None` пропускается намеренно — поле опционально ради обратной
совместимости с уже настроенными клиентами. Ужесточение до обязательного ломает
работающие вызовы и потому вынесено в отдельное решение.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from mcp.guards import assert_row_workspace

pytestmark = pytest.mark.unit


def test_matching_workspace_passes() -> None:
    assert_row_workspace({"id": "c1", "workspace_id": "disruption"}, "disruption") is None


def test_foreign_workspace_is_404_not_403() -> None:
    with pytest.raises(HTTPException) as exc:
        assert_row_workspace({"id": "c1", "workspace_id": "disruption"}, "design")
    assert exc.value.status_code == 404, (
        "403 подтверждает существование объекта и превращает гвард в оракул "
        "для перебора id — отказ обязан быть неотличим от «не найдено»"
    )


@pytest.mark.parametrize("workspace", [None, "", "   "])
def test_absent_workspace_is_backwards_compatible(workspace) -> None:
    """Клиенты, настроенные до появления поля, не должны сломаться."""
    assert_row_workspace({"id": "c1", "workspace_id": "disruption"}, workspace) is None


def test_missing_row_is_left_to_the_tool() -> None:
    """Отсутствие строки — это 404 самого инструмента, а не работа гварда."""
    assert_row_workspace(None, "design") is None


def test_row_without_workspace_column_does_not_pretend_to_pass() -> None:
    """Гвард, который не может проверить, не имеет права молча одобрить.

    Строка без `workspace_id` означает, что схема разъехалась или выборка не
    достала колонку. Пропускаем (иначе сломали бы работающие инструменты),
    но в лог обязано попасть предупреждение — иначе это тихая дыра.
    """
    assert_row_workspace({"id": "c1"}, "design") is None


def test_works_with_attribute_style_rows() -> None:
    """asyncpg-строки и Row-объекты приходят не только словарями."""

    class _Row:
        workspace_id = "disruption"

    assert_row_workspace(_Row(), "disruption") is None
    with pytest.raises(HTTPException):
        assert_row_workspace(_Row(), "design")


def test_every_id_lookup_tool_checks_the_row_workspace() -> None:
    """Структурная проверка: новый инструмент не должен тихо проехать мимо.

    Ловит ровно тот перекос, из-за которого пункт 11 и возник: гвард применялся
    в семи местах и не применялся в пяти, и заметить это можно было только
    вычитыванием файла целиком.
    """
    import ast
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "mcp" / "tools" / "observability.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    # Инструменты, которые достают строку по идентификатору и потому обязаны
    # проверять её принадлежность.
    id_lookup_tools = {
        "get_cluster_details",
        "get_cluster_evidence",
        "get_missing_signal_details",
        "get_signal_timeline",
    }
    problems = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef) or node.name not in id_lookup_tools:
            continue
        body = ast.dump(node)
        if "assert_row_workspace" not in body:
            problems.append(node.name)

    assert not problems, (
        f"инструменты читают строку по id и не проверяют её воркспейс: {problems}. "
        "Пока проверки нет, любой знающий id читает чужой воркспейс — при том, "
        "что workspace_id объявлен обязательным в каждой таблице."
    )
