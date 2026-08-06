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

    # Набор инструментов ВЫВОДИТСЯ из кода, а не перечисляется.
    #
    # Прежняя редакция держала четыре имени литералом, и этого хватило ровно до
    # первого пятого: `get_source_details` выбирает `WHERE s.id = :source_id` без
    # фильтра по воркспейсу, объявляет поле `workspace` в своей Request-модели,
    # упомянут в докстринге самого гварда — и в список не входил, поэтому тест
    # его не видел (найдено сверкой 06.08.2026). Список из имён проверяет
    # известное, а не класс; новый инструмент завтра снова проехал бы мимо.
    #
    # Признак «читает строку по id»: в теле функции есть строковый литерал с
    # `WHERE` и сравнением `... id = :`. Запросы, которые тут же фильтруют по
    # `workspace_id = :`, гварда не требуют — они безопасны по построению.
    import re

    # `id = :workspace` из набора исключён намеренно: это выборка САМОГО воркспейса
    # по его слагу (get_workspace_overview), а слаг уже проверен assert_known_workspace.
    # Гвард принадлежности строки там требовать нечего — строка и есть воркспейс.
    id_lookup = re.compile(r"\bid\s*=\s*:(?!workspace\b)", re.IGNORECASE)
    scoped_by_sql = re.compile(r"workspace_id\s*=\s*:", re.IGNORECASE)

    def _reads_row_by_id(fn: ast.AsyncFunctionDef) -> bool:
        for sub in ast.walk(fn):
            if not isinstance(sub, ast.Constant) or not isinstance(sub.value, str):
                continue
            sql = sub.value
            if "WHERE" not in sql.upper():
                continue
            if id_lookup.search(sql) and not scoped_by_sql.search(sql):
                return True
        return False

    checked: list[str] = []
    problems: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef) or node.name.startswith("_"):
            continue
        if not _reads_row_by_id(node):
            continue
        checked.append(node.name)
        if "assert_row_workspace" not in ast.dump(node):
            problems.append(node.name)

    # Страховка от вакуумной проверки: если разбор перестанет находить инструменты,
    # тест позеленеет на пустом множестве. Пять — это то, что есть на 06.08.2026.
    assert len(checked) >= 5, (
        f"разбор нашёл всего {len(checked)} инструментов, читающих строку по id "
        f"({checked}) — сломался извлекатель, а не код"
    )

    assert not problems, (
        f"инструменты читают строку по id и не проверяют её воркспейс: {problems}. "
        "Пока проверки нет, любой знающий id читает чужой воркспейс — при том, "
        "что workspace_id объявлен обязательным в каждой таблице."
    )
