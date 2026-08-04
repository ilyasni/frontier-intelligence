"""Контракт: набор инструментов в шлюзе не должен расходиться с REST.

Зачем этот тест. REST-сервер (mcp/server.py + mcp/tools/*.py) и шлюз
(mcp/mcp_gateway.py) — две поверхности, и вторая написана руками как обёртки
над первой. Они разъехались молча: на 04.08.2026 REST отдавал 32 инструмента,
шлюз — 22, и недостающие десять составляли ВЕСЬ контур одобрения RSI. Клиент
ходит только через шлюз, поэтому 86 предложений слияния висели pending с 07.07,
а у всех 26 911 relevance_decisions поле audit_status осталось NULL за всю
историю: кнопку «одобрить» было не на чем нажать.

Разошлось это не по злому умыслу, а потому что ничто не проверяло совпадение.
Тест закрывает именно это: добавил инструмент в REST — обязан добавить в шлюз,
иначе клиент его не увидит и никто об этом не узнает.

Разбор идёт по AST, а не регулярками: декораторы многострочные
(@mcp.tool(description=(...))), и текстовый поиск их пропускает — на этом
я и ошибся при первом подсчёте.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
GATEWAY = REPO / "mcp" / "mcp_gateway.py"
TOOLS_DIR = REPO / "mcp" / "tools"

# Инструменты, которых в шлюзе нет СОЗНАТЕЛЬНО. Пусто — и это не случайность:
# любое исключение должно быть решением с причиной, записанной здесь.
DELIBERATELY_NOT_IN_GATEWAY: set[str] = set()


def _decorator_calls(node: ast.AST) -> list[ast.Call]:
    return [d for d in getattr(node, "decorator_list", []) if isinstance(d, ast.Call)]


def _rest_tool_names() -> set[str]:
    """Имена REST-инструментов.

    Схем регистрации ДВЕ, и обе надо учесть — на этом легко ошибиться:

    1. Роутер монтируется с общим префиксом `/tools`, а имя инструмента стоит
       в самом маршруте: `@router.post("/get_graph_health")`.
       Так сделаны observability, threshold_proposals, graph_health, editorial.
    2. Роутер монтируется с ПОИМЁННЫМ префиксом `/tools/search_frontier`,
       а маршрут внутри модуля пустой: `@router.post("")`.
       Так сделаны семь поисковых/графовых инструментов и ingest_url.

    Разбирать импорты server.py, чтобы связать переменную роутера с модулем,
    не нужно: достаточно объединить имена из маршрутов (схема 1) с именами
    из префиксов вида `/tools/<name>` (схема 2) — это ровно то множество,
    которое отдаёт живой `GET /tools`.
    """
    names: set[str] = set()

    # Схема 1: имя в маршруте.
    for path in sorted(TOOLS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
                continue
            for call in _decorator_calls(node):
                func = call.func
                if not (isinstance(func, ast.Attribute) and func.attr == "post"):
                    continue
                if not (isinstance(func.value, ast.Name) and func.value.id == "router"):
                    continue
                if call.args and isinstance(call.args[0], ast.Constant):
                    route = str(call.args[0].value).lstrip("/")
                    if route:  # пустой маршрут — это схема 2, имя берём из префикса
                        names.add(route)

    # Схема 2: имя в префиксе include_router.
    server = REPO / "mcp" / "server.py"
    tree = ast.parse(server.read_text(encoding="utf-8"), filename=str(server))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "include_router"):
            continue
        for kw in node.keywords:
            if kw.arg != "prefix" or not isinstance(kw.value, ast.Constant):
                continue
            prefix = str(kw.value.value)
            if prefix.startswith("/tools/"):
                tail = prefix[len("/tools/") :].strip("/")
                if tail:
                    names.add(tail)
    return names


def _gateway_tool_names() -> set[str]:
    """Имена инструментов шлюза: @mcp.tool(...) над async def."""
    tree = ast.parse(GATEWAY.read_text(encoding="utf-8"), filename=str(GATEWAY))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        for call in _decorator_calls(node):
            func = call.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr == "tool"
                and isinstance(func.value, ast.Name)
                and func.value.id == "mcp"
            ):
                names.add(node.name)
    return names


@pytest.mark.unit
def test_extractors_find_something() -> None:
    """Страховка от тихого нуля: сломанный разбор дал бы пустые множества,
    и тест на равенство прошёл бы, ничего не проверив."""
    assert len(_rest_tool_names()) >= 20
    assert len(_gateway_tool_names()) >= 20


@pytest.mark.unit
def test_every_rest_tool_is_exposed_through_gateway() -> None:
    rest = _rest_tool_names()
    gateway = _gateway_tool_names()
    missing = rest - gateway - DELIBERATELY_NOT_IN_GATEWAY
    assert not missing, (
        "Инструменты есть в REST, но не выведены в шлюз, значит MCP-клиент их не увидит: "
        f"{sorted(missing)}. Добавь обёртку в mcp/mcp_gateway.py либо, если это осознанное "
        "решение, впиши имя в DELIBERATELY_NOT_IN_GATEWAY с причиной."
    )


@pytest.mark.unit
def test_gateway_exposes_nothing_that_rest_does_not_have() -> None:
    rest = _rest_tool_names()
    gateway = _gateway_tool_names()
    extra = gateway - rest
    assert not extra, (
        "В шлюзе объявлены инструменты, которых нет в REST — вызов уйдёт в 404: "
        f"{sorted(extra)}"
    )


@pytest.mark.unit
def test_rsi_approval_loop_is_reachable() -> None:
    """Отдельно и по именам — контур одобрения RSI.

    Общего сравнения множеств мало: если однажды кто-то решит вынести весь
    контур в DELIBERATELY_NOT_IN_GATEWAY, тест выше промолчит, а контур снова
    станет недостижимым — ровно то состояние, из которого его вытаскивали.
    """
    gateway = _gateway_tool_names()
    required = {
        "get_graph_health",
        "list_entity_merge_proposals",
        "approve_entity_merge",
        "reject_entity_merge",
        "list_threshold_proposals",
        "list_underrated_signals",
        "list_relevance_audit_sample",
        "mark_relevance_audit",
        "approve_threshold_change",
        "reject_threshold_change",
    }
    missing = required - gateway
    assert not missing, (
        f"Контур одобрения RSI снова недостижим через шлюз: {sorted(missing)}. "
        "Без этих инструментов предложения копятся в pending, а вердикт поставить нечем."
    )
