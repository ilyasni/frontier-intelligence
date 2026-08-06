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


# ── Сверка по ПАРАМЕТРАМ, а не только по именам ──────────────────────────────
#
# Заведено 2026-08-06. Проверки выше сравнивают множества ИМЁН инструментов и
# работают: 32 = 32. Но контракт объявлен трижды — pydantic-модель в mcp/tools/,
# рукописный inputSchema в mcp/server.py и сигнатура обёртки шлюза, — а сверяется
# одна ось из трёх. По второй он уже разъехался и никто этого не видел: поле
# `workspace` было объявлено в четырёх Request-моделях как гвард изоляции
# воркспейсов, а в шлюз не выведено. Единственный клиент ходит через шлюз, значит
# прислать он мог только None, а при None гвард делает ранний return. Защита была
# написана, снабжена комментарием и задеплоена — и не срабатывала ни разу.

# Параметры обёрток, которых нет в модели СОЗНАТЕЛЬНО. Пусто — и это решение,
# а не случайность: каждое исключение обязано быть записано здесь с причиной.
GATEWAY_ONLY_PARAMS: dict[str, set[str]] = {}

# Поля модели, которые сознательно не выводятся в шлюз.
MODEL_ONLY_FIELDS: dict[str, set[str]] = {}


def _request_models() -> dict[str, set[str]]:
    """Имя инструмента → множество полей его Request-модели.

    Связь «инструмент ↔ модель» берётся из аннотации единственного аргумента
    хендлера (`async def get_cluster_details(req: ClusterDetailsRequest)`),
    а не из совпадения имён: имена моделей и инструментов не совпадают.
    """
    models: dict[str, set[str]] = {}
    for path in sorted(TOOLS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

        fields: dict[str, set[str]] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            names = {
                stmt.target.id
                for stmt in node.body
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)
            }
            if names:
                fields[node.name] = names

        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
                continue
            args = node.args.args
            if len(args) != 1 or args[0].annotation is None:
                continue
            annotation = args[0].annotation
            if not isinstance(annotation, ast.Name):
                continue
            if annotation.id in fields:
                models[node.name] = fields[annotation.id]
    return models


def _gateway_params() -> dict[str, set[str]]:
    """Имя инструмента шлюза → множество имён его параметров."""
    tree = ast.parse(GATEWAY.read_text(encoding="utf-8"), filename=str(GATEWAY))
    params: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        if node.name not in _gateway_tool_names():
            continue
        args = node.args
        params[node.name] = {
            a.arg for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)
        }
    return params


def test_parameter_extraction_is_not_vacuous() -> None:
    """Страховка: разбор обязан что-то найти, иначе сверка ниже сравнивает пустоты."""
    models = _request_models()
    params = _gateway_params()
    assert len(models) >= 20, f"разобрано моделей: {len(models)}"
    assert len(params) >= 30, f"разобрано обёрток: {len(params)}"
    assert len(set(models) & set(params)) >= 20, "инструменты не сматчились по именам"


def test_gateway_signature_matches_the_request_model() -> None:
    """Обёртка шлюза обязана принимать те же поля, что и модель REST-хендлера.

    Проверка только для инструментов, у которых хендлер принимает ОДНУ
    pydantic-модель: остальные (например, принимающие примитивы) сверять нечем.
    """
    models = _request_models()
    params = _gateway_params()

    drift: dict[str, dict[str, list[str]]] = {}
    for tool, fields in sorted(models.items()):
        if tool not in params:
            continue  # отсутствие инструмента в шлюзе ловят проверки выше
        missing = fields - params[tool] - MODEL_ONLY_FIELDS.get(tool, set())
        extra = params[tool] - fields - GATEWAY_ONLY_PARAMS.get(tool, set())
        if missing or extra:
            drift[tool] = {"нет в шлюзе": sorted(missing), "лишнее в шлюзе": sorted(extra)}

    assert not drift, (
        "сигнатуры обёрток шлюза разошлись с Request-моделями REST: "
        f"{drift}. Поле, объявленное в модели и не выведенное в шлюз, клиент "
        "прислать не может — именно так гвард изоляции воркспейсов простоял "
        "мёртвым. Если расхождение осознанное, впиши его в GATEWAY_ONLY_PARAMS "
        "или MODEL_ONLY_FIELDS с причиной."
    )


# ── Отказ обязан доезжать до клиента с причиной ──────────────────────────────


def _load_detail_helper():
    """Достаёт `_raise_for_status_with_detail` из шлюза, не импортируя модуль.

    Импортировать `mcp.mcp_gateway` из тестов нельзя: каталог репозитория тоже
    называется `mcp`, и `from mcp.server.fastmcp import FastMCP` внутри модуля
    ушёл бы в наш `mcp/server.py`. Сам шлюз обходит это строкой
    `sys.path.insert(0, "/app")`, но в тестах такого пути нет. Поэтому берём
    ровно одну функцию из AST и исполняем её с настоящим httpx — проверяется
    рабочий код, а не его копия.
    """
    import httpx

    tree = ast.parse(GATEWAY.read_text(encoding="utf-8"), filename=str(GATEWAY))
    node = next(
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "_raise_for_status_with_detail"
    )
    namespace: dict[str, object] = {"httpx": httpx}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(GATEWAY), "exec"), namespace)
    return namespace["_raise_for_status_with_detail"]


def _response(status: int, payload: object) -> "object":
    import httpx

    return httpx.Response(
        status_code=status,
        json=payload,
        request=httpx.Request("POST", "http://mcp:8100/tools/list_clusters"),
    )


def test_validation_error_detail_reaches_the_caller() -> None:
    """422 от FastAPI приходит СПИСКОМ, и раньше он проваливался мимо разбора.

    `isinstance(raw, str)` на списке не срабатывал, управление уходило в голый
    `raise_for_status`, и автор видел «Client error '422 Unprocessable Entity'»
    без единого признака, какое поле не понравилось. Случай не экзотический:
    45 полей моделей несут ge/le/min_length, ни одно из этих ограничений не
    доезжает до клиента в схеме, поэтому 422 — нормальный исход обычного вызова.
    """
    import httpx

    raise_for_status_with_detail = _load_detail_helper()
    response = _response(
        422,
        {
            "detail": [
                {"loc": ["body", "limit"], "msg": "Input should be less than or equal to 100", "type": "less_than_equal"},
                {"loc": ["body", "cards", 0, "entity_id"], "msg": "Field required", "type": "missing"},
            ]
        },
    )

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        raise_for_status_with_detail(response)

    message = str(excinfo.value)
    assert "limit: Input should be less than or equal to 100" in message
    assert "cards.0.entity_id: Field required" in message
    # `body` — это про транспорт, а не про поле; в подсказке автору он лишний.
    assert "body" not in message


def test_plain_string_detail_still_works() -> None:
    """Регрессия на прежнее поведение: строковый detail должен доезжать как раньше."""
    import httpx

    raise_for_status_with_detail = _load_detail_helper()
    response = _response(400, {"detail": "workspace is not bootstrapped"})

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        raise_for_status_with_detail(response)
    assert "workspace is not bootstrapped" in str(excinfo.value)


def test_every_wrapper_routes_failures_through_the_detail_helper() -> None:
    """Ни одна обёртка не имеет права звать голый raise_for_status.

    До 06.08.2026 хелпер стоял в 11 обёртках из 32; у остальных 21 отказ приходил
    к автору без причины. Показательно, что жертвой был ровно тот инструмент,
    ради которого хелпер писали: record_card_feedback собирает карточки руками,
    промах по форме даёт 422 — а card_feedback до сих пор 0 строк.
    """
    source = GATEWAY.read_text(encoding="utf-8")
    bare = [
        line.strip()
        for line in source.splitlines()
        if line.strip().endswith("r.raise_for_status()")
    ]
    assert not bare, (
        f"обёртки зовут голый raise_for_status ({len(bare)} шт.) — отказ придёт "
        "к автору без причины. Единственное допустимое место — фолбэк внутри "
        "_raise_for_status_with_detail, и там переменная называется response."
    )
    # Все обёртки возвращаются через `_finish(r)`: он и считает метрики, и зовёт
    # разбор detail. Одна точка на пути каждого вызова — её нельзя забыть,
    # добавляя тридцать третью обёртку.
    assert source.count("return _finish(r)") >= 30, (
        "обёртки перестали возвращаться через _finish — вместе с ним пропали "
        "и счётчик вызовов, и разбор detail"
    )
