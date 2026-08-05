"""Мосты между воркспейсами: чтение `workspaces.cross_workspace_bridges` ради поведения.

Поле было заполнено у пяти воркспейсов из шести, имело три пути записи и редактор в
админке — и ни одного чтения ради поведения. Оно только попадало в ответ
`list_workspaces`/`get_workspace_overview`, то есть модель-клиент читала обещание
поведения, которого нет.

Здесь проверяется ровно то, что теперь это поведение есть и что оно не врёт:

1. `include_bridges=False` — набор воркспейсов не расширяется и в БД никто не ходит
   (поведение существующих клиентов не меняется, ответ прежний);
2. `include_bridges=True` — набор расширяется РОВНО объявленными мостами;
3. каждый результат несёт воркспейс происхождения, и чужой отличим от своего — без
   этого мосты включать нельзя вообще: `disruption` начнёт молча получать сигналы
   `ai_trends`, и пользователь не отличит своё от чужого;
4. мост на несуществующий воркспейс не роняет запрос (строка в БД переживает
   удаление воркспейса, чужая опечатка не должна ронять поиск);
5. схема `inputSchema` в `mcp/server.py` совпадает с обёрткой в `mcp/mcp_gateway.py` —
   рассинхрон этих двух поверхностей даёт расхождение между Claude Code и Desktop.
"""

from __future__ import annotations

import ast
import copy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from mcp import bridges
from mcp.tools import search_frontier as sf
from shared.search_contracts import SearchRequest

REPO = Path(__file__).resolve().parents[1]

# Слаги из config/workspaces.yml (их читает allowlist в mcp/guards.py) плюс один
# намеренно несуществующий: он и есть проверка пункта 4.
OWN = "disruption"
BRIDGE = "ai_trends"
GHOST = "ghost_workspace_that_never_existed"


def _rows(mapping: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"id": key, "cross_workspace_bridges": value} for key, value in mapping.items()]


def _patch_bridge_rows(monkeypatch, mapping: dict[str, Any]) -> dict[str, int]:
    """Подменить чтение БД. Возвращает счётчик вызовов — им проверяется пункт 1."""
    calls = {"count": 0}

    async def _fetch(workspace_ids: list[str]) -> list[dict[str, Any]]:
        calls["count"] += 1
        return [row for row in _rows(mapping) if row["id"] in workspace_ids]

    monkeypatch.setattr(bridges, "_fetch_bridge_rows", _fetch)
    return calls


# ── 1. Выключено по умолчанию ───────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_include_bridges_false_does_not_expand_and_does_not_read_db(monkeypatch) -> None:
    """Путь по умолчанию: набор равен запрошенному и запроса в БД нет ни одного.

    Проверяется не только результат, но и счётчик обращений: расширение, которое
    отфильтровали постфактум, всё равно стоило бы запроса в PostgreSQL на каждый поиск.
    """
    calls = _patch_bridge_rows(monkeypatch, {OWN: [BRIDGE]})

    effective, meta = await bridges.resolve_bridge_workspaces([OWN], include_bridges=False)

    assert effective == [OWN]
    assert meta["bridged"] == []
    assert meta["include_bridges"] is False
    assert calls["count"] == 0


@pytest.mark.unit
def test_search_request_defaults_to_no_bridges() -> None:
    assert SearchRequest(query="ev").include_bridges is False


# ── 2. Включено — расширяется ровно объявленным ─────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_include_bridges_true_expands_by_declared_bridges_only(monkeypatch) -> None:
    """Расширение — ровно по колонке, и ни на слаг больше.

    `design` объявлен мостом у `ai_trends`, но `ai_trends` пришёл сюда сам мостом, а не
    запросом: транзитивного раскрытия нет и быть не должно — иначе один вызов у
    `disruption` разворачивается во все шесть воркспейсов разом.
    """
    _patch_bridge_rows(
        monkeypatch,
        {OWN: [BRIDGE, "design"], BRIDGE: ["design", "ai_research"]},
    )

    effective, meta = await bridges.resolve_bridge_workspaces([OWN], include_bridges=True)

    assert effective == [OWN, BRIDGE, "design"]
    assert meta["bridged"] == [BRIDGE, "design"]
    assert "ai_research" not in effective
    assert meta["skipped_unknown"] == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_requested_workspaces_are_never_dropped_or_duplicated(monkeypatch) -> None:
    _patch_bridge_rows(monkeypatch, {OWN: [BRIDGE, OWN], BRIDGE: [OWN]})

    effective, _meta = await bridges.resolve_bridge_workspaces(
        [OWN, BRIDGE], include_bridges=True
    )

    assert effective == [OWN, BRIDGE]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, []),
        ([], []),
        (["ai_trends", " design "], ["ai_trends", "design"]),
        ('["ai_trends", "design"]', ["ai_trends", "design"]),
        ("ai_trends, design", ["ai_trends", "design"]),
        ({"targets": ["ai_trends"]}, ["ai_trends"]),
        (42, []),
    ],
)
def test_normalize_bridge_value_handles_every_write_path(raw: object, expected: list[str]) -> None:
    """Путей записи в колонку три (bootstrap, PATCH, редактор), формы приезжают разные."""
    assert bridges.normalize_bridge_value(raw) == expected


# ── 3. Атрибуция происхождения ──────────────────────────────────────────────


def _hit(doc_id: str, score: float) -> dict[str, Any]:
    return {
        "id": doc_id,
        "score": score,
        "payload": {"post_id": doc_id, "title": doc_id, "source_id": f"src-{doc_id}"},
    }


class _FakeQdrant:
    """Qdrant, отдающий свой набор хитов на каждый воркспейс, и считающий вызовы."""

    def __init__(self, by_workspace: dict[str, list[dict[str, Any]]]) -> None:
        self._by_workspace = by_workspace
        self.searched: list[str] = []

    async def hybrid_search(
        self,
        vector: list[float],
        workspace: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.searched.append(workspace)
        # Копия: run_search_request правит список и сами хиты на месте.
        return copy.deepcopy(self._by_workspace.get(workspace, []))

    async def close(self) -> None:
        return None


class _ForbiddenSearxng:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("SearXNG must not be reached from run_search_request in this test")


async def _run_search(
    monkeypatch,
    *,
    include_bridges: bool,
    by_workspace: dict[str, list[dict[str, Any]]],
    bridge_map: dict[str, Any],
    limit: int = 10,
) -> tuple[dict[str, Any], _FakeQdrant]:
    """`run_search_request` целиком; наружу не ходит ничего: эмбеддер, Qdrant, БД, SearXNG."""
    fake = _FakeQdrant(by_workspace)
    _patch_bridge_rows(monkeypatch, bridge_map)

    async def _embedding(query: str, settings: Any) -> list[float]:
        return [0.1, 0.2]

    async def _source_scores(source_ids: set[str]) -> dict[str, dict[str, float]]:
        return {}

    settings = SimpleNamespace(
        gigachat_embeddings_model="EmbeddingsGigaR",
        searxng_enabled=False,
        own_stake_enabled=False,
    )
    monkeypatch.setattr(sf, "get_settings", lambda: settings)
    monkeypatch.setattr(sf, "_get_embedding", _embedding)
    monkeypatch.setattr(sf, "_load_source_scores", _source_scores)
    monkeypatch.setattr(sf, "QdrantFrontierClient", lambda: fake)
    monkeypatch.setattr(sf, "SearXNGClient", _ForbiddenSearxng)

    response = await sf.run_search_request(
        SearchRequest(
            query="assisted driving handover",
            workspace=OWN,
            limit=limit,
            include_bridges=include_bridges,
        )
    )
    return response, fake


@pytest.mark.unit
@pytest.mark.asyncio
async def test_search_without_bridges_queries_own_workspace_only(monkeypatch) -> None:
    """Инвариант 1 на уровне выдачи: один запрос в Qdrant и ни одного нового ключа.

    Ключи `origin_workspace`/`bridged`/`bridges` не появляются вовсе: ответ при
    выключенных мостах обязан остаться прежним, иначе сравнение двух выдач перестаёт
    быть честной проверкой.
    """
    response, fake = await _run_search(
        monkeypatch,
        include_bridges=False,
        by_workspace={OWN: [_hit("own-1", 0.5)], BRIDGE: [_hit("bridge-1", 0.9)]},
        bridge_map={OWN: [BRIDGE]},
    )

    assert fake.searched == [OWN]
    assert [hit["id"] for hit in response["results"]] == ["own-1"]
    assert "bridges" not in response
    assert "include_bridges" not in response["applied_filters"]
    for hit in response["results"]:
        assert "origin_workspace" not in hit
        assert "bridged" not in hit


@pytest.mark.unit
@pytest.mark.asyncio
async def test_every_result_carries_origin_workspace_and_bridged_flag(monkeypatch) -> None:
    """Условие, без которого мосты делать было нельзя.

    Каждый результат несёт воркспейс происхождения, и результат из моста отличим от
    своего плоским булевым флагом — не по косвенным признакам вроде payload.workspace_id,
    которого в выдаче может не оказаться.
    """
    response, fake = await _run_search(
        monkeypatch,
        include_bridges=True,
        by_workspace={
            OWN: [_hit("own-1", 0.50), _hit("own-2", 0.20)],
            BRIDGE: [_hit("bridge-1", 0.90), _hit("bridge-2", 0.30)],
        },
        bridge_map={OWN: [BRIDGE]},
    )

    assert fake.searched == [OWN, BRIDGE]
    results = response["results"]
    # Ни одного результата без атрибуции.
    assert all(hit.get("origin_workspace") for hit in results)
    origin_by_id = {hit["id"]: hit["origin_workspace"] for hit in results}
    assert origin_by_id == {
        "own-1": OWN,
        "own-2": OWN,
        "bridge-1": BRIDGE,
        "bridge-2": BRIDGE,
    }
    bridged_by_id = {hit["id"]: hit["bridged"] for hit in results}
    assert bridged_by_id == {
        "own-1": False,
        "own-2": False,
        "bridge-1": True,
        "bridge-2": True,
    }
    # Своё от чужого отличимо и на уровне ответа целиком.
    assert response["bridges"]["bridged"] == [BRIDGE]
    assert response["bridges"]["per_workspace_hits"] == {OWN: 2, BRIDGE: 2}
    assert response["applied_filters"]["include_bridges"] is True
    assert response["applied_filters"]["workspaces"] == [OWN, BRIDGE]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_limit_caps_the_merged_result_set(monkeypatch) -> None:
    """`limit` — потолок ВСЕЙ выдачи, а не потолок на воркспейс.

    Иначе включение мостов у `disruption` (четыре моста) молча превращает limit=10 в 50.
    Порядок — по `score`, воркспейсы для этого сравнимы: коллекция в Qdrant одна,
    изоляция — payload-фильтром.
    """
    response, _fake = await _run_search(
        monkeypatch,
        include_bridges=True,
        by_workspace={
            OWN: [_hit("own-1", 0.40), _hit("own-2", 0.10)],
            BRIDGE: [_hit("bridge-1", 0.90), _hit("bridge-2", 0.80)],
        },
        bridge_map={OWN: [BRIDGE]},
        limit=3,
    )

    assert [hit["id"] for hit in response["results"]] == ["bridge-1", "bridge-2", "own-1"]
    assert response["bridges"]["per_workspace_hits"] == {OWN: 1, BRIDGE: 2}


# ── 4. Неизвестный мост не роняет запрос ────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
async def test_unknown_bridge_target_is_skipped_not_fatal(monkeypatch) -> None:
    """Мост на несуществующий воркспейс: пропускаем и записываем, а не 400/500.

    Строка в БД переживает удаление или переименование воркспейса, а редактор мостов
    в админке ничего не валидирует. Поиск не должен зависеть от чужой опечатки.
    """
    _patch_bridge_rows(monkeypatch, {OWN: [GHOST, BRIDGE]})

    effective, meta = await bridges.resolve_bridge_workspaces([OWN], include_bridges=True)

    assert effective == [OWN, BRIDGE]
    assert meta["skipped_unknown"] == [GHOST]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_search_survives_unknown_bridge_target(monkeypatch) -> None:
    response, fake = await _run_search(
        monkeypatch,
        include_bridges=True,
        by_workspace={OWN: [_hit("own-1", 0.5)]},
        bridge_map={OWN: [GHOST]},
    )

    assert fake.searched == [OWN]
    assert [hit["id"] for hit in response["results"]] == ["own-1"]
    assert response["bridges"]["skipped_unknown"] == [GHOST]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_db_failure_degrades_instead_of_raising(monkeypatch) -> None:
    """Мосты — расширение выдачи, а не её условие: недоступная БД даёт degraded, не 500."""

    async def _boom(workspace_ids: list[str]) -> list[dict[str, Any]]:
        raise RuntimeError("postgres is down")

    monkeypatch.setattr(bridges, "_fetch_bridge_rows", _boom)

    effective, meta = await bridges.resolve_bridge_workspaces([OWN], include_bridges=True)

    assert effective == [OWN]
    assert meta["degraded"] is True
    assert meta["bridged"] == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_missing_workspace_row_yields_no_bridges(monkeypatch) -> None:
    """Воркспейс без строки в БД (или с NULL в колонке) — просто ноль мостов."""
    _patch_bridge_rows(monkeypatch, {})

    effective, meta = await bridges.resolve_bridge_workspaces([OWN], include_bridges=True)

    assert effective == [OWN]
    assert meta["bridges_by_workspace"] == {OWN: []}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bridge_fan_out_is_capped(monkeypatch) -> None:
    """Потолок на добавленные мостами воркспейсы: стоимость вызова ограничена сверху."""
    allowed = sorted(bridges.get_workspace_allowlist() - {OWN})
    monkeypatch.setattr(bridges, "MAX_BRIDGED_WORKSPACES", 2)
    _patch_bridge_rows(monkeypatch, {OWN: allowed})

    effective, meta = await bridges.resolve_bridge_workspaces([OWN], include_bridges=True)

    assert effective[0] == OWN
    assert len(meta["bridged"]) == 2
    assert len(effective) == 3


# ── 5. Схема REST и обёртка шлюза не разъезжаются ───────────────────────────
#
# Разбор по AST, а не импортом: mcp/server.py тянет весь стек (Qdrant, Redis, роутеры),
# а проверяется здесь текст объявления, а не рантайм.

SERVER = REPO / "mcp" / "server.py"
GATEWAY = REPO / "mcp" / "mcp_gateway.py"
BRIDGE_AWARE_TOOLS = ("search_frontier", "get_frontier_brief")


def _const_key_map(node: ast.Dict) -> dict[str, ast.AST]:
    mapping: dict[str, ast.AST] = {}
    for key, value in zip(node.keys, node.values):
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            mapping[key.value] = value
    return mapping


def _server_tool_properties(tool_name: str) -> set[str]:
    tree = ast.parse(SERVER.read_text(encoding="utf-8"), filename=str(SERVER))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        entry = _const_key_map(node)
        name_node = entry.get("name")
        if not (isinstance(name_node, ast.Constant) and name_node.value == tool_name):
            continue
        schema = entry.get("inputSchema")
        if not isinstance(schema, ast.Dict):
            continue
        properties = _const_key_map(schema).get("properties")
        if not isinstance(properties, ast.Dict):
            continue
        return set(_const_key_map(properties))
    raise AssertionError(f"tool {tool_name} not found in {SERVER}")


def _gateway_tool_surface(tool_name: str) -> tuple[set[str], set[str]]:
    """(имена параметров обёртки, ключи, которые обёртка форвардит в REST)."""
    tree = ast.parse(GATEWAY.read_text(encoding="utf-8"), filename=str(GATEWAY))
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        if node.name != tool_name:
            continue
        params = {arg.arg for arg in [*node.args.args, *node.args.kwonlyargs]}
        forwarded: set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Dict):
                forwarded |= set(_const_key_map(sub))
        return params, forwarded
    raise AssertionError(f"wrapper {tool_name} not found in {GATEWAY}")


@pytest.mark.unit
def test_schema_extractors_are_not_silently_empty() -> None:
    """Страховка от тихого нуля: сломанный разбор дал бы пустые множества."""
    assert len(_server_tool_properties("search_frontier")) >= 8
    params, forwarded = _gateway_tool_surface("search_frontier")
    assert len(params) >= 8 and len(forwarded) >= 8


@pytest.mark.unit
@pytest.mark.parametrize("tool_name", BRIDGE_AWARE_TOOLS)
def test_include_bridges_declared_on_both_surfaces(tool_name: str) -> None:
    """Описание в REST и обёртка в шлюзе — две разные поверхности для одного клиента.

    Claude Code ходит через шлюз, Desktop видит описания из `GET /tools`; параметр,
    объявленный только в одном месте, даёт расхождение между ними.
    """
    assert "include_bridges" in _server_tool_properties(tool_name)
    params, forwarded = _gateway_tool_surface(tool_name)
    assert "include_bridges" in params, "обёртка шлюза не принимает include_bridges"
    assert "include_bridges" in forwarded, "обёртка шлюза не форвардит include_bridges в REST"


@pytest.mark.unit
@pytest.mark.parametrize("tool_name", BRIDGE_AWARE_TOOLS)
def test_server_schema_and_gateway_wrapper_agree(tool_name: str) -> None:
    """Полное совпадение набора параметров, а не только по новому полю."""
    server_props = _server_tool_properties(tool_name)
    params, forwarded = _gateway_tool_surface(tool_name)
    assert server_props == params, (
        f"{tool_name}: inputSchema в mcp/server.py и обёртка в mcp/mcp_gateway.py разошлись. "
        f"только в REST: {sorted(server_props - params)}; только в шлюзе: {sorted(params - server_props)}"
    )
    assert server_props <= forwarded, (
        f"{tool_name}: шлюз принимает параметры, но не форвардит их в REST: "
        f"{sorted(server_props - forwarded)}"
    )
