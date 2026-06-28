"""Тесты MCP человек-гейта Контура D+ (approve/reject слияния концептов графа).

Security-критично: только явный approve мутирует граф; reject и любой невалидный путь
граф не трогают. Проверяем guard keep/drop (фикс #3), переходы статусов, что Neo4j не
дёргается до прохождения проверок. Postgres и Neo4j мокаются.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from mcp.tools import graph_health as gh
from mcp.tools.graph_health import (
    ApproveEntityMergeRequest,
    GraphHealthRequest,
    ListEntityMergeProposalsRequest,
    RejectEntityMergeRequest,
)

pytestmark = pytest.mark.unit


class _Result:
    def __init__(self, first=None, rows=None, rowcount=0):
        self._first = first
        self._rows = rows or []
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._first

    def all(self):
        return self._rows


class _Session:
    def __init__(self, results):
        self._results = list(results)
        self.executed: list[tuple[str, dict]] = []
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, statement, params=None):
        self.executed.append((str(statement), dict(params or {})))
        return self._results.pop(0) if self._results else _Result()

    async def commit(self):
        self.committed = True


class _Neo:
    def __init__(self, merge_result=None, health=None, duplicates=None):
        self.merge_result = merge_result if merge_result is not None else {"merged": True, "canonical": "x"}
        self.health = health or {"concept_count": 10}
        self.duplicates = duplicates or []
        self.merge_calls: list[tuple] = []
        self.closed = False

    async def merge_concept_pair(self, ws, keep, drop):
        self.merge_calls.append((ws, keep, drop))
        return self.merge_result

    async def graph_health(self, ws):
        return self.health

    async def find_duplicate_entities(self, ws, limit=25):
        return self.duplicates

    async def close(self):
        self.closed = True


def _patch_session(monkeypatch, session):
    monkeypatch.setattr(gh, "get_engine", lambda: object())
    monkeypatch.setattr(gh, "AsyncSession", lambda _engine: session)
    return session


def _patch_neo(monkeypatch, neo):
    calls: list[int] = []

    def factory():
        calls.append(1)
        return neo

    monkeypatch.setattr(gh, "Neo4jFrontierClient", factory)
    return calls


def _row(canonical_norm="hmi", norm_a="hmi", norm_b="human machine interface", status="pending"):
    return {
        "workspace_id": "disruption",
        "norm_a": norm_a,
        "norm_b": norm_b,
        "canonical_norm": canonical_norm,
        "canonical_name": "HMI",
        "status": status,
    }


# ── keep/drop guard (фикс #3) ────────────────────────────────────────────────


async def test_approve_rejects_ambiguous_canonical(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(first=_row(canonical_norm="something_else"))]))
    neo = _Neo()
    calls = _patch_neo(monkeypatch, neo)
    with pytest.raises(HTTPException) as ei:
        await gh.approve_entity_merge(ApproveEntityMergeRequest(proposal_id="m1"))
    assert ei.value.status_code == 409
    assert "neither" in ei.value.detail
    assert calls == []  # граф не трогали
    assert neo.merge_calls == []


async def test_approve_keep_norm_a_drops_norm_b(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=_row(canonical_norm="hmi"))]))
    neo = _Neo(merge_result={"merged": True, "canonical": "HMI"})
    _patch_neo(monkeypatch, neo)
    out = await gh.approve_entity_merge(ApproveEntityMergeRequest(proposal_id="m1"))
    assert out["status"] == "approved"
    assert neo.merge_calls == [("disruption", "hmi", "human machine interface")]
    assert neo.closed and s.committed


async def test_approve_keep_norm_b_drops_norm_a(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(first=_row(canonical_norm="human machine interface"))]))
    neo = _Neo()
    _patch_neo(monkeypatch, neo)
    await gh.approve_entity_merge(ApproveEntityMergeRequest(proposal_id="m1"))
    assert neo.merge_calls == [("disruption", "human machine interface", "hmi")]


# ── переходы статусов / провал слияния ───────────────────────────────────────


async def test_approve_non_pending_409_before_neo(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(first=_row(status="approved"))]))
    neo = _Neo()
    calls = _patch_neo(monkeypatch, neo)
    with pytest.raises(HTTPException) as ei:
        await gh.approve_entity_merge(ApproveEntityMergeRequest(proposal_id="m1"))
    assert ei.value.status_code == 409
    assert calls == []  # Neo4j не инстанцируется


async def test_approve_missing_404(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(first=None)]))
    _patch_neo(monkeypatch, _Neo())
    with pytest.raises(HTTPException) as ei:
        await gh.approve_entity_merge(ApproveEntityMergeRequest(proposal_id="nope"))
    assert ei.value.status_code == 404


async def test_approve_merge_failed_leaves_pending(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=_row())]))
    neo = _Neo(merge_result={"merged": False})
    _patch_neo(monkeypatch, neo)
    with pytest.raises(HTTPException) as ei:
        await gh.approve_entity_merge(ApproveEntityMergeRequest(proposal_id="m1"))
    assert ei.value.status_code == 409
    assert "merge failed" in ei.value.detail
    assert neo.closed  # клиент закрыт в finally
    assert not s.committed  # статус не переведён в approved
    assert len(s.executed) == 1  # только SELECT, без UPDATE статуса


# ── reject не трогает граф ───────────────────────────────────────────────────


async def test_reject_pending_does_not_touch_graph(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1)]))
    calls = _patch_neo(monkeypatch, _Neo())
    out = await gh.reject_entity_merge(RejectEntityMergeRequest(proposal_id="m1", note="нет"))
    assert out["status"] == "rejected"
    assert s.committed
    assert calls == []  # Neo4j вообще не создавался


async def test_reject_non_pending_409(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(rowcount=0)]))
    _patch_neo(monkeypatch, _Neo())
    with pytest.raises(HTTPException) as ei:
        await gh.reject_entity_merge(RejectEntityMergeRequest(proposal_id="m1"))
    assert ei.value.status_code == 409


# ── get_graph_health / list ──────────────────────────────────────────────────


async def test_get_graph_health_returns_and_closes(monkeypatch):
    neo = _Neo(health={"concept_count": 42, "duplicate_clusters": 3}, duplicates=[{"acro": "hmi"}])
    _patch_neo(monkeypatch, neo)
    out = await gh.get_graph_health(GraphHealthRequest(workspace="disruption"))
    assert out["status"] == "ok"
    assert out["health"]["concept_count"] == 42
    assert out["duplicate_groups"] == [{"acro": "hmi"}]
    assert neo.closed


async def test_list_entity_merge_workspace_filter(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rows=[])]))
    await gh.list_entity_merge_proposals(
        ListEntityMergeProposalsRequest(workspace="disruption")
    )
    sql, params = s.executed[0]
    assert "workspace_id = :ws" in sql
    assert params["ws"] == "disruption"
