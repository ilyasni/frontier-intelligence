"""Тесты MCP человек-гейта Контура A/C (approve/reject порогов).

Это security-критичный гейт: «рекурсию RSI разрывает человек». Проверяем allowlist
ключей, валидацию диапазона значений (фикс runaway), маршрутизацию записи и переходы
статусов. БД мокается — реальный Postgres не нужен.
"""

from __future__ import annotations

import math

import pytest
from fastapi import HTTPException

from mcp.tools import threshold_proposals as tp
from mcp.tools.threshold_proposals import (
    ApproveThresholdChangeRequest,
    ListThresholdProposalsRequest,
    MarkRelevanceAuditRequest,
    RejectThresholdChangeRequest,
)

pytestmark = pytest.mark.unit


class _Result:
    """Мок результата session.execute: поддерживает mappings().first()/.all() и rowcount."""

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
    """Мок AsyncSession как async-context-manager. Отдаёт результаты по порядку execute."""

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


def _patch_session(monkeypatch, session):
    monkeypatch.setattr(tp, "get_engine", lambda: object())
    monkeypatch.setattr(tp, "AsyncSession", lambda _engine: session)
    return session


def _pending_row(key="weak_signal_min_score", value=0.55, status="pending"):
    return {
        "id": "p1",
        "workspace_id": "disruption",
        "threshold_key": key,
        "proposed_value": value,
        "status": status,
    }


# ── APPROVABLE_KEYS allowlist ────────────────────────────────────────────────


async def test_approve_rejects_non_approvable_key(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=_pending_row(key="evil_arbitrary_field"))]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 400
    assert "not approvable" in ei.value.detail
    assert len(s.executed) == 1  # только SELECT, никакой записи в конфиг


async def test_approvable_keys_is_true_allowlist():
    assert tp.APPROVABLE_KEYS == tp.WEAK_GATE_KEYS | {"relevance_threshold"}
    assert "relevance_weights" not in tp.APPROVABLE_KEYS
    assert "updated_at" not in tp.APPROVABLE_KEYS


# ── Валидация диапазона значения (фикс #1: стоп runaway) ─────────────────────


async def test_approve_rejects_value_above_range(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=_pending_row(value=999))]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 400
    assert "out of range" in ei.value.detail
    assert len(s.executed) == 1  # значение не доехало до UPDATE


async def test_approve_rejects_negative_relevance_threshold(monkeypatch):
    s = _patch_session(
        monkeypatch, _Session([_Result(first=_pending_row(key="relevance_threshold", value=-5))])
    )
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 400
    assert len(s.executed) == 1


async def test_approve_rejects_non_numeric_value(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=_pending_row(value="abc"))]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 400
    assert "not numeric" in ei.value.detail


async def test_approve_rejects_nan_value(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=_pending_row(value=math.nan))]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 400
    assert "finite" in ei.value.detail


async def test_source_count_bounds_allow_int_range(monkeypatch):
    # weak_signal_min_source_count диапазон [1, 100]; 0 — вне диапазона.
    s = _patch_session(
        monkeypatch, _Session([_Result(first=_pending_row(key="weak_signal_min_source_count", value=0))])
    )
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 400


# ── Маршрутизация записи (фикс #2 концепции: правильная ветка) ───────────────


async def test_approve_weak_key_writes_cluster_analysis(monkeypatch):
    s = _patch_session(
        monkeypatch,
        _Session([_Result(first=_pending_row(key="weak_signal_min_score", value=0.55)), _Result(), _Result()]),
    )
    out = await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert out["status"] == "approved"
    assert out["applied_value"] == 0.55
    assert s.committed
    # вторая команда — UPDATE workspaces в ветку cluster_analysis, не relevance_weights
    update_sql = s.executed[1][0]
    assert "cluster_analysis" in update_sql
    assert "relevance_weights" not in update_sql
    assert s.executed[1][1]["value"] == 0.55


async def test_approve_relevance_threshold_writes_relevance_weights(monkeypatch):
    s = _patch_session(
        monkeypatch,
        _Session(
            [_Result(first=_pending_row(key="relevance_threshold", value=0.5)), _Result(), _Result()]
        ),
    )
    out = await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert out["status"] == "approved"
    update_sql = s.executed[1][0]
    assert "relevance_weights" in update_sql
    assert "cluster_analysis" not in update_sql


# ── Переходы статусов / идемпотентность ──────────────────────────────────────


async def test_approve_missing_proposal_404(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(first=None)]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="nope"))
    assert ei.value.status_code == 404


async def test_approve_already_approved_409(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=_pending_row(status="approved"))]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 409
    assert len(s.executed) == 1  # без повторного применения


async def test_approve_already_rejected_409(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(first=_pending_row(status="rejected"))]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(ApproveThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 409


async def test_reject_pending_ok(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rowcount=1)]))
    out = await tp.reject_threshold_change(RejectThresholdChangeRequest(proposal_id="p1", note="нет"))
    assert out["status"] == "rejected"
    assert s.committed


async def test_reject_non_pending_409(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(rowcount=0)]))
    with pytest.raises(HTTPException) as ei:
        await tp.reject_threshold_change(RejectThresholdChangeRequest(proposal_id="p1"))
    assert ei.value.status_code == 409


# ── Инъекция / параметризация ────────────────────────────────────────────────


async def test_injection_shaped_id_is_bound_param(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(first=None)]))
    with pytest.raises(HTTPException) as ei:
        await tp.approve_threshold_change(
            ApproveThresholdChangeRequest(proposal_id="' OR '1'='1")
        )
    assert ei.value.status_code == 404  # не матчит ничего — связанный параметр
    assert s.executed[0][1]["id"] == "' OR '1'='1"


# ── mark_relevance_audit / list ──────────────────────────────────────────────


async def test_mark_relevance_audit_invalid_verdict(monkeypatch):
    _patch_session(monkeypatch, _Session([]))
    with pytest.raises(HTTPException) as ei:
        await tp.mark_relevance_audit(MarkRelevanceAuditRequest(post_id="x", verdict="maybe"))
    assert ei.value.status_code == 400


async def test_mark_relevance_audit_not_found(monkeypatch):
    _patch_session(monkeypatch, _Session([_Result(rowcount=0)]))
    with pytest.raises(HTTPException) as ei:
        await tp.mark_relevance_audit(
            MarkRelevanceAuditRequest(post_id="x", verdict="false_negative")
        )
    assert ei.value.status_code == 404


async def test_list_threshold_proposals_workspace_filter(monkeypatch):
    s = _patch_session(monkeypatch, _Session([_Result(rows=[])]))
    await tp.list_threshold_proposals(
        ListThresholdProposalsRequest(workspace="disruption", status="pending")
    )
    sql, params = s.executed[0]
    assert "workspace_id = :ws" in sql
    assert params["ws"] == "disruption"
