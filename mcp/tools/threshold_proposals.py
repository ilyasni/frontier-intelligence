"""MCP-гейт Контура A (RSI): просмотр и утверждение предложений по порогам.

Ретро-петля пишет предложения в threshold_proposals (status=pending). Человек смотрит их
через list_threshold_proposals и применяет через approve_threshold_change — approve пишет
новое значение в workspaces.extra->cluster_analysis (кластеризация читает его на следующем
прогоне, без редеплоя). Никакого авто-применения: гейт — это человек.
"""

from __future__ import annotations

import math

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from mcp.guards import assert_known_workspace
from shared.db import get_engine

router = APIRouter()

# Только эти пороги петля может предлагать и человек — применять. Защита от записи произвольных ключей.
# weak_* пишутся в workspaces.extra->cluster_analysis; relevance_threshold — в relevance_weights->threshold.
WEAK_GATE_KEYS = {
    "weak_signal_min_score",
    "weak_signal_min_confidence",
    "weak_signal_min_source_diversity",
    "weak_signal_min_source_count",
}
APPROVABLE_KEYS = WEAK_GATE_KEYS | {"relevance_threshold"}

# Допустимый диапазон значения по каждому ключу. Человек-гейт обязан остановить runaway-
# предложение (напр. relevance_threshold=-5 или weak_signal_min_score=999), даже если оно
# попало в БД из-за бага петли или ручной правки строки.
KEY_BOUNDS: dict[str, tuple[float, float]] = {
    "weak_signal_min_score": (0.0, 1.0),
    "weak_signal_min_confidence": (0.0, 1.0),
    "weak_signal_min_source_diversity": (0.0, 1.0),
    "weak_signal_min_source_count": (1.0, 100.0),
    "relevance_threshold": (0.0, 1.0),
}


def _validate_proposed_value(threshold_key: str, raw_value: object) -> float:
    """Проверить значение предложения перед записью в конфиг (защита человек-гейта)."""
    try:
        value = float(raw_value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="proposed_value is not numeric")
    if not math.isfinite(value):
        raise HTTPException(status_code=400, detail="proposed_value is not finite")
    lo, hi = KEY_BOUNDS[threshold_key]
    if not (lo <= value <= hi):
        raise HTTPException(
            status_code=400,
            detail=f"proposed_value {value} out of range [{lo}, {hi}] for {threshold_key}",
        )
    return value


class RelevanceAuditSampleRequest(BaseModel):
    workspace: str | None = None
    days_back: int = Field(default=30, ge=1, le=180)
    limit: int = Field(default=20, ge=1, le=100)


class MarkRelevanceAuditRequest(BaseModel):
    post_id: str
    verdict: str  # 'false_negative' | 'correct_reject'
    reviewed_by: str = "operator"
    note: str | None = None


class ListThresholdProposalsRequest(BaseModel):
    workspace: str | None = None
    status: str = "pending"
    limit: int = Field(default=50, ge=1, le=200)


class ListUnderratedSignalsRequest(BaseModel):
    workspace: str | None = None
    days_back: int = Field(default=30, ge=1, le=180)
    limit: int = Field(default=30, ge=1, le=100)


class ApproveThresholdChangeRequest(BaseModel):
    proposal_id: str
    reviewed_by: str = "operator"


class RejectThresholdChangeRequest(BaseModel):
    proposal_id: str
    reviewed_by: str = "operator"
    note: str | None = None


@router.post("/list_threshold_proposals")
async def list_threshold_proposals(req: ListThresholdProposalsRequest) -> dict:
    """Список предложений по порогам weak-signal детектора (по умолчанию pending)."""
    assert_known_workspace(req.workspace)
    clauses = ["status = :status"]
    params: dict = {"status": req.status, "limit": req.limit}
    if req.workspace:
        clauses.append("workspace_id = :ws")
        params["ws"] = req.workspace
    async with AsyncSession(get_engine()) as session:
        rows = await session.execute(
            text(
                f"""
                SELECT id, workspace_id, threshold_key, current_value, proposed_value, direction,
                       status, cohort_days, rationale, evidence, threshold_version, created_at, updated_at
                FROM threshold_proposals
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            params,
        )
        proposals = [dict(r) for r in rows.mappings().all()]
    return {"status": "ok", "count": len(proposals), "proposals": proposals}


@router.post("/list_underrated_signals")
async def list_underrated_signals(req: ListUnderratedSignalsRequest) -> dict:
    """Контур B: weak-кандидаты, которых судья другой семьи счёл новыми (пойманная слепота primary)."""
    assert_known_workspace(req.workspace)
    clauses = [
        "judge_verdict IS NOT NULL",
        "(judge_verdict->>'underrated')::boolean = true",
        "detected_at >= NOW() - make_interval(days => :days)",
    ]
    params: dict = {"days": req.days_back, "limit": req.limit}
    if req.workspace:
        clauses.append("workspace_id = :ws")
        params["ws"] = req.workspace
    async with AsyncSession(get_engine()) as session:
        rows = await session.execute(
            text(
                f"""
                SELECT DISTINCT ON (signal_key)
                    signal_key, workspace_id, title, keywords, signal_score, burst_score,
                    source_count, source_diversity, judge_verdict, detected_at
                FROM weak_signal_snapshots
                WHERE {' AND '.join(clauses)}
                ORDER BY signal_key, detected_at DESC
                LIMIT :limit
                """
            ),
            params,
        )
        signals = [dict(r) for r in rows.mappings().all()]
    signals.sort(key=lambda s: float((s.get("judge_verdict") or {}).get("novelty_score") or 0), reverse=True)
    return {"status": "ok", "count": len(signals), "signals": signals}


@router.post("/list_relevance_audit_sample")
async def list_relevance_audit_sample(req: RelevanceAuditSampleRequest) -> dict:
    """Контур C: выборка отклонённых постов на ручной аудит (топ по score — ближе к порогу = вероятнее ошибка)."""
    assert_known_workspace(req.workspace)
    clauses = [
        "relevant = false",
        "audit_status IS NULL",
        "decided_at >= NOW() - make_interval(days => :days)",
    ]
    params: dict = {"days": req.days_back, "limit": req.limit}
    if req.workspace:
        clauses.append("workspace_id = :ws")
        params["ws"] = req.workspace
    async with AsyncSession(get_engine()) as session:
        rows = await session.execute(
            text(
                f"""
                SELECT post_id, workspace_id, score, threshold, category, reasoning,
                       content_excerpt, source_id, decided_at
                FROM relevance_decisions
                WHERE {' AND '.join(clauses)}
                ORDER BY score DESC
                LIMIT :limit
                """
            ),
            params,
        )
        sample = [dict(r) for r in rows.mappings().all()]
    return {"status": "ok", "count": len(sample), "sample": sample}


@router.post("/mark_relevance_audit")
async def mark_relevance_audit(req: MarkRelevanceAuditRequest) -> dict:
    """Контур C: вердикт человека по отклонённому посту (false_negative = зря отклонили)."""
    verdict = req.verdict.strip().lower()
    if verdict not in {"false_negative", "correct_reject"}:
        raise HTTPException(status_code=400, detail="verdict must be false_negative or correct_reject")
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(
            text(
                """
                UPDATE relevance_decisions
                SET audit_status = :verdict, audit_note = COALESCE(:note, audit_note), updated_at = NOW()
                WHERE post_id = :post_id
                """
            ),
            {"verdict": verdict, "note": req.note, "post_id": req.post_id},
        )
        await session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="relevance decision not found for post_id")
    return {"status": "ok", "post_id": req.post_id, "audit_status": verdict, "reviewed_by": req.reviewed_by}


@router.post("/approve_threshold_change")
async def approve_threshold_change(req: ApproveThresholdChangeRequest) -> dict:
    """Утвердить предложение: применить новый порог в workspaces.extra->cluster_analysis."""
    async with AsyncSession(get_engine()) as session:
        row = (
            await session.execute(
                text(
                    """
                    SELECT id, workspace_id, threshold_key, proposed_value, status
                    FROM threshold_proposals WHERE id = :id FOR UPDATE
                    """
                ),
                {"id": req.proposal_id},
            )
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="proposal not found")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"proposal is {row['status']}, not pending")
        if row["threshold_key"] not in APPROVABLE_KEYS:
            raise HTTPException(status_code=400, detail=f"threshold_key {row['threshold_key']} not approvable")
        value = _validate_proposed_value(row["threshold_key"], row["proposed_value"])

        if row["threshold_key"] == "relevance_threshold":
            # Порог релевантности живёт в relevance_weights->threshold (читается enrichment'ом).
            await session.execute(
                text(
                    """
                    UPDATE workspaces
                    SET relevance_weights = jsonb_set(
                            COALESCE(relevance_weights, '{}'::jsonb),
                            '{threshold}',
                            to_jsonb(CAST(:value AS double precision)),
                            true
                        ),
                        updated_at = NOW()
                    WHERE id = :ws
                    """
                ),
                {"ws": row["workspace_id"], "value": value},
            )
        else:
            # weak-гейты — в per-workspace override (читается _cluster_settings на следующем прогоне).
            await session.execute(
                text(
                    """
                    UPDATE workspaces
                    SET extra = jsonb_set(
                            jsonb_set(
                                COALESCE(extra, '{}'::jsonb),
                                '{cluster_analysis}',
                                COALESCE(extra->'cluster_analysis', '{}'::jsonb),
                                true
                            ),
                            ARRAY['cluster_analysis', :key],
                            to_jsonb(CAST(:value AS double precision)),
                            true
                        ),
                        updated_at = NOW()
                    WHERE id = :ws
                    """
                ),
                {"ws": row["workspace_id"], "key": row["threshold_key"], "value": value},
            )
        await session.execute(
            text(
                """
                UPDATE threshold_proposals
                SET status = 'approved', reviewed_by = :by, reviewed_at = NOW(),
                    applied_value = :value, updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": req.proposal_id, "by": req.reviewed_by, "value": value},
        )
        await session.commit()
    return {
        "status": "approved",
        "proposal_id": req.proposal_id,
        "workspace_id": row["workspace_id"],
        "threshold_key": row["threshold_key"],
        "applied_value": value,
        "note": "Применится на следующем прогоне кластеризации; новые снимки получат новый threshold_version.",
    }


@router.post("/reject_threshold_change")
async def reject_threshold_change(req: RejectThresholdChangeRequest) -> dict:
    """Отклонить предложение (порог не меняется)."""
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(
            text(
                """
                UPDATE threshold_proposals
                SET status = 'rejected', reviewed_by = :by, reviewed_at = NOW(),
                    rationale = COALESCE(:note, rationale), updated_at = NOW()
                WHERE id = :id AND status = 'pending'
                """
            ),
            {"id": req.proposal_id, "by": req.reviewed_by, "note": req.note},
        )
        await session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=409, detail="proposal not found or not pending")
    return {"status": "rejected", "proposal_id": req.proposal_id}
