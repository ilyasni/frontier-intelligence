"""MCP-инструмент Контура D (RSI): здоровье концепт-графа Neo4j + дубли сущностей."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_engine
from worker.integrations.neo4j_client import Neo4jFrontierClient

router = APIRouter()


class GraphHealthRequest(BaseModel):
    workspace: str
    duplicates_limit: int = Field(default=25, ge=1, le=100)


class ListEntityMergeProposalsRequest(BaseModel):
    workspace: str | None = None
    status: str = "pending"
    limit: int = Field(default=50, ge=1, le=200)


class ApproveEntityMergeRequest(BaseModel):
    proposal_id: str
    reviewed_by: str = "operator"


class RejectEntityMergeRequest(BaseModel):
    proposal_id: str
    reviewed_by: str = "operator"
    note: str | None = None


@router.post("/get_graph_health")
async def get_graph_health(req: GraphHealthRequest) -> dict:
    """Метрики здоровья графа (orphan, дубль-кластеры, плотность) + группы дублей для аудита.

    Слияние дублей запускается отдельно операторским job run_graph_resolution (мутация графа).
    """
    client = Neo4jFrontierClient()
    try:
        health = await client.graph_health(req.workspace)
        duplicates = await client.find_duplicate_entities(req.workspace, limit=req.duplicates_limit)
    finally:
        await client.close()
    return {
        "status": "ok",
        "workspace": req.workspace,
        "health": health,
        "duplicate_groups": duplicates,
    }


@router.post("/list_entity_merge_proposals")
async def list_entity_merge_proposals(req: ListEntityMergeProposalsRequest) -> dict:
    """Контур D+: семантические предложения слияния концептов (акроним↔расшифровка, по умолчанию pending)."""
    clauses = ["status = :status"]
    params: dict = {"status": req.status, "limit": req.limit}
    if req.workspace:
        clauses.append("workspace_id = :ws")
        params["ws"] = req.workspace
    async with AsyncSession(get_engine()) as session:
        rows = await session.execute(
            text(
                f"""
                SELECT id, workspace_id, name_a, name_b, canonical_name, confidence,
                       reasoning, cooccurrence, judge_provider, status, created_at
                FROM entity_merge_proposals
                WHERE {' AND '.join(clauses)}
                ORDER BY confidence DESC, cooccurrence DESC
                LIMIT :limit
                """
            ),
            params,
        )
        proposals = [dict(r) for r in rows.mappings().all()]
    return {"status": "ok", "count": len(proposals), "proposals": proposals}


@router.post("/approve_entity_merge")
async def approve_entity_merge(req: ApproveEntityMergeRequest) -> dict:
    """Контур D+: утвердить слияние — сливает два концепта в графе (canonical + alias)."""
    async with AsyncSession(get_engine()) as session:
        row = (
            await session.execute(
                text(
                    "SELECT workspace_id, norm_a, norm_b, canonical_norm, canonical_name, status "
                    "FROM entity_merge_proposals WHERE id = :id FOR UPDATE"
                ),
                {"id": req.proposal_id},
            )
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="proposal not found")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"proposal is {row['status']}, not pending")

        keep = row["canonical_norm"]
        drop = row["norm_a"] if keep == row["norm_b"] else row["norm_b"]
        neo = Neo4jFrontierClient()
        try:
            merge = await neo.merge_concept_pair(row["workspace_id"], keep, drop)
        finally:
            await neo.close()
        if not merge.get("merged"):
            raise HTTPException(status_code=409, detail="merge failed (node missing in graph)")

        await session.execute(
            text(
                "UPDATE entity_merge_proposals SET status='approved', reviewed_by=:by, "
                "reviewed_at=NOW(), updated_at=NOW() WHERE id=:id"
            ),
            {"id": req.proposal_id, "by": req.reviewed_by},
        )
        await session.commit()
    return {
        "status": "approved",
        "proposal_id": req.proposal_id,
        "workspace_id": row["workspace_id"],
        "canonical": merge.get("canonical") or row["canonical_name"],
    }


@router.post("/reject_entity_merge")
async def reject_entity_merge(req: RejectEntityMergeRequest) -> dict:
    """Контур D+: отклонить предложение слияния (граф не трогаем)."""
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(
            text(
                "UPDATE entity_merge_proposals SET status='rejected', reviewed_by=:by, "
                "reviewed_at=NOW(), reasoning=COALESCE(:note, reasoning), updated_at=NOW() "
                "WHERE id=:id AND status='pending'"
            ),
            {"id": req.proposal_id, "by": req.reviewed_by, "note": req.note},
        )
        await session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=409, detail="proposal not found or not pending")
    return {"status": "rejected", "proposal_id": req.proposal_id}
