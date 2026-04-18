from fastapi import APIRouter, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine

router = APIRouter()
ALLOWED_SIGNAL_STAGES = {"weak", "emerging", "stable", "fading"}


def _normalize_signal_stages(stages: list[str] | None, default: tuple[str, ...]) -> list[str]:
    values = [stage.strip().lower() for stage in (stages or []) if stage and stage.strip()]
    if not values:
        return list(default)
    return [stage for stage in values if stage in ALLOWED_SIGNAL_STAGES] or list(default)


async def _fetch_one(sql: str, params: dict):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(text(sql), params)
        row = result.mappings().first()
        return dict(row) if row else None


@router.get("/semantic")
async def list_semantic_clusters(workspace_id: str | None = None, limit: int = 50):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT *
                FROM semantic_clusters
                WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
                ORDER BY detected_at DESC
                LIMIT :limit
                """
            ),
            {"ws": workspace_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]


@router.get("/trends")
async def list_trend_clusters(workspace_id: str | None = None, pipeline: str = "stable", limit: int = 50):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT *
                FROM trend_clusters
                WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
                  AND pipeline = :pipeline
                ORDER BY detected_at DESC, signal_score DESC, burst_score DESC
                LIMIT :limit
                """
            ),
            {"ws": workspace_id, "pipeline": pipeline, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]


@router.get("/emerging")
async def list_emerging_signals(
    workspace_id: str | None = None,
    limit: int = 50,
    stages: list[str] | None = Query(default=None),
):
    signal_stages = _normalize_signal_stages(stages, default=("emerging",))
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT *
                FROM emerging_signals
                WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
                  AND signal_stage = ANY(:stages)
                ORDER BY detected_at DESC, signal_score DESC
                LIMIT :limit
                """
            ),
            {"ws": workspace_id, "stages": signal_stages, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]


@router.get("/runs")
async def list_cluster_runs(workspace_id: str | None = None, limit: int = 50):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT *
                FROM cluster_runs
                WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
                ORDER BY started_at DESC
                LIMIT :limit
                """
            ),
            {"ws": workspace_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]


@router.get("/semantic/{cluster_id}")
async def get_semantic_cluster(cluster_id: str):
    row = await _fetch_one("SELECT * FROM semantic_clusters WHERE id = :id", {"id": cluster_id})
    return row or {}


@router.get("/trends/{cluster_id}")
async def get_trend_cluster(cluster_id: str):
    row = await _fetch_one("SELECT * FROM trend_clusters WHERE id = :id", {"id": cluster_id})
    return row or {}


@router.get("/emerging/{signal_id}")
async def get_emerging_signal(signal_id: str):
    row = await _fetch_one("SELECT * FROM emerging_signals WHERE id = :id", {"id": signal_id})
    return row or {}


@router.get("/missing")
async def list_missing_signals(workspace_id: str | None = None, limit: int = 50):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT *
                FROM missing_signals
                WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
                ORDER BY gap_score DESC, updated_at DESC
                LIMIT :limit
                """
            ),
            {"ws": workspace_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]


@router.get("/missing/{signal_id}")
async def get_missing_signal(signal_id: str):
    row = await _fetch_one("SELECT * FROM missing_signals WHERE id = :id", {"id": signal_id})
    return row or {}


@router.get("/timeline")
async def get_signal_timeline(
    entity_kind: str,
    entity_id: str,
    workspace_id: str | None = None,
):
    if entity_kind not in {"semantic", "trend", "emerging"}:
        return {"error": "Unsupported entity_kind"}
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT *
                FROM signal_time_series
                WHERE entity_kind = :entity_kind
                  AND entity_id = :entity_id
                  AND (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
                ORDER BY window_start ASC
                """
            ),
            {"entity_kind": entity_kind, "entity_id": entity_id, "ws": workspace_id},
        )
        series = [dict(row) for row in result.mappings().all()]
        cluster = None
        if entity_kind == "semantic":
            cluster = await _fetch_one("SELECT * FROM semantic_clusters WHERE id = :id", {"id": entity_id})
        elif entity_kind == "trend":
            cluster = await _fetch_one("SELECT * FROM trend_clusters WHERE id = :id", {"id": entity_id})
        else:
            cluster = await _fetch_one("SELECT * FROM emerging_signals WHERE id = :id", {"id": entity_id})
    return {"entity_kind": entity_kind, "entity_id": entity_id, "cluster": cluster or {}, "series": series}
