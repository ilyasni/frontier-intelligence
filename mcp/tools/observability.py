from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_engine
from shared.config import get_settings
from shared.redis_streams import collect_redis_stream_snapshot
from shared.source_quality import source_quality_payload

router = APIRouter()
SignalStage = Literal["weak", "emerging", "stable", "fading"]
ALLOWED_SIGNAL_STAGES = {"weak", "emerging", "stable", "fading"}


class WorkspaceListRequest(BaseModel):
    active_only: bool = False


class SourcesHealthRequest(BaseModel):
    workspace: str | None = None
    limit: int = Field(default=100, ge=1, le=500)


class PipelineStatsRequest(BaseModel):
    workspace: str | None = None
    recent_limit: int = Field(default=20, ge=1, le=100)


class ClusterListRequest(BaseModel):
    workspace: str | None = None
    kind: Literal["all", "semantic", "trend", "emerging"] = "all"
    pipeline: str = "stable"
    limit: int = Field(default=20, ge=1, le=100)
    stages: list[SignalStage] | None = None


class ClusterEvidenceRequest(BaseModel):
    cluster_id: str
    kind: Literal["auto", "semantic", "trend", "emerging"] = "auto"
    evidence_limit: int = Field(default=6, ge=1, le=20)


class EmergingSignalListRequest(BaseModel):
    workspace: str | None = None
    limit: int = Field(default=20, ge=1, le=100)
    stages: list[SignalStage] | None = None


class MissingSignalListRequest(BaseModel):
    workspace: str | None = None
    limit: int = Field(default=20, ge=1, le=100)


class ClusterDetailsRequest(BaseModel):
    cluster_id: str
    kind: Literal["auto", "semantic", "trend", "emerging", "missing"] = "auto"


class MissingSignalDetailsRequest(BaseModel):
    signal_id: str


class WorkspaceOverviewRequest(BaseModel):
    workspace: str
    recent_limit: int = Field(default=8, ge=1, le=30)
    sources_limit: int = Field(default=8, ge=1, le=30)
    clusters_limit: int = Field(default=6, ge=1, le=30)


class SourceDetailsRequest(BaseModel):
    source_id: str
    recent_runs_limit: int = Field(default=10, ge=1, le=30)
    recent_posts_limit: int = Field(default=10, ge=1, le=30)


class SignalTimelineRequest(BaseModel):
    entity_kind: Literal["semantic", "trend", "emerging"]
    entity_id: str
    workspace: str | None = None


async def _fetch_rows(sql: str, params: dict) -> list[dict]:
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(text(sql), params)
        return [dict(row) for row in result.mappings().all()]


async def _fetch_one(sql: str, params: dict) -> dict | None:
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(text(sql), params)
        row = result.mappings().first()
        return dict(row) if row else None


def _normalize_signal_stages(stages: list[str] | None, default: tuple[str, ...]) -> list[str]:
    values = [stage.strip().lower() for stage in (stages or []) if stage and stage.strip()]
    if not values:
        return list(default)
    return [stage for stage in values if stage in ALLOWED_SIGNAL_STAGES] or list(default)


async def _load_cluster_posts(doc_ids: list[str], limit: int) -> list[dict]:
    if not doc_ids:
        return []
    return await _fetch_rows(
        """
        SELECT
            p.id AS post_id,
            p.workspace_id,
            p.source_id,
            s.name AS source_name,
            COALESCE(p.extra->>'url', '') AS url,
            p.category,
            p.relevance_score,
            p.published_at,
            LEFT(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(COALESCE(p.content, ''), '<[^>]+>', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                ),
                280
            ) AS preview,
            COALESCE(s.source_score, s.source_authority, 0) AS source_score,
            COALESCE(s.source_authority, 0.5) AS source_authority
        FROM posts p
        LEFT JOIN sources s ON s.id = p.source_id
        WHERE p.id = ANY(:doc_ids)
        ORDER BY COALESCE(p.relevance_score, 0) DESC, p.published_at DESC NULLS LAST
        LIMIT :limit
        """,
        {"doc_ids": doc_ids, "limit": limit},
    )


@router.post("/list_workspaces")
async def list_workspaces(req: WorkspaceListRequest) -> dict:
    rows = await _fetch_rows(
        """
        SELECT
            id, name, description, categories, relevance_weights,
            design_lenses, cross_workspace_bridges, is_active, updated_at
        FROM workspaces
        WHERE (:active_only = FALSE OR is_active = TRUE)
        ORDER BY is_active DESC, updated_at DESC, id ASC
        """,
        {"active_only": req.active_only},
    )
    return {"workspaces": rows}


@router.post("/list_sources_health")
async def list_sources_health(req: SourcesHealthRequest) -> dict:
    rows = await _fetch_rows(
        """
        SELECT
            s.id,
            s.workspace_id,
            s.source_type,
            s.name,
            s.url,
            s.tg_channel,
            s.schedule_cron,
            s.is_enabled,
            s.proxy_config,
            s.extra,
            s.source_authority,
            s.source_score,
            s.source_score_updated_at,
            sc.last_success_at,
            sc.last_error,
            sc.last_seen_published_at,
            COALESCE(metrics.recent_success_count, 0) AS recent_success_count,
            COALESCE(metrics.recent_error_count, 0) AS recent_error_count,
            COALESCE(post_metrics.relevant_ratio, 0) AS relevant_ratio,
            COALESCE(post_metrics.avg_tag_count, 0) AS avg_tag_count,
            COALESCE(post_metrics.linked_ratio, 0) AS linked_ratio,
            post_metrics.freshness_hours AS freshness_hours,
            sr.status AS last_run_status,
            sr.started_at AS last_run_started_at,
            sr.finished_at AS last_run_finished_at,
            sr.fetched_count AS last_run_fetched_count,
            sr.emitted_count AS last_run_emitted_count,
            sr.error_text AS last_run_error_text
        FROM sources s
        LEFT JOIN source_checkpoints sc ON sc.source_id = s.id
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE status = 'success') AS recent_success_count,
                COUNT(*) FILTER (WHERE status = 'error') AS recent_error_count
            FROM source_runs
            WHERE source_id = s.id
              AND started_at >= NOW() - INTERVAL '14 days'
        ) metrics ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                AVG(CASE WHEN COALESCE(p.relevance_score, 0) >= 0.6 THEN 1.0 ELSE 0.0 END) AS relevant_ratio,
                AVG(jsonb_array_length(COALESCE(p.tags, '[]'::jsonb))) AS avg_tag_count,
                AVG(CASE WHEN jsonb_array_length(COALESCE(pe.data->'items', '[]'::jsonb)) > 0 THEN 1.0 ELSE 0.0 END) AS linked_ratio,
                EXTRACT(EPOCH FROM (NOW() - MAX(p.published_at))) / 3600.0 AS freshness_hours
            FROM posts p
            LEFT JOIN post_enrichments pe
                ON pe.post_id = p.id
               AND pe.kind = 'crawl'
            WHERE p.source_id = s.id
              AND p.created_at >= NOW() - INTERVAL '30 days'
        ) post_metrics ON TRUE
        LEFT JOIN LATERAL (
            SELECT *
            FROM source_runs
            WHERE source_id = s.id
            ORDER BY started_at DESC
            LIMIT 1
        ) sr ON TRUE
        WHERE (CAST(:ws AS text) IS NULL OR s.workspace_id = CAST(:ws AS text))
        ORDER BY COALESCE(s.source_score, s.source_authority, 0) DESC, s.updated_at DESC
        LIMIT :limit
        """,
        {"ws": req.workspace, "limit": req.limit},
    )

    items = []
    for row in rows:
        health = source_quality_payload(row)
        items.append(
            {
                "id": row["id"],
                "workspace_id": row["workspace_id"],
                "source_type": row["source_type"],
                "name": row["name"],
                "url": row.get("url"),
                "tg_channel": row.get("tg_channel"),
                "is_enabled": row["is_enabled"],
                "schedule_cron": row["schedule_cron"],
                "source_authority": health["source_authority"],
                "source_score": health["source_score"],
                "score_breakdown": health["score_breakdown"],
                "quality_tier": health["quality_tier"],
                "recommended_content_mode": health["recommended_content_mode"],
                "last_run_status": row.get("last_run_status"),
                "last_run_fetched_count": row.get("last_run_fetched_count"),
                "last_run_emitted_count": row.get("last_run_emitted_count"),
                "last_run_error_text": row.get("last_run_error_text"),
                "recent_success_count": row.get("recent_success_count"),
                "recent_error_count": row.get("recent_error_count"),
                "relevant_ratio": float(row.get("relevant_ratio") or 0.0),
                "avg_tag_count": float(row.get("avg_tag_count") or 0.0),
                "linked_ratio": float(row.get("linked_ratio") or 0.0),
                "freshness_hours": row.get("freshness_hours"),
            }
        )
    return {"sources": items}


@router.post("/get_pipeline_stats")
async def get_pipeline_stats(req: PipelineStatsRequest) -> dict:
    async with AsyncSession(get_engine()) as session:
        ws_where = "WHERE p.workspace_id = :ws" if req.workspace else ""
        params = {"ws": req.workspace, "recent_limit": req.recent_limit} if req.workspace else {"recent_limit": req.recent_limit}

        stats_result = await session.execute(
            text(
                f"""
                SELECT
                    i.embedding_status AS status,
                    COUNT(*) AS count,
                    AVG(p.relevance_score) AS avg_score
                FROM indexing_status i
                JOIN posts p ON p.id = i.post_id
                {ws_where}
                GROUP BY i.embedding_status
                """
            ),
            params,
        )
        stats = {
            row["status"]: {"count": row["count"], "avg_score": float(row["avg_score"] or 0.0)}
            for row in stats_result.mappings().all()
        }

        recent_result = await session.execute(
            text(
                f"""
                SELECT
                    p.id,
                    p.source_id,
                    p.workspace_id,
                    COALESCE(src.name, p.source_id) AS source_name,
                    COALESCE(p.extra->>'url', '') AS url,
                    LEFT(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(COALESCE(p.content, ''), '<[^>]+>', ' ', 'g'),
                            '\\s+',
                            ' ',
                            'g'
                        ),
                        180
                    ) AS preview,
                    p.published_at,
                    p.relevance_score,
                    p.category,
                    i.embedding_status,
                    i.retry_count,
                    i.error_message
                FROM posts p
                LEFT JOIN indexing_status i ON i.post_id = p.id
                LEFT JOIN sources src ON src.id = p.source_id
                {ws_where}
                ORDER BY p.created_at DESC
                LIMIT :recent_limit
                """
            ),
            params,
        )
        recent = [dict(row) for row in recent_result.mappings().all()]

    stream_snapshot = await collect_redis_stream_snapshot(get_settings().redis_url)
    return {"stats": stats, "recent": recent, "stream_queues": stream_snapshot.get("streams", [])}


@router.post("/get_workspace_overview")
async def get_workspace_overview(req: WorkspaceOverviewRequest) -> dict:
    workspace = await _fetch_one(
        """
        SELECT
            id, name, description, categories, relevance_weights,
            design_lenses, cross_workspace_bridges, is_active, updated_at
        FROM workspaces
        WHERE id = :workspace
        """,
        {"workspace": req.workspace},
    )
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")

    stats = (await get_pipeline_stats(PipelineStatsRequest(workspace=req.workspace, recent_limit=req.recent_limit)))["stats"]
    top_sources = (
        await list_sources_health(
            SourcesHealthRequest(workspace=req.workspace, limit=req.sources_limit)
        )
    )["sources"]
    clusters = await list_clusters(
        ClusterListRequest(
            workspace=req.workspace,
            kind="all",
            pipeline="stable",
            limit=req.clusters_limit,
        )
    )

    counts = await _fetch_one(
        """
        SELECT
            COUNT(*) AS post_count,
            COUNT(*) FILTER (WHERE COALESCE(relevance_score, 0) >= 0.6) AS relevant_post_count,
            COUNT(*) FILTER (WHERE semantic_cluster_id IS NOT NULL) AS clustered_post_count,
            COUNT(DISTINCT source_id) AS active_source_count,
            MAX(published_at) AS latest_published_at
        FROM posts
        WHERE workspace_id = :workspace
        """,
        {"workspace": req.workspace},
    ) or {}

    return {
        "workspace": workspace,
        "summary": {
            "post_count": counts.get("post_count", 0),
            "relevant_post_count": counts.get("relevant_post_count", 0),
            "clustered_post_count": counts.get("clustered_post_count", 0),
            "active_source_count": counts.get("active_source_count", 0),
            "latest_published_at": counts.get("latest_published_at"),
            "embedding_status": stats,
            "semantic_cluster_count": len(clusters.get("semantic", [])),
            "trend_cluster_count": len(clusters.get("trends", [])),
            "emerging_signal_count": len(clusters.get("emerging", [])),
        },
        "top_sources": top_sources,
        "clusters": clusters,
        "recent": (
            await get_pipeline_stats(
                PipelineStatsRequest(workspace=req.workspace, recent_limit=req.recent_limit)
            )
        )["recent"],
    }


@router.post("/list_clusters")
async def list_clusters(req: ClusterListRequest) -> dict:
    response: dict[str, object] = {"kind": req.kind}

    if req.kind in {"all", "semantic"}:
        response["semantic"] = await _fetch_rows(
            """
            SELECT
                id, workspace_id, title, representative_post_id, post_count, source_count,
                lifecycle_state, avg_relevance, avg_source_score, freshness_score, coherence_score,
                source_ids, top_concepts, time_window, embedding_version,
                first_seen_at, last_seen_at, detected_at
            FROM semantic_clusters
            WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
            ORDER BY detected_at DESC
            LIMIT :limit
            """,
            {"ws": req.workspace, "limit": req.limit},
        )

    if req.kind in {"all", "trend"}:
        response["trends"] = await _fetch_rows(
            """
            SELECT
                id, workspace_id, pipeline, title, insight, opportunity, time_horizon,
                burst_score, coherence, novelty, source_diversity_score, freshness_score,
                evidence_strength_score, velocity_score, acceleration_score, baseline_rate,
                current_rate, change_point_count, change_point_strength, has_recent_change_point,
                signal_score, signal_stage, doc_count, source_count,
                keywords, category, detected_at
            FROM trend_clusters
            WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
              AND pipeline = :pipeline
            ORDER BY detected_at DESC, burst_score DESC
            LIMIT :limit
            """,
            {"ws": req.workspace, "pipeline": req.pipeline, "limit": req.limit},
        )

    if req.kind in {"all", "emerging"}:
        response["emerging"] = await _fetch_rows(
            """
            SELECT
                id, workspace_id, title, signal_stage, signal_score, confidence,
                velocity_score, acceleration_score, baseline_rate, current_rate,
                change_point_count, change_point_strength, has_recent_change_point,
                source_count, keywords, recommended_watch_action, detected_at
            FROM emerging_signals
            WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
              AND signal_stage = ANY(:stages)
            ORDER BY detected_at DESC, signal_score DESC
            LIMIT :limit
            """,
            {
                "ws": req.workspace,
                "stages": _normalize_signal_stages(req.stages, default=("emerging",)),
                "limit": req.limit,
            },
        )

    return response


@router.post("/get_source_details")
async def get_source_details(req: SourceDetailsRequest) -> dict:
    rows = await _fetch_rows(
        """
        SELECT
            s.id,
            s.workspace_id,
            s.source_type,
            s.name,
            s.url,
            s.tg_channel,
            s.schedule_cron,
            s.is_enabled,
            s.proxy_config,
            s.extra,
            s.source_authority,
            s.source_score,
            s.source_score_updated_at,
            sc.last_success_at,
            sc.last_error,
            sc.last_seen_published_at,
            COALESCE(metrics.recent_success_count, 0) AS recent_success_count,
            COALESCE(metrics.recent_error_count, 0) AS recent_error_count,
            COALESCE(post_metrics.relevant_ratio, 0) AS relevant_ratio,
            COALESCE(post_metrics.avg_tag_count, 0) AS avg_tag_count,
            COALESCE(post_metrics.linked_ratio, 0) AS linked_ratio,
            post_metrics.freshness_hours AS freshness_hours,
            sr.status AS last_run_status,
            sr.started_at AS last_run_started_at,
            sr.finished_at AS last_run_finished_at,
            sr.fetched_count AS last_run_fetched_count,
            sr.emitted_count AS last_run_emitted_count,
            sr.error_text AS last_run_error_text
        FROM sources s
        LEFT JOIN source_checkpoints sc ON sc.source_id = s.id
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE status = 'success') AS recent_success_count,
                COUNT(*) FILTER (WHERE status = 'error') AS recent_error_count
            FROM source_runs
            WHERE source_id = s.id
              AND started_at >= NOW() - INTERVAL '14 days'
        ) metrics ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                AVG(CASE WHEN COALESCE(p.relevance_score, 0) >= 0.6 THEN 1.0 ELSE 0.0 END) AS relevant_ratio,
                AVG(jsonb_array_length(COALESCE(p.tags, '[]'::jsonb))) AS avg_tag_count,
                AVG(CASE WHEN jsonb_array_length(COALESCE(pe.data->'items', '[]'::jsonb)) > 0 THEN 1.0 ELSE 0.0 END) AS linked_ratio,
                EXTRACT(EPOCH FROM (NOW() - MAX(p.published_at))) / 3600.0 AS freshness_hours
            FROM posts p
            LEFT JOIN post_enrichments pe
                ON pe.post_id = p.id
               AND pe.kind = 'crawl'
            WHERE p.source_id = s.id
              AND p.created_at >= NOW() - INTERVAL '30 days'
        ) post_metrics ON TRUE
        LEFT JOIN LATERAL (
            SELECT *
            FROM source_runs
            WHERE source_id = s.id
            ORDER BY started_at DESC
            LIMIT 1
        ) sr ON TRUE
        WHERE s.id = :source_id
        """,
        {"source_id": req.source_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Source not found")

    row = rows[0]
    health = source_quality_payload(row)
    recent_runs = await _fetch_rows(
        """
        SELECT
            id, started_at, finished_at, status, fetched_count, emitted_count, error_text
        FROM source_runs
        WHERE source_id = :source_id
        ORDER BY started_at DESC
        LIMIT :limit
        """,
        {"source_id": req.source_id, "limit": req.recent_runs_limit},
    )
    recent_posts = await _fetch_rows(
        """
        SELECT
            p.id,
            p.workspace_id,
            COALESCE(p.extra->>'url', '') AS url,
            LEFT(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(COALESCE(p.content, ''), '<[^>]+>', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                ),
                220
            ) AS preview,
            p.published_at,
            p.category,
            p.relevance_score,
            COALESCE(i.embedding_status, 'pending') AS embedding_status
        FROM posts p
        LEFT JOIN indexing_status i ON i.post_id = p.id
        WHERE p.source_id = :source_id
        ORDER BY COALESCE(p.published_at, p.created_at) DESC, p.created_at DESC
        LIMIT :limit
        """,
        {"source_id": req.source_id, "limit": req.recent_posts_limit},
    )

    return {
        "source": {
            "id": row["id"],
            "workspace_id": row["workspace_id"],
            "source_type": row["source_type"],
            "name": row["name"],
            "url": row.get("url"),
            "tg_channel": row.get("tg_channel"),
            "schedule_cron": row["schedule_cron"],
            "is_enabled": row["is_enabled"],
            "proxy_config": row.get("proxy_config"),
            "extra": row.get("extra") or {},
            "source_authority": health["source_authority"],
            "source_score": health["source_score"],
            "score_breakdown": health["score_breakdown"],
            "quality_tier": health["quality_tier"],
            "recommended_content_mode": health["recommended_content_mode"],
            "last_run_status": row.get("last_run_status"),
            "last_run_fetched_count": row.get("last_run_fetched_count"),
            "last_run_emitted_count": row.get("last_run_emitted_count"),
            "last_run_error_text": row.get("last_run_error_text"),
            "last_success_at": row.get("last_success_at"),
            "last_seen_published_at": row.get("last_seen_published_at"),
            "freshness_hours": row.get("freshness_hours"),
            "relevant_ratio": float(row.get("relevant_ratio") or 0.0),
            "avg_tag_count": float(row.get("avg_tag_count") or 0.0),
            "linked_ratio": float(row.get("linked_ratio") or 0.0),
        },
        "recent_runs": recent_runs,
        "recent_posts": recent_posts,
    }


@router.post("/list_emerging_signals")
async def list_emerging_signals(req: EmergingSignalListRequest) -> dict:
    return {
        "signals": await _fetch_rows(
            """
            SELECT
                id, workspace_id, title, signal_stage, signal_score, confidence,
                velocity_score, acceleration_score, baseline_rate, current_rate,
                change_point_count, change_point_strength, has_recent_change_point,
                source_count, keywords, recommended_watch_action, detected_at
            FROM emerging_signals
            WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
              AND signal_stage = ANY(:stages)
            ORDER BY detected_at DESC, signal_score DESC
            LIMIT :limit
            """,
            {
                "ws": req.workspace,
                "stages": _normalize_signal_stages(req.stages, default=("emerging",)),
                "limit": req.limit,
            },
        )
    }


@router.post("/list_missing_signals")
async def list_missing_signals(req: MissingSignalListRequest) -> dict:
    return {
        "signals": await _fetch_rows(
            """
            SELECT
                id, workspace_id, topic, gap_score, opportunity,
                searxng_frequency, frontier_frequency, evidence_urls, category,
                updated_at
            FROM missing_signals
            WHERE (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
            ORDER BY gap_score DESC, updated_at DESC
            LIMIT :limit
            """,
            {"ws": req.workspace, "limit": req.limit},
        )
    }


@router.post("/get_cluster_details")
async def get_cluster_details(req: ClusterDetailsRequest) -> dict:
    if req.kind in {"auto", "semantic"}:
        row = await _fetch_one("SELECT * FROM semantic_clusters WHERE id = :id", {"id": req.cluster_id})
        if row:
            return {"kind": "semantic", "cluster": row}
    if req.kind in {"auto", "trend"}:
        row = await _fetch_one("SELECT * FROM trend_clusters WHERE id = :id", {"id": req.cluster_id})
        if row:
            return {"kind": "trend", "cluster": row}
    if req.kind in {"auto", "emerging"}:
        row = await _fetch_one("SELECT * FROM emerging_signals WHERE id = :id", {"id": req.cluster_id})
        if row:
            return {"kind": "emerging", "cluster": row}
    if req.kind in {"auto", "missing"}:
        row = await _fetch_one("SELECT * FROM missing_signals WHERE id = :id", {"id": req.cluster_id})
        if row:
            return {"kind": "missing", "cluster": row}
    raise HTTPException(status_code=404, detail="Cluster not found")


@router.post("/get_missing_signal_details")
async def get_missing_signal_details(req: MissingSignalDetailsRequest) -> dict:
    row = await _fetch_one("SELECT * FROM missing_signals WHERE id = :id", {"id": req.signal_id})
    if not row:
        raise HTTPException(status_code=404, detail="Missing signal not found")
    return {"signal": row}


@router.post("/get_cluster_evidence")
async def get_cluster_evidence(req: ClusterEvidenceRequest) -> dict:
    semantic_row = None
    trend_row = None
    emerging_row = None

    if req.kind in {"auto", "semantic"}:
        semantic_row = await _fetch_one(
            """
            SELECT *
            FROM semantic_clusters
            WHERE id = :id
            """,
            {"id": req.cluster_id},
        )
    if req.kind in {"auto", "trend"} and semantic_row is None:
        trend_row = await _fetch_one(
            """
            SELECT *
            FROM trend_clusters
            WHERE id = :id
            """,
            {"id": req.cluster_id},
        )
    if req.kind in {"auto", "emerging"} and semantic_row is None and trend_row is None:
        emerging_row = await _fetch_one(
            """
            SELECT *
            FROM emerging_signals
            WHERE id = :id
            """,
            {"id": req.cluster_id},
        )

    if semantic_row:
        evidence = await _load_cluster_posts(semantic_row.get("doc_ids") or [], req.evidence_limit)
        return {
            "kind": "semantic",
            "cluster": {
                "id": semantic_row["id"],
                "workspace_id": semantic_row["workspace_id"],
                "title": semantic_row["title"],
                "representative_post_id": semantic_row.get("representative_post_id"),
                "post_count": semantic_row["post_count"],
                "source_ids": semantic_row.get("source_ids") or [],
                "top_concepts": semantic_row.get("top_concepts") or [],
                "time_window": semantic_row.get("time_window"),
                "embedding_version": semantic_row.get("embedding_version"),
                "first_seen_at": semantic_row.get("first_seen_at"),
                "last_seen_at": semantic_row.get("last_seen_at"),
                "detected_at": semantic_row.get("detected_at"),
            },
            "stored_evidence": semantic_row.get("evidence") or [],
            "evidence": evidence,
        }

    if trend_row:
        evidence = await _load_cluster_posts(trend_row.get("doc_ids") or [], req.evidence_limit)
        return {
            "kind": "trend",
            "cluster": {
                "id": trend_row["id"],
                "workspace_id": trend_row["workspace_id"],
                "pipeline": trend_row["pipeline"],
                "title": trend_row["title"],
                "insight": trend_row.get("insight"),
                "opportunity": trend_row.get("opportunity"),
                "time_horizon": trend_row.get("time_horizon"),
                "burst_score": trend_row.get("burst_score"),
                "coherence": trend_row.get("coherence"),
                "novelty": trend_row.get("novelty"),
                "velocity_score": trend_row.get("velocity_score"),
                "acceleration_score": trend_row.get("acceleration_score"),
                "baseline_rate": trend_row.get("baseline_rate"),
                "current_rate": trend_row.get("current_rate"),
                "change_point_count": trend_row.get("change_point_count"),
                "change_point_strength": trend_row.get("change_point_strength"),
                "has_recent_change_point": trend_row.get("has_recent_change_point"),
                "doc_count": trend_row.get("doc_count"),
                "keywords": trend_row.get("keywords") or [],
                "category": trend_row.get("category"),
                "detected_at": trend_row.get("detected_at"),
            },
            "evidence": evidence,
        }

    if emerging_row:
        evidence = await _load_cluster_posts(emerging_row.get("doc_ids") or [], req.evidence_limit)
        return {
            "kind": "emerging",
            "cluster": {
                "id": emerging_row["id"],
                "workspace_id": emerging_row["workspace_id"],
                "title": emerging_row["title"],
                "signal_stage": emerging_row.get("signal_stage"),
                "signal_score": emerging_row.get("signal_score"),
                "confidence": emerging_row.get("confidence"),
                "velocity_score": emerging_row.get("velocity_score"),
                "acceleration_score": emerging_row.get("acceleration_score"),
                "baseline_rate": emerging_row.get("baseline_rate"),
                "current_rate": emerging_row.get("current_rate"),
                "change_point_count": emerging_row.get("change_point_count"),
                "change_point_strength": emerging_row.get("change_point_strength"),
                "has_recent_change_point": emerging_row.get("has_recent_change_point"),
                "source_count": emerging_row.get("source_count"),
                "keywords": emerging_row.get("keywords") or [],
                "recommended_watch_action": emerging_row.get("recommended_watch_action"),
                "detected_at": emerging_row.get("detected_at"),
            },
            "stored_evidence": emerging_row.get("evidence") or [],
            "evidence": evidence,
        }

    raise HTTPException(status_code=404, detail="Cluster not found")


@router.post("/get_signal_timeline")
async def get_signal_timeline(req: SignalTimelineRequest) -> dict:
    cluster = None
    if req.entity_kind == "semantic":
        cluster = await _fetch_one("SELECT * FROM semantic_clusters WHERE id = :id", {"id": req.entity_id})
    elif req.entity_kind == "trend":
        cluster = await _fetch_one("SELECT * FROM trend_clusters WHERE id = :id", {"id": req.entity_id})
    elif req.entity_kind == "emerging":
        cluster = await _fetch_one("SELECT * FROM emerging_signals WHERE id = :id", {"id": req.entity_id})

    if not cluster:
        raise HTTPException(status_code=404, detail="Signal not found")

    series = await _fetch_rows(
        """
        SELECT
            entity_kind, entity_id, workspace_id, window_start, window_end, doc_count,
            source_count, avg_relevance, avg_source_score, freshness_score, window_rate, metadata_json
        FROM signal_time_series
        WHERE entity_kind = :entity_kind
          AND entity_id = :entity_id
          AND (CAST(:ws AS text) IS NULL OR workspace_id = CAST(:ws AS text))
        ORDER BY window_start ASC
        """,
        {"entity_kind": req.entity_kind, "entity_id": req.entity_id, "ws": req.workspace},
    )

    explainability = cluster.get("explainability") or {}
    change_points = (explainability.get("change_points") or {}) if isinstance(explainability, dict) else {}
    scores = (explainability.get("scores") or {}) if isinstance(explainability, dict) else {}

    return {
        "entity_kind": req.entity_kind,
        "entity_id": req.entity_id,
        "cluster": cluster,
        "series": series,
        "breakpoints": change_points.get("breakpoints") or [],
        "last_breakpoint_at": change_points.get("last_breakpoint_at"),
        "score_breakdown": scores,
    }
