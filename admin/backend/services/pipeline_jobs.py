from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from shared.source_quality import source_quality_payload
from worker.services.semantic_clustering import run_semantic_clustering, run_signal_analysis

logger = logging.getLogger(__name__)


async def list_active_workspace_ids() -> list[str]:
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT id
                FROM workspaces
                WHERE COALESCE(is_active, true) = true
                ORDER BY created_at ASC, id ASC
                """
            )
        )
        return [str(row["id"]) for row in result.mappings().all()]


async def refresh_source_scores(workspace_id: str | None = None) -> dict:
    engine = get_engine()
    sql = """
        SELECT
            s.*,
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
                AVG(CASE WHEN pe.id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS linked_ratio,
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
    """
    async with AsyncSession(engine) as session:
        result = await session.execute(text(sql), {"ws": workspace_id})
        rows = [dict(row) for row in result.mappings().all()]
        refreshed = []
        for row in rows:
            payload = source_quality_payload(row)
            await session.execute(
                text(
                    """
                    UPDATE sources
                    SET source_authority = :source_authority,
                        source_score = :source_score,
                        source_score_updated_at = NOW(),
                        updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {
                    "id": row["id"],
                    "source_authority": payload["source_authority"],
                    "source_score": payload["source_score"],
                },
            )
            refreshed.append(
                {
                    "id": row["id"],
                    "source_score": payload["source_score"],
                    "recommended_content_mode": payload["recommended_content_mode"],
                }
            )
        await session.commit()
    logger.info(
        "Refreshed source scores for workspace=%s, sources=%d",
        workspace_id,
        len(refreshed),
    )
    return {"status": "ok", "workspace_id": workspace_id, "refreshed": refreshed}


async def run_semantic_cluster_job(workspace_id: str | None = None) -> dict:
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            results.append(await run_semantic_clustering(active_workspace_id))
        totals = {
            "semantic_clusters": sum(int(item.get("semantic_clusters") or 0) for item in results),
            "trend_clusters": sum(int(item.get("trend_clusters") or 0) for item in results),
            "emerging_signals": sum(int(item.get("emerging_signals") or 0) for item in results),
        }
        logger.info(
            "Cluster analysis completed for all workspaces=%s, semantic_clusters=%s, trend_clusters=%s, emerging_signals=%s",
            [item.get("workspace_id") for item in results],
            totals["semantic_clusters"],
            totals["trend_clusters"],
            totals["emerging_signals"],
        )
        return {"status": "ok", "workspace_id": None, "results": results, **totals}

    result = await run_semantic_clustering(workspace_id)
    logger.info(
        "Cluster analysis completed for workspace=%s, semantic_clusters=%s, trend_clusters=%s, emerging_signals=%s",
        workspace_id,
        result.get("semantic_clusters"),
        result.get("trend_clusters"),
        result.get("emerging_signals"),
    )
    return {"status": "ok", **result}


async def run_signal_analysis_job(workspace_id: str | None = None) -> dict:
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            results.append(await run_signal_analysis(active_workspace_id))
        totals = {
            "semantic_clusters": sum(int(item.get("semantic_clusters") or 0) for item in results),
            "trend_clusters": sum(int(item.get("trend_clusters") or 0) for item in results),
            "emerging_signals": sum(int(item.get("emerging_signals") or 0) for item in results),
            "missing_signals": sum(int(item.get("missing_signals") or 0) for item in results),
        }
        logger.info(
            "Signal analysis completed for all workspaces=%s, semantic_clusters=%s, trend_clusters=%s, emerging_signals=%s, missing_signals=%s",
            [item.get("workspace_id") for item in results],
            totals["semantic_clusters"],
            totals["trend_clusters"],
            totals["emerging_signals"],
            totals["missing_signals"],
        )
        return {"status": "ok", "workspace_id": None, "results": results, **totals}

    result = await run_signal_analysis(workspace_id)
    logger.info(
        "Signal analysis completed for workspace=%s, semantic_clusters=%s, trend_clusters=%s, emerging_signals=%s, missing_signals=%s",
        workspace_id,
        result.get("semantic_clusters"),
        result.get("trend_clusters"),
        result.get("emerging_signals"),
        result.get("missing_signals"),
    )
    return {"status": "ok", **result}
