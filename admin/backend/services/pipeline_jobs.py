from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from shared.source_quality import source_quality_payload
from worker.services.entity_resolution import run_entity_resolution
from worker.services.graph_maintenance import run_graph_maintenance
from worker.services.missing_signals import run_missing_signals_refresh
from worker.services.novelty_judge import run_novelty_judge
from worker.services.relevance_audit import run_relevance_audit_metrics
from worker.services.retrospective import run_retrospective_review
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


async def run_retrospective_review_job(workspace_id: str | None = None) -> dict:
    """Контур A (RSI): ретро-обзор weak-кандидатов → pending-предложения по порогам."""
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            results.append(await run_retrospective_review(active_workspace_id))
        totals = {
            "mature_snapshots": sum(int(item.get("mature_snapshots") or 0) for item in results),
            "proposals": sum(len(item.get("proposals") or []) for item in results),
        }
        logger.info(
            "Retrospective review completed for all workspaces=%s, mature_snapshots=%s, proposals=%s",
            [item.get("workspace_id") for item in results],
            totals["mature_snapshots"],
            totals["proposals"],
        )
        return {"status": "ok", "workspace_id": None, "results": results, **totals}

    result = await run_retrospective_review(workspace_id)
    logger.info(
        "Retrospective review completed for workspace=%s, mature_snapshots=%s, proposals=%s",
        workspace_id,
        result.get("mature_snapshots"),
        len(result.get("proposals") or []),
    )
    return {"status": "ok", **result}


async def _run_graph_for_workspaces(workspace_id: str | None, *, apply: bool) -> dict:
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            results.append(await run_graph_maintenance(active_workspace_id, apply=apply))
        totals = {
            "duplicate_groups_found": sum(int(item.get("duplicate_groups_found") or 0) for item in results),
            "merged_groups": sum(int(item.get("merged_groups") or 0) for item in results),
        }
        logger.info(
            "Graph maintenance (apply=%s) completed for all workspaces=%s, dup_groups=%s, merged=%s",
            apply,
            [item.get("workspace_id") for item in results],
            totals["duplicate_groups_found"],
            totals["merged_groups"],
        )
        return {"status": "ok", "workspace_id": None, "apply": apply, "results": results, **totals}

    result = await run_graph_maintenance(workspace_id, apply=apply)
    logger.info(
        "Graph maintenance (apply=%s) completed for workspace=%s, dup_groups=%s, merged=%s",
        apply, workspace_id, result.get("duplicate_groups_found"), result.get("merged_groups"),
    )
    return {"status": "ok", **result}


async def run_graph_maintenance_job(workspace_id: str | None = None) -> dict:
    """Контур D (RSI): метрики здоровья графа + детекция дублей (БЕЗ слияния)."""
    return await _run_graph_for_workspaces(workspace_id, apply=False)


async def run_entity_resolution_job(workspace_id: str | None = None) -> dict:
    """Контур D+ (RSI): кандидаты-акронимы → LLM-судья → pending-предложения слияния."""
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            results.append(await run_entity_resolution(active_workspace_id))
        totals = {
            "candidates": sum(int(item.get("candidates") or 0) for item in results),
            "proposals": sum(int(item.get("proposals") or 0) for item in results),
        }
        logger.info(
            "Entity resolution completed for all workspaces=%s, candidates=%s, proposals=%s",
            [item.get("workspace_id") for item in results],
            totals["candidates"], totals["proposals"],
        )
        return {"status": "ok", "workspace_id": None, "results": results, **totals}

    result = await run_entity_resolution(workspace_id)
    logger.info(
        "Entity resolution completed for workspace=%s, candidates=%s, proposals=%s",
        workspace_id, result.get("candidates"), result.get("proposals"),
    )
    return {"status": "ok", **result}


async def run_graph_resolution_job(workspace_id: str | None = None) -> dict:
    """Контур D (RSI): применить entity-resolution merge (мутирует граф) — операторский job."""
    return await _run_graph_for_workspaces(workspace_id, apply=True)


async def run_relevance_audit_job(workspace_id: str | None = None) -> dict:
    """Контур C (RSI): метрики аудита false-negative релевантности + предложение порога."""
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            results.append(await run_relevance_audit_metrics(active_workspace_id))
        totals = {
            "rejected_30d": sum(int(item.get("rejected_30d") or 0) for item in results),
            "false_negatives_30d": sum(int(item.get("false_negatives_30d") or 0) for item in results),
            "proposals": sum(1 for item in results if item.get("proposal")),
        }
        logger.info(
            "Relevance audit completed for all workspaces=%s, rejected=%s, false_negatives=%s, proposals=%s",
            [item.get("workspace_id") for item in results],
            totals["rejected_30d"],
            totals["false_negatives_30d"],
            totals["proposals"],
        )
        return {"status": "ok", "workspace_id": None, "results": results, **totals}

    result = await run_relevance_audit_metrics(workspace_id)
    logger.info(
        "Relevance audit completed for workspace=%s, rejected=%s, false_negatives=%s",
        workspace_id,
        result.get("rejected_30d"),
        result.get("false_negatives_30d"),
    )
    return {"status": "ok", **result}


async def run_novelty_judge_job(workspace_id: str | None = None) -> dict:
    """Контур B (RSI): кросс-семейный novelty-judge по weak-снимкам."""
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            results.append(await run_novelty_judge(active_workspace_id))
        totals = {
            "judged": sum(int(item.get("judged") or 0) for item in results),
            "underrated": sum(int(item.get("underrated") or 0) for item in results),
        }
        logger.info(
            "Novelty judge completed for all workspaces=%s, judged=%s, underrated=%s",
            [item.get("workspace_id") for item in results],
            totals["judged"],
            totals["underrated"],
        )
        return {"status": "ok", "workspace_id": None, "results": results, **totals}

    result = await run_novelty_judge(workspace_id)
    logger.info(
        "Novelty judge completed for workspace=%s, judged=%s, underrated=%s",
        workspace_id,
        result.get("judged"),
        result.get("underrated"),
    )
    return {"status": "ok", **result}


async def run_missing_signals_job(workspace_id: str | None = None) -> dict:
    if workspace_id is None:
        results = []
        for active_workspace_id in await list_active_workspace_ids():
            engine = get_engine()
            async with AsyncSession(engine) as session:
                items = await run_missing_signals_refresh(
                    session,
                    workspace_id=active_workspace_id,
                )
                await session.commit()
            results.append(
                {
                    "workspace_id": active_workspace_id,
                    "missing_signals": len(items),
                }
            )
        total = sum(int(item.get("missing_signals") or 0) for item in results)
        logger.info(
            "Missing signals refresh completed for all workspaces=%s, missing_signals=%s",
            [item.get("workspace_id") for item in results],
            total,
        )
        return {
            "status": "ok",
            "workspace_id": None,
            "results": results,
            "missing_signals": total,
        }

    engine = get_engine()
    async with AsyncSession(engine) as session:
        items = await run_missing_signals_refresh(session, workspace_id=workspace_id)
        await session.commit()
    logger.info(
        "Missing signals refresh completed for workspace=%s, missing_signals=%s",
        workspace_id,
        len(items),
    )
    return {"status": "ok", "workspace_id": workspace_id, "missing_signals": len(items)}
