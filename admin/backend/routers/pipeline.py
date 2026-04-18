"""Pipeline monitoring router — matches actual DB schema."""
import json

import redis.asyncio as aioredis
from fastapi import APIRouter, Body
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from admin.backend.scheduler import get_manual_job
from admin.backend.scheduler import launch_manual_job
from admin.backend.scheduler import list_manual_jobs
from admin.backend.scheduler import scheduler_status
from admin.backend.services.pipeline_jobs import (
    refresh_source_scores as refresh_source_scores_job,
    run_semantic_cluster_job,
    run_signal_analysis_job,
)
from shared.config import get_settings
from shared.redis_streams import collect_redis_stream_snapshot
from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls

router = APIRouter()


class WorkspaceActionRequest(BaseModel):
    workspace_id: str | None = None


class JobTriggerRequest(WorkspaceActionRequest):
    wait: bool = False


class BulkReprocessRequest(BaseModel):
    workspace_id: str
    source_ids: list[str] | None = None
    limit: int = 100


@router.get("/scheduler")
async def pipeline_scheduler_status():
    return scheduler_status()


@router.get("/jobs/manual")
async def pipeline_manual_jobs(
    job_name: str | None = None,
    workspace_id: str | None = None,
    only_running: bool = False,
    limit: int = 20,
):
    return {
        "jobs": await list_manual_jobs(
            job_name=job_name,
            workspace_id=workspace_id,
            only_running=only_running,
            limit=limit,
        )
    }


@router.get("/jobs/manual/{job_id}")
async def pipeline_manual_job(job_id: str):
    job = await get_manual_job(job_id)
    return job or {}


@router.get("/stats")
async def pipeline_stats(workspace_id: str = None):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        ws_where = "WHERE p.workspace_id = :ws" if workspace_id else ""
        params = {"ws": workspace_id} if workspace_id else {}

        # Stats by embedding status
        result = await session.execute(text(f"""
            SELECT
                i.embedding_status as status,
                COUNT(*) as count,
                AVG(p.relevance_score) as avg_score
            FROM indexing_status i
            JOIN posts p ON p.id = i.post_id
            {ws_where}
            GROUP BY i.embedding_status
        """), params)
        stats = {
            r["status"]: {"count": r["count"], "avg_score": float(r["avg_score"] or 0)}
            for r in result.mappings().all()
        }

        # Recent posts
        result2 = await session.execute(text(f"""
            SELECT
                p.id, p.source_id, p.workspace_id,
                LEFT(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(COALESCE(p.content, ''), '<[^>]+>', ' ', 'g'),
                        '\s+',
                        ' ',
                        'g'
                    ),
                    120
                ) as preview,
                p.published_at, p.relevance_score, p.category,
                i.embedding_status, i.retry_count, i.error_message
            FROM posts p
            LEFT JOIN indexing_status i ON i.post_id = p.id
            {ws_where}
            ORDER BY p.created_at DESC
            LIMIT 30
        """), params)
        recent = [dict(r) for r in result2.mappings().all()]

        return {"stats": stats, "recent": recent}


@router.get("/streams")
async def pipeline_streams():
    settings = get_settings()
    return await collect_redis_stream_snapshot(settings.redis_url)


async def _queue_post_reprocess(post_id: str) -> dict:
    """Re-queue a post for enrichment by resetting indexing_status and re-publishing the parsed event."""
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT * FROM posts WHERE id = :id"), {"id": post_id}
        )
        row = result.mappings().first()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Post not found")

        p = dict(row)

        # Сброс статуса (UPSERT — строка indexing_status могла отсутствовать)
        await session.execute(
            text(
                """
                INSERT INTO indexing_status (
                    post_id, embedding_status, retry_count, error_message, updated_at
                )
                VALUES (:id, 'pending', 0, NULL, NOW())
                ON CONFLICT (post_id) DO UPDATE SET
                    embedding_status = 'pending',
                    retry_count = 0,
                    error_message = NULL,
                    updated_at = NOW()
                """
            ),
            {"id": post_id},
        )
        await session.commit()

    # Push back to stream with полями для Vision и crawl (linked_urls из content)
    settings = get_settings()
    extra = p.get("extra") or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    media_urls = p.get("media_urls") or []
    if isinstance(media_urls, str):
        try:
            media_urls = json.loads(media_urls)
        except Exception:
            media_urls = []
    if not isinstance(media_urls, list):
        media_urls = []

    content = p.get("content") or ""
    linked = finalize_linked_urls(extract_urls_from_plain_text(content))

    # Не кладём пустой published_at: Pydantic не принимает "" для
    # Optional[datetime], и событие иначе отбрасывается со статусом pending.
    event: dict = {
        "source_id": str(p["source_id"]) if p.get("source_id") else "",
        "workspace_id": p["workspace_id"],
        "external_id": p["external_id"] or "",
        "content": content,
        "has_media": str(p.get("has_media", False)),
        "media_urls": json.dumps(media_urls),
        "linked_urls": json.dumps(linked),
        "url": extra.get("url", ""),
        "author": extra.get("author", ""),
    }
    pub = p.get("published_at")
    if pub is not None:
        event["published_at"] = pub.isoformat() if hasattr(pub, "isoformat") else str(pub)
    _gid = p.get("grouped_id")
    event["grouped_id"] = str(_gid) if _gid is not None else ""
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    await client.xadd("stream:posts:parsed", event)
    await client.aclose()

    return {"status": "ok", "post_id": post_id}


@router.post("/reprocess/{post_id}")
async def reprocess_post(post_id: str):
    return await _queue_post_reprocess(post_id)


@router.post("/reprocess")
async def reprocess_posts(request: BulkReprocessRequest):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        where = ["workspace_id = :workspace_id"]
        params: dict[str, object] = {
            "workspace_id": request.workspace_id,
            "limit": max(1, min(int(request.limit or 100), 500)),
        }
        if request.source_ids:
            where.append("source_id = ANY(:source_ids)")
            params["source_ids"] = list(dict.fromkeys(request.source_ids))

        result = await session.execute(
            text(
                f"""
                SELECT id
                FROM posts
                WHERE {' AND '.join(where)}
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            params,
        )
        post_ids = [str(row["id"]) for row in result.mappings().all()]

    requeued = []
    for post_id in post_ids:
        await _queue_post_reprocess(post_id)
        requeued.append(post_id)

    return {
        "status": "ok",
        "workspace_id": request.workspace_id,
        "requested_sources": request.source_ids or [],
        "requeued_count": len(requeued),
        "post_ids": requeued,
    }


@router.post("/refresh-source-scores")
async def refresh_source_scores(
    workspace_id: str | None = None,
    request: WorkspaceActionRequest | None = Body(default=None),
):
    target_workspace = request.workspace_id if request and request.workspace_id else workspace_id
    return await refresh_source_scores_job(target_workspace)


@router.post("/run-semantic-clusters")
async def trigger_semantic_clusters(
    workspace_id: str | None = None,
    wait: bool = False,
    request: JobTriggerRequest | None = Body(default=None),
):
    target_workspace = request.workspace_id if request and request.workspace_id else workspace_id
    should_wait = bool(request.wait) if request is not None else bool(wait)
    if should_wait:
        return await run_semantic_cluster_job(target_workspace)
    return await launch_manual_job(
        job_name="run_semantic_clusters",
        workspace_id=target_workspace,
        runner=run_semantic_cluster_job,
    )


@router.post("/run-signal-analysis")
async def trigger_signal_analysis(
    workspace_id: str | None = None,
    wait: bool = False,
    request: JobTriggerRequest | None = Body(default=None),
):
    target_workspace = request.workspace_id if request and request.workspace_id else workspace_id
    should_wait = bool(request.wait) if request is not None else bool(wait)
    if should_wait:
        return await run_signal_analysis_job(target_workspace)
    return await launch_manual_job(
        job_name="run_signal_analysis",
        workspace_id=target_workspace,
        runner=run_signal_analysis_job,
    )
