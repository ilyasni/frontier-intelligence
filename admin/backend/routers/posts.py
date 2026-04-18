"""Posts router for admin UI."""

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine

router = APIRouter()


@router.get("")
async def list_posts(
    workspace_id: str | None = None,
    source_id: str | None = None,
    status: str | None = None,
    has_media: bool | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    engine = get_engine()
    clauses: list[str] = []
    params: dict[str, object] = {"limit": limit}
    if workspace_id:
        clauses.append("p.workspace_id = :workspace_id")
        params["workspace_id"] = workspace_id
    if source_id:
        clauses.append("p.source_id = :source_id")
        params["source_id"] = source_id
    if status:
        clauses.append("COALESCE(i.embedding_status, 'pending') = :status")
        params["status"] = status
    if has_media is not None:
        clauses.append("p.has_media = :has_media")
        params["has_media"] = has_media
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                f"""
                SELECT
                    p.id,
                    p.workspace_id,
                    p.source_id,
                    p.external_id,
                    p.grouped_id,
                    p.category,
                    p.relevance_score,
                    p.has_media,
                    p.media_urls,
                    p.tags,
                    p.published_at,
                    p.created_at,
                    LEFT(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(COALESCE(p.content, ''), '<[^>]+>', ' ', 'g'),
                            '\s+',
                            ' ',
                            'g'
                        ),
                        240
                    ) AS preview,
                    COALESCE(i.embedding_status, 'pending') AS embedding_status,
                    COALESCE(i.vision_status, 'pending') AS vision_status,
                    COALESCE(i.graph_status, 'pending') AS graph_status,
                    i.retry_count,
                    i.error_message,
                    i.qdrant_point_id
                FROM posts p
                LEFT JOIN indexing_status i ON i.post_id = p.id
                {where}
                ORDER BY COALESCE(p.published_at, p.created_at) DESC, p.created_at DESC
                LIMIT :limit
                """
            ),
            params,
        )
        return [dict(r) for r in result.mappings().all()]


@router.get("/{post_id}")
async def get_post(post_id: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT
                    p.*,
                    COALESCE(i.embedding_status, 'pending') AS embedding_status,
                    COALESCE(i.vision_status, 'pending') AS vision_status,
                    COALESCE(i.graph_status, 'pending') AS graph_status,
                    i.retry_count,
                    i.error_message,
                    i.qdrant_point_id
                FROM posts p
                LEFT JOIN indexing_status i ON i.post_id = p.id
                WHERE p.id = :id
                """
            ),
            {"id": post_id},
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Post not found")

        enrichments = await session.execute(
            text(
                """
                SELECT id, kind, data, s3_key, created_at, updated_at
                FROM post_enrichments
                WHERE post_id = :id
                ORDER BY updated_at DESC, created_at DESC
                """
            ),
            {"id": post_id},
        )

        related_album = None
        if row.get("grouped_id"):
            album = await session.execute(
                text(
                    """
                    SELECT
                        id, grouped_id, item_count, assembled,
                        vision_summary_s3_key, vision_labels
                    FROM media_groups
                    WHERE workspace_id = :workspace_id
                      AND source_id = :source_id
                      AND grouped_id = :grouped_id
                    LIMIT 1
                    """
                ),
                {
                    "workspace_id": row["workspace_id"],
                    "source_id": row["source_id"],
                    "grouped_id": row["grouped_id"],
                },
            )
            related_album = album.mappings().first()

        return {
            "post": dict(row),
            "enrichments": [dict(r) for r in enrichments.mappings().all()],
            "album": dict(related_album) if related_album else None,
        }
