"""Albums router for admin UI."""

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine

router = APIRouter()


@router.get("")
async def list_albums(
    workspace_id: str | None = None,
    source_id: str | None = None,
    assembled: bool | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    engine = get_engine()
    clauses: list[str] = []
    params: dict[str, object] = {"limit": limit}
    if workspace_id:
        clauses.append("mg.workspace_id = :workspace_id")
        params["workspace_id"] = workspace_id
    if source_id:
        clauses.append("mg.source_id = :source_id")
        params["source_id"] = source_id
    if assembled is not None:
        clauses.append("mg.assembled = :assembled")
        params["assembled"] = assembled
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                f"""
                SELECT
                    mg.id,
                    mg.workspace_id,
                    mg.source_id,
                    mg.grouped_id,
                    mg.item_count,
                    mg.assembled,
                    mg.vision_summary_s3_key,
                    mg.vision_labels,
                    mg.created_at,
                    mg.updated_at,
                    COUNT(p.id) AS posts_count,
                    MAX(COALESCE(p.published_at, p.created_at)) AS last_post_at
                FROM media_groups mg
                LEFT JOIN posts p
                  ON p.workspace_id = mg.workspace_id
                 AND p.source_id = mg.source_id
                 AND p.grouped_id = mg.grouped_id
                {where}
                GROUP BY mg.id
                ORDER BY COALESCE(MAX(COALESCE(p.published_at, p.created_at)), mg.updated_at) DESC
                LIMIT :limit
                """
            ),
            params,
        )
        return [dict(r) for r in result.mappings().all()]


@router.get("/{album_id}")
async def get_album(album_id: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT * FROM media_groups WHERE id = :id"),
            {"id": album_id},
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Album not found")

        posts = await session.execute(
            text(
                """
                SELECT
                    p.id,
                    p.external_id,
                    p.content,
                    p.has_media,
                    p.media_urls,
                    p.published_at,
                    p.created_at,
                    COALESCE(i.embedding_status, 'pending') AS embedding_status,
                    COALESCE(i.vision_status, 'pending') AS vision_status,
                    COALESCE(i.graph_status, 'pending') AS graph_status
                FROM posts p
                LEFT JOIN indexing_status i ON i.post_id = p.id
                WHERE p.workspace_id = :workspace_id
                  AND p.source_id = :source_id
                  AND p.grouped_id = :grouped_id
                ORDER BY COALESCE(p.published_at, p.created_at) DESC, p.created_at DESC
                """
            ),
            {
                "workspace_id": row["workspace_id"],
                "source_id": row["source_id"],
                "grouped_id": row["grouped_id"],
            },
        )

        return {"album": dict(row), "posts": [dict(r) for r in posts.mappings().all()]}
