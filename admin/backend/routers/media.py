"""Media router for admin UI."""

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine

router = APIRouter()


@router.get("")
async def list_media(
    workspace_id: str | None = None,
    mime_type: str | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    engine = get_engine()
    clauses: list[str] = []
    params: dict[str, object] = {"limit": limit}
    if workspace_id:
        clauses.append("mo.workspace_id = :workspace_id")
        params["workspace_id"] = workspace_id
    if mime_type:
        clauses.append("mo.mime_type = :mime_type")
        params["mime_type"] = mime_type
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                f"""
                SELECT
                    mo.sha256,
                    mo.s3_key,
                    mo.mime_type,
                    mo.size_bytes,
                    mo.workspace_id,
                    mo.created_at,
                    (
                        SELECT COUNT(DISTINCT p.id)
                        FROM posts p
                        JOIN LATERAL jsonb_array_elements_text(COALESCE(p.media_urls, '[]'::jsonb)) AS mu(value) ON TRUE
                        WHERE mu.value = mo.s3_key
                    ) AS posts_count
                FROM media_objects mo
                {where}
                ORDER BY mo.created_at DESC
                LIMIT :limit
                """
            ),
            params,
        )
        return [dict(r) for r in result.mappings().all()]


@router.get("/{sha256}")
async def get_media(sha256: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT * FROM media_objects WHERE sha256 = :sha"),
            {"sha": sha256},
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Media object not found")

        posts = await session.execute(
            text(
                """
                SELECT
                    p.id,
                    p.workspace_id,
                    p.source_id,
                    p.grouped_id,
                    p.content,
                    p.published_at,
                    p.created_at
                FROM posts p
                JOIN LATERAL jsonb_array_elements_text(COALESCE(p.media_urls, '[]'::jsonb)) AS mu(value) ON TRUE
                WHERE mu.value = :s3_key
                ORDER BY COALESCE(p.published_at, p.created_at) DESC, p.created_at DESC
                """
            ),
            {"s3_key": row["s3_key"]},
        )

        vision = await session.execute(
            text(
                """
                SELECT pe.post_id, pe.kind, pe.data, pe.s3_key, pe.updated_at
                FROM post_enrichments pe
                JOIN posts p ON p.id = pe.post_id
                JOIN LATERAL jsonb_array_elements_text(COALESCE(p.media_urls, '[]'::jsonb)) AS mu(value) ON TRUE
                WHERE pe.kind = 'vision' AND mu.value = :s3_key
                ORDER BY pe.updated_at DESC
                """
            ),
            {"s3_key": row["s3_key"]},
        )

        return {
            "media": dict(row),
            "posts": [dict(r) for r in posts.mappings().all()],
            "vision": [dict(r) for r in vision.mappings().all()],
        }
