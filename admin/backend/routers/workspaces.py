"""Workspaces router."""
import json
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from admin.backend.services.bootstrap_configs import bootstrap_workspaces_from_config
from shared.workspace_relevance import merge_relevance_weights

router = APIRouter()


class WorkspaceCreate(BaseModel):
    id: str
    name: str
    description: str = ""
    categories: list[str] = Field(default_factory=list)
    relevance_threshold: float = 0.6
    design_lenses: list[str] = Field(default_factory=list)
    cross_workspace_bridges: list[str] = Field(default_factory=list)
    extra: dict = Field(default_factory=dict)


class WorkspaceUpdate(BaseModel):
    """Частичное обновление (PATCH); только переданные поля применяются."""

    name: Optional[str] = None
    description: Optional[str] = None
    categories: Optional[list[str]] = None
    relevance_threshold: Optional[float] = None
    design_lenses: Optional[list[str]] = None
    cross_workspace_bridges: Optional[list[str]] = None
    extra: Optional[dict] = None
    is_active: Optional[bool] = None


@router.get("")
async def list_workspaces():
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(text("SELECT * FROM workspaces ORDER BY created_at DESC"))
        rows = result.mappings().all()
        return [dict(r) for r in rows]


@router.post("/bootstrap")
async def bootstrap_workspaces(workspace_id: str | None = None):
    return await bootstrap_workspaces_from_config(workspace_id)


@router.get("/{workspace_id}")
async def get_workspace(workspace_id: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT * FROM workspaces WHERE id = :id"), {"id": workspace_id}
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Workspace not found")
        return dict(row)


@router.post("")
async def create_workspace(ws: WorkspaceCreate):
    """Создание или upsert по id; relevance_weights сливаются с существующими в БД."""
    engine = get_engine()
    async with AsyncSession(engine) as session:
        prev = await session.execute(
            text("SELECT relevance_weights FROM workspaces WHERE id = :id"),
            {"id": ws.id},
        )
        old_weights = prev.scalar_one_or_none()
        merged_w = merge_relevance_weights(old_weights, new_threshold=ws.relevance_threshold)

        await session.execute(
            text("""
            INSERT INTO workspaces (id, name, description, categories, relevance_weights,
                design_lenses, cross_workspace_bridges, extra, is_active, created_at, updated_at)
            VALUES (:id, :name, :description, CAST(:categories AS jsonb),
                CAST(:weights AS jsonb), CAST(:lenses AS jsonb),
                CAST(:bridges AS jsonb), CAST(:extra AS jsonb), true, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name, description = EXCLUDED.description,
                categories = EXCLUDED.categories,
                relevance_weights = EXCLUDED.relevance_weights,
                design_lenses = EXCLUDED.design_lenses,
                cross_workspace_bridges = EXCLUDED.cross_workspace_bridges,
                extra = EXCLUDED.extra,
                updated_at = NOW()
        """),
            {
                "id": ws.id,
                "name": ws.name,
                "description": ws.description,
                "categories": json.dumps(ws.categories),
                "weights": json.dumps(merged_w),
                "lenses": json.dumps(ws.design_lenses),
                "bridges": json.dumps(ws.cross_workspace_bridges),
                "extra": json.dumps(ws.extra),
            },
        )
        await session.commit()
    return {"status": "ok", "id": ws.id}


@router.patch("/{workspace_id}")
async def patch_workspace(workspace_id: str, body: WorkspaceUpdate):
    """Частичное обновление workspace; threshold сливается в relevance_weights."""
    patch = body.model_dump(exclude_unset=True)
    if not patch:
        raise HTTPException(status_code=400, detail="No fields to update")

    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT * FROM workspaces WHERE id = :id"),
            {"id": workspace_id},
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Workspace not found")

        current = dict(row)
        set_parts: list[str] = []
        params: dict = {"id": workspace_id}

        if "name" in patch:
            set_parts.append("name = :name")
            params["name"] = patch["name"]
        if "description" in patch:
            set_parts.append("description = :description")
            params["description"] = patch["description"]
        if "categories" in patch:
            set_parts.append("categories = CAST(:categories AS jsonb)")
            params["categories"] = json.dumps(patch["categories"])
        if "design_lenses" in patch:
            set_parts.append("design_lenses = CAST(:design_lenses AS jsonb)")
            params["design_lenses"] = json.dumps(patch["design_lenses"])
        if "cross_workspace_bridges" in patch:
            set_parts.append("cross_workspace_bridges = CAST(:cross_workspace_bridges AS jsonb)")
            params["cross_workspace_bridges"] = json.dumps(patch["cross_workspace_bridges"])
        if "extra" in patch:
            set_parts.append("extra = CAST(:extra AS jsonb)")
            params["extra"] = json.dumps(patch["extra"])
        if "is_active" in patch:
            set_parts.append("is_active = :is_active")
            params["is_active"] = patch["is_active"]

        if "relevance_threshold" in patch:
            merged = merge_relevance_weights(
                current.get("relevance_weights"),
                new_threshold=patch["relevance_threshold"],
            )
            set_parts.append("relevance_weights = CAST(:relevance_weights AS jsonb)")
            params["relevance_weights"] = json.dumps(merged)

        if not set_parts:
            raise HTTPException(status_code=400, detail="No fields to update")

        set_parts.append("updated_at = NOW()")
        sql = "UPDATE workspaces SET " + ", ".join(set_parts) + " WHERE id = :id"
        await session.execute(text(sql), params)
        await session.commit()

    return {"status": "ok", "id": workspace_id}


@router.patch("/{workspace_id}/toggle")
async def toggle_workspace(workspace_id: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(
            text("""
            UPDATE workspaces SET is_active = NOT is_active, updated_at = NOW()
            WHERE id = :id
        """),
            {"id": workspace_id},
        )
        await session.commit()
    return {"status": "ok"}
