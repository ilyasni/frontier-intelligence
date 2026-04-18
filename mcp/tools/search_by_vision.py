"""Search posts by stored vision labels, scenes, and OCR text."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_engine

router = APIRouter()


class VisionSearchRequest(BaseModel):
    model_config = {"extra": "forbid"}

    query: str = Field(default="")
    workspace: str | None = None
    limit: int = Field(default=20, ge=1, le=100)
    has_ocr: bool | None = None

    @field_validator("query", mode="before")
    @classmethod
    def _normalize_query(cls, value: object) -> str:
        return str(value or "").strip()

    @field_validator("workspace", mode="before")
    @classmethod
    def _normalize_workspace(cls, value: object) -> str | None:
        if value is None:
            return None
        text_value = str(value).strip()
        return text_value or None


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _vision_summary(data: dict[str, Any]) -> dict[str, Any]:
    labels = [str(item) for item in _as_list(data.get("all_labels")) if str(item).strip()]
    ocr_text = str(data.get("ocr_text") or "").strip()
    scenes: list[str] = []
    design_signals: list[str] = []
    for item in _as_list(data.get("items")):
        if not isinstance(item, dict):
            continue
        if item.get("scene"):
            scenes.append(str(item["scene"]))
        design_signals.extend(str(signal) for signal in _as_list(item.get("design_signals")))
    return {
        "labels": sorted(set(labels)),
        "ocr_text": ocr_text[:1000],
        "scenes": scenes[:5],
        "design_signals": sorted({item for item in design_signals if item})[:20],
        "item_count": len(_as_list(data.get("items"))),
    }


@router.post("")
async def search_by_vision(req: VisionSearchRequest) -> dict:
    pattern = f"%{req.query}%"
    clauses = ["pe.kind = 'vision'"]
    params: dict[str, Any] = {"limit": req.limit, "workspace": req.workspace, "pattern": pattern}
    if req.workspace:
        clauses.append("p.workspace_id = :workspace")
    if req.query:
        clauses.append("(pe.data::text ILIKE :pattern OR p.content ILIKE :pattern)")
    if req.has_ocr is True:
        clauses.append("COALESCE(pe.data->>'ocr_text', '') <> ''")
    elif req.has_ocr is False:
        clauses.append("COALESCE(pe.data->>'ocr_text', '') = ''")

    sql = f"""
        SELECT
            p.id AS post_id,
            p.workspace_id,
            p.source_id,
            COALESCE(s.name, p.source_id) AS source_name,
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
                220
            ) AS preview,
            pe.data AS vision
        FROM post_enrichments pe
        JOIN posts p ON p.id = pe.post_id
        LEFT JOIN sources s ON s.id = p.source_id
        WHERE {' AND '.join(clauses)}
        ORDER BY
            CASE WHEN :pattern = '%%' THEN 0 ELSE 1 END DESC,
            COALESCE(p.published_at, p.created_at) DESC
        LIMIT :limit
    """
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(text(sql), params)
        rows = [dict(row) for row in result.mappings().all()]
    return {
        "results": [
            {
                **{key: value for key, value in row.items() if key != "vision"},
                "vision": _vision_summary(row.get("vision") or {}),
            }
            for row in rows
        ],
        "applied_filters": {
            "query": req.query,
            "workspace": req.workspace,
            "limit": req.limit,
            "has_ocr": req.has_ocr,
        },
    }
