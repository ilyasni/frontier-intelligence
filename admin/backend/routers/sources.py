"""Source management API with normalized connector config and runtime status."""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from admin.backend.services.bootstrap_configs import bootstrap_sources_from_config
from shared.source_quality import source_quality_payload
from shared.source_definitions import (
    CANONICAL_SOURCE_TYPES,
    RSS_PRESETS,
    canonical_source_type,
    validate_source_payload,
)

router = APIRouter()


class SourceCreate(BaseModel):
    id: str
    workspace_id: str
    source_type: str
    name: str
    url: str | None = None
    tg_channel: str | None = None
    tg_account_idx: int = 0
    schedule_cron: str = "*/60 * * * *"
    proxy_config: dict[str, Any] = Field(default_factory=dict)
    extra: dict[str, Any] = Field(default_factory=dict)


class SourceVisionUpdate(BaseModel):
    mode: str = Field(..., pattern="^(full|ocr_only|skip)$")
    max_media_bytes: int = Field(default=9_000_000, ge=0, le=100_000_000)


class SourceCatalogEntry(BaseModel):
    key: str
    name: str
    url: str


@router.get("/catalog")
async def get_source_catalog():
    return {
        "source_types": list(CANONICAL_SOURCE_TYPES),
        "rss_presets": [
            SourceCatalogEntry(key=key, name=value["name"], url=value["url"]).model_dump()
            for key, value in RSS_PRESETS.items()
        ],
        "starter_bundle_keys": [
            "techcrunch",
            "wired_ai",
            "wired_business",
            "medium_future",
            "medium_design",
            "medium_mobility",
            "mit_tech_review",
            "arxiv_cs_ai",
            "arxiv_cs_lg",
            "arxiv_cs_hc",
            "arxiv_cs_ro",
            "insideevs_all",
            "insideevs_autonomous",
            "electrek",
        ],
        "starter_bundles": {
            "disruption": [
                "rss_insideevs_all",
                "rss_insideevs_autonomous",
                "rss_electrek",
                "web_waymo_blog",
            ],
            "ai_trends": [
                "ai_rss_techcrunch",
                "ai_rss_wired_ai",
                "ai_rss_mit_tech_review",
                "ai_rss_arxiv_cs_ai",
                "ai_rss_arxiv_cs_lg",
                "ai_api_hn_topstories",
                "ai_rss_habr_ai_hub",
            ],
            "design": [
                "design_rss_medium_design",
                "design_rss_arxiv_cs_hc",
                "design_rss_habr_design_articles",
                "design_rss_insideevs_design",
                "design_rss_insideevs_ux",
            ],
        },
    }


@router.get("")
async def list_sources(workspace_id: str | None = None):
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
        ORDER BY s.created_at DESC
    """
    async with AsyncSession(engine) as session:
        result = await session.execute(text(sql), {"ws": workspace_id})
        rows = []
        for row in result.mappings().all():
            item = dict(row)
            item["source_type"] = canonical_source_type(item.get("source_type", ""))
            item.update(source_quality_payload(item))
            rows.append(item)
        return rows


@router.post("/bootstrap")
async def bootstrap_sources(workspace_id: str | None = None):
    return await bootstrap_sources_from_config(workspace_id)


@router.post("")
async def create_source(src: SourceCreate):
    try:
        source_type, url, tg_channel, extra = validate_source_payload(
            src.source_type,
            src.url,
            src.tg_channel,
            src.extra,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                """
                INSERT INTO sources (
                    id, workspace_id, source_type, name, url, tg_channel,
                    tg_account_idx, schedule_cron, is_enabled, proxy_config, extra,
                    source_authority,
                    created_at, updated_at
                )
                VALUES (
                    :id, :workspace_id, :source_type, :name, :url, :tg_channel,
                    :tg_account_idx, :schedule_cron, true, CAST(:proxy_config AS jsonb),
                    CAST(:extra AS jsonb), :source_authority, NOW(), NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    workspace_id = EXCLUDED.workspace_id,
                    source_type = EXCLUDED.source_type,
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    tg_channel = EXCLUDED.tg_channel,
                    tg_account_idx = EXCLUDED.tg_account_idx,
                    schedule_cron = EXCLUDED.schedule_cron,
                    proxy_config = EXCLUDED.proxy_config,
                    extra = EXCLUDED.extra,
                    source_authority = EXCLUDED.source_authority,
                    updated_at = NOW()
                """
            ),
            {
                "id": src.id,
                "workspace_id": src.workspace_id,
                "source_type": source_type,
                "name": src.name,
                "url": url,
                "tg_channel": tg_channel,
                "tg_account_idx": src.tg_account_idx,
                "schedule_cron": src.schedule_cron,
                "proxy_config": json.dumps(src.proxy_config or {}),
                "extra": json.dumps(extra),
                "source_authority": float(extra.get("source_authority", 0.5) or 0.5),
            },
        )
        await session.commit()
    return {"status": "ok", "id": src.id, "source_type": source_type}


@router.patch("/{source_id}/toggle")
async def toggle_source(source_id: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                """
                UPDATE sources
                SET is_enabled = NOT is_enabled, updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": source_id},
        )
        await session.commit()
    return {"status": "ok"}


@router.patch("/{source_id}/vision")
async def update_source_vision_policy(source_id: str, payload: SourceVisionUpdate):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT source_type, url, tg_channel, extra FROM sources WHERE id = :id"),
            {"id": source_id},
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Source not found")

        extra = dict(row.get("extra") or {})
        extra["vision"] = {
            **(extra.get("vision") or {}),
            "mode": payload.mode,
            "max_media_bytes": payload.max_media_bytes,
        }
        try:
            _, _, _, normalized_extra = validate_source_payload(
                row["source_type"],
                row.get("url"),
                row.get("tg_channel"),
                extra,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        await session.execute(
            text(
                """
                UPDATE sources
                SET extra = CAST(:extra AS jsonb), updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": source_id, "extra": json.dumps(normalized_extra)},
        )
        await session.commit()
    return {
        "status": "ok",
        "id": source_id,
        "vision": normalized_extra.get("vision") or {},
    }


@router.delete("/{source_id}")
async def delete_source(source_id: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(text("DELETE FROM sources WHERE id = :id"), {"id": source_id})
        await session.commit()
    return {"status": "ok"}
