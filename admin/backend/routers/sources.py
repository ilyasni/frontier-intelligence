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


def _normalize_checkpoint_cursor(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return raw if isinstance(raw, dict) else {}


# Substrings (case-insensitive) that mark a JSON key as sensitive. Any key whose
# name contains one of these has its value masked before the source is returned.
_SECRET_KEY_PATTERNS: tuple[str, ...] = (
    "password",
    "pass",
    "secret",
    "token",
    "api_key",
    "apikey",
    "credential",
)
_SECRET_MASK = "***"


def _looks_secret(key: str) -> bool:
    lowered = str(key).lower()
    return any(pattern in lowered for pattern in _SECRET_KEY_PATTERNS)


def _redact_secrets(value: Any) -> Any:
    """Recursively mask secret-looking values in a proxy_config/extra blob.

    Accepts a dict, a JSON string, or anything else. JSON strings are parsed so
    nested secrets are masked too; unparseable strings are returned unchanged.
    The input is never mutated — a redacted copy is returned.
    """
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
        return _redact_secrets(parsed)
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if _looks_secret(key):
                redacted[key] = _SECRET_MASK
            else:
                redacted[key] = _redact_secrets(item)
        return redacted
    if isinstance(value, list):
        return [_redact_secrets(item) for item in value]
    return value


def _redact_source_secrets(item: dict[str, Any]) -> dict[str, Any]:
    """Mask secrets in the proxy_config/extra fields of a source response dict."""
    for field in ("proxy_config", "extra"):
        if field in item and item[field] is not None:
            item[field] = _redact_secrets(item[field])
    return item


def _build_telegram_diagnostics(item: dict[str, Any]) -> dict[str, Any] | None:
    if canonical_source_type(item.get("source_type", "")) != "telegram":
        return None

    configured_channel = str(item.get("tg_channel") or "").strip()
    configured_username = configured_channel.lstrip("@").strip().lower()
    checkpoint = _normalize_checkpoint_cursor(item.get("cursor_json"))
    peer = checkpoint.get("telegram_peer") or {}
    peer = peer if isinstance(peer, dict) else {}

    resolved_username = str(peer.get("username") or "").strip().lstrip("@").lower()
    entity_id = peer.get("entity_id")
    last_error = str(item.get("last_error") or item.get("last_run_error_text") or "").strip()

    status = "unresolved"
    if entity_id and resolved_username and configured_username:
        status = "stable" if resolved_username == configured_username else "drift"
    elif entity_id:
        status = "cached-id"
    elif "telegram_username_unresolved" in last_error:
        status = "unresolved"
    else:
        status = "pending"

    return {
        "status": status,
        "configured_channel": configured_channel,
        "configured_username": configured_username or None,
        "resolved_username": resolved_username or None,
        "entity_id": int(entity_id) if isinstance(entity_id, int | float) else entity_id,
        "title": str(peer.get("title") or "").strip() or None,
        "has_cached_peer": bool(entity_id),
        "username_mismatch": bool(
            configured_username and resolved_username and configured_username != resolved_username
        ),
    }


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


class SourceTelegramHandleUpdate(BaseModel):
    tg_channel: str


class SourceCatalogEntry(BaseModel):
    key: str
    name: str
    url: str


def _normalize_telegram_channel(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("Telegram channel is required")
    if value.startswith("https://t.me/"):
        value = value[len("https://t.me/") :]
    elif value.startswith("http://t.me/"):
        value = value[len("http://t.me/") :]
    elif value.startswith("t.me/"):
        value = value[len("t.me/") :]
    value = value.strip().strip("/")
    if not value:
        raise ValueError("Telegram channel is required")
    if not value.startswith("@"):
        value = f"@{value}"
    return value


def _update_checkpoint_cursor_for_handle(cursor: dict[str, Any] | None, tg_channel: str) -> dict[str, Any]:
    payload = dict(cursor or {})
    peer = payload.get("telegram_peer") or {}
    if not isinstance(peer, dict):
        peer = {}
    if peer.get("entity_id"):
        payload["telegram_peer"] = {
            **peer,
            "username": tg_channel.lstrip("@"),
        }
    return payload


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
    # post_metrics вынесены из коррелированного LATERAL (141 пересканирование постов = ~3.6с)
    # в один агрегат с GROUP BY (один проход по 30-дневным постам = ~1.2с). См. коммит-историю.
    sql = """
        WITH post_metrics AS (
            SELECT
                p.source_id,
                AVG(CASE WHEN COALESCE(p.relevance_score, 0) >= 0.6 THEN 1.0 ELSE 0.0 END) AS relevant_ratio,
                AVG(jsonb_array_length(COALESCE(p.tags, '[]'::jsonb))) AS avg_tag_count,
                AVG(CASE WHEN jsonb_array_length(COALESCE(pe.data->'items', '[]'::jsonb)) > 0 THEN 1.0 ELSE 0.0 END) AS linked_ratio,
                EXTRACT(EPOCH FROM (NOW() - MAX(p.published_at))) / 3600.0 AS freshness_hours
            FROM posts p
            LEFT JOIN post_enrichments pe
                ON pe.post_id = p.id
               AND pe.kind = 'crawl'
            WHERE p.created_at >= NOW() - INTERVAL '30 days'
            GROUP BY p.source_id
        )
        SELECT
            s.*,
            -- seen_external_ids (до 500 записей на источник) — тяжёлый, для UI не нужен, отсекаем в БД.
            (sc.cursor_json - 'seen_external_ids') AS cursor_json,
            sc.last_success_at,
            sc.last_error,
            sc.last_seen_published_at,
            COALESCE(metrics.recent_success_count, 0) AS recent_success_count,
            COALESCE(metrics.recent_error_count, 0) AS recent_error_count,
            COALESCE(pm.relevant_ratio, 0) AS relevant_ratio,
            COALESCE(pm.avg_tag_count, 0) AS avg_tag_count,
            COALESCE(pm.linked_ratio, 0) AS linked_ratio,
            pm.freshness_hours AS freshness_hours,
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
        LEFT JOIN post_metrics pm ON pm.source_id = s.id
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
            item["telegram_diagnostics"] = _build_telegram_diagnostics(item)
            item.update(source_quality_payload(item))
            _redact_source_secrets(item)
            # Слим ответа списка: фронту в таблице/деталях не нужны тяжёлые блобы
            # (cursor_json уже отсечён в SQL; proxy_config и вложенные quality-дикты не рендерятся;
            # из extra оставляем только vision-политику — она нужна для префилла модалки).
            item.pop("cursor_json", None)
            item.pop("proxy_config", None)
            item.pop("score_breakdown", None)
            item.pop("operational_status", None)
            _extra = item.get("extra")
            item["extra"] = {"vision": _extra.get("vision")} if isinstance(_extra, dict) else {}
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


@router.patch("/{source_id}/telegram-handle")
async def update_source_telegram_handle(source_id: str, payload: SourceTelegramHandleUpdate):
    try:
        normalized_channel = _normalize_telegram_channel(payload.tg_channel)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT s.id, s.source_type, s.url, s.tg_channel, s.extra, sc.cursor_json
                FROM sources s
                LEFT JOIN source_checkpoints sc ON sc.source_id = s.id
                WHERE s.id = :id
                """
            ),
            {"id": source_id},
        )
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Source not found")
        if canonical_source_type(row.get("source_type", "")) != "telegram":
            raise HTTPException(status_code=400, detail="Source is not a Telegram source")

        try:
            _, _, tg_channel, _ = validate_source_payload(
                row["source_type"],
                row.get("url"),
                normalized_channel,
                row.get("extra") or {},
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        checkpoint_cursor = _normalize_checkpoint_cursor(row.get("cursor_json"))
        updated_cursor = _update_checkpoint_cursor_for_handle(checkpoint_cursor, tg_channel or normalized_channel)

        await session.execute(
            text(
                """
                UPDATE sources
                SET tg_channel = :tg_channel,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": source_id, "tg_channel": tg_channel or normalized_channel},
        )
        await session.execute(
            text(
                """
                INSERT INTO source_checkpoints (
                    source_id, cursor_json, last_error, updated_at
                )
                VALUES (
                    :source_id, CAST(:cursor_json AS jsonb), NULL, NOW()
                )
                ON CONFLICT (source_id) DO UPDATE SET
                    cursor_json = CAST(:cursor_json AS jsonb),
                    last_error = NULL,
                    updated_at = NOW()
                """
            ),
            {
                "source_id": source_id,
                "cursor_json": json.dumps(updated_cursor),
            },
        )
        await session.commit()

    diagnostics = _build_telegram_diagnostics(
        {
            "source_type": "telegram",
            "tg_channel": tg_channel or normalized_channel,
            "cursor_json": updated_cursor,
            "last_error": None,
        }
    )
    return {
        "status": "ok",
        "id": source_id,
        "tg_channel": tg_channel or normalized_channel,
        "telegram_diagnostics": diagnostics,
    }


@router.delete("/{source_id}")
async def delete_source(source_id: str):
    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(text("DELETE FROM sources WHERE id = :id"), {"id": source_id})
        await session.commit()
    return {"status": "ok"}
