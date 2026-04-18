from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from shared.source_definitions import validate_source_payload
from shared.workspace_relevance import merge_relevance_weights

CONFIG_DIR = Path("/app/config")


def _load_yaml(filename: str) -> dict[str, Any]:
    path = CONFIG_DIR / filename
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data if isinstance(data, dict) else {}


async def bootstrap_workspaces_from_config(workspace_id: str | None = None) -> dict[str, Any]:
    config = _load_yaml("workspaces.yml")
    workspaces = config.get("workspaces", [])
    engine = get_engine()

    bootstrapped = []
    async with AsyncSession(engine) as session:
        for ws in workspaces:
            if workspace_id and ws.get("id") != workspace_id:
                continue

            merged_weights = merge_relevance_weights(
                ws.get("relevance_weights"),
                new_threshold=(ws.get("relevance_weights") or {}).get("threshold"),
            )
            await session.execute(
                text(
                    """
                    INSERT INTO workspaces (
                        id, name, description, categories, relevance_weights,
                        design_lenses, cross_workspace_bridges, extra, is_active, created_at, updated_at
                    )
                    VALUES (
                        :id, :name, :description, CAST(:categories AS jsonb), CAST(:weights AS jsonb),
                        CAST(:lenses AS jsonb), CAST(:bridges AS jsonb), CAST(:extra AS jsonb), :is_active, NOW(), NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        categories = EXCLUDED.categories,
                        relevance_weights = EXCLUDED.relevance_weights,
                        design_lenses = EXCLUDED.design_lenses,
                        cross_workspace_bridges = EXCLUDED.cross_workspace_bridges,
                        extra = EXCLUDED.extra,
                        is_active = EXCLUDED.is_active,
                        updated_at = NOW()
                    """
                ),
                {
                    "id": ws["id"],
                    "name": ws["name"],
                    "description": ws.get("description", ""),
                    "categories": json.dumps(ws.get("categories", [])),
                    "weights": json.dumps(merged_weights),
                    "lenses": json.dumps(ws.get("design_lenses", [])),
                    "bridges": json.dumps(ws.get("cross_workspace_bridges", [])),
                    "extra": json.dumps(ws.get("extra", {})),
                    "is_active": bool(ws.get("is_active", True)),
                },
            )
            bootstrapped.append(ws["id"])
        await session.commit()

    return {"status": "ok", "workspace_id": workspace_id, "bootstrapped": bootstrapped}


async def bootstrap_sources_from_config(workspace_id: str | None = None) -> dict[str, Any]:
    config = _load_yaml("sources.yml")
    sources = config.get("sources", [])
    engine = get_engine()

    bootstrapped = []
    async with AsyncSession(engine) as session:
        for src in sources:
            if workspace_id and src.get("workspace_id") != workspace_id:
                continue

            source_type, url, tg_channel, extra = validate_source_payload(
                src.get("source_type", ""),
                src.get("url"),
                src.get("tg_channel"),
                src.get("extra"),
            )
            await session.execute(
                text(
                    """
                    INSERT INTO sources (
                        id, workspace_id, source_type, name, url, tg_channel,
                        tg_account_idx, schedule_cron, is_enabled, proxy_config, extra,
                        source_authority, created_at, updated_at
                    )
                    VALUES (
                        :id, :workspace_id, :source_type, :name, :url, :tg_channel,
                        :tg_account_idx, :schedule_cron, :is_enabled, CAST(:proxy_config AS jsonb),
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
                        is_enabled = EXCLUDED.is_enabled,
                        proxy_config = EXCLUDED.proxy_config,
                        extra = EXCLUDED.extra,
                        source_authority = EXCLUDED.source_authority,
                        updated_at = NOW()
                    """
                ),
                {
                    "id": src["id"],
                    "workspace_id": src["workspace_id"],
                    "source_type": source_type,
                    "name": src["name"],
                    "url": url,
                    "tg_channel": tg_channel,
                    "tg_account_idx": int(src.get("tg_account_idx", 0) or 0),
                    "schedule_cron": src.get("schedule_cron") or "*/60 * * * *",
                    "is_enabled": bool(src.get("is_enabled", True)),
                    "proxy_config": json.dumps(src.get("proxy_config") or {}),
                    "extra": json.dumps(extra),
                    "source_authority": float(extra.get("source_authority", 0.5) or 0.5),
                },
            )
            bootstrapped.append(src["id"])
        await session.commit()

    return {"status": "ok", "workspace_id": workspace_id, "bootstrapped": bootstrapped}
