"""Workspace brief composition for MCP consumers."""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator

from mcp.bridges import resolve_bridge_workspaces
from mcp.guards import assert_known_workspace
from shared.config import get_settings
from worker.llm_router_client import LLMRouterClient
from worker.llm_json import parse_llm_json_object

from .observability import (
    ClusterListRequest,
    MissingSignalListRequest,
    WorkspaceOverviewRequest,
    get_workspace_overview,
    list_clusters,
    list_missing_signals,
)

router = APIRouter()


class FrontierBriefRequest(BaseModel):
    model_config = {"extra": "forbid"}

    workspace: str | None = None
    workspaces: list[str] | None = None
    recent_limit: int = Field(default=8, ge=1, le=30)
    clusters_limit: int = Field(default=8, ge=1, le=30)
    missing_limit: int = Field(default=6, ge=0, le=30)
    synthesize: bool = True
    # Добавить к брифу воркспейсы, объявленные в workspaces.cross_workspace_bridges.
    # По умолчанию выключено; при include_bridges=true каждый блок брифа несёт
    # origin_workspace/bridged, иначе мост неотличим от собственного сигнала.
    include_bridges: bool = False

    @field_validator("workspace", mode="before")
    @classmethod
    def _normalize_workspace(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator("workspaces", mode="before")
    @classmethod
    def _normalize_workspaces(cls, value: object) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            items = value.split(",")
        elif isinstance(value, list):
            items = value
        else:
            return None
        normalized = [str(item).strip() for item in items if str(item).strip()]
        return normalized[:10] or None

    def workspace_ids(self) -> list[str]:
        values = list(self.workspaces or [])
        if self.workspace:
            values.insert(0, self.workspace)
        deduped: list[str] = []
        for value in values or ["disruption"]:
            if value not in deduped:
                deduped.append(value)
        return deduped


def _compact_workspace(payload: dict[str, Any]) -> dict[str, Any]:
    clusters = payload.get("clusters") or {}
    return {
        "workspace": payload.get("workspace"),
        "summary": payload.get("summary"),
        "top_sources": [
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "source_score": item.get("source_score"),
                "quality_tier": item.get("quality_tier"),
                "freshness_hours": item.get("freshness_hours"),
            }
            for item in (payload.get("top_sources") or [])[:8]
        ],
        "trends": [
            {
                "id": item.get("id"),
                "title": item.get("title_ru") or item.get("title"),
                "signal_stage": item.get("signal_stage"),
                "signal_score": item.get("signal_score"),
                "burst_score": item.get("burst_score"),
                "keywords": item.get("keywords") or [],
            }
            for item in (clusters.get("trends") or [])[:8]
        ],
        "emerging": [
            {
                "id": item.get("id"),
                "title": item.get("title"),
                "signal_stage": item.get("signal_stage"),
                "signal_score": item.get("signal_score"),
                "recommended_watch_action": item.get("recommended_watch_action"),
            }
            for item in (clusters.get("emerging") or [])[:8]
        ],
        "recent": payload.get("recent") or [],
    }


async def _synthesize_brief(
    workspaces: list[dict[str, Any]],
    missing: dict[str, Any],
) -> dict[str, Any] | None:
    settings = get_settings()
    client = LLMRouterClient(service_name="mcp")
    try:
        response = await client.chat(
            system=(
                "You are an analytical briefing engine. Return only valid JSON "
                "based on the provided Frontier data."
            ),
            user=(
                "Return JSON with keys: executive_summary, strongest_signals, weak_signals, "
                "missing_signals, risks, recommended_next_actions, confidence.\n\n"
                + json.dumps(
                    {
                        "workspaces": workspaces,
                        "missing_signals": missing,
                    },
                    ensure_ascii=False,
                    default=str,
                )[:12000]
            ),
            task="mcp_synthesis",
            # См. пункт 41: голый model_override давал гарантированный 404 на
            # первом кандидате семейства. Пара задаётся целиком или никак.
            pro=True,
            max_tokens=900,
        )
        try:
            parsed = parse_llm_json_object(response.content)
        except Exception:
            parsed = None
        return {
            "parsed": parsed,
            "raw": response.content,
            "model": response.model,
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "precached_prompt_tokens": response.usage.precached_prompt_tokens,
            "billable_tokens": response.usage.billable_tokens,
        }
    finally:
        await client.close()


@router.post("")
async def get_frontier_brief(req: FrontierBriefRequest) -> dict:
    requested = req.workspace_ids()
    for workspace in requested:
        assert_known_workspace(workspace)
    # Мосты — из БД, а не из config/workspaces.yml: YAML запечён в образ и расходится
    # с базой, где мосты правит редактор в админке. include_bridges=false — тождество,
    # запроса к БД нет, набор равен запрошенному.
    workspace_ids, bridges_meta = await resolve_bridge_workspaces(
        requested, include_bridges=req.include_bridges
    )
    requested_set = set(requested)
    compact = []
    missing_by_workspace: dict[str, Any] = {}
    for workspace in workspace_ids:
        overview = await get_workspace_overview(
            WorkspaceOverviewRequest(
                workspace=workspace,
                recent_limit=req.recent_limit,
                sources_limit=8,
                clusters_limit=req.clusters_limit,
            )
        )
        clusters = await list_clusters(
            ClusterListRequest(
                workspace=workspace,
                kind="emerging",
                limit=req.clusters_limit,
                stages=["weak", "emerging"],
            )
        )
        overview["clusters"]["emerging"] = clusters.get("emerging", [])
        missing = (
            await list_missing_signals(
                MissingSignalListRequest(workspace=workspace, limit=req.missing_limit)
            )
            if req.missing_limit
            else {"signals": []}
        )
        missing_by_workspace[workspace] = missing.get("signals") or []
        entry = _compact_workspace(overview)
        if req.include_bridges:
            # Атрибуция происхождения. `entry["workspace"]` — строка воркспейса из БД,
            # и слаг в ней есть, но он утоплен в объект: и синтезатор, и человек читают
            # бриф поблочно. Явные плоские ключи — условие, при котором мосты вообще
            # разрешены: без них чужой воркспейс неотличим от своего.
            entry["origin_workspace"] = workspace
            entry["bridged"] = workspace not in requested_set
        compact.append(entry)

    synthesis = await _synthesize_brief(compact, missing_by_workspace) if req.synthesize else None
    response = {
        "workspaces": workspace_ids,
        "brief": compact,
        "missing_signals": missing_by_workspace,
        "synthesize": req.synthesize,
        "synthesis": synthesis,
    }
    if req.include_bridges:
        response["bridges"] = bridges_meta
    return response
