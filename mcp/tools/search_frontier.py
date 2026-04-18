"""HTTP endpoint for frontier search; shared retrieval helpers are reused by balanced search."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from collections import defaultdict
from typing import Any

import httpx
from fastapi import APIRouter
from openai import AsyncOpenAI
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import get_settings
from shared.db import get_engine
from shared.qdrant_sparse import HAS_SPARSE
from shared.search_contracts import SearchRequest
from shared.source_quality import normalize_source_authority
from worker.gigachat_client import GigaChatClient
from worker.integrations.qdrant_client import QdrantFrontierClient
from worker.llm_json import parse_llm_json_object
from worker.services.searxng_client import SearXNGClient

logger = logging.getLogger(__name__)

router = APIRouter()

_EMBED_TTL = 7 * 24 * 3600


def _embed_cache_key(model: str, text: str) -> str:
    digest = hashlib.sha256(f"{model}:{text}".encode()).hexdigest()
    return f"emb:mcp:{digest}"


async def _get_embedding(query: str, settings) -> list[float]:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    cache_key = _embed_cache_key(settings.gigachat_embeddings_model, query[:2000])

    async with Redis.from_url(redis_url, decode_responses=False) as redis:
        cached = await redis.get(cache_key)
        if cached:
            logger.info("embedding_cache_hit", extra={"key": cache_key})
            return json.loads(cached)

        async with httpx.AsyncClient(timeout=httpx.Timeout(90.0, connect=15.0)) as http_client:
            oai = AsyncOpenAI(
                base_url=settings.openai_api_base,
                api_key="gigachat",
                http_client=http_client,
                max_retries=3,
            )
            emb = await oai.embeddings.create(
                model=settings.gigachat_embeddings_model,
                input=query[:2000],
            )
            vector = emb.data[0].embedding

        await redis.setex(cache_key, _EMBED_TTL, json.dumps(vector))
        return vector


async def _load_source_scores(source_ids: set[str]) -> dict[str, dict[str, float]]:
    if not source_ids:
        return {}
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(
            text(
                """
                SELECT id, source_score, source_authority
                FROM sources
                WHERE id = ANY(:ids)
                """
            ),
            {"ids": list(source_ids)},
        )
        return {
            row["id"]: {
                "source_score": float(row["source_score"] if row["source_score"] is not None else row["source_authority"] or 0.0),
                "source_authority": normalize_source_authority(row["source_authority"]),
            }
            for row in result.mappings().all()
        }


def _maybe_hydrate_score(hit: dict[str, Any], score_map: dict[str, dict[str, float]]) -> dict[str, Any]:
    payload = hit.get("payload") or {}
    source_id = payload.get("source_id")
    if not source_id:
        return hit
    extra = score_map.get(source_id)
    if not extra:
        return hit
    if not payload.get("source_score"):
        payload["source_score"] = extra["source_score"]
    if not payload.get("source_authority"):
        payload["source_authority"] = extra["source_authority"]
    breakdown = hit.get("score_breakdown") or {}
    if "source_score" in breakdown:
        semantic = float(breakdown.get("semantic", hit.get("raw_score", hit.get("score", 0.0))) or 0.0)
        freshness = float(breakdown.get("freshness", 0.0) or 0.0)
        final = semantic * (1.0 + float(payload.get("source_score") or 0.0) * 0.20 + freshness * 0.08)
        hit["score"] = final
        breakdown["source_score"] = round(float(payload.get("source_score") or 0.0), 4)
        hit["score_breakdown"] = breakdown
    hit["payload"] = payload
    return hit


def _applied_filters(req: SearchRequest) -> dict[str, Any]:
    filters: dict[str, Any] = {
        "workspace": req.workspace,
        "limit": req.limit,
    }
    for key in ("lang", "days_back", "valence", "signal_type", "source_region", "entities"):
        value = getattr(req, key, None)
        if value not in (None, [], ""):
            filters[key] = value
    return filters


def _searxng_time_range(days_back: int | None) -> str | None:
    if days_back is None:
        return "month"
    if days_back <= 2:
        return "day"
    if days_back <= 14:
        return "week"
    if days_back <= 45:
        return "month"
    return "year"


def _entity_token_set(payload: dict[str, Any]) -> set[str]:
    values: list[str] = []
    for key in ("content", "url", "author", "title", "category", "source_id"):
        raw = payload.get(key)
        if raw:
            values.append(str(raw))
    values.extend(str(item) for item in (payload.get("concepts") or []))
    values.extend(str(item) for item in (payload.get("tags") or []))
    merged = " ".join(values).lower()
    return set(token for token in merged.replace("/", " ").replace("-", " ").split() if token)


def entity_evidence(hits: list[dict[str, Any]], entities: list[str] | None) -> dict[str, list[dict[str, Any]]]:
    if not entities:
        return {}
    evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for hit in hits:
        payload = hit.get("payload") or {}
        haystack = " ".join(
            [
                str(payload.get("content") or ""),
                str(payload.get("url") or ""),
                str(payload.get("author") or ""),
                str(payload.get("title") or ""),
                " ".join(str(item) for item in (payload.get("concepts") or [])),
                " ".join(str(item) for item in (payload.get("tags") or [])),
            ]
        ).lower()
        for entity in entities:
            if entity.lower() in haystack:
                evidence[entity].append(hit)
    return {
        entity: sorted(items, key=lambda item: item.get("score", 0.0), reverse=True)[:3]
        for entity, items in evidence.items()
    }


def _select_synthesis_hits(hits: list[dict[str, Any]], *, limit: int = 6) -> list[dict[str, Any]]:
    selected_hits: list[dict[str, Any]] = []
    seen_clusters: set[str] = set()
    for hit in hits:
        payload = hit.get("payload", {})
        cluster_id = payload.get("semantic_cluster_id")
        if cluster_id:
            if cluster_id in seen_clusters:
                continue
            seen_clusters.add(cluster_id)
        selected_hits.append(hit)
        if len(selected_hits) >= limit:
            break
    return selected_hits or hits[:limit]


def _hits_snippet(hits: list[dict[str, Any]]) -> str:
    snippets = []
    for idx, hit in enumerate(_select_synthesis_hits(hits), start=1):
        payload = hit.get("payload", {})
        snippets.append(
            "\n".join(
                [
                    f"[{idx}] score={hit.get('score', 0):.3f}",
                    f"title={payload.get('title') or ''}",
                    f"category={payload.get('category') or ''}",
                    f"author={payload.get('author') or ''}",
                    f"semantic_cluster_id={payload.get('semantic_cluster_id') or ''}",
                    f"lang={payload.get('lang') or ''}",
                    f"source_region={payload.get('source_region') or ''}",
                    f"valence={payload.get('valence') or ''}",
                    f"signal_type={payload.get('signal_type') or ''}",
                    f"content={str(payload.get('content') or '')[:500]}",
                ]
            )
        )
    return "\n\n".join(snippets)


async def _synthesize_results(req: SearchRequest, hits: list[dict[str, Any]], settings) -> dict[str, Any] | None:
    if not hits:
        return None
    combined = _hits_snippet(hits)
    prefer_pro = len(hits) > 3 or len(combined) > 1800
    client = GigaChatClient(service_name="mcp")
    try:
        response = await client.chat(
            system="Ты аналитик. Синтезируй только факты и сигналы из найденных документов. Верни только валидный JSON.",
            user=(
                f"Запрос пользователя: {req.query}\n"
                f"Workspace: {req.workspace}\n"
                f"Фильтры: {json.dumps(_applied_filters(req), ensure_ascii=False)}\n\n"
                "Верни JSON:\n"
                '{ "summary": "<3-5 предложений>", "themes": ["..."], "confidence": <0.0-1.0>, "known_blind_spots": ["..."] }\n\n'
                f"{combined}"
            ),
            task="mcp_synthesis",
            model_override=settings.gigachat_model_pro if prefer_pro else None,
            pro=prefer_pro,
            max_tokens=500,
        )
        parsed: dict[str, Any] | None
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


async def run_search_request(
    req: SearchRequest,
    *,
    valence_override: str | list[str] | None = None,
    signal_type_override: str | list[str] | None = None,
    days_back_override: int | None = None,
    source_region_override: str | None = None,
    limit_override: int | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    vector = await _get_embedding(req.query, settings)
    qdrant = QdrantFrontierClient()
    try:
        hits = await qdrant.hybrid_search(
            vector,
            req.workspace,
            limit=limit_override or req.limit,
            query_text=req.query,
            lang=req.lang,
            days_back=days_back_override if days_back_override is not None else req.days_back,
            valence=valence_override if valence_override is not None else req.valence,
            signal_type=signal_type_override if signal_type_override is not None else req.signal_type,
            source_region=source_region_override if source_region_override is not None else req.source_region,
        )
    finally:
        await qdrant.close()

    score_map = await _load_source_scores(
        {hit.get("payload", {}).get("source_id") for hit in hits if hit.get("payload", {}).get("source_id")}
    )
    hydrated = [_maybe_hydrate_score(hit, score_map) for hit in hits]
    hydrated.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    external_results: list[dict[str, Any]] = []
    if settings.searxng_enabled and len(hydrated) < max(2, min(req.limit, 3)):
        try:
            external_results = await SearXNGClient(service_name="mcp").search(
                req.query,
                categories=settings.searxng_categories,
                language=req.lang,
                time_range=_searxng_time_range(
                    days_back_override if days_back_override is not None else req.days_back
                ),
                limit=min(settings.searxng_max_results, max(3, req.limit)),
                mode="search_grounding",
            )
        except Exception:
            logger.exception("searxng_grounding_failed query=%s", req.query[:80])
    synthesis = await _synthesize_results(req, hydrated, settings) if req.synthesize else None
    return {
        "results": hydrated,
        "external_results": external_results,
        "external_grounding_used": bool(external_results),
        "sparse_enabled": HAS_SPARSE,
        "synthesize": req.synthesize,
        "synthesis": synthesis,
        "applied_filters": _applied_filters(req),
        "entity_evidence": entity_evidence(hydrated, req.entities),
    }


@router.post("")
async def search_frontier(req: SearchRequest) -> dict[str, Any]:
    return await run_search_request(req)
