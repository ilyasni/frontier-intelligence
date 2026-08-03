"""HTTP endpoint for frontier search; shared retrieval helpers are reused by balanced search."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from collections import defaultdict
from typing import Any

from fastapi import APIRouter
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from mcp.guards import assert_known_workspace
from shared.config import Settings, get_settings
from shared.db import get_engine
from shared.qdrant_sparse import HAS_SPARSE
from shared.search_contracts import SearchRequest
from shared.source_quality import normalize_source_authority
from worker.llm_router_client import LLMRouterClient
from worker.integrations.qdrant_client import QdrantFrontierClient, _final_rank_score
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
    purpose = "query"
    cache_key = _embed_cache_key(
        settings.gigachat_embeddings_model,
        f"{purpose}:{query[:2000]}",
    )

    async with Redis.from_url(redis_url, decode_responses=False) as redis:
        cached = await redis.get(cache_key)
        if cached:
            logger.info("embedding_cache_hit", extra={"key": cache_key})
            return json.loads(cached)

        client = LLMRouterClient(service_name="mcp", redis=redis)
        try:
            vector = await client.embed(query[:2000], purpose=purpose)
        finally:
            await client.close()

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
        # Формула ранжирования одна и живёт в _final_rank_score (qdrant_client.py:88).
        # Здесь она не повторяется: hybrid_search считал буст по payload, где у источника,
        # не оценённого на момент индексации, лежит source_score = 0.0; гидрация подставила
        # актуальное значение из БД, и пересчёт по нему — ровно то, ради чего она нужна.
        semantic = float(breakdown.get("semantic", hit.get("raw_score", hit.get("score", 0.0))) or 0.0)
        hit["score"], hit["score_breakdown"] = _final_rank_score(semantic, payload)
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
    client = LLMRouterClient(service_name="mcp")
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


def _own_stake_enabled(settings: Settings) -> bool:
    """Флаг второй оси.

    getattr, а не прямой доступ: поля own_stake_* живут в shared/config.py, и если образ
    mcp уехал вперёд конфига, поиск обязан работать без own_stake, а не отдавать 500 на
    каждый запрос. Отсутствующее поле читается как «выключено».
    """
    return bool(getattr(settings, "own_stake_enabled", False))


def _stake_quadrant(
    relevance: float,
    own_stake: float,
    *,
    relevance_high: float,
    own_stake_high: float,
) -> str:
    """Квадрант по двум независимым осям. На самом пороге значение считается высоким.

    post                — поток греет тему и у автора есть свой замер;
    run_your_own        — поток греет, своего замера нет: идти и делать;
    personal_blind_spot — свой замер есть, а поток по теме молчит: слепое пятно мониторинга;
    noise               — ни того, ни другого.

    Пороги приходят из настроек и калибруются эмпирически (задача C): при включённом
    sparse `score` — это RRF-балл масштаба 0.02-0.06, а не косинус, и relevance_high,
    поставленный «на глаз» в 0.5, схлопнет выдачу в два нижних квадранта.
    """
    hot = relevance >= relevance_high
    mine = own_stake >= own_stake_high
    if hot:
        return "post" if mine else "run_your_own"
    return "personal_blind_spot" if mine else "noise"


def _hit_doc_ids(hits: list[dict[str, Any]]) -> list[str]:
    """doc_id хитов в порядке выдачи, без повторов. payload['post_id'] и есть doc_id."""
    doc_ids: list[str] = []
    seen: set[str] = set()
    for hit in hits:
        payload = hit.get("payload") or {}
        doc_id = str(payload.get("post_id") or payload.get("doc_id") or "").strip()
        if doc_id and doc_id not in seen:
            seen.add(doc_id)
            doc_ids.append(doc_id)
    return doc_ids


async def _own_corpus_size(qdrant: QdrantFrontierClient) -> int | None:
    """Число точек в личном корпусе. None — измерить не удалось.

    Размер обязан доезжать до карточки рядом со значением: на корпусе в полсотни чанков
    own_stake читается как «сигнал есть/нет», а не как величина, и без этого числа
    случайное совпадение 0.7 выглядит как измерение.
    """
    sizer = getattr(qdrant, "own_corpus_size", None)
    if callable(sizer):
        return int(await sizer())
    collection = str(getattr(qdrant, "own_corpus_collection", "") or "").strip()
    if not collection:
        return None
    result = await qdrant.client.count(collection_name=collection, exact=False)
    return int(getattr(result, "count", 0) or 0)


async def _attach_own_stake(hits: list[dict[str, Any]], settings: Settings) -> dict[str, Any]:
    """Вторая ось: косинус хита к личному корпусу автора и квадрант по двум порогам.

    Пишет два ключа ВЕРХНЕГО уровня хита — own_stake и stake_quadrant (не внутрь
    score_breakdown: это имя в кодовой базе уже занято тремя разными смыслами). Возвращает
    метаданные ответа — они кладутся один раз на выдачу, а не на карточку.

    Инвариант 1: `score` не изменяется ни на одном пути, порядок списка не изменяется.
    Функция вызывается уже после hydrated.sort(), и ниже по коду сортировок нет.

    Любая поломка Qdrant или отсутствие серверной половины задачи B деградируют до
    degraded=True без ключей на хитах: поиск не может падать из-за личного корпуса.
    Ноль вместо неизвестности не пишется — 0.0 читается как «у автора нет замера»,
    а это разные вещи.
    """
    top_k = max(1, int(getattr(settings, "own_stake_top_k", 3) or 1))
    thresholds: dict[str, float] = {
        "own_stake_high": float(getattr(settings, "own_stake_high", 0.5) or 0.0),
        "relevance_high": float(getattr(settings, "relevance_high", 0.5) or 0.0),
    }
    meta: dict[str, Any] = {
        "size": None,
        "top_k": top_k,
        "scored_hits": 0,
        "thresholds": thresholds,
        "degraded": True,
    }
    vectors: dict[str, list[float]] = {}
    scores: dict[str, float] = {}
    qdrant = QdrantFrontierClient()
    try:
        meta["size"] = await _own_corpus_size(qdrant)
        scorer = getattr(qdrant, "own_stake_scores", None)
        if not callable(scorer):
            logger.warning("own_stake_unavailable reason=own_stake_scores_missing")
            return meta
        doc_ids = _hit_doc_ids(hits)
        documents = await qdrant.fetch_documents(doc_ids)
        # fetch_documents отдаёт point-id, а не doc_id, молча пропускает ненайденные точки
        # и не держит порядок — обратная карта строится тем же uuid5, что и прямая.
        by_point_id = {str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)): doc_id for doc_id in doc_ids}
        for document in documents:
            payload = document.get("payload") or {}
            doc_id = by_point_id.get(str(document.get("id") or "")) or str(
                payload.get("doc_id") or payload.get("post_id") or ""
            )
            vector = document.get("vector")
            if doc_id and vector:
                vectors[doc_id] = list(vector)
        if hits and not vectors:
            logger.warning("own_stake_unavailable reason=no_hit_vectors hits=%d", len(hits))
            return meta
        raw_scores = await scorer(vectors, top_k=top_k)
        if raw_scores is None:
            # Qdrant не ответил или ответ не выровнен — замера не было. Именно здесь
            # раньше приезжал словарь нулей и уходил на карточку с degraded=False:
            # выдуманное измерение, которое разметка обратной связи фиксирует
            # в card_feedback навсегда.
            logger.warning("own_stake_unavailable reason=scorer_degraded hits=%d", len(hits))
            return meta
        if not isinstance(raw_scores, dict):
            logger.warning(
                "own_stake_unavailable reason=bad_scores_type type=%s",
                type(raw_scores).__name__,
            )
            return meta
        scores = raw_scores
    except Exception:
        logger.exception("own_stake_failed hits=%d", len(hits))
        return meta
    finally:
        await qdrant.close()

    scored = 0
    for hit in hits:
        payload = hit.get("payload") or {}
        doc_id = str(payload.get("post_id") or payload.get("doc_id") or "").strip()
        if doc_id not in vectors or doc_id not in scores:
            # Вектор хита не достался или замера по нему не вернулось — измерения
            # не было. None, а не 0.0: отсутствие ключа в scores нельзя подменять
            # дефолтом, иначе непроведённый замер выглядит как проведённый.
            hit["own_stake"] = None
            hit["stake_quadrant"] = None
            continue
        scored += 1
        # Косинус приходит из [-1, 1]; отрицательный означает противоположную тему и для
        # оси «свой замер есть/нет» неотличим от его отсутствия.
        own_stake = round(max(0.0, min(1.0, float(scores[doc_id] or 0.0))), 4)
        hit["own_stake"] = own_stake
        hit["stake_quadrant"] = _stake_quadrant(
            float(hit.get("score", 0.0) or 0.0),
            own_stake,
            relevance_high=thresholds["relevance_high"],
            own_stake_high=thresholds["own_stake_high"],
        )
    meta["scored_hits"] = scored
    meta["degraded"] = False
    return meta


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
            embedding_version=str(settings.gigachat_embeddings_model or "").strip() or None,
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
    # Врезка второй оси — строго ПОСЛЕ сортировки. Порядок списка на этот момент
    # окончателен, ниже сортировок нет, и own_stake физически нечем повлиять на выдачу
    # (инвариант 1). Ключи только добавляются, `score` не трогается.
    own_corpus: dict[str, Any] | None = None
    if hydrated and _own_stake_enabled(settings):
        own_corpus = await _attach_own_stake(hydrated, settings)
    external_results: list[dict[str, Any]] = []
    if settings.searxng_enabled:
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
    response: dict[str, Any] = {
        "results": hydrated,
        "external_results": external_results,
        "external_grounding_used": bool(external_results),
        "sparse_enabled": HAS_SPARSE,
        "synthesize": req.synthesize,
        "synthesis": synthesis,
        "applied_filters": _applied_filters(req),
        "entity_evidence": entity_evidence(hydrated, req.entities),
    }
    if own_corpus is not None:
        # Ключ появляется только при включённой второй оси: при OWN_STAKE_ENABLED=false
        # ответ побайтово прежний, и сравнение двух выдач остаётся честной проверкой.
        response["own_corpus"] = own_corpus
    return response


@router.post("")
async def search_frontier(req: SearchRequest) -> dict[str, Any]:
    assert_known_workspace(req.workspace)
    return await run_search_request(req)
