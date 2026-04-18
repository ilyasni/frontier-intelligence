"""Balanced analytical search with growth, counter-signals, competitors, and RU checks."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from fastapi import APIRouter

from shared.config import get_settings
from shared.search_contracts import COUNTER_SIGNAL_TYPES, BalancedSearchRequest, SearchRequest
from worker.gigachat_client import GigaChatClient
from worker.llm_json import parse_llm_json_object
from worker.services.searxng_client import SearXNGClient

from .search_frontier import _searxng_time_range, entity_evidence, run_search_request

logger = logging.getLogger(__name__)

router = APIRouter()

_RU_TERMS = {"ru", "russia", "russian", "россия", "россии", "россий", "российский", "русск"}
_GLOBAL_TERMS = {"global", "world", "worldwide", "международ", "глобальн"}
_COUNTER_TERMS = {
    "risk",
    "failure",
    "failed",
    "closure",
    "delay",
    "recall",
    "lawsuit",
    "ban",
    "reject",
    "ризик",
    "риск",
    "провал",
    "закрыт",
    "задерж",
    "отзыв",
    "суд",
    "запрет",
}
_SIGNAL_TYPE_TERMS = {
    "regulation": {"regulation", "regulatory", "law", "policy", "закон", "регулир", "политик"},
    "adoption": {"adoption", "adopt", "rollout", "внедрен", "приняти"},
    "growth": {"growth", "scale", "rising", "surge", "рост", "растет", "масштаб"},
    "investment": {"investment", "funding", "raise", "инвест", "финансир"},
    "partnership": {"partnership", "partner", "deal", "партнер", "сделк"},
    "launch": {"launch", "release", "announce", "запуск", "релиз", "анонс"},
}


def _contains_any(text: str, terms: set[str]) -> bool:
    return any(term in text for term in terms)


def _infer_days_back(text: str, default: int | None) -> int | None:
    match = re.search(r"\b(?:last|за)?\s*(\d{1,3})\s*(?:d|day|days|дн)", text)
    if match:
        return max(1, min(365, int(match.group(1))))
    if any(term in text for term in ("today", "сегодня")):
        return 1
    if any(term in text for term in ("week", "недел")):
        return 7
    if any(term in text for term in ("month", "месяц")):
        return 30
    if any(term in text for term in ("quarter", "квартал")):
        return 90
    return default


def _parse_intent(req: BalancedSearchRequest) -> dict[str, Any]:
    text = req.query.lower()
    source_region = req.source_region
    region_reason = "explicit_request" if source_region else ""
    if source_region is None and _contains_any(text, _RU_TERMS):
        source_region = "ru"
        region_reason = "query_mentions_russia"
    elif source_region is None and _contains_any(text, _GLOBAL_TERMS):
        source_region = "global"
        region_reason = "query_mentions_global_scope"

    signal_type_hints = [
        signal_type
        for signal_type, terms in _SIGNAL_TYPE_TERMS.items()
        if _contains_any(text, terms)
    ]
    days_back = _infer_days_back(text, req.days_back)
    wants_counter = _contains_any(text, _COUNTER_TERMS)
    confidence = 0.35
    if source_region:
        confidence += 0.15
    if signal_type_hints:
        confidence += 0.20
    if wants_counter:
        confidence += 0.15
    if req.entities:
        confidence += 0.10
    if days_back != req.days_back:
        confidence += 0.05

    return {
        "source_region": source_region,
        "source_region_reason": region_reason,
        "days_back": days_back,
        "signal_type_hints": signal_type_hints[:3],
        "wants_counter_signals": wants_counter,
        "entities": req.entities or [],
        "confidence": round(min(confidence, 0.95), 2),
    }


def _blind_spots(
    *,
    intent: dict[str, Any],
    ru_results: list[dict[str, Any]],
    counter_results: list[dict[str, Any]],
    competitor_evidence: dict[str, list[dict[str, Any]]],
    requested_entities: list[str] | None = None,
) -> list[str]:
    gaps = []
    if float(intent.get("confidence") or 0.0) < 0.45:
        gaps.append("low_intent_confidence")
    if not ru_results:
        gaps.append("no_ru_verification_evidence")
    if not counter_results:
        gaps.append("no_counter_signals_found")
    if requested_entities and not any(competitor_evidence.values()):
        gaps.append("no_competitor_evidence")
    return gaps


async def _synthesize_balanced(
    req: BalancedSearchRequest,
    *,
    intent: dict[str, Any],
    main_results: list[dict[str, Any]],
    counter_results: list[dict[str, Any]],
    ru_results: list[dict[str, Any]],
    competitor_evidence: dict[str, list[dict[str, Any]]],
    external_results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not req.synthesize:
        return None

    def _compact(items: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
        compact = []
        for hit in items[:limit]:
            payload = hit.get("payload") or {}
            compact.append(
                {
                    "score": round(float(hit.get("score", 0.0) or 0.0), 4),
                    "source_id": payload.get("source_id"),
                    "source_region": payload.get("source_region"),
                    "valence": payload.get("valence"),
                    "signal_type": payload.get("signal_type"),
                    "published_at": payload.get("published_at"),
                    "content": str(payload.get("content") or "")[:260],
                    "concepts": payload.get("concepts") or [],
                }
            )
        return compact

    blind_spots = _blind_spots(
        intent=intent,
        ru_results=ru_results,
        counter_results=counter_results,
        competitor_evidence=competitor_evidence,
        requested_entities=req.entities,
    )
    prompt_payload = {
        "query": req.query,
        "workspace": req.workspace,
        "filters": {
            "lang": req.lang,
            "source_region": req.source_region,
            "days_back": req.days_back,
            "entities": req.entities or [],
        },
        "intent": intent,
        "signals": _compact(main_results),
        "counter_signals": _compact(counter_results),
        "ru_verification": _compact(ru_results),
        "competitors": {
            entity: _compact(items, limit=2)
            for entity, items in competitor_evidence.items()
        },
        "external_grounding": [
            {
                "title": item.get("title"),
                "url": item.get("url"),
                "engine": item.get("engine"),
                "score": item.get("score"),
                "content": str(item.get("content") or "")[:220],
            }
            for item in external_results[:4]
        ],
        "known_blind_spots": blind_spots,
    }

    settings = get_settings()
    client = GigaChatClient(service_name="mcp")
    try:
        response = await client.chat(
            system=(
                "Ты аналитик сигналов. Сбалансируй growth и counter-signals, "
                "отдельно проверь российское "
                "подтверждение и competitor evidence. Верни только валидный JSON."
            ),
            user=(
                "На основе данных ниже верни JSON:\n"
                "{"
                '"signals": [{"title":"...", "summary":"...", "evidence_ids":["..."]}], '
                '"counter_signals": [{"title":"...", "summary":"...", "evidence_ids":["..."]}], '
                '"competitors": {"players_detected":["..."], '
                '"evidence_by_player": {"player":["..."]}, '
                '"who_is_already_building_this":["..."], "who_failed":["..."], '
                '"who_is_absent":["..."]}, '
                '"ru_verification": {"status":"confirmed|mixed|unverified", '
                '"summary":"...", "evidence_ids":["..."]}, '
                '"confidence": 0.0, '
                '"known_blind_spots": ["..."], '
                '"synthesis": "..."'
                "}\n\n"
                f"{json.dumps(prompt_payload, ensure_ascii=False)}"
            ),
            task="mcp_synthesis",
            model_override=settings.gigachat_model_pro,
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
async def search_balanced(req: BalancedSearchRequest) -> dict[str, Any]:
    intent = _parse_intent(req)
    effective_days_back = int(intent.get("days_back") or req.days_back or 7)
    effective_source_region = req.source_region or intent.get("source_region")
    signal_type_hints = intent.get("signal_type_hints") or None
    base = SearchRequest(
        query=req.query,
        workspace=req.workspace,
        limit=req.limit,
        synthesize=False,
        lang=req.lang,
        days_back=effective_days_back,
        source_region=effective_source_region,
        entities=req.entities,
    )
    main_task = run_search_request(
        base,
        valence_override=["positive", "neutral"],
        signal_type_override=signal_type_hints,
    )
    counter_task = run_search_request(
        base,
        valence_override="negative",
        signal_type_override=list(COUNTER_SIGNAL_TYPES),
        days_back_override=max(effective_days_back, 30),
    )
    ru_task = run_search_request(
        base,
        source_region_override="ru",
        days_back_override=max(effective_days_back, 30),
    )
    main_search, counter_search, ru_search = await asyncio.gather(main_task, counter_task, ru_task)
    external_results: list[dict[str, Any]] = []
    if get_settings().searxng_enabled and (
        not main_search["results"]
        or not ru_search["results"]
        or not counter_search["results"]
        or len(main_search["results"]) < max(2, min(req.limit, 3))
    ):
        try:
            external_results = await SearXNGClient(service_name="mcp").search(
                req.query,
                categories=get_settings().searxng_categories,
                language=req.lang,
                time_range=_searxng_time_range(effective_days_back),
                limit=min(get_settings().searxng_max_results, max(3, req.limit)),
                mode="balanced_grounding",
            )
        except Exception:
            logger.exception("searxng_balanced_grounding_failed query=%s", req.query[:80])

    combined_hits = (
        list(main_search["results"])
        + list(counter_search["results"])
        + list(ru_search["results"])
    )
    competitor_evidence = entity_evidence(combined_hits, req.entities)
    known_blind_spots = _blind_spots(
        intent=intent,
        ru_results=ru_search["results"],
        counter_results=counter_search["results"],
        competitor_evidence=competitor_evidence,
        requested_entities=req.entities,
    )
    synthesis = await _synthesize_balanced(
        req,
        intent=intent,
        main_results=main_search["results"],
        counter_results=counter_search["results"],
        ru_results=ru_search["results"],
        competitor_evidence=competitor_evidence,
        external_results=external_results,
    )
    return {
        "signals": main_search["results"],
        "counter_signals": counter_search["results"],
        "global_signals": main_search["results"],
        "external_grounding": {
            "used": bool(external_results),
            "results": external_results,
        },
        "ru_verification": {
            "results": ru_search["results"],
            "status": "confirmed" if ru_search["results"] else "unverified",
        },
        "unverified_in_ru": not bool(ru_search["results"]),
        "competitors": {
            "requested_entities": req.entities or [],
            "players_detected": sorted(
                [entity for entity, items in competitor_evidence.items() if items]
            ),
            "evidence_by_player": competitor_evidence,
            "who_is_absent": sorted(
                [entity for entity in (req.entities or []) if not competitor_evidence.get(entity)]
            ),
        },
        "intent": intent,
        "applied_filters": {
            "workspace": req.workspace,
            "lang": req.lang,
            "days_back": effective_days_back,
            "source_region": effective_source_region,
            "signal_type_hints": signal_type_hints or [],
        },
        "known_blind_spots": known_blind_spots,
        "confidence": (
            synthesis.get("parsed", {}).get("confidence")
            if synthesis and synthesis.get("parsed")
            else None
        ),
        "synthesis": synthesis,
    }
