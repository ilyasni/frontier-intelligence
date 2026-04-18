from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import get_settings
from worker.gigachat_client import GigaChatClient
from worker.llm_json import parse_llm_json_object

from .searxng_client import SearXNGClient

logger = logging.getLogger(__name__)


def _terms(value: str) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-_]{2,}", (value or "").lower())


def _digest(value: str, prefix: str) -> str:
    return f"{prefix}:{hashlib.sha1(value.encode('utf-8')).hexdigest()[:16]}"


def _string_list(values: Any) -> list[str]:
    if isinstance(values, list):
        return [str(value).strip() for value in values if str(value).strip()]
    return []


def _signal_documents(
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for item in semantic:
        parts = [
            str(item.get("title") or ""),
            " ".join(str(value) for value in (item.get("top_concepts") or [])),
            " ".join(
                str(value)
                for value in ((item.get("explainability") or {}).get("top_terms") or [])
            ),
        ]
        tokens = set(_terms(" ".join(parts)))
        if tokens:
            documents.append({"tokens": tokens, "weight": 1.0})
    for item in [*stable, *emerging]:
        parts = [
            str(item.get("title") or ""),
            " ".join(str(value) for value in (item.get("keywords") or [])),
            " ".join(
                str(value)
                for value in ((item.get("explainability") or {}).get("top_terms") or [])
            ),
        ]
        tokens = set(_terms(" ".join(parts)))
        if tokens:
            documents.append({"tokens": tokens, "weight": 1.25 if item in stable else 1.1})
    return documents


def _topic_overlap_score(topic_tokens: set[str], signal_tokens: set[str]) -> float:
    if not topic_tokens or not signal_tokens:
        return 0.0
    shared = topic_tokens & signal_tokens
    if not shared:
        return 0.0
    return len(shared) / max(len(topic_tokens), 1)


def _frontier_frequency(
    *,
    topic: str,
    query: str,
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
) -> float:
    topic_tokens = set(_terms(f"{topic} {query}"))
    if not topic_tokens:
        return 0.0
    matches = 0.0
    for item in _signal_documents(semantic, stable, emerging):
        overlap = _topic_overlap_score(topic_tokens, item["tokens"])
        if overlap >= 0.34:
            matches += item["weight"] * overlap
    return round(matches, 4)


def _external_signal_strength(results: list[dict[str, Any]], max_results: int) -> float:
    if not results:
        return 0.0
    domains = {urlparse(item["url"]).netloc.lower() for item in results if item.get("url")}
    avg_score = sum(float(item.get("score") or 0.0) for item in results) / max(len(results), 1)
    return round(
        min(
            1.0,
            (len(results) / max(max_results, 1)) * 0.55
            + (len(domains) / max(max_results, 1)) * 0.3
            + min(avg_score / 3.0, 1.0) * 0.15,
        ),
        4,
    )


def _gap_score(*, frontier_frequency: float, external_strength: float) -> float:
    frontier_presence = min(1.0, frontier_frequency / 3.5)
    return round(max(0.0, external_strength - frontier_presence), 4)


def _fallback_topics(
    *,
    workspace: dict[str, Any],
    topic_limit: int,
) -> list[dict[str, str]]:
    topics: list[dict[str, str]] = []
    for value in _string_list(workspace.get("design_lenses")) + _string_list(workspace.get("categories")):
        label = value.replace("_", " ").strip()
        if not label:
            continue
        topics.append(
            {
                "topic": label,
                "query": label,
                "category": label.split()[0],
                "why_expected": f"Derived from workspace profile for {workspace.get('name') or workspace.get('id')}.",
            }
        )
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in topics:
        key = item["topic"].lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= topic_limit:
            break
    return deduped


async def _generate_candidate_topics(
    *,
    workspace: dict[str, Any],
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
) -> list[dict[str, str]]:
    settings = get_settings()
    topic_limit = max(3, int(settings.missing_signals_topic_limit))
    signal_titles = [str(item.get("title") or "") for item in [*stable, *emerging, *semantic][:8]]
    top_concepts = [
        name
        for name, _ in Counter(
            concept
            for item in semantic[:10]
            for concept in (item.get("top_concepts") or [])
        ).most_common(12)
    ]
    payload = {
        "workspace": {
            "id": workspace.get("id"),
            "name": workspace.get("name"),
            "description": workspace.get("description"),
            "categories": _string_list(workspace.get("categories")),
            "design_lenses": _string_list(workspace.get("design_lenses")),
        },
        "current_frontier_signals": signal_titles,
        "top_concepts": top_concepts,
        "max_topics": topic_limit,
    }

    client = GigaChatClient(service_name="worker")
    try:
        response = await client.chat(
            system=(
                "Ты аналитик missing signals. Для заданного workspace предложи темы, которые ожидаемы "
                "для мониторинга, но могут быть недопокрыты внутренними источниками. Верни только JSON."
            ),
            user=(
                "Верни JSON вида "
                '{"topics":[{"topic":"...", "query":"...", "category":"...", "why_expected":"..."}]}\n\n'
                f"{json.dumps(payload, ensure_ascii=False)}"
            ),
            task="mcp_synthesis",
            model_override=settings.gigachat_model_pro,
            pro=True,
            max_tokens=700,
        )
        parsed = parse_llm_json_object(response.content)
    except Exception:
        logger.exception("missing_signals_topic_generation_failed workspace=%s", workspace.get("id"))
        return _fallback_topics(workspace=workspace, topic_limit=topic_limit)
    finally:
        await client.close()

    raw_topics = parsed.get("topics") if isinstance(parsed, dict) else None
    topics: list[dict[str, str]] = []
    for item in raw_topics if isinstance(raw_topics, list) else []:
        if not isinstance(item, dict):
            continue
        topic = str(item.get("topic") or "").strip()
        query = str(item.get("query") or topic).strip()
        if not topic or not query:
            continue
        topics.append(
            {
                "topic": topic[:120],
                "query": query[:180],
                "category": str(item.get("category") or "").strip()[:80],
                "why_expected": str(item.get("why_expected") or "").strip()[:220],
            }
        )
    if not topics:
        return _fallback_topics(workspace=workspace, topic_limit=topic_limit)

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in topics:
        key = item["topic"].lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= topic_limit:
            break
    return deduped or _fallback_topics(workspace=workspace, topic_limit=topic_limit)


async def _generate_opportunities(
    *,
    workspace: dict[str, Any],
    items: list[dict[str, Any]],
) -> dict[str, str]:
    if not items:
        return {}

    settings = get_settings()
    compact = [
        {
            "topic": item["topic"],
            "category": item.get("category"),
            "gap_score": item["gap_score"],
            "why_expected": item.get("why_expected"),
            "evidence": [
                {
                    "title": evidence.get("title"),
                    "url": evidence.get("url"),
                    "engine": evidence.get("engine"),
                }
                for evidence in item.get("evidence", [])[:3]
            ],
        }
        for item in items
    ]

    client = GigaChatClient(service_name="worker")
    try:
        response = await client.chat(
            system=(
                "Ты аналитик стратегических gaps. Для каждой темы предложи короткую opportunity-формулировку "
                "в 1-2 предложениях. Верни только JSON."
            ),
            user=(
                "Верни JSON вида "
                '{"items":[{"topic":"...", "opportunity":"..."}]}\n\n'
                f"{json.dumps({'workspace': workspace, 'gaps': compact}, ensure_ascii=False)}"
            ),
            task="mcp_synthesis",
            model_override=settings.gigachat_model_pro,
            pro=True,
            max_tokens=700,
        )
        parsed = parse_llm_json_object(response.content)
    except Exception:
        logger.exception("missing_signals_opportunity_generation_failed workspace=%s", workspace.get("id"))
        return {
            item["topic"]: (
                f"External evidence suggests growing attention around {item['topic']}; "
                f"expand frontier sources or tracking queries for this area."
            )
            for item in items
        }
    finally:
        await client.close()

    mapping: dict[str, str] = {}
    for item in parsed.get("items") if isinstance(parsed, dict) else []:
        if not isinstance(item, dict):
            continue
        topic = str(item.get("topic") or "").strip()
        opportunity = str(item.get("opportunity") or "").strip()
        if topic and opportunity:
            mapping[topic] = opportunity[:500]
    return mapping


async def _replace_missing_signals(
    session: AsyncSession,
    *,
    workspace_id: str,
    items: list[dict[str, Any]],
) -> None:
    await session.execute(
        text("DELETE FROM missing_signals WHERE workspace_id = :workspace_id"),
        {"workspace_id": workspace_id},
    )
    for item in items:
        await session.execute(
            text(
                """
                INSERT INTO missing_signals (
                    id, workspace_id, topic, gap_score, opportunity,
                    searxng_frequency, frontier_frequency, evidence_urls, category,
                    created_at, updated_at
                )
                VALUES (
                    :id, :workspace_id, :topic, :gap_score, :opportunity,
                    :searxng_frequency, :frontier_frequency, CAST(:evidence_urls AS jsonb), :category,
                    NOW(), NOW()
                )
                """
            ),
            {
                "id": item["id"],
                "workspace_id": workspace_id,
                "topic": item["topic"],
                "gap_score": item["gap_score"],
                "opportunity": item.get("opportunity"),
                "searxng_frequency": item["searxng_frequency"],
                "frontier_frequency": item["frontier_frequency"],
                "evidence_urls": json.dumps(item["evidence_urls"], ensure_ascii=False),
                "category": item.get("category"),
            },
        )


async def run_missing_signals_analysis(
    session: AsyncSession,
    *,
    workspace_id: str | None,
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    settings = get_settings()
    if not workspace_id or not settings.searxng_enabled or not settings.missing_signals_enabled:
        return []

    workspace = (
        await session.execute(
            text(
                """
                SELECT id, name, description, categories, design_lenses, extra
                FROM workspaces
                WHERE id = :workspace_id
                """
            ),
            {"workspace_id": workspace_id},
        )
    ).mappings().first()
    if not workspace:
        return []

    topics = await _generate_candidate_topics(
        workspace=dict(workspace),
        semantic=semantic,
        stable=stable,
        emerging=emerging,
    )
    client = SearXNGClient(service_name="worker")
    items: list[dict[str, Any]] = []
    for topic in topics:
        results = await client.search(
            topic["query"],
            categories=settings.searxng_categories,
            language=settings.missing_signals_language,
            time_range=settings.missing_signals_time_range,
            limit=settings.searxng_max_results,
            mode="missing_signals",
        )
        if len(results) < int(settings.missing_signals_min_external_results):
            continue
        frontier_frequency = _frontier_frequency(
            topic=topic["topic"],
            query=topic["query"],
            semantic=semantic,
            stable=stable,
            emerging=emerging,
        )
        external_strength = _external_signal_strength(results, int(settings.searxng_max_results))
        gap_score = _gap_score(
            frontier_frequency=frontier_frequency,
            external_strength=external_strength,
        )
        if gap_score < float(settings.missing_signals_min_gap_score):
            continue
        items.append(
            {
                "id": _digest(f"{workspace_id}|{topic['topic']}", "missing"),
                "topic": topic["topic"],
                "query": topic["query"],
                "category": topic.get("category"),
                "why_expected": topic.get("why_expected"),
                "gap_score": gap_score,
                "searxng_frequency": float(len(results)),
                "frontier_frequency": frontier_frequency,
                "evidence_urls": [
                    result["url"]
                    for result in results[: int(settings.missing_signals_max_evidence_urls)]
                ],
                "evidence": results,
            }
        )

    items.sort(key=lambda item: item["gap_score"], reverse=True)
    if not items:
        await _replace_missing_signals(session, workspace_id=workspace_id, items=[])
        return []

    top_items = items[: int(settings.missing_signals_topic_limit)]
    opportunity_map = await _generate_opportunities(workspace=dict(workspace), items=top_items)
    for item in top_items:
        item["opportunity"] = opportunity_map.get(
            item["topic"],
            f"External evidence suggests growing attention around {item['topic']}; expand frontier coverage here.",
        )

    await _replace_missing_signals(session, workspace_id=workspace_id, items=top_items)
    return top_items
