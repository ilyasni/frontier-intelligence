"""Reindex enriched posts after crawl or vision data arrives."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from shared.config import get_settings
from shared.redis_client import RedisClient
from shared.reindex import GROUP_POSTS_REINDEX, STREAM_POSTS_REINDEX
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS
from worker.chains.concept_chain import ConceptChain
from worker.gigachat_client import GigaChatClient
from worker.integrations.neo4j_client import Neo4jFrontierClient
from worker.integrations.qdrant_client import QdrantFrontierClient

logger = logging.getLogger(__name__)

CONSUMER = f"reindex-{uuid.uuid4().hex[:8]}"
CLAIM_IDLE_MS = 600_000


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _clean_text(value: Any, limit: int = 1200) -> str:
    text_value = " ".join(str(value or "").split())
    return text_value[:limit]


def _source_metadata(source_extra: Any) -> tuple[str, str]:
    extra = _as_dict(source_extra)
    source_region = str(extra.get("source_region") or "global").strip().lower() or "global"
    market_scope = str(extra.get("market_scope") or "global").strip().lower() or "global"
    return source_region, market_scope


def _concepts_from_enrichment(data: dict[str, Any]) -> list[dict[str, Any]]:
    concepts = []
    for item in _as_list(data.get("items")):
        if not isinstance(item, dict):
            continue
        name = _clean_text(item.get("name"), 100)
        if name:
            concepts.append(
                {
                    "name": name,
                    "category": _clean_text(item.get("category") or "other", 50),
                    "weight": max(1, min(5, int(item.get("weight") or 1))),
                }
            )
    return concepts


def _merge_concepts(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for group in groups:
        for item in group:
            name = _clean_text(item.get("name"), 100)
            if not name:
                continue
            current = merged.get(name)
            weight = max(1, min(5, int(item.get("weight") or 1)))
            category = _clean_text(item.get("category") or "other", 50)
            if current is None or weight > current["weight"]:
                merged[name] = {"name": name, "category": category, "weight": weight}
    return sorted(merged.values(), key=lambda item: (-item["weight"], item["name"]))[:20]


def _crawl_parts(data: dict[str, Any]) -> list[str]:
    parts = [
        _clean_text(data.get("title"), 240),
        _clean_text(data.get("description"), 400),
        _clean_text(data.get("md_excerpt"), 1600),
    ]
    for item in _as_list(data.get("urls"))[:3]:
        if not isinstance(item, dict):
            continue
        parts.extend(
            [
                _clean_text(item.get("title"), 240),
                _clean_text(item.get("description"), 400),
                _clean_text(item.get("md_excerpt"), 1200),
            ]
        )
    return [part for part in parts if part]


def _vision_parts(data: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    labels = [_clean_text(item, 80) for item in _as_list(data.get("all_labels"))]
    labels = [label for label in labels if label]
    parts = []
    if labels:
        parts.append("vision labels: " + ", ".join(sorted(set(labels))[:30]))
    if data.get("ocr_text"):
        parts.append("vision ocr: " + _clean_text(data.get("ocr_text"), 1600))

    concepts = [
        {"name": label, "category": "vision", "weight": 3}
        for label in sorted(set(labels))[:20]
    ]
    for item in _as_list(data.get("items")):
        if not isinstance(item, dict):
            continue
        if item.get("scene"):
            parts.append("vision scene: " + _clean_text(item.get("scene"), 400))
        for signal in _as_list(item.get("design_signals")):
            text_signal = _clean_text(signal, 80)
            if text_signal:
                concepts.append({"name": text_signal, "category": "vision", "weight": 3})
    return parts, concepts


def _published_at_iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return dt.isoformat()
    if value:
        return str(value)
    return None


class ReindexTask:
    """Consumes post reindex requests and patches the canonical Qdrant point."""

    def __init__(self):
        self.settings = get_settings()
        self.redis = RedisClient(self.settings.redis_url)
        self.engine = create_async_engine(
            self.settings.database_url,
            pool_size=3,
            max_overflow=5,
            **ASYNC_ENGINE_POOL_KWARGS,
        )
        self.Session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.gigachat: GigaChatClient | None = None
        self.concept: ConceptChain | None = None
        self.qdrant: QdrantFrontierClient | None = None
        self.neo4j: Neo4jFrontierClient | None = None

    async def setup(self) -> None:
        await self.redis.connect()
        await self.redis.ensure_consumer_group(STREAM_POSTS_REINDEX, GROUP_POSTS_REINDEX)
        self.gigachat = GigaChatClient(redis=self.redis.redis)
        self.concept = ConceptChain(self.gigachat)
        self.qdrant = QdrantFrontierClient()
        self.neo4j = Neo4jFrontierClient()
        logger.info("ReindexTask ready, consumer=%s", CONSUMER)

    async def _fetch_bundle(self, post_id: str) -> dict[str, Any] | None:
        async with self.Session() as session:
            result = await session.execute(
                text(
                    """
                    SELECT
                        p.id, p.workspace_id, p.source_id, p.content, p.category,
                        p.relevance_score, p.has_media, p.media_urls, p.published_at,
                        p.tags, p.extra, p.semantic_cluster_id,
                        i.embedding_status, i.graph_status, i.vision_status,
                        s.source_score, s.source_authority, s.extra AS source_extra
                    FROM posts p
                    LEFT JOIN indexing_status i ON i.post_id = p.id
                    LEFT JOIN sources s ON s.id = p.source_id
                    WHERE p.id = :post_id
                    """
                ),
                {"post_id": post_id},
            )
            row = result.mappings().first()
            if not row:
                return None
            enrichments_result = await session.execute(
                text(
                    """
                    SELECT kind, data
                    FROM post_enrichments
                    WHERE post_id = :post_id
                    """
                ),
                {"post_id": post_id},
            )
            enrichments = {
                item["kind"]: _as_dict(item["data"])
                for item in enrichments_result.mappings().all()
            }
            return {"post": dict(row), "enrichments": enrichments}

    def _build_index_document(
        self,
        bundle: dict[str, Any],
        *,
        extra_concepts: list[dict[str, Any]] | None = None,
    ) -> tuple[str, dict[str, Any], list[dict[str, Any]], str]:
        post = bundle["post"]
        enrichments = bundle["enrichments"]
        post_extra = _as_dict(post.get("extra"))
        source_region, market_scope = _source_metadata(post.get("source_extra"))
        concepts_base = _concepts_from_enrichment(enrichments.get("concepts") or {})
        valence = enrichments.get("valence") or {}
        tags = _as_list(post.get("tags")) or _as_list((enrichments.get("tags") or {}).get("tags"))

        parts = [_clean_text(post.get("content"), 2200)]
        content_sources = ["post"]
        crawl_text = ""
        crawl = enrichments.get("crawl") or {}
        if crawl:
            crawl_segments = _crawl_parts(crawl)
            if crawl_segments:
                content_sources.append("crawl")
                crawl_text = "\n".join(crawl_segments)
                parts.append("crawl content:\n" + crawl_text)

        vision_concepts: list[dict[str, Any]] = []
        vision = enrichments.get("vision") or {}
        if vision:
            vision_segments, vision_concepts = _vision_parts(vision)
            if vision_segments:
                content_sources.append("vision")
                parts.append("\n".join(vision_segments))

        graph_concepts = _merge_concepts(
            concepts_base,
            vision_concepts,
            extra_concepts or [],
        )
        index_text = "\n\n".join(part for part in parts if part).strip()
        payload = {
            "workspace_id": post["workspace_id"],
            "source_id": post["source_id"],
            "post_id": post["id"],
            "content": index_text[:900],
            "url": post_extra.get("url") or "",
            "category": post.get("category") or "unknown",
            "relevance_score": float(post.get("relevance_score") or 0.0),
            "published_at": _published_at_iso(post.get("published_at")),
            "lang": str(post_extra.get("lang") or "unknown").strip().lower() or "unknown",
            "concepts": [item["name"] for item in graph_concepts],
            "tags": [str(item) for item in tags if str(item).strip()][:20],
            "valence": str(valence.get("valence") or "neutral").strip().lower(),
            "signal_type": str(valence.get("signal_type") or "other").strip().lower(),
            "source_region": source_region,
            "market_scope": market_scope,
            "source_score": float(post.get("source_score") or 0.0),
            "source_authority": float(post.get("source_authority") or 0.5),
            "semantic_cluster_id": post.get("semantic_cluster_id") or "",
            "embedding_version": self.settings.gigachat_embeddings_model,
            "indexed_content_sources": content_sources,
            "has_crawl": bool(crawl),
            "has_vision": bool(vision),
            "reindex_version": "enriched-v1",
        }
        if crawl:
            payload.update(
                {
                    "crawl_url": crawl.get("url") or "",
                    "crawl_title": crawl.get("title") or "",
                    "crawl_word_count": int(crawl.get("word_count") or 0),
                }
            )
        if vision:
            payload.update(
                {
                    "vision_labels": sorted(set(payload["concepts"]))[:30],
                    "vision_ocr": _clean_text(vision.get("ocr_text"), 500),
                }
            )
        return index_text, payload, graph_concepts, crawl_text

    async def _mark_done(self, post_id: str, *, graph_done: bool) -> None:
        async with self.Session() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO indexing_status (
                        post_id, embedding_status, qdrant_point_id, graph_status,
                        retry_count, error_message, updated_at
                    )
                    VALUES (
                        :post_id, 'done', :post_id,
                        CASE WHEN :graph_done THEN 'done' ELSE 'skipped' END,
                        0, '', NOW()
                    )
                    ON CONFLICT (post_id) DO UPDATE SET
                        embedding_status = 'done',
                        qdrant_point_id = EXCLUDED.qdrant_point_id,
                        graph_status = CASE
                            WHEN :graph_done THEN 'done'
                            ELSE indexing_status.graph_status
                        END,
                        retry_count = 0,
                        error_message = '',
                        updated_at = NOW()
                    """
                ),
                {"post_id": post_id, "graph_done": graph_done},
            )
            await session.commit()

    async def _mark_error(self, post_id: str, error: str) -> None:
        async with self.Session() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO indexing_status (
                        post_id, embedding_status, retry_count, error_message, updated_at
                    )
                    VALUES (:post_id, 'error', 1, :error, NOW())
                    ON CONFLICT (post_id) DO UPDATE SET
                        retry_count = indexing_status.retry_count + 1,
                        error_message = :error,
                        updated_at = NOW()
                    """
                ),
                {"post_id": post_id, "error": error[:500]},
            )
            await session.commit()

    async def reindex_post(self, post_id: str, *, reason: str) -> dict[str, Any]:
        bundle = await self._fetch_bundle(post_id)
        if not bundle:
            return {"status": "skipped", "reason": "post_not_found"}
        post = bundle["post"]
        if post.get("embedding_status") != "done":
            return {
                "status": "skipped",
                "reason": "embedding_status_not_done",
                "embedding_status": post.get("embedding_status"),
            }

        initial_text, _, _, crawl_text = self._build_index_document(bundle)
        if not initial_text:
            return {"status": "skipped", "reason": "empty_index_text"}

        extracted_concepts: list[dict[str, Any]] = []
        should_extract_crawl = "crawl" in reason or reason in {"manual", "backfill"}
        if should_extract_crawl and crawl_text and self.concept:
            extracted_concepts = await self.concept.run(crawl_text)

        index_text, payload, graph_concepts, _ = self._build_index_document(
            bundle,
            extra_concepts=extracted_concepts,
        )
        assert self.gigachat is not None
        assert self.qdrant is not None
        assert self.neo4j is not None
        vector = await self.gigachat.embed(index_text)
        await self.qdrant.upsert_document(post_id, vector, payload, index_text)
        graph_done = bool(graph_concepts)
        if graph_done:
            await self.neo4j.upsert_concepts(post["workspace_id"], post_id, graph_concepts)
        await self._mark_done(post_id, graph_done=graph_done)
        return {
            "status": "done",
            "post_id": post_id,
            "sources": payload["indexed_content_sources"],
            "concept_count": len(graph_concepts),
        }

    async def _handle_failed_event(self, msg_id: str, data: dict[str, Any], exc: Exception) -> None:
        delivery_count = int(data.get("delivery_count", 0) or 0) + 1
        post_id = str(data.get("post_id") or "")
        if delivery_count >= max(1, int(self.settings.indexing_max_retries or 1)):
            if post_id:
                await self._mark_error(post_id, str(exc))
            await self.redis.xack(STREAM_POSTS_REINDEX, GROUP_POSTS_REINDEX, msg_id)
            logger.error("Reindex event failed permanently post=%s err=%s", post_id[:8], exc)
            return
        await self.redis.xack(STREAM_POSTS_REINDEX, GROUP_POSTS_REINDEX, msg_id)
        await self.redis.xadd(
            STREAM_POSTS_REINDEX,
            {**data, "delivery_count": str(delivery_count)},
        )
        logger.warning(
            "Reindex event requeued post=%s delivery_count=%d err=%s",
            post_id[:8],
            delivery_count,
            exc,
        )

    async def process_event(self, msg_id: str, data: dict[str, Any]) -> None:
        post_id = str(data.get("post_id") or "")
        if not post_id:
            await self.redis.xack(STREAM_POSTS_REINDEX, GROUP_POSTS_REINDEX, msg_id)
            return
        reason = str(data.get("reason") or "unknown")
        try:
            result = await self.reindex_post(post_id, reason=reason)
            await self.redis.xack(STREAM_POSTS_REINDEX, GROUP_POSTS_REINDEX, msg_id)
            logger.info(
                "Reindex %s post=%s reason=%s result=%s",
                result.get("status"),
                post_id[:8],
                reason,
                result,
            )
        except Exception as exc:
            await self._handle_failed_event(msg_id, data, exc)

    async def _reclaim_pending(self) -> list:
        _, messages = await self.redis.xautoclaim(
            STREAM_POSTS_REINDEX,
            GROUP_POSTS_REINDEX,
            CONSUMER,
            CLAIM_IDLE_MS,
            start_id="0-0",
            count=10,
        )
        return messages

    async def run_loop(self) -> None:
        await self.setup()
        logger.info("Starting reindex loop, consumer=%s", CONSUMER)
        while True:
            try:
                for mid, data in await self._reclaim_pending():
                    await self.process_event(mid, data)

                messages = await self.redis.xreadgroup(
                    STREAM_POSTS_REINDEX,
                    GROUP_POSTS_REINDEX,
                    CONSUMER,
                    count=8,
                    block_ms=5000,
                )
                for mid, data in messages:
                    await self.process_event(mid, data)
            except Exception as exc:
                logger.error("Reindex loop error: %s", exc)
                await asyncio.sleep(5)

    async def close(self) -> None:
        await self.redis.disconnect()
        await self.engine.dispose()
        if self.gigachat:
            await self.gigachat.close()
        if self.qdrant:
            await self.qdrant.close()
        if self.neo4j:
            await self.neo4j.close()
