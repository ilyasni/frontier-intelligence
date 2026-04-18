"""Main enrichment consumer."""
import asyncio
import json
import logging
import uuid

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from shared.config import get_settings
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS
from shared.redis_client import RedisClient
from shared.events.posts_parsed_v1 import PostParsedEvent
from shared.events.posts_vision_v1 import PostVisionEvent
from shared.source_quality import source_quality_payload
from worker.gigachat_client import GigaChatClient
from worker.chains.relevance_chain import RelevanceChain
from worker.chains.concept_chain import ConceptChain
from worker.chains.relevance_concepts_chain import RelevanceConceptsChain
from worker.chains.valence_chain import ValenceChain
from worker.integrations.qdrant_client import QdrantFrontierClient
from worker.integrations.neo4j_client import Neo4jFrontierClient

logger = logging.getLogger(__name__)

STREAM_IN = "stream:posts:parsed"
STREAM_OUT = "stream:posts:enriched"
STREAM_CRAWL = "stream:posts:crawl"
STREAM_VISION = "stream:posts:vision"
GROUP = "enrichment_workers"
CONSUMER = f"worker-{uuid.uuid4().hex[:8]}"


class EnrichmentTask:
    def __init__(self):
        settings = get_settings()
        self.settings = settings
        self.redis = RedisClient(settings.redis_url)
        self.engine = create_async_engine(
            settings.database_url,
            pool_size=5,
            max_overflow=10,
            **ASYNC_ENGINE_POOL_KWARGS,
        )
        self.Session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.gigachat = None
        self.relevance = None
        self.concept = None
        self.relevance_concepts = None
        self.valence = None
        self.qdrant = None
        self.neo4j = None
        self._workspace_cache = {}
        self._workspace_cache_ts = 0
        self._source_cache: dict = {}

    async def setup(self):
        await self.redis.connect()
        await self.redis.ensure_consumer_group(STREAM_IN, GROUP)
        self.gigachat = GigaChatClient(redis=self.redis.redis)
        self.relevance = RelevanceChain(self.gigachat)
        self.concept = ConceptChain(self.gigachat)
        self.relevance_concepts = RelevanceConceptsChain(self.gigachat)
        self.valence = ValenceChain(self.gigachat)
        self.qdrant = QdrantFrontierClient()
        self.neo4j = Neo4jFrontierClient()
        logger.info("EnrichmentTask ready, consumer=%s", CONSUMER)

    # ── Source validation ────────────────────────────────────────────────────

    async def _get_source(self, source_id: str) -> dict:
        """Look up source from DB with 5-minute cache."""
        now = asyncio.get_event_loop().time()
        cached = self._source_cache.get(source_id)
        if cached and now - cached.get("_ts", 0) < 300:
            return cached
        async with self.Session() as session:
            row = await session.execute(
                text("""
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
                            AVG(CASE WHEN pe.id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS linked_ratio,
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
                    WHERE s.id = :id
                """),
                {"id": source_id},
            )
            src = row.mappings().first()
            if src:
                result = dict(src)
                result.update(source_quality_payload(result))
                result["_ts"] = now
                self._source_cache[source_id] = result
                return result
        return {}

    def _validate_source_event(self, event: PostParsedEvent, source: dict) -> str | None:
        """Return error string if event payload doesn't match source config, else None."""
        if not source:
            return f"Unknown source_id={event.source_id!r}"
        if not source.get("is_enabled"):
            return f"Source {event.source_id!r} is disabled"
        if source.get("source_type") == "telegram":
            channel = (source.get("tg_channel") or "").lstrip("@").lower()
            if not channel:
                return f"Telegram source {event.source_id!r} has no tg_channel configured"
            url = event.url or ""
            if not url:
                return f"Missing url for telegram source {event.source_id!r}"
            expected_prefix = f"https://t.me/{channel}/"
            if not url.lower().startswith(expected_prefix):
                return (
                    f"URL {url!r} doesn't match configured channel @{channel} "
                    f"for source {event.source_id!r}"
                )
            author = (event.author or "").lstrip("@").lower()
            if author != channel:
                return (
                    f"Author {event.author!r} doesn't match configured channel @{channel} "
                    f"for source {event.source_id!r}"
                )
            if not event.external_id.lstrip("-").isdigit():
                return (
                    f"Non-numeric external_id {event.external_id!r} "
                    f"for telegram source {event.source_id!r}"
                )
        return None

    # ── Workspace cache ──────────────────────────────────────────────────────

    async def _get_workspace(self, workspace_id):
        now = asyncio.get_event_loop().time()
        # Короткий TTL: правки categories/relevance_weights из админки подхватываются быстрее (план admin)
        if now - self._workspace_cache_ts > 90 or workspace_id not in self._workspace_cache:
            async with self.Session() as session:
                row = await session.execute(
                    text("SELECT * FROM workspaces WHERE id = :id"),
                    {"id": workspace_id},
                )
                ws = row.mappings().first()
                if ws:
                    self._workspace_cache[workspace_id] = dict(ws)
                    self._workspace_cache_ts = now
        return self._workspace_cache.get(workspace_id, {})

    # ── DB helpers ───────────────────────────────────────────────────────────

    async def _save_post(self, event: PostParsedEvent) -> str:
        """Insert post into DB. Returns post_id."""
        post_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{event.source_id}:{event.external_id}"))
        extra = dict(event.extra or {})
        if event.url:
            extra["url"] = event.url
        if event.author:
            extra["author"] = event.author

        async with self.Session() as session:
            await session.execute(text("""
                INSERT INTO posts (id, workspace_id, source_id, external_id,
                    grouped_id, content, has_media, media_urls, published_at,
                    extra, created_at, updated_at)
                VALUES (:id, :workspace_id, :source_id, :external_id,
                    :grouped_id, :content, :has_media, CAST(:media_urls AS jsonb),
                    :published_at, CAST(:extra AS jsonb), NOW(), NOW())
                ON CONFLICT (source_id, external_id) DO NOTHING
            """), {
                "id": post_id,
                "workspace_id": event.workspace_id,
                "source_id": event.source_id,
                "external_id": event.external_id,
                "grouped_id": event.grouped_id,
                "content": event.content,
                "has_media": event.has_media,
                "media_urls": json.dumps(event.media_urls),
                "published_at": event.published_at,
                "extra": json.dumps(extra),
            })
            await session.commit()
        return post_id

    async def _upsert_media_group(
        self, workspace_id: str, source_id: str, grouped_id: str, item_count: int
    ):
        """Create or update media_groups record for an album."""
        group_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_id}:{grouped_id}"))
        async with self.Session() as session:
            await session.execute(text("""
                INSERT INTO media_groups (id, workspace_id, source_id, grouped_id,
                    item_count, assembled, created_at, updated_at)
                VALUES (:id, :ws, :src, :gid, :cnt, FALSE, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET
                    item_count = GREATEST(media_groups.item_count, EXCLUDED.item_count),
                    updated_at = NOW()
            """), {
                "id": group_id, "ws": workspace_id, "src": source_id,
                "gid": grouped_id, "cnt": item_count,
            })
            await session.commit()

    async def _update_post_enrichment(self, post_id: str, score: float, category: str):
        """Update relevance_score and category on the posts table."""
        async with self.Session() as session:
            await session.execute(text("""
                UPDATE posts SET relevance_score = :score, category = :category, updated_at = NOW()
                WHERE id = :id
            """), {"id": post_id, "score": score, "category": category})
            await session.commit()

    async def _update_post_tags(self, post_id: str, tags: list[str]):
        """Write tags to posts.tags column."""
        async with self.Session() as session:
            await session.execute(text("""
                UPDATE posts SET tags = CAST(:tags AS jsonb), updated_at = NOW()
                WHERE id = :id
            """), {"id": post_id, "tags": json.dumps(tags)})
            await session.commit()

    async def _save_enrichment(self, post_id: str, kind: str, data: dict):
        """Upsert a record into post_enrichments."""
        enrichment_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{post_id}:{kind}"))
        async with self.Session() as session:
            await session.execute(text("""
                INSERT INTO post_enrichments (id, post_id, kind, data, created_at, updated_at)
                VALUES (:id, :post_id, :kind, CAST(:data AS jsonb), NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
            """), {
                "id": enrichment_id, "post_id": post_id, "kind": kind,
                "data": json.dumps(data),
            })
            await session.commit()

    async def _update_vision_status(self, post_id: str, status: str):
        async with self.Session() as session:
            await session.execute(text("""
                INSERT INTO indexing_status (post_id, embedding_status, vision_status, updated_at)
                VALUES (:post_id, 'pending', :status, NOW())
                ON CONFLICT (post_id) DO UPDATE SET
                    vision_status = EXCLUDED.vision_status,
                    updated_at = NOW()
            """), {"post_id": post_id, "status": status})
            await session.commit()

    async def _mark_vision_skipped(self, post_id: str, reason: str) -> None:
        await self._save_enrichment(
            post_id,
            "vision",
            {
                "vision_mode": "skip",
                "vision_skip_reason": reason,
                "items": [],
                "all_labels": [],
                "ocr_text": "",
            },
        )
        await self._update_vision_status(post_id, "skipped")

    @staticmethod
    def _source_metadata(source: dict) -> tuple[str, str]:
        extra = source.get("extra") or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except json.JSONDecodeError:
                extra = {}
        if not isinstance(extra, dict):
            extra = {}
        source_region = str(extra.get("source_region") or "global").strip().lower() or "global"
        market_scope = str(extra.get("market_scope") or "global").strip().lower() or "global"
        return source_region, market_scope

    @staticmethod
    def _source_vision_policy(source: dict) -> tuple[str, int]:
        extra = source.get("extra") or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except json.JSONDecodeError:
                extra = {}
        if not isinstance(extra, dict):
            extra = {}
        vision = extra.get("vision") or {}
        if not isinstance(vision, dict):
            vision = {}

        mode = str(vision.get("mode") or "full").strip().lower()
        if mode not in {"full", "ocr_only", "skip"}:
            mode = "full"
        try:
            max_media_bytes = int(vision.get("max_media_bytes") or 9_000_000)
        except (TypeError, ValueError):
            max_media_bytes = 9_000_000
        return mode, max(0, max_media_bytes)

    def _use_joint_relevance_concepts(self, event: PostParsedEvent) -> bool:
        if not bool(getattr(self.settings, "gigachat_rc_joint_enabled", False)):
            return False

        workspace_allow = {
            item.strip()
            for item in str(
                getattr(self.settings, "gigachat_rc_joint_workspaces", "") or ""
            ).split(",")
            if item.strip()
        }
        source_allow = {
            item.strip()
            for item in str(
                getattr(self.settings, "gigachat_rc_joint_sources", "") or ""
            ).split(",")
            if item.strip()
        }
        if not workspace_allow and not source_allow:
            return True
        return event.workspace_id in workspace_allow or event.source_id in source_allow

    async def _runtime_vision_enabled(self) -> bool:
        if self.gigachat and hasattr(self.gigachat, "refresh_runtime_overrides"):
            await self.gigachat.refresh_runtime_overrides()
            return self.gigachat.setting_bool("vision_enabled", self.settings.vision_enabled)
        return bool(self.settings.vision_enabled)

    async def _update_indexing_status(self, post_id: str, status: str,
                                       error: str = "", qdrant_id: str = "",
                                       graph_status: str = ""):
        """Upsert into indexing_status."""
        async with self.Session() as session:
            if graph_status:
                await session.execute(text("""
                    INSERT INTO indexing_status (post_id, embedding_status, qdrant_point_id,
                        graph_status, retry_count, error_message, updated_at)
                    VALUES (:post_id, :status, :qdrant_id, :graph_status, 0, :error, NOW())
                    ON CONFLICT (post_id) DO UPDATE SET
                        embedding_status = EXCLUDED.embedding_status,
                        qdrant_point_id = EXCLUDED.qdrant_point_id,
                        graph_status = EXCLUDED.graph_status,
                        error_message = EXCLUDED.error_message,
                        updated_at = NOW()
                """), {
                    "post_id": post_id, "status": status, "qdrant_id": qdrant_id,
                    "graph_status": graph_status, "error": error,
                })
            else:
                await session.execute(text("""
                    INSERT INTO indexing_status (post_id, embedding_status, qdrant_point_id,
                        retry_count, error_message, updated_at)
                    VALUES (:post_id, :status, :qdrant_id, 0, :error, NOW())
                    ON CONFLICT (post_id) DO UPDATE SET
                        embedding_status = EXCLUDED.embedding_status,
                        qdrant_point_id = EXCLUDED.qdrant_point_id,
                        error_message = EXCLUDED.error_message,
                        updated_at = NOW()
                """), {"post_id": post_id, "status": status, "qdrant_id": qdrant_id, "error": error})
            await session.commit()

    async def _get_existing_qdrant_id(self, post_id: str) -> str:
        """Return current qdrant_point_id from indexing_status, or empty string."""
        async with self.Session() as session:
            row = await session.execute(
                text("SELECT qdrant_point_id FROM indexing_status WHERE post_id = :id"),
                {"id": post_id},
            )
            result = row.scalar()
            return result or ""

    # ── Consumer housekeeping ────────────────────────────────────────────────

    async def _startup_reclaim(self):
        """On startup: reclaim all messages pending > claim_idle_ms from dead consumers."""
        idle_ms = self.settings.indexing_claim_idle_ms
        start_id = "0-0"
        total = 0
        while True:
            next_id, messages = await self.redis.xautoclaim(
                STREAM_IN, GROUP, CONSUMER, idle_ms, start_id=start_id, count=50
            )
            if messages:
                await self._gather_process_bounded(messages)
                total += len(messages)
            # XAUTOCLAIM can skip deleted PEL entries and return no messages for
            # a non-terminal cursor. Continue until Redis returns 0-0.
            if next_id == "0-0":
                break
            start_id = next_id
        if total:
            logger.info("Startup reclaim: processed %d stale pending messages", total)

    async def _reclaim_pending(self) -> list:
        """Reclaim up to batch_size messages idle > claim_idle_ms."""
        _, messages = await self.redis.xautoclaim(
            STREAM_IN, GROUP, CONSUMER,
            self.settings.indexing_claim_idle_ms,
            start_id="0-0",
            count=self.settings.indexing_batch_size,
        )
        return messages

    async def _cleanup_dead_consumers(self):
        """Delete consumers that are idle > 1h and have no pending messages."""
        try:
            consumers = await self.redis.xinfo_consumers(STREAM_IN, GROUP)
            for c in consumers:
                name = c.get("name", "")
                idle_ms = c.get("idle", 0)
                pending = c.get("pending", 0)
                # Skip ourselves
                if name == CONSUMER:
                    continue
                # Delete consumers idle > 1h with no pending messages
                if idle_ms > 3_600_000 and pending == 0:
                    await self.redis.xdel_consumer(STREAM_IN, GROUP, name)
                    logger.info("Deleted dead consumer %s (idle=%ds, pending=0)", name, idle_ms // 1000)
        except Exception as exc:
            logger.warning("Consumer cleanup error: %s", exc)

    # ── Main processing ──────────────────────────────────────────────────────

    async def process_event(self, msg_id, data):
        retry_count = int(data.get("retry_count", 0))
        try:
            event = PostParsedEvent(**{k: v for k, v in data.items()
                                       if k in PostParsedEvent.model_fields})
        except Exception as exc:
            logger.warning("Bad event %s: %s", msg_id, exc)
            filtered = {k: v for k, v in data.items() if k in PostParsedEvent.model_fields}
            sid, eid = filtered.get("source_id"), filtered.get("external_id")
            if sid and eid:
                pid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{sid}:{eid}"))
                await self._update_indexing_status(pid, "error", error=f"bad_event: {exc}"[:500])
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            return

        ws = await self._get_workspace(event.workspace_id)
        if not ws:
            logger.warning("Unknown workspace %s, skipping", event.workspace_id)
            pid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{event.source_id}:{event.external_id}"))
            await self._update_indexing_status(
                pid, "error", error=f"unknown_workspace:{event.workspace_id}"[:500],
            )
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            return

        source = await self._get_source(event.source_id)
        validation_error = self._validate_source_event(event, source)
        if validation_error:
            logger.error("Source validation failed, rejecting event %s: %s", msg_id, validation_error)
            pid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{event.source_id}:{event.external_id}"))
            await self._update_indexing_status(pid, "error", error=validation_error[:500])
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            return

        post_id = await self._save_post(event)
        await self._update_indexing_status(post_id, "pending")

        # Create media_group if this is an album
        if event.grouped_id:
            await self._upsert_media_group(
                workspace_id=event.workspace_id,
                source_id=event.source_id,
                grouped_id=event.grouped_id,
                item_count=len(event.media_urls),
            )

        # Publish vision event before relevance check — vision is independent
        if event.has_media and event.media_urls:
            try:
                if await self._runtime_vision_enabled():
                    vision_mode, max_media_bytes = self._source_vision_policy(source)
                    vision_event = PostVisionEvent(
                        post_id=post_id,
                        workspace_id=event.workspace_id,
                        source_id=event.source_id,
                        grouped_id=event.grouped_id or "",
                        media_s3_keys=event.media_urls,
                        album_total_items=len(event.media_urls),
                        vision_mode=vision_mode,
                        max_media_bytes=max_media_bytes,
                    )
                    await self.redis.xadd(STREAM_VISION, vision_event.model_dump(mode="json"))
                else:
                    runtime_mode = getattr(self.gigachat, "runtime_mode", "custom")
                    reason = (
                        "runtime_mode_no_vision"
                        if runtime_mode in {"no-vision", "gigachat-2-only"}
                        else "vision_disabled"
                    )
                    await self._mark_vision_skipped(post_id, reason)
            except Exception as exc:
                logger.warning("Failed to publish vision event for %s: %s", post_id[:8], exc)

        qdrant_upserted = False
        try:
            categories = ws.get("categories") or ["technology"]
            if isinstance(categories, str):
                categories = json.loads(categories)

            rw = ws.get("relevance_weights") or {}
            if isinstance(rw, str):
                rw = json.loads(rw)
            threshold = float(rw.get("threshold", self.settings.default_relevance_threshold))

            concepts = None
            if self._use_joint_relevance_concepts(event):
                rel, concepts = await self.relevance_concepts.run(
                    content=event.content,
                    workspace_name=ws.get("name", event.workspace_id),
                    categories=categories,
                    threshold=threshold,
                )
                joint_meta = getattr(self.relevance_concepts, "last_meta", {}) or {}
                logger.info(
                    "gigachat_task task=relevance_concepts post=%s model=%s prompt_tokens=%s completion_tokens=%s "
                    "precached_prompt_tokens=%s billable_tokens=%s escalation_reason=%s",
                    post_id[:8],
                    joint_meta.get("model", ""),
                    getattr(joint_meta.get("usage"), "prompt_tokens", 0),
                    getattr(joint_meta.get("usage"), "completion_tokens", 0),
                    getattr(joint_meta.get("usage"), "precached_prompt_tokens", 0),
                    getattr(joint_meta.get("usage"), "billable_tokens", 0),
                    "fallback_to_pro" if joint_meta.get("escalated") else "",
                )
            else:
                rel = await self.relevance.run(
                    content=event.content,
                    workspace_name=ws.get("name", event.workspace_id),
                    categories=categories,
                    threshold=threshold,
                )
                logger.info(
                    "gigachat_task task=relevance post=%s model=%s prompt_tokens=%s completion_tokens=%s "
                    "precached_prompt_tokens=%s billable_tokens=%s escalation_reason=%s",
                    post_id[:8],
                    rel.get("_model", ""),
                    getattr(rel.get("_usage"), "prompt_tokens", 0),
                    getattr(rel.get("_usage"), "completion_tokens", 0),
                    getattr(rel.get("_usage"), "precached_prompt_tokens", 0),
                    getattr(rel.get("_usage"), "billable_tokens", 0),
                    "fallback_to_pro" if rel.get("_escalated") else "",
                )

            if not rel["relevant"]:
                # Не даём исключению из вспомогательных шагов оставить embedding_status=pending без ACK
                try:
                    await self._update_post_enrichment(post_id, rel["score"], rel["category"])
                except Exception as exc:
                    logger.warning(
                        "update_post_enrichment (dropped branch) failed post=%s: %s",
                        post_id[:8],
                        exc,
                    )
                try:
                    existing_qdrant_id = await self._get_existing_qdrant_id(post_id)
                    if existing_qdrant_id:
                        try:
                            await self.qdrant.delete_document(post_id)
                            logger.info("Deleted Qdrant point for dropped post %s", post_id[:8])
                        except Exception as exc:
                            logger.warning(
                                "Failed to delete Qdrant point for %s: %s", post_id[:8], exc
                            )
                except Exception as exc:
                    logger.warning("qdrant lookup/delete (dropped) failed post=%s: %s", post_id[:8], exc)
                try:
                    await self._update_indexing_status(
                        post_id,
                        "dropped",
                        qdrant_id="",
                        graph_status="skipped",
                    )
                except Exception as exc:
                    logger.error(
                        "indexing_status dropped failed post=%s: %s", post_id[:8], exc
                    )
                await self.redis.xack(STREAM_IN, GROUP, msg_id)
                logger.debug("Dropped %s score=%.2f", post_id[:8], rel["score"])
                return

            if concepts is None:
                concepts = await self.concept.run(event.content)
                concept_meta = getattr(self.concept, "last_meta", {}) or {}
                logger.info(
                    "gigachat_task task=concepts post=%s model=%s prompt_tokens=%s completion_tokens=%s "
                    "precached_prompt_tokens=%s billable_tokens=%s escalation_reason=%s",
                    post_id[:8],
                    concept_meta.get("model", ""),
                    getattr(concept_meta.get("usage"), "prompt_tokens", 0),
                    getattr(concept_meta.get("usage"), "completion_tokens", 0),
                    getattr(concept_meta.get("usage"), "precached_prompt_tokens", 0),
                    getattr(concept_meta.get("usage"), "billable_tokens", 0),
                    "fallback_to_pro" if concept_meta.get("escalated") else "",
                )

            # Tags: high-weight concept names (weight >= 3)
            tags = [c["name"] for c in concepts if c.get("weight", 0) >= 3]
            valence = await self.valence.run(event.content)
            valence_meta = getattr(self.valence, "last_meta", {}) or {}
            logger.info(
                "gigachat_task task=valence post=%s model=%s prompt_tokens=%s completion_tokens=%s "
                "precached_prompt_tokens=%s billable_tokens=%s escalation_reason=%s",
                post_id[:8],
                valence_meta.get("model", ""),
                getattr(valence_meta.get("usage"), "prompt_tokens", 0),
                getattr(valence_meta.get("usage"), "completion_tokens", 0),
                getattr(valence_meta.get("usage"), "precached_prompt_tokens", 0),
                getattr(valence_meta.get("usage"), "billable_tokens", 0),
                "fallback_to_pro" if valence_meta.get("escalated") else "",
            )
            source_region, market_scope = self._source_metadata(source)

            embed_text = event.content[:2000]
            vector = await self.gigachat.embed(embed_text)

            payload = {
                "workspace_id": event.workspace_id,
                "source_id": event.source_id,
                "post_id": post_id,
                "content": event.content[:500],
                "url": event.url,
                "author": event.author,
                "category": rel["category"],
                "relevance_score": rel["score"],
                "published_at": event.published_at.isoformat() if event.published_at else None,
                "lang": str((event.extra or {}).get("lang") or "unknown").strip().lower() or "unknown",
                "concepts": [c["name"] for c in concepts],
                "tags": tags,
                "valence": valence["valence"],
                "signal_type": valence["signal_type"],
                "source_region": source_region,
                "market_scope": market_scope,
                "source_score": float(source.get("source_score") or 0.0),
                "source_authority": float(source.get("source_authority") or 0.5),
                "embedding_version": self.settings.gigachat_embeddings_model,
            }
            await self.qdrant.upsert_document(post_id, vector, payload, embed_text)
            qdrant_upserted = True

            if concepts:
                await self.neo4j.upsert_concepts(event.workspace_id, post_id, concepts)

            # Persist enrichments
            await self._save_enrichment(post_id, "concepts", {"items": concepts})
            if tags:
                await self._save_enrichment(post_id, "tags", {"tags": tags})
                await self._update_post_tags(post_id, tags)
            await self._save_enrichment(post_id, "valence", valence)

            await self._update_post_enrichment(post_id, rel["score"], rel["category"])
            await self._update_indexing_status(
                post_id, "done", qdrant_id=post_id,
                graph_status="done" if concepts else "skipped",
            )

            # Crawl только внешние ссылки (linked_urls), не permalink t.me
            if event.linked_urls:
                try:
                    await self.redis.xadd(STREAM_CRAWL, {
                        "post_id": post_id,
                        "workspace_id": event.workspace_id,
                        "urls": json.dumps(event.linked_urls),
                        "trace_id": str(uuid.uuid4()),
                    })
                except Exception as exc:
                    logger.warning("Failed to publish crawl event for %s: %s", post_id[:8], exc)

            await self.redis.xadd(STREAM_OUT, {
                "post_id": post_id,
                "workspace_id": event.workspace_id,
                "source_id": event.source_id,
                "category": rel["category"],
                "relevance_score": str(rel["score"]),
                "concept_count": str(len(concepts)),
            })
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            logger.info("Enriched %s score=%.2f cat=%s concepts=%d tags=%d",
                post_id[:8], rel["score"], rel["category"], len(concepts), len(tags))

        except Exception as exc:
            if qdrant_upserted:
                try:
                    await self.qdrant.delete_document(post_id)
                    logger.info("Rolled back Qdrant point for failed post %s", post_id[:8])
                except Exception as del_exc:
                    logger.warning("Failed to roll back Qdrant point for %s: %s", post_id[:8], del_exc)
            if retry_count < self.settings.indexing_max_retries:
                logger.warning("Enrichment failed (retry %d): %s", retry_count + 1, exc)
                await self.redis.xack(STREAM_IN, GROUP, msg_id)
                await self.redis.xadd(STREAM_IN, {**data, "retry_count": str(retry_count + 1)})
            else:
                logger.error("Max retries exceeded for %s: %s", post_id, exc)
                await self._update_indexing_status(post_id, "error", error=str(exc)[:500])
                await self.redis.xack(STREAM_IN, GROUP, msg_id)

    async def _gather_process_bounded(self, pairs: list[tuple[str, dict]]) -> None:
        """Обработка батча с ограничением параллелизма (снижает пики 429 к GigaChat)."""
        if not pairs:
            return
        limit = max(1, self.settings.indexing_max_concurrency)
        sem = asyncio.Semaphore(limit)

        async def _one(mid: str, data: dict) -> None:
            async with sem:
                await self.process_event(mid, data)

        await asyncio.gather(*(_one(mid, d) for mid, d in pairs), return_exceptions=True)

    async def run_loop(self):
        await self.setup()
        await self._startup_reclaim()
        batch_size = self.settings.indexing_batch_size
        last_cleanup = asyncio.get_event_loop().time()
        logger.info(
            "Starting enrichment loop, batch=%d max_concurrency=%d consumer=%s",
            batch_size,
            self.settings.indexing_max_concurrency,
            CONSUMER,
        )
        while True:
            try:
                # Reclaim stale pending first
                reclaimed = await self._reclaim_pending()
                if reclaimed:
                    await self._gather_process_bounded(reclaimed)

                # Normal new messages
                messages = await self.redis.xreadgroup(
                    STREAM_IN, GROUP, CONSUMER, count=batch_size, block_ms=5000)
                if messages:
                    await self._gather_process_bounded(messages)

                # Periodic dead consumer cleanup
                now = asyncio.get_event_loop().time()
                if now - last_cleanup > self.settings.indexing_consumer_cleanup_interval:
                    await self._cleanup_dead_consumers()
                    last_cleanup = now

            except Exception as exc:
                logger.error("Loop error: %s", exc)
                await asyncio.sleep(5)

    async def close(self):
        await self.redis.disconnect()
        await self.engine.dispose()
        if self.gigachat:
            await self.gigachat.close()
        if self.qdrant:
            await self.qdrant.close()
        if self.neo4j:
            await self.neo4j.close()
