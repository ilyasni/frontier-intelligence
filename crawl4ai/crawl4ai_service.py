"""Crawl4AI service — Redis stream consumer for stream:posts:crawl."""
import asyncio
import json
import uuid
from datetime import UTC, datetime

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from crawl4ai.enrichment_engine import EnrichmentEngine
from shared.config import get_settings
from shared.db_stale_retry import run_twice_on_stale_pool
from shared.redis_client import RedisClient
from shared.reindex import STREAM_POSTS_REINDEX, build_post_reindex_event
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS
from shared.stream_consumers import cleanup_dead_consumers, consumer_name

log = structlog.get_logger()

STREAM_IN = "stream:posts:crawl"
GROUP = "crawl4ai_workers"
# Имя консьюмера стабильно между пересозданиями контейнера (пункт 53 реестра).
# Прежняя форма `f"{service}-{uuid4().hex[:8]}"` давала новое имя на КАЖДЫЙ старт
# процесса, и каждая мёртвая запись оставалась в группе навсегда: у crawl4ai
# 84 рестарта за 45 суток дали ровно 85 записей.
CONSUMER = consumer_name("crawl4ai")
CLAIM_IDLE_MS = 120_000   # 2 min — crawl tasks can take longer


class Crawl4AIService:
    """Consumes stream:posts:crawl, crawls each URL, stores kind='crawl' enrichment."""

    def __init__(self):
        settings = get_settings()
        self.settings = settings
        self.redis = RedisClient(settings.redis_url)
        self.engine_db = create_async_engine(
            settings.database_url,
            pool_size=3,
            max_overflow=5,
            **ASYNC_ENGINE_POOL_KWARGS,
        )
        self.Session = sessionmaker(self.engine_db, class_=AsyncSession, expire_on_commit=False)
        self.engine: EnrichmentEngine | None = None

    async def setup(self):
        await self.redis.connect()
        await self.redis.ensure_consumer_group(STREAM_IN, GROUP)
        self.engine = EnrichmentEngine(self.redis.redis, self.settings)
        await self.engine.start()
        log.info("Crawl4AIService ready", consumer=CONSUMER)

    async def _save_enrichment(self, post_id: str, data: dict) -> None:
        """Сохранение crawl enrichment; при «connection closed» — dispose пула и один retry."""
        enrichment_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{post_id}:crawl"))
        payload = json.dumps(data)
        sql = text("""
                INSERT INTO post_enrichments (id, post_id, kind, data, created_at, updated_at)
                VALUES (:id, :post_id, 'crawl', CAST(:data AS jsonb), NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
            """)
        params = {"id": enrichment_id, "post_id": post_id, "data": payload}

        async def _op() -> None:
            async with self.Session() as session:
                await session.execute(sql, params)
                await session.commit()

        await run_twice_on_stale_pool(
            self.engine_db,
            _op,
            on_retry=lambda **kw: log.warning(
                "crawl4ai DB save stale connection, dispose pool and retry",
                **kw,
            ),
            post_id=post_id[:16],
        )

    async def process_event(self, msg_id: str, data: dict):
        post_id = data.get("post_id")
        workspace_id = data.get("workspace_id", "")
        urls_raw = data.get("urls", "[]")
        trace_id = data.get("trace_id", msg_id)

        if isinstance(urls_raw, str):
            try:
                urls = json.loads(urls_raw)
            except Exception:
                urls = [urls_raw] if urls_raw.startswith("http") else []
        else:
            urls = list(urls_raw)

        if not post_id or not urls:
            await self.redis.xack(STREAM_IN, GROUP, msg_id)
            return

        results = []
        for url in urls:
            try:
                result = await self.engine.enrich_url(url, workspace_id, post_id)
                if result:
                    results.append(result)
            except Exception as exc:
                log.warning("Crawl failed", url=url, post_id=post_id, error=str(exc))

        if results:
            combined = {
                "urls": results,
                "url": results[0].get("url"),
                "title": next((r.get("title") for r in results if r.get("title")), None),
                "description": next(
                    (r.get("description") for r in results if r.get("description")),
                    None,
                ),
                "og": next((r.get("og") for r in results if r.get("og")), {}),
                "word_count": sum(r.get("word_count", 0) for r in results),
                "md_excerpt": results[0].get("md_excerpt", ""),
                "crawled_at": datetime.now(UTC).isoformat(),
                "trace_id": trace_id,
                "source": "crawl4ai",
            }
            await self._save_enrichment(post_id, combined)
            await self.redis.xadd(
                STREAM_POSTS_REINDEX,
                build_post_reindex_event(
                    post_id=post_id,
                    workspace_id=workspace_id,
                    reason="crawl",
                    trace_id=trace_id,
                    source="crawl4ai",
                    extra={"urls_count": len(results)},
                ),
            )
            log.info("Crawl enrichment saved",
                     post_id=post_id, urls_count=len(results), trace_id=trace_id)
        else:
            log.info("No crawl results", post_id=post_id, urls=urls)

        await self.redis.xack(STREAM_IN, GROUP, msg_id)

    async def _reclaim_pending(self) -> list:
        """Claim messages idle > CLAIM_IDLE_MS."""
        _, messages = await self.redis.xautoclaim(
            STREAM_IN, GROUP, CONSUMER, CLAIM_IDLE_MS, start_id="0-0", count=5
        )
        return messages

    async def run_loop(self):
        await self.setup()
        log.info("Starting crawl loop", consumer=CONSUMER)
        # Здесь уборки не было вовсе — и именно этот потребитель накопил
        # 85 призрачных записей: 84 рестарта за 45 суток плюс текущий процесс.
        # Стабильное имя консьюмера чинит корень, уборка разгребает то, что
        # осталось от прежних имён.
        cleanup_interval_sec = 3600
        next_cleanup = 0.0
        while True:
            try:
                now = asyncio.get_running_loop().time()
                if now >= next_cleanup:
                    await cleanup_dead_consumers(self.redis, STREAM_IN, GROUP, keep=CONSUMER)
                    next_cleanup = now + cleanup_interval_sec

                # Reclaim stale pending
                reclaimed = await self._reclaim_pending()
                if reclaimed:
                    for mid, d in reclaimed:
                        await self.process_event(mid, d)

                # New messages
                messages = await self.redis.xreadgroup(
                    STREAM_IN, GROUP, CONSUMER, count=5, block_ms=2000
                )
                if messages:
                    for mid, d in messages:
                        await self.process_event(mid, d)

            except Exception as exc:
                log.error("Crawl loop error", error=str(exc))
                await asyncio.sleep(5)

    async def close(self):
        if self.engine:
            await self.engine.stop()
        await self.redis.disconnect()
        await self.engine_db.dispose()
