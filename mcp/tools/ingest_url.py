"""Постановка URL в очередь crawl для существующего поста (post_id обязателен — FK в post_enrichments)."""
import json
import logging
import uuid

import redis.asyncio as aioredis
from fastapi import APIRouter
from pydantic import BaseModel, Field

from mcp.guards import assert_known_workspace, assert_public_http_url
from shared.config import get_settings

logger = logging.getLogger(__name__)

router = APIRouter()

STREAM_CRAWL = "stream:posts:crawl"


class IngestUrlRequest(BaseModel):
    url: str
    workspace: str = Field(default="disruption")
    post_id: str = Field(
        ...,
        description="ID существующего поста в PostgreSQL (иначе crawl4ai не сможет записать enrichment)",
    )


@router.post("")
async def ingest_url(req: IngestUrlRequest) -> dict:
    assert_known_workspace(req.workspace)
    # SSRF-защита: crawl4ai ходит по URL напрямую, поэтому отсекаем приватные/внутренние
    # адреса ДО постановки в очередь (xadd). Резолв host'а вынесен в executor.
    await assert_public_http_url(req.url)
    settings = get_settings()
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.xadd(
            STREAM_CRAWL,
            {
                "post_id": req.post_id,
                "workspace_id": req.workspace,
                "urls": json.dumps([req.url]),
                "trace_id": str(uuid.uuid4()),
            },
        )
    finally:
        await client.aclose()
    logger.info("ingest_url queued crawl post_id=%s url=%s", req.post_id[:16], req.url[:80])
    return {"status": "queued", "stream": STREAM_CRAWL, "post_id": req.post_id}
