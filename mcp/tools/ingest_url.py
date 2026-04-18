"""Постановка URL в очередь crawl для существующего поста (post_id обязателен — FK в post_enrichments)."""
import json
import logging
import uuid

import redis.asyncio as aioredis
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

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
    if not req.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="url must be http(s)")
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
    logger.info("ingest_url queued crawl", post_id=req.post_id[:16], url=req.url[:80])
    return {"status": "queued", "stream": STREAM_CRAWL, "post_id": req.post_id}
