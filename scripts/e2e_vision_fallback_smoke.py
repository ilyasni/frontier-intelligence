"""End-to-end smoke test for forced Polza vision fallback on a real S3 image."""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from shared.config import get_settings
from shared.s3 import make_s3_client
from worker.llm_router_client import LLMRouterClient
from worker.openrouter_guard import QUARANTINE_KEY


async def _pick_recent_s3_key(session_factory) -> str:
    async with session_factory() as session:  # type: AsyncSession
        result = await session.execute(
            text(
                """
                SELECT s3_key
                FROM media_objects
                WHERE mime_type LIKE 'image/%'
                  AND size_bytes BETWEEN 2048 AND 12000000
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
        )
        row = result.mappings().first()
        if not row or not row.get("s3_key"):
            raise RuntimeError("no_recent_image_media_objects_found")
        return str(row["s3_key"])


async def _download_image(settings, s3_key: str) -> bytes:
    s3, bucket = make_s3_client(settings)
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    return response["Body"].read()


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-key", default="", help="Optional explicit S3 key to test.")
    parser.add_argument(
        "--quality-tier",
        default="standard",
        help="Vision quality tier used for the routed call.",
    )
    parser.add_argument(
        "--quarantine-seconds",
        type=int,
        default=180,
        help="How long to quarantine OpenRouter free before the call.",
    )
    args = parser.parse_args()

    settings = get_settings()
    if not settings.polza_api_key or not settings.polza_vision_model:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "polza_vision_fallback_not_configured",
                }
            )
        )
        return 2

    engine = create_async_engine(settings.database_url)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    router = LLMRouterClient(redis=redis, service_name="worker")

    try:
        s3_key = str(args.s3_key or "").strip() or await _pick_recent_s3_key(session_factory)
        image_bytes = await _download_image(settings, s3_key)
        quarantine_ttl = max(60, int(args.quarantine_seconds or 180))
        quarantine_until = time.time() + quarantine_ttl
        await redis.set(QUARANTINE_KEY, str(quarantine_until), ex=quarantine_ttl)
        response = await router.vision(
            image_bytes,
            quality_tier=str(args.quality_tier or "standard").strip().lower() or "standard",
        )
        parsed_keys = []
        if isinstance(response.parsed, dict):
            parsed_keys = sorted(list(response.parsed.keys()))
        payload = {
            "ok": True,
            "s3_key": s3_key,
            "image_bytes": len(image_bytes),
            "provider": response.provider,
            "requested_model": response.requested_model,
            "actual_model": response.actual_model,
            "fallback_reason": response.fallback_reason,
            "has_content": bool((response.content or "").strip()),
            "parsed_type": type(response.parsed).__name__,
            "parsed_keys": parsed_keys,
            "content_preview": str(response.content or "")[:240],
        }
        print(json.dumps(payload, ensure_ascii=False))
        return 0 if response.provider == "polza" else 1
    finally:
        await redis.delete(QUARANTINE_KEY)
        await router.close()
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
