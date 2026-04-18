from __future__ import annotations

import time

import redis.asyncio as aioredis

from shared.reindex import GROUP_POSTS_REINDEX, STREAM_POSTS_REINDEX

DEFAULT_STREAM_GROUPS: tuple[tuple[str, str], ...] = (
    ("stream:posts:parsed", "enrichment_workers"),
    ("stream:posts:vision", "vision_workers"),
    ("stream:posts:crawl", "crawl4ai_workers"),
    (STREAM_POSTS_REINDEX, GROUP_POSTS_REINDEX),
)


def _message_age_seconds(message_id: str) -> float:
    try:
        ts_ms = int(str(message_id).split("-", 1)[0])
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, time.time() - ts_ms / 1000.0)


async def collect_redis_stream_snapshot(
    redis_url: str,
    *,
    stream_groups: tuple[tuple[str, str], ...] = DEFAULT_STREAM_GROUPS,
) -> dict:
    client = aioredis.from_url(redis_url, decode_responses=True)
    try:
        streams = []
        for stream, expected_group in stream_groups:
            try:
                groups_info = await client.xinfo_groups(stream)
            except Exception:
                groups_info = []

            matching_group = next(
                (
                    group_info
                    for group_info in groups_info
                    if group_info.get("name") == expected_group
                ),
                None,
            )
            lag = int((matching_group or {}).get("lag") or 0)
            pending = int((matching_group or {}).get("pending") or 0)
            last_delivered_id = str((matching_group or {}).get("last-delivered-id") or "")

            oldest_pending_age_seconds = 0.0
            pending_items = []
            try:
                if pending > 0:
                    pending_items = await client.xpending_range(
                        stream,
                        expected_group,
                        "-",
                        "+",
                        1,
                    )
            except Exception:
                pending_items = []
            if pending_items:
                oldest_pending_age_seconds = _message_age_seconds(
                    str(pending_items[0].get("message_id") or "")
                )

            consumers = []
            try:
                consumers_info = await client.xinfo_consumers(stream, expected_group)
            except Exception:
                consumers_info = []
            for consumer in consumers_info or []:
                consumers.append(
                    {
                        "name": str(consumer.get("name") or ""),
                        "pending": int(consumer.get("pending") or 0),
                        "idle_seconds": round(float(consumer.get("idle") or 0) / 1000.0, 3),
                    }
                )

            streams.append(
                {
                    "stream": stream,
                    "group": expected_group,
                    "lag": lag,
                    "pending": pending,
                    "oldest_pending_age_seconds": round(oldest_pending_age_seconds, 3),
                    "last_delivered_id": last_delivered_id,
                    "consumers": consumers,
                }
            )
        return {"streams": streams}
    finally:
        await client.aclose()
