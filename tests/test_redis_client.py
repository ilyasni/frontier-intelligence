from unittest.mock import AsyncMock, MagicMock

import redis.asyncio as aioredis

from shared.redis_client import RedisClient


async def test_xautoclaim_recreates_group_on_nogroup() -> None:
    client = RedisClient("redis://localhost:6379/0")
    client.redis = MagicMock()
    client.redis.xautoclaim = AsyncMock(
        side_effect=aioredis.ResponseError(
            "NOGROUP No such key 'stream:posts:parsed' or consumer group 'enrichment_workers'"
        )
    )
    client.ensure_consumer_group = AsyncMock()

    next_id, messages = await client.xautoclaim(
        "stream:posts:parsed",
        "enrichment_workers",
        "worker-test",
        60_000,
    )

    assert next_id == "0-0"
    assert messages == []
    client.ensure_consumer_group.assert_awaited_once_with(
        "stream:posts:parsed",
        "enrichment_workers",
    )
