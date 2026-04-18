import json
from typing import Optional
import redis.asyncio as aioredis
from .config import get_settings

_pool: Optional[aioredis.ConnectionPool] = None


def get_pool() -> aioredis.ConnectionPool:
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = aioredis.ConnectionPool.from_url(
            settings.redis_url,
            max_connections=20,
            decode_responses=True,
        )
    return _pool


def get_client() -> aioredis.Redis:
    return aioredis.Redis(connection_pool=get_pool())


async def xadd(stream: str, data: dict, maxlen: int = 100_000) -> str:
    """Add message to Redis Stream."""
    client = get_client()
    payload = {k: json.dumps(v) if not isinstance(v, str) else v for k, v in data.items()}
    return await client.xadd(stream, payload, maxlen=maxlen, approximate=True)


async def xreadgroup(
    group: str,
    consumer: str,
    stream: str,
    count: int = 1,
    block: int = 2000,
) -> list:
    """Read messages from Redis Stream consumer group."""
    client = get_client()
    try:
        results = await client.xreadgroup(
            group, consumer, {stream: ">"}, count=count, block=block
        )
        return results or []
    except aioredis.ResponseError as e:
        if "NOGROUP" in str(e):
            await client.xgroup_create(stream, group, id="0", mkstream=True)
            return []
        raise


async def xack(stream: str, group: str, message_id: str) -> int:
    """Acknowledge a message in a consumer group."""
    client = get_client()
    return await client.xack(stream, group, message_id)


async def ensure_consumer_group(stream: str, group: str) -> None:
    """Create consumer group if it doesn't exist."""
    client = get_client()
    try:
        await client.xgroup_create(stream, group, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


class RedisClient:
    """Object-oriented wrapper for async Redis with Streams helpers."""

    def __init__(self, url: str):
        self.url = url
        self.redis = None

    async def connect(self):
        import redis.asyncio as aioredis
        pool = aioredis.ConnectionPool.from_url(
            self.url, max_connections=20, decode_responses=True
        )
        self.redis = aioredis.Redis(connection_pool=pool)

    async def disconnect(self):
        if self.redis:
            await self.redis.aclose()

    async def xadd(self, stream: str, data: dict, maxlen: int = 100_000) -> str:
        import json
        payload = {k: json.dumps(v) if not isinstance(v, (str, int, float)) else str(v)
                   for k, v in data.items()}
        return await self.redis.xadd(stream, payload, maxlen=maxlen, approximate=True)

    async def xreadgroup(
        self, stream: str, group: str, consumer: str,
        count: int = 1, block_ms: int = 2000
    ) -> list:
        import redis.asyncio as aioredis
        try:
            results = await self.redis.xreadgroup(
                group, consumer, {stream: ">"}, count=count, block=block_ms
            )
            if not results:
                return []
            msgs = []
            for _stream, messages in results:
                for msg_id, data in messages:
                    msgs.append((msg_id, data))
            return msgs
        except aioredis.ResponseError as e:
            if "NOGROUP" in str(e):
                await self.ensure_consumer_group(stream, group)
            return []

    async def xreadgroup_pending(
        self, stream: str, group: str, consumer: str, count: int = 100
    ) -> list:
        import redis.asyncio as aioredis
        try:
            results = await self.redis.xreadgroup(
                group, consumer, {stream: "0"}, count=count
            )
            if not results:
                return []
            msgs = []
            for _stream, messages in results:
                for msg_id, data in messages:
                    if msg_id:
                        msgs.append((msg_id, data))
            return msgs
        except aioredis.ResponseError:
            return []

    async def xack(self, stream: str, group: str, *msg_ids: str) -> int:
        return await self.redis.xack(stream, group, *msg_ids)

    async def xautoclaim(
        self, stream: str, group: str, consumer: str,
        min_idle_ms: int, start_id: str = "0-0", count: int = 10
    ) -> tuple[str, list]:
        """Claim messages idle > min_idle_ms from any consumer. Returns (next_id, messages)."""
        result = await self.redis.xautoclaim(
            stream, group, consumer, min_idle_ms,
            start_id=start_id, count=count,
        )
        # result is (next_start_id, [(msg_id, data), ...], [deleted_ids])
        next_id = result[0] if result else "0-0"
        messages = []
        if result and len(result) > 1:
            for msg_id, data in (result[1] or []):
                if msg_id:
                    messages.append((msg_id, data))
        return next_id, messages

    async def xpending_summary(self, stream: str, group: str) -> dict:
        """Return XPENDING summary: {pending, min_id, max_id, consumers: {name: count}}."""
        result = await self.redis.xpending(stream, group)
        consumers = {}
        if result and result.get("consumers"):
            consumers = {c["name"]: c["pending"] for c in result["consumers"]}
        return {
            "pending": result.get("pending", 0) if result else 0,
            "min_id": result.get("min", "") if result else "",
            "max_id": result.get("max", "") if result else "",
            "consumers": consumers,
        }

    async def xinfo_consumers(self, stream: str, group: str) -> list[dict]:
        """Return list of consumer info dicts from XINFO CONSUMERS."""
        return await self.redis.xinfo_consumers(stream, group)

    async def xdel_consumer(self, stream: str, group: str, consumer: str) -> int:
        """Delete a consumer from a consumer group (XGROUP DELCONSUMER)."""
        return await self.redis.xgroup_delconsumer(stream, group, consumer)

    async def ensure_consumer_group(self, stream: str, group: str):
        import redis.asyncio as aioredis
        try:
            await self.redis.xgroup_create(stream, group, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
