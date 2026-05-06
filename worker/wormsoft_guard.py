"""Shared Redis-backed guardrail for Wormsoft provider traffic."""
from __future__ import annotations

import time

from shared.config import get_settings

QUARANTINE_KEY = "wormsoft:quarantine_until"
LAST_START_KEY = "wormsoft:last_started_at"
FAILURE_BURST_KEY = "wormsoft:failure_burst"

_RESERVE_SLOT_LUA = """
local quarantine_key = KEYS[1]
local last_start_key = KEYS[2]
local now_ts = tonumber(ARGV[1])
local min_interval_ms = tonumber(ARGV[2])

local quarantine_until = tonumber(redis.call('GET', quarantine_key) or '0')
if quarantine_until > now_ts then
  return {'guard_quarantine', quarantine_until, 0}
end

local last_started_at = tonumber(redis.call('GET', last_start_key) or '0')
local next_allowed_at = last_started_at + (min_interval_ms / 1000.0)
if next_allowed_at > now_ts then
  return {'guard_interval', quarantine_until, next_allowed_at - now_ts}
end

redis.call('SET', last_start_key, tostring(now_ts), 'EX', 120)
return {'ok', quarantine_until, 0}
"""


class WormsoftSharedGuard:
    """Coordinates Wormsoft pacing/quarantine across multiple workers."""

    def __init__(self, redis=None, *, service_name: str = "worker", settings=None) -> None:
        self._settings = settings or get_settings()
        self._redis = redis
        self._managed_redis = None
        self._service_name = service_name

    async def close(self) -> None:
        if self._managed_redis is not None:
            await self._managed_redis.aclose()

    async def _client(self):
        if self._redis is not None:
            return self._redis
        if self._managed_redis is None:
            try:
                import redis.asyncio as aioredis

                redis_url = str(getattr(self._settings, "redis_url", "") or "").strip()
                if not redis_url:
                    return None
                self._managed_redis = aioredis.from_url(
                    redis_url,
                    decode_responses=True,
                )
            except Exception:
                return None
        return self._managed_redis

    async def reserve_slot(self) -> tuple[bool, str]:
        if not bool(getattr(self._settings, "wormsoft_shared_guard_enabled", False)):
            return True, "ok"
        try:
            redis = await self._client()
        except Exception:
            return False, "guard_unavailable"
        if redis is None:
            return True, "ok"

        now = time.time()
        interval_ms = max(
            0,
            int(getattr(self._settings, "wormsoft_min_request_interval_ms", 2000) or 2000),
        )
        try:
            if hasattr(redis, "eval"):
                raw_result = await redis.eval(
                    _RESERVE_SLOT_LUA,
                    2,
                    QUARANTINE_KEY,
                    LAST_START_KEY,
                    str(now),
                    str(interval_ms),
                )
                result = list(raw_result or [])
                reason = str(result[0] if len(result) > 0 else "guard_unavailable")
                retry_after = float(result[2] if len(result) > 2 else 0.0)
                if reason == "guard_interval" and retry_after > 0:
                    await time_sleep(min(retry_after, 5.0))
                    return await self.reserve_slot()
                return reason == "ok", reason

            quarantine_until = float(await redis.get(QUARANTINE_KEY) or 0.0)
            if quarantine_until > now:
                return False, "guard_quarantine"
            last_started_at = float(await redis.get(LAST_START_KEY) or 0.0)
            wait_for = (last_started_at + (interval_ms / 1000.0)) - now
            if wait_for > 0:
                await time_sleep(min(wait_for, 5.0))
                return await self.reserve_slot()
            await redis.set(LAST_START_KEY, str(now), ex=120)
        except Exception:
            return False, "guard_unavailable"
        return True, "ok"

    async def _set_quarantine_until(self, until_ts: float) -> None:
        redis = await self._client()
        if redis is None:
            return
        now = time.time()
        current = float(await redis.get(QUARANTINE_KEY) or 0.0)
        effective_until = max(current, float(until_ts or 0.0))
        ttl = max(60, int(effective_until - now))
        await redis.set(QUARANTINE_KEY, str(effective_until), ex=ttl)

    async def record_success(self) -> None:
        try:
            redis = await self._client()
            if redis is None:
                return
            await redis.delete(FAILURE_BURST_KEY)
        except Exception:
            return

    async def record_failure(
        self,
        *,
        status_code: int | None,
        reason: str = "",
        retryable: bool = False,
    ) -> None:
        try:
            now = time.time()
            if status_code == 429:
                await self._set_quarantine_until(
                    now
                    + float(
                        getattr(self._settings, "wormsoft_quarantine_rate_limit_sec", 120)
                        or 120
                    )
                )
                return
            if status_code is not None and 500 <= status_code <= 599 and not retryable:
                await self._set_quarantine_until(
                    now
                    + float(
                        getattr(
                            self._settings,
                            "wormsoft_quarantine_upstream_error_sec",
                            180,
                        )
                        or 180
                    )
                )
                return
            if reason not in {"connection", "timeout", "upstream_5xx"}:
                return
            redis = await self._client()
            if redis is None:
                return
            burst = int(await redis.incr(FAILURE_BURST_KEY) or 0)
            await redis.expire(
                FAILURE_BURST_KEY,
                int(getattr(self._settings, "wormsoft_failure_burst_window_sec", 300) or 300),
            )
            if burst >= int(getattr(self._settings, "wormsoft_failure_burst_threshold", 3) or 3):
                await self._set_quarantine_until(
                    now
                    + float(
                        getattr(
                            self._settings,
                            "wormsoft_quarantine_upstream_error_sec",
                            180,
                        )
                        or 180
                    )
                )
        except Exception:
            return

    async def get_state(self) -> dict[str, float | bool]:
        try:
            redis = await self._client()
            if redis is None:
                return {"quarantined": False, "quarantine_until": 0.0, "last_started_at": 0.0}
            now = time.time()
            quarantine_until = float(await redis.get(QUARANTINE_KEY) or 0.0)
            last_started_at = float(await redis.get(LAST_START_KEY) or 0.0)
            return {
                "quarantined": quarantine_until > now,
                "quarantine_until": quarantine_until,
                "last_started_at": last_started_at,
            }
        except Exception:
            return {"quarantined": False, "quarantine_until": 0.0, "last_started_at": 0.0}


async def time_sleep(delay: float) -> None:
    import asyncio

    await asyncio.sleep(max(0.0, float(delay or 0.0)))
