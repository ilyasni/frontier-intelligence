"""Redis-backed guardrails for OpenRouter free usage across tasks."""
from __future__ import annotations

import time

from shared.config import get_settings
from shared.metrics import (
    set_openrouter_vision_quarantine,
    set_openrouter_vision_rpd_used,
)

QUARANTINE_KEY = "or:free:quarantine_until"
RPD_KEY = "or:free:rpd:{date}"
RPM_KEY = "or:free:rpm:{minute}"
FIVE_XX_BURST_KEY = "or:free:5xx_burst"

_RESERVE_SLOT_LUA = """
local quarantine_key = KEYS[1]
local rpm_key = KEYS[2]
local rpd_key = KEYS[3]

local now_ts = tonumber(ARGV[1])
local rpm_limit = tonumber(ARGV[2])
local rpd_limit = tonumber(ARGV[3])
local rpd_ttl = tonumber(ARGV[4])
local rpm_ttl = tonumber(ARGV[5])

local quarantine_until = tonumber(redis.call('GET', quarantine_key) or '0')
if quarantine_until > now_ts then
  return {'guard_quarantine', quarantine_until, tonumber(redis.call('GET', rpd_key) or '0')}
end

local rpm_used = tonumber(redis.call('GET', rpm_key) or '0')
if rpm_used >= rpm_limit then
  return {'guard_rpm_throttle', quarantine_until, tonumber(redis.call('GET', rpd_key) or '0')}
end

local rpd_used = tonumber(redis.call('GET', rpd_key) or '0')
if rpd_used >= rpd_limit then
  return {'guard_rpd_soft_cap', quarantine_until, rpd_used}
end

rpd_used = tonumber(redis.call('INCR', rpd_key) or '0')
if rpd_used == 1 then
  redis.call('EXPIRE', rpd_key, rpd_ttl)
end

local rpm_used_after = tonumber(redis.call('INCR', rpm_key) or '0')
if rpm_used_after == 1 then
  redis.call('EXPIRE', rpm_key, rpm_ttl)
end

return {'ok', quarantine_until, rpd_used}
"""


def _utc_date_key() -> str:
    return time.strftime("%Y%m%d", time.gmtime())


def _utc_minute_key() -> str:
    return time.strftime("%Y%m%d%H%M", time.gmtime())


class OpenRouterFreeGuard:
    """Global request budget and quarantine guard for OpenRouter free traffic."""

    def __init__(
        self,
        redis=None,
        *,
        service_name: str = "worker",
        publish_vision_metrics: bool = False,
    ) -> None:
        self._settings = get_settings()
        self._redis = redis
        self._service_name = service_name
        self._managed_redis = None
        self._publish_vision_metrics = publish_vision_metrics

    async def close(self) -> None:
        if self._managed_redis is not None:
            await self._managed_redis.aclose()

    async def _client(self):
        if self._redis is not None:
            return self._redis
        if self._managed_redis is None:
            import redis.asyncio as aioredis

            self._managed_redis = aioredis.from_url(
                self._settings.redis_url,
                decode_responses=True,
            )
        return self._managed_redis

    async def _get_quarantine_until(self) -> float:
        redis = await self._client()
        raw = await redis.get(QUARANTINE_KEY)
        try:
            return float(raw or 0.0)
        except Exception:
            return 0.0

    async def _set_quarantine_until(self, until_ts: float) -> None:
        redis = await self._client()
        now = time.time()
        try:
            current_raw = await redis.get(QUARANTINE_KEY)
            current_until = float(current_raw or 0.0)
        except Exception:
            current_until = 0.0
        effective_until = max(float(until_ts or 0.0), current_until)
        ttl = max(60, int(effective_until - now))
        await redis.set(QUARANTINE_KEY, str(effective_until), ex=ttl)
        if self._publish_vision_metrics:
            set_openrouter_vision_quarantine(self._service_name, effective_until > now)

    async def reserve_slot(self) -> tuple[bool, str]:
        try:
            redis = await self._client()
        except Exception:
            return False, "guard_unavailable"

        now = time.time()
        quarantine_until = await self._get_quarantine_until()
        if self._publish_vision_metrics:
            set_openrouter_vision_quarantine(self._service_name, quarantine_until > now)
        if quarantine_until > now:
            return False, "guard_quarantine"

        rpm_key = RPM_KEY.format(minute=_utc_minute_key())
        rpd_key = RPD_KEY.format(date=_utc_date_key())
        try:
            if hasattr(redis, "eval"):
                raw_result = await redis.eval(
                    _RESERVE_SLOT_LUA,
                    3,
                    QUARANTINE_KEY,
                    rpm_key,
                    rpd_key,
                    str(now),
                    str(int(self._settings.openrouter_free_rpm_throttle or 18)),
                    str(int(self._settings.openrouter_free_rpd_soft_cap or 850)),
                    "90000",
                    "120",
                )
                result = list(raw_result or [])
                reason = str(result[0] if len(result) > 0 else "guard_unavailable")
                quarantine_until = float(result[1] if len(result) > 1 else 0.0)
                rpd_used = int(result[2] if len(result) > 2 else 0)
                if self._publish_vision_metrics:
                    set_openrouter_vision_quarantine(self._service_name, quarantine_until > now)
                    set_openrouter_vision_rpd_used(self._service_name, rpd_used)
                return reason == "ok", reason

            rpm_used = int(await redis.get(rpm_key) or 0)
            if rpm_used >= int(self._settings.openrouter_free_rpm_throttle or 18):
                return False, "guard_rpm_throttle"

            rpd_used = int(await redis.get(rpd_key) or 0)
            if self._publish_vision_metrics:
                set_openrouter_vision_rpd_used(self._service_name, rpd_used)
            if rpd_used >= int(self._settings.openrouter_free_rpd_soft_cap or 850):
                return False, "guard_rpd_soft_cap"

            updated_rpd = int(await redis.incr(rpd_key) or 0)
            await redis.expire(rpd_key, 90_000)
            await redis.incr(rpm_key)
            await redis.expire(rpm_key, 120)
        except Exception:
            return False, "guard_unavailable"

        if self._publish_vision_metrics:
            set_openrouter_vision_rpd_used(self._service_name, updated_rpd)
        return True, "ok"

    async def record_failure(
        self,
        *,
        status_code: int | None,
        reset_at: float | None = None,
    ) -> None:
        try:
            now = time.time()
            if status_code == 429:
                quarantine_until = max(now + 60.0, float(reset_at or 0.0))
                await self._set_quarantine_until(quarantine_until)
                return
            if status_code == 402:
                await self._set_quarantine_until(
                    now + float(self._settings.openrouter_free_quarantine_5xx_sec or 900)
                )
                return
            if status_code is None or not (500 <= status_code <= 599):
                return

            redis = await self._client()
            burst = int(await redis.incr(FIVE_XX_BURST_KEY) or 0)
            await redis.expire(FIVE_XX_BURST_KEY, 300)
            if burst >= 3:
                await self._set_quarantine_until(
                    now + float(self._settings.openrouter_free_quarantine_5xx_sec or 900)
                )
        except Exception:
            return


class OpenRouterVisionGuard(OpenRouterFreeGuard):
    """Compatibility wrapper for vision routing."""

    def __init__(self, redis=None, *, service_name: str = "worker") -> None:
        super().__init__(
            redis=redis,
            service_name=service_name,
            publish_vision_metrics=True,
        )
