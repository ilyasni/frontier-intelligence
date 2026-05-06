"""Redis-backed runtime budget accounting for provider executions."""
from __future__ import annotations

import time
import uuid
from typing import Any

from shared.config import get_settings
from shared.llm_control_plane import BudgetWindowState
from shared.llm_routing import normalize_provider


def _day_key() -> str:
    return time.strftime("%Y%m%d", time.gmtime())


class ProviderBudgetManager:
    """Tracks runtime reservations/commits for provider requests."""

    def __init__(self, redis=None, *, settings=None) -> None:
        self._settings = settings or get_settings()
        self._redis = redis
        self._managed_redis = None

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

    @staticmethod
    def _runtime_budget_key(provider: str, day: str | None = None) -> str:
        return f"llm:budget:runtime:{normalize_provider(provider)}:{day or _day_key()}"

    async def reserve(
        self,
        *,
        provider: str,
        model: str,
        task_family: str,
        requested_units: float = 1.0,
        unit: str = "requests",
    ) -> dict[str, Any]:
        reservation = {
            "reservation_id": uuid.uuid4().hex,
            "provider": normalize_provider(provider),
            "model": str(model or "").strip(),
            "task_family": str(task_family or "").strip(),
            "requested_units": float(requested_units or 0.0),
            "unit": str(unit or "requests"),
            "reserved_at": time.time(),
            "day": _day_key(),
        }
        redis = await self._client()
        if redis is None:
            return reservation
        key = self._runtime_budget_key(reservation["provider"], reservation["day"])
        ttl = 3 * 24 * 3600
        try:
            await redis.hincrbyfloat(key, "reserved_units", reservation["requested_units"])
            await redis.hincrby(key, "reserved_requests", 1)
            await redis.hincrby(key, "active_requests", 1)
            await redis.hset(
                key,
                mapping={
                    "provider": reservation["provider"],
                    "unit": reservation["unit"],
                    "updated_at": str(time.time()),
                },
            )
            await redis.expire(key, ttl)
        except Exception:
            return reservation
        return reservation

    async def commit(
        self,
        reservation: dict[str, Any] | None,
        *,
        actual_units: float | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        billable_tokens: int = 0,
    ) -> dict[str, Any]:
        payload = dict(reservation or {})
        provider = normalize_provider(payload.get("provider"))
        if not provider:
            return payload
        payload["actual_units"] = float(
            actual_units
            if actual_units is not None
            else payload.get("requested_units") or 0.0
        )
        payload["committed_at"] = time.time()
        redis = await self._client()
        if redis is None:
            return payload
        key = self._runtime_budget_key(provider, payload.get("day"))
        ttl = 3 * 24 * 3600
        try:
            await redis.hincrbyfloat(key, "committed_units", payload["actual_units"])
            await redis.hincrby(key, "committed_requests", 1)
            await redis.hincrby(key, "active_requests", -1)
            await redis.hincrby(key, "prompt_tokens", int(prompt_tokens or 0))
            await redis.hincrby(key, "completion_tokens", int(completion_tokens or 0))
            await redis.hincrby(key, "billable_tokens", int(billable_tokens or 0))
            await redis.hset(key, "updated_at", str(time.time()))
            await redis.expire(key, ttl)
        except Exception:
            return payload
        return payload

    async def release(self, reservation: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(reservation or {})
        provider = normalize_provider(payload.get("provider"))
        if not provider:
            return payload
        payload["released_at"] = time.time()
        redis = await self._client()
        if redis is None:
            return payload
        key = self._runtime_budget_key(provider, payload.get("day"))
        ttl = 3 * 24 * 3600
        try:
            await redis.hincrby(key, "released_requests", 1)
            await redis.hincrby(key, "active_requests", -1)
            await redis.hset(key, "updated_at", str(time.time()))
            await redis.expire(key, ttl)
        except Exception:
            return payload
        return payload

    async def snapshot(self, providers: list[str]) -> list[BudgetWindowState]:
        redis = await self._client()
        now = time.time()
        if redis is None:
            return [
                BudgetWindowState(
                    provider=provider,
                    scope="runtime_usage",
                    window_label=_day_key(),
                    unit="requests",
                    status="unavailable",
                    refreshed_at=now,
                )
                for provider in providers
            ]

        result: list[BudgetWindowState] = []
        for provider in sorted({normalize_provider(provider) for provider in providers}):
            key = self._runtime_budget_key(provider)
            try:
                raw = await redis.hgetall(key)
            except Exception:
                raw = {}

            def _num(name: str) -> float:
                try:
                    return float(raw.get(name) or 0.0)
                except (TypeError, ValueError):
                    return 0.0

            reserved_units = _num("reserved_units")
            committed_units = _num("committed_units")
            active_requests = _num("active_requests")
            released_requests = _num("released_requests")
            result.append(
                BudgetWindowState(
                    provider=provider,
                    scope="runtime_usage",
                    window_label=_day_key(),
                    limit=None,
                    remaining=None,
                    used=committed_units,
                    reserved=reserved_units,
                    committed=committed_units,
                    released=released_requests,
                    outstanding=max(0.0, active_requests),
                    unit=str(raw.get("unit") or "requests"),
                    status="ok" if raw else "empty",
                    refreshed_at=_num("updated_at") or now,
                )
            )
        return result
