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
    def _runtime_budget_key(
        provider: str,
        *,
        day: str | None = None,
        scope: str = "runtime_usage",
        model: str = "",
        task_family: str = "",
        execution_role: str = "",
    ) -> str:
        normalized_provider = normalize_provider(provider)
        suffix = ["llm:budget:runtime", normalized_provider, scope]
        if model:
            suffix.append(str(model).replace(":", "__").replace("/", "__"))
        if task_family:
            suffix.append(str(task_family))
        if execution_role:
            suffix.append(str(execution_role))
        suffix.append(day or _day_key())
        return ":".join(suffix)

    @staticmethod
    def _reservation_scopes(reservation: dict[str, Any]) -> list[dict[str, str]]:
        provider = normalize_provider(reservation.get("provider"))
        model = str(reservation.get("model") or "").strip()
        task_family = str(reservation.get("task_family") or "").strip()
        execution_role = str(reservation.get("execution_role") or "primary").strip()
        scopes = [
            {
                "scope": "runtime_usage",
                "provider": provider,
                "model": "",
                "task_family": "",
                "execution_role": "",
            },
        ]
        if model:
            scopes.append(
                {
                    "scope": "runtime_model",
                    "provider": provider,
                    "model": model,
                    "task_family": "",
                    "execution_role": "",
                }
            )
        if task_family:
            scopes.append(
                {
                    "scope": "runtime_task_family",
                    "provider": provider,
                    "model": "",
                    "task_family": task_family,
                    "execution_role": "",
                }
            )
        if execution_role:
            scopes.append(
                {
                    "scope": "runtime_execution_role",
                    "provider": provider,
                    "model": "",
                    "task_family": "",
                    "execution_role": execution_role,
                }
            )
        return scopes

    async def reserve(
        self,
        *,
        provider: str,
        model: str,
        task_family: str,
        requested_units: float = 1.0,
        unit: str = "requests",
        execution_role: str = "primary",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        reservation = {
            "reservation_id": uuid.uuid4().hex,
            "provider": normalize_provider(provider),
            "model": str(model or "").strip(),
            "task_family": str(task_family or "").strip(),
            "requested_units": float(requested_units or 0.0),
            "unit": str(unit or "requests"),
            "execution_role": str(execution_role or "primary"),
            "reserved_at": time.time(),
            "day": _day_key(),
        }
        if metadata:
            reservation.update(dict(metadata))
        redis = await self._client()
        if redis is None:
            return reservation
        ttl = 3 * 24 * 3600
        try:
            for scope in self._reservation_scopes(reservation):
                key = self._runtime_budget_key(
                    scope["provider"],
                    day=reservation["day"],
                    scope=scope["scope"],
                    model=scope["model"],
                    task_family=scope["task_family"],
                    execution_role=scope["execution_role"],
                )
                await redis.hincrbyfloat(key, "reserved_units", reservation["requested_units"])
                await redis.hincrby(key, "reserved_requests", 1)
                await redis.hincrby(key, "active_requests", 1)
                await redis.hset(
                    key,
                    mapping={
                        "provider": reservation["provider"],
                        "unit": reservation["unit"],
                        "scope": scope["scope"],
                        "model": scope["model"],
                        "task_family": scope["task_family"],
                        "execution_role": scope["execution_role"],
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
        payload["prompt_tokens"] = int(prompt_tokens or 0)
        payload["completion_tokens"] = int(completion_tokens or 0)
        payload["billable_tokens"] = int(billable_tokens or 0)
        payload["committed_at"] = time.time()
        redis = await self._client()
        if redis is None:
            return payload
        ttl = 3 * 24 * 3600
        try:
            for scope in self._reservation_scopes(payload):
                key = self._runtime_budget_key(
                    scope["provider"],
                    day=payload.get("day"),
                    scope=scope["scope"],
                    model=scope["model"],
                    task_family=scope["task_family"],
                    execution_role=scope["execution_role"],
                )
                await redis.hincrbyfloat(key, "committed_units", payload["actual_units"])
                await redis.hincrby(key, "committed_requests", 1)
                await redis.hincrby(key, "active_requests", -1)
                await redis.hincrby(key, "prompt_tokens", payload["prompt_tokens"])
                await redis.hincrby(key, "completion_tokens", payload["completion_tokens"])
                await redis.hincrby(key, "billable_tokens", payload["billable_tokens"])
                await redis.hset(key, mapping={"updated_at": str(time.time())})
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
        ttl = 3 * 24 * 3600
        try:
            for scope in self._reservation_scopes(payload):
                key = self._runtime_budget_key(
                    scope["provider"],
                    day=payload.get("day"),
                    scope=scope["scope"],
                    model=scope["model"],
                    task_family=scope["task_family"],
                    execution_role=scope["execution_role"],
                )
                await redis.hincrby(key, "released_requests", 1)
                await redis.hincrby(key, "active_requests", -1)
                await redis.hset(key, mapping={"updated_at": str(time.time())})
                await redis.expire(key, ttl)
        except Exception:
            return payload
        return payload

    async def snapshot(
        self,
        providers: list[str],
        *,
        models_by_provider: dict[str, list[str]] | None = None,
        task_families: list[str] | None = None,
        execution_roles: list[str] | None = None,
    ) -> list[BudgetWindowState]:
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
        models_by_provider = models_by_provider or {}
        normalized_roles = sorted({str(role or "").strip() for role in (execution_roles or []) if str(role or "").strip()})
        normalized_families = sorted({str(family or "").strip() for family in (task_families or []) if str(family or "").strip()})
        for provider in sorted({normalize_provider(provider) for provider in providers}):
            scope_descriptors = [
                {"scope": "runtime_usage", "model": "", "task_family": "", "execution_role": ""}
            ]
            for model in sorted({str(item or "").strip() for item in models_by_provider.get(provider, []) if str(item or "").strip()}):
                scope_descriptors.append(
                    {
                        "scope": "runtime_model",
                        "model": model,
                        "task_family": "",
                        "execution_role": "",
                    }
                )
            for task_family in normalized_families:
                scope_descriptors.append(
                    {
                        "scope": "runtime_task_family",
                        "model": "",
                        "task_family": task_family,
                        "execution_role": "",
                    }
                )
            for execution_role in normalized_roles:
                scope_descriptors.append(
                    {
                        "scope": "runtime_execution_role",
                        "model": "",
                        "task_family": "",
                        "execution_role": execution_role,
                    }
                )

            for descriptor in scope_descriptors:
                key = self._runtime_budget_key(
                    provider,
                    scope=descriptor["scope"],
                    model=descriptor["model"],
                    task_family=descriptor["task_family"],
                    execution_role=descriptor["execution_role"],
                )
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
                        scope=descriptor["scope"],
                        window_label=_day_key(),
                        model=str(raw.get("model") or descriptor["model"] or ""),
                        task_family=str(raw.get("task_family") or descriptor["task_family"] or ""),
                        execution_role=str(raw.get("execution_role") or descriptor["execution_role"] or ""),
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
