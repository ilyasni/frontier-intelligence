"""Published quota guard backed by cached admin snapshots in Redis."""
from __future__ import annotations

import json
from typing import Any

from shared.config import get_settings
from shared.llm_routing import PROVIDER_GIGACHAT, PROVIDER_OPENROUTER, normalize_provider

_OPENROUTER_KEY_CACHE_KEY = "admin:openrouter_key:last_ok"
_GIGACHAT_BALANCE_CACHE_KEY = "admin:gigachat_balance:last_ok"


class ProviderPublishedQuotaGuard:
    """Reads cached provider quota snapshots and answers lightweight admission checks."""

    def __init__(self, redis=None, *, settings=None) -> None:
        self._redis = redis
        self._managed_redis = None
        self._settings = settings or get_settings()

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
                self._managed_redis = aioredis.from_url(redis_url, decode_responses=True)
            except Exception:
                return None
        return self._managed_redis

    async def _get_json(self, key: str) -> dict[str, Any] | None:
        redis = await self._client()
        if redis is None:
            return None
        try:
            raw = await redis.get(key)
        except Exception:
            return None
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    async def allow(
        self,
        *,
        provider: str,
        model: str,
        execution_role: str = "primary",
    ) -> tuple[bool, str]:
        normalized = normalize_provider(provider)
        if normalized == PROVIDER_OPENROUTER:
            return await self._allow_openrouter(model=model, execution_role=execution_role)
        if normalized == PROVIDER_GIGACHAT:
            return await self._allow_gigachat(execution_role=execution_role)
        return True, "ok"

    async def _allow_openrouter(
        self,
        *,
        model: str,
        execution_role: str,
    ) -> tuple[bool, str]:
        payload = await self._get_json(_OPENROUTER_KEY_CACHE_KEY)
        if not payload:
            return True, "ok"
        normalized_model = str(model or "").strip().lower()
        is_free_model = normalized_model == "openrouter/free" or normalized_model.endswith(":free")
        limit_remaining = float(payload.get("limit_remaining") or 0.0)
        daily_limit = float(payload.get("free_model_daily_limit") or 0.0)
        usage_daily = float(payload.get("usage_daily") or 0.0)
        if is_free_model:
            if daily_limit > 0 and usage_daily >= daily_limit:
                return False, "published_hard_cap:openrouter_free_daily"
            if (
                execution_role == "shadow"
                and daily_limit > 0
                and usage_daily >= daily_limit * 0.9
            ):
                return False, "published_soft_cap:openrouter_free_daily"
            return True, "ok"
        if limit_remaining <= 0.0:
            return False, "published_hard_cap:openrouter_credit"
        return True, "ok"

    async def _allow_gigachat(self, *, execution_role: str) -> tuple[bool, str]:
        payload = await self._get_json(_GIGACHAT_BALANCE_CACHE_KEY)
        if not payload:
            return True, "ok"
        items = payload.get("balance") or []
        if not isinstance(items, list) or not items:
            return True, "ok"
        total = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                total += int(item.get("value") or 0)
            except (TypeError, ValueError):
                continue
        threshold = int(getattr(self._settings, "gigachat_balance_alert_threshold", 0) or 0)
        if total <= 0:
            return False, "published_hard_cap:gigachat_balance"
        if execution_role == "shadow" and threshold > 0 and total <= threshold:
            return False, "published_soft_cap:gigachat_low_balance"
        return True, "ok"
