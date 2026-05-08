"""Redis-backed provider/model circuit breaker for multi-provider routing."""
from __future__ import annotations

import json
import time
from typing import Any

from shared.config import get_settings
from shared.llm_control_plane import (
    CIRCUIT_LEVEL_MODEL,
    CIRCUIT_LEVEL_PROVIDER,
    CIRCUIT_STATE_CLOSED,
    CIRCUIT_STATE_OPEN,
    CircuitState,
    ERROR_CONNECTION,
    ERROR_PROVIDER_UNAVAILABLE,
    ERROR_QUOTA_EXHAUSTED,
    ERROR_RATE_LIMITED,
    ERROR_TIMEOUT,
    ERROR_THROTTLED_LOCAL,
    ERROR_UPSTREAM_5XX,
    ProviderError,
)
from shared.llm_routing import normalize_provider


def _safe_model_key(model: str) -> str:
    return str(model or "").strip().replace(":", "__").replace("/", "__")


class ProviderCircuitBreaker:
    """Coordinates provider/model circuit state across workers via Redis."""

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

    def _provider_key(self, provider: str) -> str:
        return f"llm:circuit:provider:{normalize_provider(provider)}"

    def _model_key(self, provider: str, model: str) -> str:
        return f"llm:circuit:model:{normalize_provider(provider)}:{_safe_model_key(model)}"

    def _burst_key(self, provider: str, model: str, level: str) -> str:
        normalized_provider = normalize_provider(provider)
        suffix = _safe_model_key(model) if level == CIRCUIT_LEVEL_MODEL else "provider"
        return f"llm:circuit:burst:{level}:{normalized_provider}:{suffix}"

    async def _read_state(self, key: str) -> CircuitState | None:
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
        if not isinstance(payload, dict):
            return None
        return CircuitState.model_validate(payload)

    async def _write_state(self, key: str, state: CircuitState) -> None:
        redis = await self._client()
        if redis is None:
            return
        ttl = max(60, int((state.opened_until or time.time()) - time.time()))
        try:
            await redis.set(key, state.model_dump_json(), ex=ttl)
        except Exception:
            return

    async def _clear_state(self, key: str) -> None:
        redis = await self._client()
        if redis is None:
            return
        try:
            await redis.delete(key)
        except Exception:
            return

    async def reserve(self, provider: str, model: str = "") -> tuple[bool, str]:
        normalized_provider = normalize_provider(provider)
        provider_state = await self._read_state(self._provider_key(normalized_provider))
        now = time.time()
        if (
            provider_state is not None
            and provider_state.state == CIRCUIT_STATE_OPEN
            and float(provider_state.opened_until or 0.0) > now
        ):
            return False, provider_state.reason or "provider_circuit_open"
        if model:
            model_state = await self._read_state(self._model_key(normalized_provider, model))
            if (
                model_state is not None
                and model_state.state == CIRCUIT_STATE_OPEN
                and float(model_state.opened_until or 0.0) > now
            ):
                return False, model_state.reason or "model_circuit_open"
        return True, "ok"

    async def record_success(self, provider: str, model: str = "") -> None:
        await self._clear_state(self._provider_key(provider))
        if model:
            await self._clear_state(self._model_key(provider, model))

    async def _burst_count(self, provider: str, model: str, level: str) -> int:
        redis = await self._client()
        if redis is None:
            return 0
        key = self._burst_key(provider, model, level)
        try:
            count = int(await redis.incr(key) or 0)
            await redis.expire(key, int(self._settings.llm_circuit_failure_window_sec or 300))
        except Exception:
            return 0
        return count

    def _cooldown_for_error(self, error: ProviderError, level: str) -> float:
        if error.category == ERROR_THROTTLED_LOCAL:
            return 0.0
        if error.category in {ERROR_RATE_LIMITED, ERROR_QUOTA_EXHAUSTED}:
            return float(self._settings.llm_circuit_rate_limit_quarantine_sec or 120)
        if error.category in {ERROR_PROVIDER_UNAVAILABLE, ERROR_UPSTREAM_5XX}:
            return float(self._settings.llm_circuit_provider_quarantine_sec or 300)
        if error.category in {ERROR_CONNECTION, ERROR_TIMEOUT}:
            if level == CIRCUIT_LEVEL_PROVIDER:
                return float(self._settings.llm_circuit_provider_quarantine_sec or 300)
            return float(self._settings.llm_circuit_model_quarantine_sec or 180)
        return 0.0

    async def _open_circuit(
        self,
        *,
        provider: str,
        model: str,
        level: str,
        error: ProviderError,
        failure_count: int,
    ) -> None:
        cooldown = self._cooldown_for_error(error, level)
        if cooldown <= 0:
            return
        opened_at = time.time()
        state = CircuitState(
            level=level,
            provider=provider,
            model=model if level == CIRCUIT_LEVEL_MODEL else "",
            state=CIRCUIT_STATE_OPEN,
            opened_at=opened_at,
            opened_until=opened_at + cooldown,
            reason=error.reason or error.category,
            failure_count=failure_count,
            last_error_category=error.category,
        )
        key = (
            self._provider_key(provider)
            if level == CIRCUIT_LEVEL_PROVIDER
            else self._model_key(provider, model)
        )
        await self._write_state(key, state)

    async def record_failure(self, provider: str, model: str, error: ProviderError) -> None:
        immediate_model = error.category in {ERROR_RATE_LIMITED, ERROR_QUOTA_EXHAUSTED}
        immediate_provider = error.category in {
            ERROR_PROVIDER_UNAVAILABLE,
            ERROR_UPSTREAM_5XX,
        } and not error.retryable

        if immediate_provider:
            await self._open_circuit(
                provider=provider,
                model="",
                level=CIRCUIT_LEVEL_PROVIDER,
                error=error,
                failure_count=1,
            )
            return
        if immediate_model:
            await self._open_circuit(
                provider=provider,
                model=model,
                level=CIRCUIT_LEVEL_MODEL,
                error=error,
                failure_count=1,
            )
            return
        if error.category not in {ERROR_CONNECTION, ERROR_TIMEOUT, ERROR_UPSTREAM_5XX}:
            return

        model_failures = await self._burst_count(provider, model, CIRCUIT_LEVEL_MODEL)
        provider_failures = await self._burst_count(provider, "", CIRCUIT_LEVEL_PROVIDER)
        threshold = max(1, int(self._settings.llm_circuit_failure_threshold or 3))
        if model_failures >= threshold:
            await self._open_circuit(
                provider=provider,
                model=model,
                level=CIRCUIT_LEVEL_MODEL,
                error=error,
                failure_count=model_failures,
            )
        if provider_failures >= threshold:
            await self._open_circuit(
                provider=provider,
                model="",
                level=CIRCUIT_LEVEL_PROVIDER,
                error=error,
                failure_count=provider_failures,
            )

    async def snapshot(
        self,
        *,
        providers: list[str],
        models_by_provider: dict[str, list[str]] | None = None,
    ) -> list[CircuitState]:
        states: list[CircuitState] = []
        models_by_provider = models_by_provider or {}
        for provider in providers:
            provider_state = await self._read_state(self._provider_key(provider))
            if provider_state is not None:
                states.append(provider_state)
            for model in models_by_provider.get(normalize_provider(provider), []):
                model_state = await self._read_state(self._model_key(provider, model))
                if model_state is not None:
                    states.append(model_state)
        return states
