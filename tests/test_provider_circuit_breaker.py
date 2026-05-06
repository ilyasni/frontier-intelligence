from types import SimpleNamespace

import pytest

from shared.llm_control_plane import ERROR_CONNECTION, ProviderError
from worker.provider_circuit_breaker import ProviderCircuitBreaker


class _FakeRedis:
    def __init__(self):
        self.data = {}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, value, ex=None):
        self.data[key] = value

    async def delete(self, key):
        self.data.pop(key, None)

    async def incr(self, key):
        self.data[key] = int(self.data.get(key, 0) or 0) + 1
        return self.data[key]

    async def expire(self, key, ttl):
        return True


def _settings():
    return SimpleNamespace(
        redis_url="redis://redis:6379",
        llm_circuit_failure_window_sec=300,
        llm_circuit_failure_threshold=3,
        llm_circuit_provider_quarantine_sec=300,
        llm_circuit_model_quarantine_sec=180,
        llm_circuit_rate_limit_quarantine_sec=120,
    )


@pytest.mark.asyncio
async def test_provider_circuit_breaker_opens_model_circuit_after_burst() -> None:
    breaker = ProviderCircuitBreaker(redis=_FakeRedis(), settings=_settings())
    error = ProviderError(
        provider="wormsoft",
        category=ERROR_CONNECTION,
        message="connection failed",
        retryable=True,
        reason="wormsoft_connection",
    )

    await breaker.record_failure("wormsoft", "wormsoft/agent/medium", error)
    await breaker.record_failure("wormsoft", "wormsoft/agent/medium", error)
    await breaker.record_failure("wormsoft", "wormsoft/agent/medium", error)

    allowed, reason = await breaker.reserve("wormsoft", "wormsoft/agent/medium")

    assert allowed is False
    assert reason == "wormsoft_connection"
