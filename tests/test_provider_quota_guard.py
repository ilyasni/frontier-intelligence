import json
from types import SimpleNamespace

import pytest

from worker.provider_quota_guard import ProviderPublishedQuotaGuard


class _FakeRedis:
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)


def _settings():
    return SimpleNamespace(
        redis_url="redis://redis:6379",
        gigachat_balance_alert_threshold=100_000,
    )


@pytest.mark.asyncio
async def test_provider_quota_guard_blocks_openrouter_free_daily_limit() -> None:
    redis = _FakeRedis()
    redis.values["admin:openrouter_key:last_ok"] = json.dumps(
        {
            "available": True,
            "usage_daily": 50,
            "free_model_daily_limit": 50,
            "limit_remaining": 0.0,
        }
    )
    guard = ProviderPublishedQuotaGuard(redis=redis, settings=_settings())

    allowed, reason = await guard.allow(
        provider="openrouter",
        model="qwen/qwen2.5-7b-instruct:free",
    )

    assert allowed is False
    assert reason == "published_hard_cap:openrouter_free_daily"


@pytest.mark.asyncio
async def test_provider_quota_guard_blocks_shadow_gigachat_on_low_balance() -> None:
    redis = _FakeRedis()
    redis.values["admin:gigachat_balance:last_ok"] = json.dumps(
        {
            "balance": [
                {"usage": "GigaChat-2", "value": 50_000},
            ]
        }
    )
    guard = ProviderPublishedQuotaGuard(redis=redis, settings=_settings())

    allowed, reason = await guard.allow(
        provider="gigachat",
        model="GigaChat-2",
        execution_role="shadow",
    )

    assert allowed is False
    assert reason == "published_soft_cap:gigachat_low_balance"
