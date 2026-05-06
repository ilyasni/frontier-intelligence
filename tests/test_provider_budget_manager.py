from types import SimpleNamespace

import pytest

from worker.provider_budget_manager import ProviderBudgetManager


class _FakeRedis:
    def __init__(self):
        self.data = {}

    async def hgetall(self, key):
        return dict(self.data.get(key, {}))

    async def hincrbyfloat(self, key, field, amount):
        bucket = self.data.setdefault(key, {})
        bucket[field] = float(bucket.get(field, 0.0) or 0.0) + float(amount)
        return bucket[field]

    async def hincrby(self, key, field, amount):
        bucket = self.data.setdefault(key, {})
        bucket[field] = int(bucket.get(field, 0) or 0) + int(amount)
        return bucket[field]

    async def hset(self, key, mapping=None, *args):
        bucket = self.data.setdefault(key, {})
        if mapping:
            bucket.update(mapping)
        elif len(args) == 2:
            bucket[args[0]] = args[1]
        return True

    async def expire(self, key, ttl):
        return True


def _settings():
    return SimpleNamespace(
        redis_url="redis://redis:6379",
        openrouter_free_rpd_soft_cap=850,
        llm_runtime_provider_openrouter_daily_request_soft_cap=0,
        llm_runtime_provider_openrouter_daily_request_limit=0,
        llm_runtime_shadow_daily_request_soft_cap=250,
        llm_runtime_shadow_daily_request_limit=0,
        llm_runtime_embeddings_daily_request_soft_cap=0,
        llm_runtime_embeddings_daily_request_limit=0,
        llm_runtime_provider_wormsoft_daily_request_soft_cap=0,
        llm_runtime_provider_wormsoft_daily_request_limit=0,
        llm_runtime_provider_polza_daily_request_soft_cap=0,
        llm_runtime_provider_polza_daily_request_limit=0,
        llm_runtime_provider_gigachat_daily_request_soft_cap=0,
        llm_runtime_provider_gigachat_daily_request_limit=0,
    )


@pytest.mark.asyncio
async def test_provider_budget_manager_tracks_runtime_commit_snapshot() -> None:
    manager = ProviderBudgetManager(redis=_FakeRedis(), settings=_settings())

    reservation = await manager.reserve(
        provider="wormsoft",
        model="wormsoft/agent/medium",
        task_family="text_generation",
        requested_units=1.0,
    )
    await manager.commit(
        reservation,
        actual_units=379.0,
        prompt_tokens=306,
        completion_tokens=73,
        billable_tokens=379,
    )

    snapshot = await manager.snapshot(
        ["wormsoft"],
        models_by_provider={"wormsoft": ["wormsoft/agent/medium"]},
        task_families=["text_generation"],
        execution_roles=["primary"],
    )

    provider_window = next(item for item in snapshot if item.scope == "runtime_usage")
    model_window = next(item for item in snapshot if item.scope == "runtime_model")
    family_window = next(item for item in snapshot if item.scope == "runtime_task_family")
    role_window = next(item for item in snapshot if item.scope == "runtime_execution_role")

    assert provider_window.provider == "wormsoft"
    assert provider_window.committed == 379.0
    assert provider_window.outstanding == 0.0
    assert model_window.model == "wormsoft/agent/medium"
    assert model_window.committed == 379.0
    assert family_window.task_family == "text_generation"
    assert family_window.committed == 379.0
    assert role_window.execution_role == "primary"
    assert role_window.committed == 379.0


@pytest.mark.asyncio
async def test_provider_budget_manager_blocks_soft_capped_openrouter_usage() -> None:
    settings = _settings()
    settings.llm_runtime_provider_openrouter_daily_request_soft_cap = 1
    manager = ProviderBudgetManager(redis=_FakeRedis(), settings=settings)

    reservation = await manager.reserve(
        provider="openrouter",
        model="openrouter/free",
        task_family="text_generation",
        requested_units=1.0,
    )
    await manager.commit(reservation, actual_units=1.0)

    allowed, reason, snapshots = await manager.allow_reservation(
        provider="openrouter",
        model="openrouter/free",
        task_family="text_generation",
    )

    assert allowed is False
    assert reason == "runtime_soft_cap:runtime_usage"
    provider_window = next(item for item in snapshots if item.scope == "runtime_usage")
    assert provider_window.status == "soft_limited"
