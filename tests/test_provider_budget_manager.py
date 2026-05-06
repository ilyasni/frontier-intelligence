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
    return SimpleNamespace(redis_url="redis://redis:6379")


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

    snapshot = await manager.snapshot(["wormsoft"])

    assert len(snapshot) == 1
    assert snapshot[0].provider == "wormsoft"
    assert snapshot[0].committed == 379.0
    assert snapshot[0].outstanding == 0.0
