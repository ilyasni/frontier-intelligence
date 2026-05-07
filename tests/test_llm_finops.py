import json

import pytest

from admin.backend.services.llm_finops import fetch_llm_finops_snapshot
from shared.llm_control_plane import ExecutionReceipt
from worker.provider_budget_manager import ProviderBudgetManager


class _FakeRedis:
    def __init__(self):
        self.data = {}
        self.values = {}

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

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value):
        self.values[key] = value
        return True

    async def scan_iter(self, match=None):
        import fnmatch

        for key in list(self.data.keys()):
            if match is None or fnmatch.fnmatch(key, match):
                yield key


@pytest.mark.asyncio
async def test_fetch_llm_finops_snapshot_reconciles_openrouter_gap(monkeypatch) -> None:
    redis = _FakeRedis()
    manager = ProviderBudgetManager(redis=redis)
    await manager.record_execution_receipt(
        ExecutionReceipt(
            task="relevance",
            task_family="text_generation",
            status="ok",
            requested_provider="openrouter",
            requested_model="qwen/qwen2.5-7b-instruct:free",
            actual_provider="openrouter",
            actual_model="qwen/qwen2.5-7b-instruct:free",
            cost_estimate=0.0,
            actual_cost=0.0,
            cost_drift=0.0,
        )
    )
    await redis.set(
        "admin:openrouter_key:last_ok",
        json.dumps(
            {
                "status": "ok",
                "usage_daily": 20,
                "free_model_daily_limit": 50,
                "limit_remaining": 0.0,
                "is_free_tier": True,
                "fetched_at": 123.0,
            }
        ),
    )
    monkeypatch.setattr("admin.backend.services.llm_finops.get_client", lambda: redis)

    payload = await fetch_llm_finops_snapshot()

    openrouter = next(
        item for item in payload["reconciliations"] if item["provider"] == "openrouter"
    )
    assert openrouter["gap_kind"] == "openrouter_free_daily_requests"
    assert openrouter["gap_value"] == -19.0
    assert openrouter["published_limit"] == 50.0
    assert openrouter["published_usage"] == 20.0
    assert openrouter["published_remaining"] == 30.0
