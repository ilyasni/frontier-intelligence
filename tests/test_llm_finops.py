import json
from types import SimpleNamespace

import pytest

from admin.backend.services.llm_finops import fetch_llm_finops_snapshot
from shared.llm_control_plane import ExecutionReceipt
from worker.provider_budget_manager import ProviderBudgetManager


class _FakeRedis:
    def __init__(self):
        self.data = {}
        self.values = {}
        self.zsets = {}

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

    @staticmethod
    def _score(value):
        if value == "-inf":
            return float("-inf"), False
        if value == "+inf":
            return float("inf"), False
        raw = str(value)
        exclusive = raw.startswith("(")
        return float(raw[1:] if exclusive else raw), exclusive

    async def zadd(self, key, mapping):
        bucket = self.zsets.setdefault(key, {})
        bucket.update({str(member): float(score) for member, score in mapping.items()})
        return len(mapping)

    async def zremrangebyscore(self, key, minimum, maximum):
        bucket = self.zsets.setdefault(key, {})
        lower, lower_exclusive = self._score(minimum)
        upper, upper_exclusive = self._score(maximum)
        removed = []
        for member, score in bucket.items():
            lower_match = score > lower if lower_exclusive else score >= lower
            upper_match = score < upper if upper_exclusive else score <= upper
            if lower_match and upper_match:
                removed.append(member)
        for member in removed:
            del bucket[member]
        return len(removed)

    async def zrangebyscore(self, key, minimum, maximum):
        bucket = self.zsets.setdefault(key, {})
        lower, lower_exclusive = self._score(minimum)
        upper, upper_exclusive = self._score(maximum)
        rows = []
        for member, score in bucket.items():
            lower_match = score > lower if lower_exclusive else score >= lower
            upper_match = score < upper if upper_exclusive else score <= upper
            if lower_match and upper_match:
                rows.append((score, member))
        return [member for _, member in sorted(rows)]

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
    settings = SimpleNamespace(
        wormsoft_credit_window_limit=3_000_000.0,
        wormsoft_credit_window_seconds=14_400,
    )
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
    monkeypatch.setattr("admin.backend.services.llm_finops.get_settings", lambda: settings)

    payload = await fetch_llm_finops_snapshot()

    openrouter = next(
        item for item in payload["reconciliations"] if item["provider"] == "openrouter"
    )
    assert openrouter["gap_kind"] == "openrouter_free_daily_requests"
    assert openrouter["gap_value"] == -19.0
    assert openrouter["published_limit"] == 50.0
    assert openrouter["published_usage"] == 20.0
    assert openrouter["published_remaining"] == 30.0


@pytest.mark.asyncio
async def test_fetch_llm_finops_uses_configured_wormsoft_plan_and_local_window(
    monkeypatch,
) -> None:
    redis = _FakeRedis()
    settings = SimpleNamespace(
        wormsoft_credit_window_limit=3_000_000.0,
        wormsoft_credit_window_seconds=14_400,
    )
    manager = ProviderBudgetManager(redis=redis, settings=settings)
    await manager.add_credit_usage(
        provider="wormsoft",
        credits=600_000.0,
        window_seconds=14_400,
    )
    await redis.set(
        "admin:wormsoft_limits:last_ok",
        json.dumps(
            {
                "status": "ok",
                "plans": [
                    {"id": "Simple", "amount": 500_000, "seconds": 18_000},
                    {"id": "Payed", "amount": 3_000_000, "seconds": 14_400},
                ],
                "fetched_at": 123.0,
            }
        ),
    )
    monkeypatch.setattr("admin.backend.services.llm_finops.get_client", lambda: redis)
    monkeypatch.setattr("admin.backend.services.llm_finops.get_settings", lambda: settings)

    payload = await fetch_llm_finops_snapshot()

    wormsoft = next(
        item for item in payload["reconciliations"] if item["provider"] == "wormsoft"
    )
    assert wormsoft["published_limit"] == 3_000_000.0
    assert wormsoft["published_usage"] == pytest.approx(600_000.0)
    assert wormsoft["published_remaining"] == pytest.approx(2_400_000.0)
    assert wormsoft["metadata"]["matched_active_plan"]["id"] == "Payed"
