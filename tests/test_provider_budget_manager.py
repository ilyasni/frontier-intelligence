from types import SimpleNamespace

import pytest

from shared.llm_control_plane import ExecutionReceipt
from worker.provider_budget_manager import ProviderBudgetManager


class _FakeRedis:
    def __init__(self):
        self.data = {}
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


class _EvalRedis:
    def __init__(self, *results: str):
        self.results = list(results)
        self.calls = []

    async def eval(self, *args):
        self.calls.append(args)
        return self.results.pop(0)


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


@pytest.mark.asyncio
async def test_provider_budget_manager_tracks_cost_aggregates() -> None:
    manager = ProviderBudgetManager(redis=_FakeRedis(), settings=_settings())
    receipt = ExecutionReceipt(
        task="relevance",
        task_family="text_generation",
        status="ok",
        requested_provider="wormsoft",
        requested_model="wormsoft/agent/medium",
        actual_provider="wormsoft",
        actual_model="wormsoft/agent/medium",
        execution_role="primary",
        cost_estimate=379.0,
        actual_cost=381.5,
        cost_drift=2.5,
        budget_attribution={
            "prompt_tokens": 306,
            "completion_tokens": 73,
            "billable_tokens": 379,
        },
    )

    await manager.record_execution_receipt(receipt)

    snapshot = await manager.snapshot_costs(
        ["wormsoft"],
        models_by_provider={"wormsoft": ["wormsoft/agent/medium"]},
        task_families=["text_generation"],
        execution_roles=["primary"],
    )

    provider_window = next(item for item in snapshot if item.scope == "cost_provider")
    model_window = next(item for item in snapshot if item.scope == "cost_model")
    role_window = next(item for item in snapshot if item.scope == "cost_execution_role")

    assert provider_window.request_count == 1
    assert provider_window.success_count == 1
    assert provider_window.estimated_cost_total == 379.0
    assert provider_window.actual_cost_total == 381.5
    assert provider_window.cost_drift_total == 2.5
    assert model_window.model == "wormsoft/agent/medium"
    assert role_window.execution_role == "primary"


@pytest.mark.asyncio
async def test_provider_budget_manager_tracks_credit_window_usage() -> None:
    manager = ProviderBudgetManager(redis=_FakeRedis(), settings=_settings())

    total = await manager.add_credit_usage(
        provider="wormsoft",
        credits=125.5,
        window_seconds=18000,
    )
    usage = await manager.credit_window_usage(provider="wormsoft", window_seconds=18000)

    assert total == pytest.approx(125.5)
    assert usage == pytest.approx(125.5)


@pytest.mark.asyncio
async def test_credit_window_uses_atomic_redis_scripts_when_available() -> None:
    redis = _EvalRedis("125.5", "125.5")
    manager = ProviderBudgetManager(redis=redis, settings=_settings())

    total = await manager.add_credit_usage(
        provider="wormsoft",
        credits=125.5,
        window_seconds=14400,
    )
    usage = await manager.credit_window_usage(provider="wormsoft", window_seconds=14400)

    assert total == pytest.approx(125.5)
    assert usage == pytest.approx(125.5)
    assert len(redis.calls) == 2
    assert redis.calls[0][1] == 2
    assert redis.calls[0][2].endswith(":wormsoft:14400")
    assert redis.calls[0][3].endswith(":wormsoft:14400:total")
    assert "ZADD" in redis.calls[0][0]
    assert "ZRANGEBYSCORE" in redis.calls[1][0]


@pytest.mark.asyncio
async def test_credit_window_counts_bursts_by_event_timestamp(monkeypatch) -> None:
    now = 1_000.0
    monkeypatch.setattr("worker.provider_budget_manager.time.time", lambda: now)
    manager = ProviderBudgetManager(redis=_FakeRedis(), settings=_settings())

    await manager.add_credit_usage(provider="wormsoft", credits=100.0, window_seconds=60)
    now = 1_059.0
    await manager.add_credit_usage(provider="wormsoft", credits=25.0, window_seconds=60)
    assert await manager.credit_window_usage(
        provider="wormsoft", window_seconds=60
    ) == pytest.approx(125.0)

    now = 1_061.0
    assert await manager.credit_window_usage(
        provider="wormsoft", window_seconds=60
    ) == pytest.approx(25.0)


@pytest.mark.asyncio
async def test_credit_window_includes_event_exactly_at_cutoff(monkeypatch) -> None:
    now = 2_000.0
    monkeypatch.setattr("worker.provider_budget_manager.time.time", lambda: now)
    manager = ProviderBudgetManager(redis=_FakeRedis(), settings=_settings())

    await manager.add_credit_usage(provider="wormsoft", credits=50.0, window_seconds=60)
    now = 2_060.0

    assert await manager.credit_window_usage(
        provider="wormsoft", window_seconds=60
    ) == pytest.approx(50.0)


@pytest.mark.asyncio
async def test_credit_window_read_errors_are_not_silently_zeroed() -> None:
    class _BrokenRedis(_FakeRedis):
        async def zremrangebyscore(self, key, minimum, maximum):
            raise ConnectionError("redis unavailable")

    manager = ProviderBudgetManager(redis=_BrokenRedis(), settings=_settings())

    with pytest.raises(ConnectionError, match="redis unavailable"):
        await manager.credit_window_usage(provider="wormsoft", window_seconds=60)


@pytest.mark.asyncio
async def test_credit_window_rejects_non_finite_credits() -> None:
    manager = ProviderBudgetManager(redis=_FakeRedis(), settings=_settings())

    with pytest.raises(ValueError, match="credits must be finite"):
        await manager.add_credit_usage(
            provider="wormsoft",
            credits=float("nan"),
            window_seconds=60,
        )
