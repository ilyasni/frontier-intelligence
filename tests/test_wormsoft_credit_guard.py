from types import SimpleNamespace

import pytest

from shared.llm_control_plane import EXECUTION_ROLE_PRIMARY, EXECUTION_ROLE_SHADOW, ExecutionReceipt
from worker.wormsoft_credit_guard import (
    REASON_HARD_CAP,
    REASON_OK,
    REASON_SOFT_CAP,
    WormsoftCreditGuard,
)


class _FakeBudget:
    """Minimal ProviderBudgetManager stand-in for the credit guard."""

    def __init__(self, *, used=0.0, raise_on_usage=False):
        self._used = used
        self._raise_on_usage = raise_on_usage
        self.added: list[dict] = []

    async def credit_window_usage(self, *, provider, window_seconds):
        if self._raise_on_usage:
            raise RuntimeError("redis down")
        return self._used

    async def add_credit_usage(self, *, provider, credits, window_seconds):
        self.added.append(
            {"provider": provider, "credits": credits, "window_seconds": window_seconds}
        )
        return credits


def _settings(**overrides):
    values = dict(
        wormsoft_credit_throttle_enabled=True,
        wormsoft_credit_window_seconds=14400,
        wormsoft_credit_window_limit=3000000.0,
        wormsoft_credit_soft_cap_ratio=0.8,
        wormsoft_credit_hard_cap_ratio=0.98,
        wormsoft_credit_soft_cap_shadow_ratio=0.7,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _guard(budget, settings):
    return WormsoftCreditGuard(budget, settings=settings)


@pytest.mark.asyncio
async def test_allow_under_soft_cap():
    # 0.5 * 3M = 1.5M < soft cap 2.4M
    guard = _guard(_FakeBudget(used=1_500_000.0), _settings())
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is True
    assert reason == REASON_OK


@pytest.mark.asyncio
async def test_allow_blocks_on_soft_cap():
    # soft cap = 0.8 * 3M = 2.4M; usage at 2.5M crosses soft but not hard (2.94M)
    guard = _guard(_FakeBudget(used=2_500_000.0), _settings())
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is False
    assert reason == REASON_SOFT_CAP


@pytest.mark.asyncio
async def test_allow_blocks_on_hard_cap():
    # hard cap = 0.98 * 3M = 2.94M; usage at 2.95M crosses hard
    guard = _guard(_FakeBudget(used=2_950_000.0), _settings())
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is False
    assert reason == REASON_HARD_CAP


@pytest.mark.asyncio
async def test_shadow_role_uses_tighter_ratio():
    # shadow soft ratio 0.7 -> soft cap 2.1M; usage 2.2M blocks shadow but
    # would be allowed for primary (soft cap 2.4M).
    budget = _FakeBudget(used=2_200_000.0)
    settings = _settings()
    primary_allowed, _ = await _guard(budget, settings).allow(
        provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY
    )
    shadow_allowed, shadow_reason = await _guard(budget, settings).allow(
        provider="wormsoft", execution_role=EXECUTION_ROLE_SHADOW
    )
    assert primary_allowed is True
    assert shadow_allowed is False
    assert shadow_reason == REASON_SOFT_CAP


@pytest.mark.asyncio
async def test_disabled_throttle_allows():
    guard = _guard(_FakeBudget(used=9_999_999.0), _settings(wormsoft_credit_throttle_enabled=False))
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is True
    assert reason == REASON_OK


@pytest.mark.asyncio
async def test_non_wormsoft_provider_allowed():
    guard = _guard(_FakeBudget(used=9_999_999.0), _settings())
    allowed, reason = await guard.allow(provider="gigachat", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is True
    assert reason == REASON_OK


@pytest.mark.asyncio
async def test_zero_or_negative_limit_allows():
    guard = _guard(_FakeBudget(used=9_999_999.0), _settings(wormsoft_credit_window_limit=0.0))
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is True
    assert reason == REASON_OK


@pytest.mark.asyncio
async def test_usage_read_failure_fails_open():
    guard = _guard(_FakeBudget(raise_on_usage=True), _settings())
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is True
    assert reason == REASON_OK


def _receipt(*, provider="wormsoft", cost=1000.0):
    return ExecutionReceipt(
        task="relevance",
        task_family="text_generation",
        status="ok",
        execution_role=EXECUTION_ROLE_PRIMARY,
        requested_provider=provider,
        requested_model="wormsoft/agent/medium",
        actual_provider=provider,
        actual_model="gemma4:31b-cloud",
        actual_cost=cost,
    )


@pytest.mark.asyncio
async def test_record_adds_usage_for_wormsoft():
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(_receipt(cost=1234.0))
    assert len(budget.added) == 1
    assert budget.added[0]["credits"] == 1234.0
    assert budget.added[0]["window_seconds"] == 14400


@pytest.mark.asyncio
async def test_record_skips_non_wormsoft():
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(_receipt(provider="gigachat", cost=1234.0))
    assert budget.added == []


@pytest.mark.asyncio
async def test_record_skips_zero_cost():
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(_receipt(cost=0.0))
    assert budget.added == []


@pytest.mark.asyncio
async def test_record_skips_when_disabled():
    budget = _FakeBudget()
    await _guard(budget, _settings(wormsoft_credit_throttle_enabled=False)).record(_receipt(cost=10.0))
    assert budget.added == []
