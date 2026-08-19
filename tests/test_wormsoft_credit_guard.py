from types import SimpleNamespace

import pytest

from shared.llm_control_plane import EXECUTION_ROLE_PRIMARY, EXECUTION_ROLE_SHADOW, ExecutionReceipt
from worker.wormsoft_credit_guard import (
    REASON_HARD_CAP,
    REASON_OK,
    REASON_PRICING_UNKNOWN,
    REASON_READ_FAILED,
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
        wormsoft_credit_hard_cap_ratio=0.95,
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
async def test_primary_soft_cap_is_observability_threshold_not_a_deny():
    # 80% is an early-warning threshold; primary traffic continues until the hard cap.
    guard = _guard(_FakeBudget(used=2_500_000.0), _settings())
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is True
    assert reason == REASON_OK


@pytest.mark.asyncio
async def test_allow_blocks_on_hard_cap():
    # hard cap = 0.95 * 3M = 2.85M; usage at 2.86M crosses hard
    guard = _guard(_FakeBudget(used=2_860_000.0), _settings())
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


@pytest.mark.asyncio
async def test_usage_read_failure_fails_closed_for_primary_via_setting():
    guard = _guard(
        _FakeBudget(raise_on_usage=True),
        _settings(wormsoft_credit_fail_closed=True),
    )
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is False
    assert reason == REASON_READ_FAILED


@pytest.mark.asyncio
async def test_usage_read_failure_always_skips_optional_shadow_work():
    guard = _guard(
        _FakeBudget(raise_on_usage=True),
        _settings(wormsoft_credit_fail_closed=True),
    )
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_SHADOW)
    assert allowed is False
    assert reason == REASON_READ_FAILED


@pytest.mark.asyncio
async def test_unknown_pricing_model_is_rejected_when_enforcement_is_enabled():
    guard = _guard(_FakeBudget(used=0.0), _settings())

    allowed, reason = await guard.allow(
        provider="wormsoft",
        execution_role=EXECUTION_ROLE_PRIMARY,
        requested_model="vendor/new-model",
    )

    assert allowed is False
    assert reason == REASON_PRICING_UNKNOWN


@pytest.mark.asyncio
async def test_usage_read_failure_fails_closed_via_env(monkeypatch):
    monkeypatch.setenv("WORMSOFT_CREDIT_FAIL_CLOSED", "1")
    guard = _guard(_FakeBudget(raise_on_usage=True), _settings())
    allowed, reason = await guard.allow(provider="wormsoft", execution_role=EXECUTION_ROLE_PRIMARY)
    assert allowed is False
    assert reason == REASON_READ_FAILED


def _receipt(
    *,
    provider="wormsoft",
    cost=1000.0,
    pricing_model="wormsoft/agent/medium",
    prompt_tokens=1000,
    completion_tokens=100,
    cached_prompt_tokens=0,
):
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
        budget_attribution={
            "model": pricing_model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "cached_prompt_tokens": cached_prompt_tokens,
        },
    )


@pytest.mark.asyncio
async def test_record_adds_usage_for_wormsoft():
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(_receipt(cost=1234.0))
    assert len(budget.added) == 1
    assert budget.added[0]["credits"] == pytest.approx(130.0)
    assert budget.added[0]["window_seconds"] == 14400


@pytest.mark.asyncio
async def test_record_skips_non_wormsoft():
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(_receipt(provider="gigachat", cost=1234.0))
    assert budget.added == []


@pytest.mark.asyncio
async def test_record_uses_usage_even_when_generic_cost_is_zero():
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(_receipt(cost=0.0))
    assert budget.added[0]["credits"] == pytest.approx(130.0)


@pytest.mark.asyncio
async def test_record_keeps_shadow_accounting_when_enforcement_is_disabled():
    budget = _FakeBudget()
    await _guard(budget, _settings(wormsoft_credit_throttle_enabled=False)).record(_receipt(cost=10.0))
    assert budget.added[0]["credits"] == pytest.approx(130.0)


@pytest.mark.asyncio
async def test_record_prices_cached_tokens_separately():
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(
        _receipt(prompt_tokens=1000, completion_tokens=100, cached_prompt_tokens=400)
    )

    assert budget.added[0]["credits"] == pytest.approx(118.02)


@pytest.mark.asyncio
async def test_record_skips_unknown_pricing_model(caplog):
    budget = _FakeBudget()
    await _guard(budget, _settings()).record(_receipt(pricing_model="vendor/unknown"))

    assert budget.added == []
    assert "wormsoft_credit_record_skipped_unknown_model" in caplog.text
