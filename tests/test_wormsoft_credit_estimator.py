import logging

import pytest

from worker.llm_types import GigaChatUsage
from worker.wormsoft_credit_estimator import (
    DEFAULT_WORMSOFT_CREDIT_RATES,
    InvalidWormsoftUsageError,
    UnknownWormsoftModelError,
    WormsoftCreditEstimator,
    WormsoftCreditRates,
)

pytestmark = pytest.mark.unit


def test_medium_estimate_separates_non_cached_cached_and_output_tokens() -> None:
    estimate = WormsoftCreditEstimator().estimate(
        requested_model="wormsoft/agent/medium",
        prompt_tokens=1_000,
        completion_tokens=100,
        cached_prompt_tokens=400,
    )

    assert estimate.non_cached_prompt_tokens == 600
    assert estimate.input_credits == pytest.approx(18.0)
    assert estimate.output_credits == pytest.approx(100.0)
    assert estimate.cache_credits == pytest.approx(0.02)
    assert estimate.total_credits == pytest.approx(118.02)


@pytest.mark.parametrize(
    ("model", "expected"),
    [
        ("wormsoft/agent/low", WormsoftCreditRates(0.0005, 0.005, 0.00005)),
        ("wormsoft/agent/medium", WormsoftCreditRates(0.03, 1.0, 0.00005)),
        ("wormsoft/agent/high", WormsoftCreditRates(1.0, 4.0, 0.03)),
        ("wormsoft/code/low", WormsoftCreditRates(0.0005, 0.005, 0.00005)),
        ("wormsoft/code/medium", WormsoftCreditRates(0.03, 1.0, 0.00005)),
        ("wormsoft/code/high", WormsoftCreditRates(0.08, 3.0, 0.03)),
        ("wormsoft/vision/medium", WormsoftCreditRates(0.035, 1.1, 0.002)),
        ("deepseek-ai/deepseek-v4-pro", WormsoftCreditRates(1.2, 4.0, 0.45)),
    ],
)
def test_default_requested_alias_rates(model: str, expected: WormsoftCreditRates) -> None:
    assert DEFAULT_WORMSOFT_CREDIT_RATES[model] == expected


def test_supports_model_uses_normalized_requested_alias() -> None:
    estimator = WormsoftCreditEstimator()

    assert estimator.supports_model(" WOrmsOft/Agent/Medium ") is True
    assert estimator.supports_model("vendor/unknown") is False


def test_cached_tokens_above_prompt_do_not_make_non_cached_negative() -> None:
    estimate = WormsoftCreditEstimator().estimate(
        requested_model="wormsoft/agent/medium",
        prompt_tokens=100,
        completion_tokens=0,
        cached_prompt_tokens=150,
    )

    assert estimate.non_cached_prompt_tokens == 0
    assert estimate.input_credits == 0.0
    assert estimate.cache_credits == pytest.approx(0.0075)


def test_estimate_usage_accepts_existing_gigachat_usage_fields() -> None:
    usage = GigaChatUsage(
        prompt_tokens=300,
        completion_tokens=50,
        precached_prompt_tokens=100,
        total_tokens=350,
    )

    estimate = WormsoftCreditEstimator().estimate_usage(
        requested_model="wormsoft/agent/medium",
        usage=usage,
    )

    assert estimate.non_cached_prompt_tokens == 200
    assert estimate.total_credits == pytest.approx(56.005)


def test_vision_medium_uses_published_default_rates() -> None:
    estimate = WormsoftCreditEstimator().estimate(
        requested_model="wormsoft/vision/medium",
        prompt_tokens=20,
        completion_tokens=10,
        cached_prompt_tokens=5,
    )

    assert estimate.pricing_model == "wormsoft/vision/medium"
    assert estimate.input_credits == pytest.approx(0.525)
    assert estimate.output_credits == pytest.approx(11.0)
    assert estimate.cache_credits == pytest.approx(0.01)
    assert estimate.total_credits == pytest.approx(11.535)


def test_deepseek_v4_pro_uses_current_public_rates() -> None:
    estimate = WormsoftCreditEstimator().estimate(
        requested_model="deepseek-ai/deepseek-v4-pro",
        prompt_tokens=100,
        completion_tokens=10,
        cached_prompt_tokens=20,
    )

    assert estimate.input_credits == pytest.approx(96.0)
    assert estimate.output_credits == pytest.approx(40.0)
    assert estimate.cache_credits == pytest.approx(9.0)
    assert estimate.total_credits == pytest.approx(145.0)


def test_custom_vision_medium_rates_override_published_default() -> None:
    estimator = WormsoftCreditEstimator(
        rates_by_model={"wormsoft/vision/medium": {"input": 0.2, "output": 1.5, "cache": 0.01}}
    )

    estimate = estimator.estimate(
        requested_model="wormsoft/vision/medium",
        prompt_tokens=20,
        completion_tokens=10,
        cached_prompt_tokens=5,
    )

    assert estimate.pricing_model == "wormsoft/vision/medium"
    assert estimate.total_credits == pytest.approx(18.05)


def test_vision_medium_can_be_explicitly_aliased_to_a_priced_model() -> None:
    estimator = WormsoftCreditEstimator(
        model_aliases={"wormsoft/vision/medium": "wormsoft/agent/medium"}
    )

    estimate = estimator.estimate(
        requested_model=" WOrmsOft/Vision/Medium ",
        prompt_tokens=100,
        completion_tokens=10,
    )

    assert estimate.requested_model == "wormsoft/vision/medium"
    assert estimate.pricing_model == "wormsoft/agent/medium"
    assert estimate.total_credits == pytest.approx(13.0)


def test_custom_rates_override_a_default_alias() -> None:
    estimator = WormsoftCreditEstimator(
        rates_by_model={
            "wormsoft/agent/medium": WormsoftCreditRates(input=2.0, output=3.0, cache=4.0)
        }
    )

    estimate = estimator.estimate(
        requested_model="wormsoft/agent/medium",
        prompt_tokens=5,
        completion_tokens=2,
        cached_prompt_tokens=1,
    )

    assert estimate.total_credits == pytest.approx(18.0)


@pytest.mark.parametrize("model", ["wormsoft/vision/low", "wormsoft/vision/high"])
def test_unpublished_vision_tier_is_explicitly_unknown(
    caplog: pytest.LogCaptureFixture,
    model: str,
) -> None:
    estimator = WormsoftCreditEstimator()

    with pytest.raises(UnknownWormsoftModelError, match="unknown Wormsoft credit rates"):
        estimator.estimate(
            requested_model=model,
            prompt_tokens=100,
            completion_tokens=10,
        )

    with caplog.at_level(logging.WARNING):
        result = estimator.try_estimate(
            requested_model=model,
            prompt_tokens=100,
            completion_tokens=10,
        )

    assert result is None
    assert "wormsoft_credit_rates_unknown" in caplog.text


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("prompt_tokens", -1),
        ("completion_tokens", -1),
        ("cached_prompt_tokens", -1),
        ("prompt_tokens", True),
    ],
)
def test_invalid_usage_is_rejected(field: str, value: int) -> None:
    kwargs = {
        "requested_model": "wormsoft/agent/medium",
        "prompt_tokens": 1,
        "completion_tokens": 1,
        "cached_prompt_tokens": 0,
    }
    kwargs[field] = value

    with pytest.raises(InvalidWormsoftUsageError):
        WormsoftCreditEstimator().estimate(**kwargs)


@pytest.mark.parametrize("rate", [-1.0, float("inf"), float("nan")])
def test_invalid_rates_are_rejected(rate: float) -> None:
    with pytest.raises(ValueError, match="finite and non-negative"):
        WormsoftCreditRates(input=rate, output=1.0, cache=0.0)
