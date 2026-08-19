import pytest

from shared import metrics

pytestmark = pytest.mark.unit


def test_wormsoft_credit_estimate_exports_component_and_total_counters() -> None:
    counter = metrics.WORMSOFT_CREDITS_ESTIMATED_TOTAL
    if counter is None:
        pytest.skip("prometheus_client unavailable")

    labels = {
        "service": "metrics-test",
        "task": "relevance",
        "requested_model": "wormsoft/agent/medium",
        "actual_model": "gemma4:31b-cloud",
        "execution_role": "primary",
    }
    before = counter.labels(**labels, kind="total")._value.get()

    metrics.note_wormsoft_credit_estimate(
        **labels,
        input_credits=3.0,
        output_credits=10.0,
        cache_credits=0.1,
        total_credits=13.1,
    )

    assert counter.labels(**labels, kind="input")._value.get() >= 3.0
    assert counter.labels(**labels, kind="output")._value.get() >= 10.0
    assert counter.labels(**labels, kind="cache")._value.get() >= 0.1
    assert counter.labels(**labels, kind="total")._value.get() - before == pytest.approx(13.1)


def test_wormsoft_credit_window_exports_utilization() -> None:
    gauge = metrics.WORMSOFT_CREDIT_UTILIZATION_RATIO
    if gauge is None:
        pytest.skip("prometheus_client unavailable")

    metrics.set_wormsoft_credit_window(
        "metrics-test",
        used=600_000.0,
        limit=3_000_000.0,
        soft_cap_ratio=0.8,
        hard_cap_ratio=0.95,
    )

    value = gauge.labels(service="metrics-test")._value.get()
    assert value == pytest.approx(0.2)
    assert (
        metrics.WORMSOFT_CREDIT_WINDOW_REFRESH_TIMESTAMP.labels(service="metrics-test")._value.get()
        > 0
    )
    assert metrics.WORMSOFT_CREDIT_SOFT_CAP_RATIO.labels(
        service="metrics-test"
    )._value.get() == pytest.approx(0.8)
    assert metrics.WORMSOFT_CREDIT_HARD_CAP_RATIO.labels(
        service="metrics-test"
    )._value.get() == pytest.approx(0.95)


def test_wormsoft_accounting_errors_are_visible_in_shadow_mode() -> None:
    counter = metrics.WORMSOFT_CREDIT_ACCOUNTING_ERRORS_TOTAL
    if counter is None:
        pytest.skip("prometheus_client unavailable")

    labels = {
        "service": "metrics-test",
        "operation": "read",
        "reason": "redis_error",
    }
    before = counter.labels(**labels)._value.get()

    metrics.note_wormsoft_credit_accounting_error(**labels)

    assert counter.labels(**labels)._value.get() - before == pytest.approx(1.0)
