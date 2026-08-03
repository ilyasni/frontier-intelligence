"""Регрессия на вечный карантин моделей OpenRouter (инцидент 2026-08-01).

admin health-probe парсил ``X-RateLimit-Reset`` сырым ``float()``, а OpenRouter
отдаёт его в миллисекундах. 429 с ``X-RateLimit-Reset: 1785628800000`` дал
``in_quarantine_until = 1785628800000.0`` — 29 апреля 58554 года — и вывел
14 из 16 бесплатных моделей из ротации навсегда.
"""

from __future__ import annotations

import pytest

from shared.openrouter_limits import (
    MAX_QUARANTINE_SEC,
    clamp_quarantine_until,
    parse_rate_limit_reset,
)

pytestmark = pytest.mark.unit

# Момент «сейчас» для детерминизма: 2026-08-02 ~10:00 UTC.
NOW = 1785664800.0

# Ровно то значение, что пришло от OpenRouter в проде.
PROD_HEADER_MS = "1785628800000"


def test_milliseconds_header_is_converted_to_seconds() -> None:
    """Продовое значение обязано дать 2026 год, а не 58554."""
    parsed = parse_rate_limit_reset(PROD_HEADER_MS, now=NOW)

    assert parsed == 1785628800.0
    # Ключевая проверка: карантин не должен уходить в необозримое будущее.
    assert parsed - NOW < MAX_QUARANTINE_SEC


def test_seconds_epoch_header_passes_through() -> None:
    assert parse_rate_limit_reset("1785628800", now=NOW) == 1785628800.0


def test_relative_offset_is_added_to_now() -> None:
    """Малые значения — это «через N секунд», а не epoch."""
    assert parse_rate_limit_reset("30", now=NOW) == NOW + 30.0


@pytest.mark.parametrize("raw", ["", "   ", None, "not-a-number", "abc123"])
def test_unparseable_values_return_none(raw) -> None:
    assert parse_rate_limit_reset(raw, now=NOW) is None


def test_clamp_caps_absurd_quarantine() -> None:
    """Даже если провайдер пришлёт мусор, модель не выпадает навсегда."""
    absurd = 1785628800000.0  # то самое непропарсенное значение

    capped = clamp_quarantine_until(absurd, now=NOW)

    assert capped == NOW + MAX_QUARANTINE_SEC
    assert capped - NOW <= MAX_QUARANTINE_SEC


def test_clamp_preserves_reasonable_value() -> None:
    reasonable = NOW + 900.0
    assert clamp_quarantine_until(reasonable, now=NOW) == reasonable


@pytest.mark.parametrize("candidate", [None, 0.0, NOW - 1.0])
def test_clamp_returns_zero_for_expired_or_missing(candidate) -> None:
    assert clamp_quarantine_until(candidate, now=NOW) == 0.0


def test_end_to_end_prod_scenario_recovers_within_a_day() -> None:
    """Полный путь: заголовок -> парсер -> потолок -> срок карантина."""
    reset_at = parse_rate_limit_reset(PROD_HEADER_MS, now=NOW)
    quarantine_until = clamp_quarantine_until(reset_at, now=NOW)

    # Значение уже в прошлом относительно NOW, значит карантина быть не должно.
    assert quarantine_until == 0.0
