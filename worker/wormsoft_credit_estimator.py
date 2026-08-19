"""Model-aware Wormsoft credit estimation.

Wormsoft publishes separate input, output, and cache rates in credits per token.
This module deliberately depends only on those rates and token counts; routing and
credit-window policy stay outside it.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

logger = logging.getLogger(__name__)


class WormsoftCreditEstimationError(ValueError):
    """Base error for unsafe or impossible credit estimates."""


class UnknownWormsoftModelError(WormsoftCreditEstimationError):
    """Raised when no pricing rates exist for the requested model."""

    def __init__(self, requested_model: str, pricing_model: str | None = None) -> None:
        self.requested_model = requested_model
        self.pricing_model = pricing_model
        suffix = "" if not pricing_model else f" (resolved pricing model: {pricing_model})"
        super().__init__(f"unknown Wormsoft credit rates for {requested_model!r}{suffix}")


class InvalidWormsoftUsageError(WormsoftCreditEstimationError):
    """Raised when token counts cannot be priced safely."""


class WormsoftUsageLike(Protocol):
    """Usage fields exposed by :class:`worker.llm_types.GigaChatUsage`."""

    prompt_tokens: int
    completion_tokens: int
    precached_prompt_tokens: int


@dataclass(frozen=True, slots=True)
class WormsoftCreditRates:
    """Per-token Wormsoft rates for one requested model."""

    input: float
    output: float
    cache: float

    def __post_init__(self) -> None:
        for name, value in (
            ("input", self.input),
            ("output", self.output),
            ("cache", self.cache),
        ):
            numeric = float(value)
            if not math.isfinite(numeric) or numeric < 0.0:
                raise ValueError(f"Wormsoft {name} rate must be finite and non-negative")
            object.__setattr__(self, name, numeric)


@dataclass(frozen=True, slots=True)
class WormsoftCreditEstimate:
    """Auditable decomposition of the estimated credits for one response."""

    requested_model: str
    pricing_model: str
    prompt_tokens: int
    completion_tokens: int
    cached_prompt_tokens: int
    non_cached_prompt_tokens: int
    input_credits: float
    output_credits: float
    cache_credits: float
    total_credits: float


RateConfig = WormsoftCreditRates | Mapping[str, float]


def _rates(input_rate: float, output_rate: float, cache_rate: float) -> WormsoftCreditRates:
    return WormsoftCreditRates(input=input_rate, output=output_rate, cache=cache_rate)


# Snapshot verified against the account/public Wormsoft pricing APIs on 2026-08-19.
# Values are credits/token, not the visually published credits-per-million figures. Account
# pricing is authoritative for subscription aliases; public pricing supplies direct models and
# vision/medium when they are absent from that account snapshot. Vision low/high remain unknown.
# Reconcile this table with ``admin:wormsoft_limits:last_ok`` before changing enforcement.
DEFAULT_WORMSOFT_CREDIT_RATES: Mapping[str, WormsoftCreditRates] = MappingProxyType(
    {
        "wormsoft/agent/low": _rates(0.0005, 0.005, 0.00005),
        "wormsoft/agent/medium": _rates(0.03, 1.0, 0.00005),
        "wormsoft/agent/high": _rates(1.0, 4.0, 0.03),
        "wormsoft/code/low": _rates(0.0005, 0.005, 0.00005),
        "wormsoft/code/medium": _rates(0.03, 1.0, 0.00005),
        "wormsoft/code/high": _rates(0.08, 3.0, 0.03),
        "wormsoft/vision/medium": _rates(0.035, 1.1, 0.002),
        "deepseek-ai/deepseek-v4-pro": _rates(1.2, 4.0, 0.45),
    }
)


def _normalize_model(model: str) -> str:
    return str(model or "").strip().lower()


def _coerce_rates(config: RateConfig) -> WormsoftCreditRates:
    if isinstance(config, WormsoftCreditRates):
        return config
    try:
        return WormsoftCreditRates(
            input=float(config["input"]),
            output=float(config["output"]),
            cache=float(config["cache"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Wormsoft rate config requires numeric input/output/cache values") from exc


def _token_count(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidWormsoftUsageError(f"{name} must be a non-negative integer")
    return value


class WormsoftCreditEstimator:
    """Estimate credits using requested-model pricing with explicit unknown handling."""

    def __init__(
        self,
        *,
        rates_by_model: Mapping[str, RateConfig] | None = None,
        model_aliases: Mapping[str, str] | None = None,
    ) -> None:
        rates = dict(DEFAULT_WORMSOFT_CREDIT_RATES)
        for model, config in (rates_by_model or {}).items():
            normalized = _normalize_model(model)
            if not normalized:
                raise ValueError("Wormsoft pricing model must not be empty")
            rates[normalized] = _coerce_rates(config)
        self._rates = rates

        aliases: dict[str, str] = {}
        for requested, pricing_model in (model_aliases or {}).items():
            normalized_requested = _normalize_model(requested)
            normalized_pricing = _normalize_model(pricing_model)
            if not normalized_requested or not normalized_pricing:
                raise ValueError("Wormsoft model aliases must not be empty")
            aliases[normalized_requested] = normalized_pricing
        self._aliases = aliases

    def supports_model(self, requested_model: str) -> bool:
        """Return whether the requested billing model has a known rate table."""
        normalized_model = _normalize_model(requested_model)
        pricing_model = self._aliases.get(normalized_model, normalized_model)
        return bool(pricing_model and pricing_model in self._rates)

    def estimate(
        self,
        *,
        requested_model: str,
        prompt_tokens: int,
        completion_tokens: int,
        cached_prompt_tokens: int = 0,
    ) -> WormsoftCreditEstimate:
        """Return a decomposed estimate or raise for unknown/invalid inputs."""
        normalized_model = _normalize_model(requested_model)
        pricing_model = self._aliases.get(normalized_model, normalized_model)
        rates = self._rates.get(pricing_model)
        if rates is None:
            raise UnknownWormsoftModelError(normalized_model, pricing_model)

        prompt = _token_count("prompt_tokens", prompt_tokens)
        completion = _token_count("completion_tokens", completion_tokens)
        cached = _token_count("cached_prompt_tokens", cached_prompt_tokens)
        non_cached = max(prompt - cached, 0)

        input_credits = non_cached * rates.input
        output_credits = completion * rates.output
        cache_credits = cached * rates.cache
        total_credits = input_credits + output_credits + cache_credits
        return WormsoftCreditEstimate(
            requested_model=normalized_model,
            pricing_model=pricing_model,
            prompt_tokens=prompt,
            completion_tokens=completion,
            cached_prompt_tokens=cached,
            non_cached_prompt_tokens=non_cached,
            input_credits=input_credits,
            output_credits=output_credits,
            cache_credits=cache_credits,
            total_credits=total_credits,
        )

    def estimate_usage(
        self,
        *,
        requested_model: str,
        usage: WormsoftUsageLike,
    ) -> WormsoftCreditEstimate:
        """Estimate from the usage object currently returned by provider clients."""
        return self.estimate(
            requested_model=requested_model,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            cached_prompt_tokens=usage.precached_prompt_tokens,
        )

    def try_estimate(
        self,
        *,
        requested_model: str,
        prompt_tokens: int,
        completion_tokens: int,
        cached_prompt_tokens: int = 0,
    ) -> WormsoftCreditEstimate | None:
        """Return ``None`` (never zero credits) when a requested model is unknown."""
        try:
            return self.estimate(
                requested_model=requested_model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cached_prompt_tokens=cached_prompt_tokens,
            )
        except UnknownWormsoftModelError:
            logger.warning(
                "wormsoft_credit_rates_unknown requested_model=%s",
                _normalize_model(requested_model) or "<empty>",
            )
            return None
