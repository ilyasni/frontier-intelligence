"""Shared LLM response types across provider clients."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class GigaChatUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    precached_prompt_tokens: int = 0
    total_tokens: int = 0

    @property
    def billable_tokens(self) -> int:
        return self.total_tokens

    @property
    def cached_prompt_tokens(self) -> int:
        return self.precached_prompt_tokens


@dataclass(frozen=True)
class GigaChatResponse:
    content: str
    model: str
    requested_model: str = ""
    provider: str = "gigachat"
    usage: GigaChatUsage = field(default_factory=GigaChatUsage)
    parsed: dict[str, Any] | None = None
    fallback_reason: str = ""

    @property
    def actual_model(self) -> str:
        return self.model


def usage_from_openai_response(resp: Any) -> GigaChatUsage:
    usage = getattr(resp, "usage", None)
    if usage is None:
        return GigaChatUsage()

    def _coerce(*names: str) -> int:
        missing = object()
        value: Any = 0
        for name in names:
            candidate = getattr(usage, name, missing)
            if candidate is not missing and candidate is not None:
                value = candidate
                break
        try:
            return max(0, int(value or 0))
        except Exception:
            return 0

    cached_tokens = _coerce("precached_prompt_tokens")
    if cached_tokens <= 0:
        details = getattr(usage, "prompt_tokens_details", None) or getattr(
            usage, "input_tokens_details", None
        )
        cached_tokens = _coerce_usage_detail(details, "cached_tokens")

    prompt_tokens = _coerce("prompt_tokens", "input_tokens")
    completion_tokens = _coerce("completion_tokens", "output_tokens")
    total_tokens = _coerce("total_tokens") or prompt_tokens + completion_tokens

    return GigaChatUsage(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        precached_prompt_tokens=cached_tokens,
        total_tokens=total_tokens,
    )


def usage_from_openai_payload(payload: dict[str, Any] | None) -> GigaChatUsage:
    if not isinstance(payload, dict):
        return GigaChatUsage()
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return GigaChatUsage()

    def _coerce(*names: str) -> int:
        missing = object()
        value: Any = 0
        for name in names:
            candidate = usage.get(name, missing)
            if candidate is not missing and candidate is not None:
                value = candidate
                break
        try:
            return max(0, int(value or 0))
        except Exception:
            return 0

    cached_tokens = _coerce("precached_prompt_tokens")
    if cached_tokens <= 0:
        details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details")
        cached_tokens = _coerce_usage_detail(details, "cached_tokens")

    prompt_tokens = _coerce("prompt_tokens", "input_tokens")
    completion_tokens = _coerce("completion_tokens", "output_tokens")
    total_tokens = _coerce("total_tokens") or prompt_tokens + completion_tokens

    return GigaChatUsage(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        precached_prompt_tokens=cached_tokens,
        total_tokens=total_tokens,
    )


def _coerce_usage_detail(details: Any, name: str) -> int:
    if details is None:
        return 0
    value = details.get(name, 0) if isinstance(details, dict) else getattr(details, name, 0)
    try:
        return max(0, int(value or 0))
    except Exception:
        return 0
