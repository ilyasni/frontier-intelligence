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

    def _coerce(name: str) -> int:
        value = getattr(usage, name, 0)
        try:
            return int(value or 0)
        except Exception:
            return 0

    return GigaChatUsage(
        prompt_tokens=_coerce("prompt_tokens"),
        completion_tokens=_coerce("completion_tokens"),
        precached_prompt_tokens=_coerce("precached_prompt_tokens"),
        total_tokens=_coerce("total_tokens"),
    )


def usage_from_openai_payload(payload: dict[str, Any] | None) -> GigaChatUsage:
    if not isinstance(payload, dict):
        return GigaChatUsage()
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return GigaChatUsage()

    def _coerce(name: str) -> int:
        value = usage.get(name, 0)
        try:
            return int(value or 0)
        except Exception:
            return 0

    return GigaChatUsage(
        prompt_tokens=_coerce("prompt_tokens"),
        completion_tokens=_coerce("completion_tokens"),
        precached_prompt_tokens=_coerce("precached_prompt_tokens"),
        total_tokens=_coerce("total_tokens"),
    )
