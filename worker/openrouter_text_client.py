"""OpenRouter text client using the OpenAI-compatible API."""
from __future__ import annotations

from shared.config import get_settings
from worker.openai_compat_text_client import OpenAICompatTextClient, OpenAICompatTextError
from worker.openrouter_client import _parse_reset_at


class OpenRouterTextError(OpenAICompatTextError):
    """Structured OpenRouter text error."""


class OpenRouterTextClient(OpenAICompatTextClient):
    provider_name = "openrouter"
    unavailable_reason = "openrouter_disabled"

    def __init__(self, *, service_name: str = "worker") -> None:
        settings = get_settings()
        self._settings = settings
        super().__init__(
            service_name=service_name,
            base_url=settings.openrouter_base_url,
            api_key=settings.openrouter_api_key,
            default_model=settings.openrouter_text_model,
            max_parallel_requests=settings.openrouter_max_simultaneous_requests,
            min_request_interval_ms=settings.openrouter_min_request_interval_ms,
            default_headers={
                "HTTP-Referer": settings.openrouter_referrer,
                "X-Title": "Frontier Intelligence",
            },
        )

    def _error_reason(self, status_code: int | None) -> str:
        if status_code == 429:
            return "openrouter_429"
        if status_code == 402:
            return "openrouter_402"
        if status_code is not None and 500 <= status_code <= 599:
            return "openrouter_5xx"
        return super()._error_reason(status_code)

    def _build_error(self, exc: Exception, *, task: str, model: str) -> OpenRouterTextError:
        base = super()._build_error(exc, task=task, model=model)
        response = getattr(exc, "response", None)
        headers = getattr(response, "headers", None)
        reset_at = _parse_reset_at(headers.get("X-RateLimit-Reset")) if headers else None
        return OpenRouterTextError(
            str(base),
            status_code=base.status_code,
            reset_at=reset_at,
            reason=base.reason,
        )
