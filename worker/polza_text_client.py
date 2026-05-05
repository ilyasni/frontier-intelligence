"""Polza text client using the OpenAI-compatible API."""
from __future__ import annotations

from shared.config import get_settings
from worker.openai_compat_text_client import OpenAICompatTextClient, OpenAICompatTextError


class PolzaTextError(OpenAICompatTextError):
    """Structured Polza text error."""


class PolzaTextClient(OpenAICompatTextClient):
    provider_name = "polza"
    unavailable_reason = "polza_disabled"

    def __init__(self, *, service_name: str = "worker") -> None:
        settings = get_settings()
        self._settings = settings
        super().__init__(
            service_name=service_name,
            base_url=settings.polza_base_url,
            api_key=settings.polza_api_key,
            default_model=settings.polza_text_model,
            max_parallel_requests=settings.gigachat_max_simultaneous_requests,
            min_request_interval_ms=settings.gigachat_min_request_interval_ms,
        )

    @property
    def synthesis_model(self) -> str:
        return str(self._settings.polza_synthesis_model or self.default_model).strip()

    def _build_error(self, exc: Exception, *, task: str, model: str) -> PolzaTextError:
        base = super()._build_error(exc, task=task, model=model)
        return PolzaTextError(
            str(base),
            status_code=base.status_code,
            reason=base.reason,
        )
