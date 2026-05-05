"""Polza vision client using the OpenAI-compatible HTTP API."""
from __future__ import annotations

import base64

import httpx

from shared.config import get_settings
from shared.metrics import (
    note_llm_request,
    note_llm_usage,
    note_polza_vision_request,
    note_rate_limit_event,
)
from worker.gigachat_client import VISION_PROMPT, _parse_vision_payload
from worker.llm_types import GigaChatResponse, usage_from_openai_payload
from worker.openrouter_client import _detect_image_mime, _extract_text_content


class PolzaVisionError(RuntimeError):
    """Structured Polza request error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        reason: str = "polza_error",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reason = reason


class PolzaVisionClient:
    """Minimal HTTP client for Polza vision fallback calls."""

    def __init__(self, *, service_name: str = "worker") -> None:
        settings = get_settings()
        self._settings = settings
        self._service_name = service_name
        self._http_client = httpx.AsyncClient(timeout=httpx.Timeout(90.0, connect=15.0))

    async def close(self) -> None:
        await self._http_client.aclose()

    @property
    def default_model(self) -> str:
        return str(self._settings.polza_vision_model or "").strip()

    @property
    def is_available(self) -> bool:
        return bool(self._settings.polza_api_key and self.default_model)

    async def vision(
        self,
        image_bytes: bytes,
        *,
        prompt: str = VISION_PROMPT,
        model_override: str | None = None,
    ) -> GigaChatResponse:
        if not self._settings.polza_api_key:
            raise PolzaVisionError("polza_api_key_missing", reason="polza_disabled")
        model = str(model_override or self.default_model).strip()
        if not model:
            raise PolzaVisionError("polza_vision_model_missing", reason="polza_disabled")

        mime = _detect_image_mime(image_bytes)
        b64 = base64.b64encode(image_bytes).decode("ascii")
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
                    ],
                }
            ],
            "temperature": 0.1,
            "max_tokens": 1024,
        }
        headers = {
            "Authorization": f"Bearer {self._settings.polza_api_key}",
            "Content-Type": "application/json",
        }

        try:
            resp = await self._http_client.post(
                f"{self._settings.polza_base_url.rstrip('/')}/chat/completions",
                headers=headers,
                json=payload,
            )
        except Exception as exc:
            note_polza_vision_request(self._service_name, "error")
            note_llm_request(self._service_name, "vision", "polza", model, "", "error")
            raise PolzaVisionError(
                f"polza_transport_error: {exc}",
                reason="polza_error",
            ) from exc
        if resp.status_code >= 400:
            note_polza_vision_request(self._service_name, "error")
            note_llm_request(self._service_name, "vision", "polza", model, "", "error")
            if resp.status_code == 429:
                note_rate_limit_event(self._service_name, "polza", "vision")
            raise PolzaVisionError(
                f"polza_request_failed: {resp.text[:300]}",
                status_code=resp.status_code,
                reason=f"polza_{resp.status_code}",
            )

        data = resp.json()
        if not isinstance(data, dict):
            note_polza_vision_request(self._service_name, "error")
            note_llm_request(self._service_name, "vision", "polza", model, "", "error")
            raise PolzaVisionError("polza_invalid_response", reason="polza_invalid_response")
        choices = data.get("choices") or []
        if not isinstance(choices, list) or not choices:
            note_polza_vision_request(self._service_name, "error")
            note_llm_request(self._service_name, "vision", "polza", model, "", "error")
            raise PolzaVisionError("polza_missing_choices", reason="polza_invalid_response")
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else {}
        content = _extract_text_content((message or {}).get("content"))
        actual_model = str(data.get("model") or model)
        parsed = _parse_vision_payload(content)
        usage = usage_from_openai_payload(data)
        note_polza_vision_request(self._service_name, "ok")
        note_llm_request(self._service_name, "vision", "polza", model, actual_model, "ok")
        note_llm_usage(
            self._service_name,
            "vision",
            "polza",
            model,
            actual_model,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            billable_tokens=usage.billable_tokens,
        )
        return GigaChatResponse(
            content=content,
            model=actual_model,
            requested_model=model,
            provider="polza",
            usage=usage,
            parsed=parsed,
        )
