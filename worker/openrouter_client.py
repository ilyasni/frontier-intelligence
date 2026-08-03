"""OpenRouter vision client using the OpenAI-compatible HTTP API."""
from __future__ import annotations

import base64
from typing import Any

import httpx

from shared.config import get_settings
from shared.metrics import (
    note_llm_request,
    note_llm_usage,
    note_openrouter_vision_request,
    note_rate_limit_event,
)
from shared.openrouter_limits import parse_rate_limit_reset
from worker.gigachat_client import VISION_PROMPT, _parse_vision_payload
from worker.llm_http import DEFAULT_LLM_HTTP_TIMEOUT
from worker.llm_types import GigaChatResponse, usage_from_openai_payload


def _detect_image_mime(image_bytes: bytes) -> str:
    if image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if image_bytes[:4] == b"GIF8":
        return "image/gif"
    if image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"


def _extract_text_content(message_content: Any) -> str:
    if isinstance(message_content, str):
        return message_content
    if isinstance(message_content, list):
        parts: list[str] = []
        for item in message_content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text":
                parts.append(str(item.get("text") or ""))
        return "\n".join(part for part in parts if part)
    return str(message_content or "")




class OpenRouterVisionError(RuntimeError):
    """Structured OpenRouter request error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        reset_at: float | None = None,
        reason: str = "openrouter_error",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reset_at = reset_at
        self.reason = reason


class OpenRouterVisionClient:
    """Minimal HTTP client for OpenRouter vision calls."""

    def __init__(self, *, service_name: str = "worker") -> None:
        settings = get_settings()
        self._settings = settings
        self._service_name = service_name
        self._http_client = httpx.AsyncClient(timeout=DEFAULT_LLM_HTTP_TIMEOUT)

    async def close(self) -> None:
        await self._http_client.aclose()

    @property
    def is_available(self) -> bool:
        return bool(self._settings.openrouter_api_key)

    @property
    def default_model(self) -> str:
        return (
            str(self._settings.openrouter_vision_model or "openrouter/free").strip()
            or "openrouter/free"
        )

    async def vision(
        self,
        image_bytes: bytes,
        *,
        prompt: str = VISION_PROMPT,
        model_override: str | None = None,
    ) -> GigaChatResponse:
        if not self.is_available:
            raise OpenRouterVisionError("openrouter_api_key_missing", reason="openrouter_disabled")

        model = str(model_override or self.default_model).strip()
        if not model:
            raise OpenRouterVisionError(
                "openrouter_vision_model_missing",
                reason="openrouter_disabled",
            )

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
            "Authorization": f"Bearer {self._settings.openrouter_api_key}",
            "HTTP-Referer": self._settings.openrouter_referrer,
            "X-Title": "Frontier Intelligence",
            "Content-Type": "application/json",
        }

        try:
            resp = await self._http_client.post(
                f"{self._settings.openrouter_base_url.rstrip('/')}/chat/completions",
                headers=headers,
                json=payload,
            )
        except Exception as exc:
            note_openrouter_vision_request(self._service_name, "error")
            note_llm_request(self._service_name, "vision", "openrouter", model, "", "error")
            raise OpenRouterVisionError(
                f"openrouter_transport_error: {exc}",
                reason="openrouter_error",
            ) from exc
        if resp.status_code >= 400:
            note_openrouter_vision_request(self._service_name, "error")
            body_preview = resp.text[:300]
            reset_at = parse_rate_limit_reset(resp.headers.get("X-RateLimit-Reset"))
            note_llm_request(self._service_name, "vision", "openrouter", model, "", "error")
            if resp.status_code == 429:
                note_rate_limit_event(self._service_name, "openrouter", "vision")
                raise OpenRouterVisionError(
                    f"openrouter_rate_limited: {body_preview}",
                    status_code=429,
                    reset_at=reset_at,
                    reason="openrouter_429",
                )
            if resp.status_code == 402:
                raise OpenRouterVisionError(
                    f"openrouter_payment_required: {body_preview}",
                    status_code=402,
                    reason="openrouter_402",
                )
            if 500 <= resp.status_code <= 599:
                raise OpenRouterVisionError(
                    f"openrouter_server_error: {body_preview}",
                    status_code=resp.status_code,
                    reason="openrouter_5xx",
                )
            raise OpenRouterVisionError(
                f"openrouter_request_failed: {body_preview}",
                status_code=resp.status_code,
                reason=f"openrouter_{resp.status_code}",
            )

        data = resp.json()
        if not isinstance(data, dict):
            note_openrouter_vision_request(self._service_name, "error")
            note_llm_request(self._service_name, "vision", "openrouter", model, "", "error")
            raise OpenRouterVisionError(
                "openrouter_invalid_response",
                reason="openrouter_invalid_response",
            )
        choices = data.get("choices") or []
        if not isinstance(choices, list) or not choices:
            note_openrouter_vision_request(self._service_name, "error")
            note_llm_request(self._service_name, "vision", "openrouter", model, "", "error")
            raise OpenRouterVisionError(
                "openrouter_missing_choices",
                reason="openrouter_invalid_response",
            )
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else {}
        content = _extract_text_content((message or {}).get("content"))
        actual_model = str(data.get("model") or model)
        parsed = _parse_vision_payload(content)
        usage = usage_from_openai_payload(data)
        note_openrouter_vision_request(self._service_name, "ok")
        note_llm_request(self._service_name, "vision", "openrouter", model, actual_model, "ok")
        note_llm_usage(
            self._service_name,
            "vision",
            "openrouter",
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
            provider="openrouter",
            usage=usage,
            parsed=parsed,
        )
