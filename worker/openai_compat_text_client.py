"""Reusable OpenAI-compatible text client helpers."""
from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import Any

import httpx
from openai import AsyncOpenAI

from shared.metrics import note_llm_request, note_llm_usage, note_rate_limit_event
from worker.llm_types import GigaChatResponse, usage_from_openai_response


def extract_message_text(message_content: Any) -> str:
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


class OpenAICompatTextError(RuntimeError):
    """Structured request error for OpenAI-compatible text providers."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        reset_at: float | None = None,
        reason: str = "provider_error",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reset_at = reset_at
        self.reason = reason


class OpenAICompatTextClient:
    """Long-lived AsyncOpenAI wrapper for text-only provider adapters."""

    provider_name = "provider"
    unavailable_reason = "provider_unavailable"

    def __init__(
        self,
        *,
        service_name: str = "worker",
        base_url: str,
        api_key: str,
        default_model: str,
        max_parallel_requests: int = 1,
        min_request_interval_ms: int = 250,
        default_headers: Mapping[str, str] | None = None,
    ) -> None:
        self._service_name = service_name
        self._api_key = str(api_key or "").strip()
        self._default_model = str(default_model or "").strip()
        self._request_sem = asyncio.Semaphore(max(1, int(max_parallel_requests or 1)))
        self._min_request_interval_s = max(0.0, float(int(min_request_interval_ms or 250)) / 1000.0)
        self._request_gap_lock = asyncio.Lock()
        self._last_request_started_at = 0.0
        self._http_client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0))
        self._client = AsyncOpenAI(
            base_url=str(base_url or "").rstrip("/"),
            api_key=self._api_key or "missing",
            http_client=self._http_client,
            default_headers=dict(default_headers or {}),
            max_retries=2,
        )

    async def close(self) -> None:
        await self._client.close()

    @property
    def default_model(self) -> str:
        return self._default_model

    @property
    def is_available(self) -> bool:
        return bool(self._api_key and self.default_model)

    async def _acquire_request_slot(self) -> None:
        await self._request_sem.acquire()
        try:
            async with self._request_gap_lock:
                now = time.monotonic()
                wait_for = self._min_request_interval_s - (now - self._last_request_started_at)
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
                self._last_request_started_at = time.monotonic()
        except Exception:
            self._request_sem.release()
            raise

    def _release_request_slot(self) -> None:
        self._request_sem.release()

    def _observe_rate_limit(self, status_code: int | None, operation: str) -> None:
        if status_code == 429:
            note_rate_limit_event(self._service_name, self.provider_name, operation)

    def _error_reason(self, status_code: int | None) -> str:
        if status_code is None:
            return f"{self.provider_name}_error"
        return f"{self.provider_name}_{status_code}"

    def _build_error(self, exc: Exception, *, task: str, model: str) -> OpenAICompatTextError:
        response = getattr(exc, "response", None)
        status_code = getattr(exc, "status_code", None) or getattr(response, "status_code", None)
        message = str(exc)
        self._observe_rate_limit(status_code, task)
        note_llm_request(self._service_name, task, self.provider_name, model, "", "error")
        return OpenAICompatTextError(
            f"{self.provider_name}_request_failed: {message[:300]}",
            status_code=status_code,
            reason=self._error_reason(status_code),
        )

    async def chat(
        self,
        system: str,
        user: str,
        *,
        task: str = "chat",
        pro: bool = False,
        model_override: str | None = None,
        provider_override: str | None = None,
        max_tokens: int = 1024,
    ) -> GigaChatResponse:
        del pro, provider_override
        if not self.is_available:
            raise RuntimeError(self.unavailable_reason)
        model = str(model_override or self.default_model).strip()
        if not model:
            raise ValueError(f"{self.provider_name}_model_missing task={task}")

        try:
            await self._acquire_request_slot()
            try:
                raw = await self._client.chat.completions.with_raw_response.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=0.1,
                    max_tokens=max_tokens,
                )
            finally:
                self._release_request_slot()
            resp = raw.parse()
            usage = usage_from_openai_response(resp)
            actual_model = str(getattr(resp, "model", model) or model)
            content = extract_message_text(resp.choices[0].message.content)
            note_llm_request(
                self._service_name,
                task,
                self.provider_name,
                model,
                actual_model,
                "ok",
            )
            note_llm_usage(
                self._service_name,
                task,
                self.provider_name,
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
                provider=self.provider_name,
                usage=usage,
            )
        except OpenAICompatTextError:
            raise
        except Exception as exc:
            raise self._build_error(exc, task=task, model=model) from exc
