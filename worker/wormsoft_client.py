"""Wormsoft text client with shared guardrails and structured errors."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

import httpx
from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    AuthenticationError,
    BadRequestError,
    RateLimitError,
)

from shared.config import get_settings
from shared.llm_control_plane import (
    ERROR_AUTH,
    ERROR_BAD_REQUEST,
    ERROR_CONNECTION,
    ERROR_MODEL_NOT_FOUND,
    ERROR_PROVIDER_UNAVAILABLE,
    ERROR_QUOTA_EXHAUSTED,
    ERROR_RATE_LIMITED,
    ERROR_TIMEOUT,
    ERROR_UNKNOWN,
    ERROR_UPSTREAM_5XX,
    ProviderError,
)
from shared.metrics import note_llm_request, note_llm_usage, note_rate_limit_event
from worker.llm_types import GigaChatResponse, usage_from_openai_response
from worker.token_budget import BudgetedText, fit_text_to_token_budget
from worker.wormsoft_guard import WormsoftSharedGuard

logger = logging.getLogger(__name__)


class WormsoftTextError(RuntimeError):
    """Structured Wormsoft text error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        reset_at: float | None = None,
        reason: str = "wormsoft_error",
        category: str = ERROR_UNKNOWN,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reset_at = reset_at
        self.reason = reason
        self.category = category
        self.retryable = retryable

    def as_provider_error(self) -> ProviderError:
        return ProviderError(
            provider="wormsoft",
            category=self.category,
            message=str(self),
            status_code=self.status_code,
            retryable=self.retryable,
            reset_at=self.reset_at,
            reason=self.reason,
        )


class WormsoftTextClient:
    """OpenAI-compatible Wormsoft client for text tasks."""

    def __init__(self, redis=None, *, service_name: str = "worker"):
        settings = get_settings()
        self._settings = settings
        self._service_name = service_name
        self._api_key = str(settings.wormsoft_api_key or "").strip()
        self._request_sem = asyncio.Semaphore(
            max(1, int(settings.wormsoft_max_simultaneous_requests or 1))
        )
        self._min_request_interval_s = (
            0.0
            if bool(getattr(settings, "wormsoft_shared_guard_enabled", False))
            else max(
                0.0,
                float(int(getattr(settings, "wormsoft_min_request_interval_ms", 2000) or 2000))
                / 1000.0,
            )
        )
        self._request_gap_lock = asyncio.Lock()
        self._last_request_started_at = 0.0
        self._guard = WormsoftSharedGuard(
            redis=redis,
            service_name=service_name,
            settings=settings,
        )
        timeout = httpx.Timeout(
            max(
                float(getattr(settings, "wormsoft_read_timeout_sec", 45.0) or 45.0),
                float(getattr(settings, "wormsoft_connect_timeout_sec", 5.0) or 5.0),
                float(getattr(settings, "wormsoft_write_timeout_sec", 45.0) or 45.0),
                float(getattr(settings, "wormsoft_pool_timeout_sec", 10.0) or 10.0),
            ),
            connect=float(getattr(settings, "wormsoft_connect_timeout_sec", 5.0) or 5.0),
            read=float(getattr(settings, "wormsoft_read_timeout_sec", 45.0) or 45.0),
            write=float(getattr(settings, "wormsoft_write_timeout_sec", 45.0) or 45.0),
            pool=float(getattr(settings, "wormsoft_pool_timeout_sec", 10.0) or 10.0),
        )
        limits = httpx.Limits(
            max_connections=max(1, int(getattr(settings, "wormsoft_max_connections", 20) or 20)),
            max_keepalive_connections=max(
                1,
                int(getattr(settings, "wormsoft_max_keepalive_connections", 5) or 5),
            ),
            keepalive_expiry=30.0,
        )
        self._http_client = httpx.AsyncClient(timeout=timeout, limits=limits)
        self._client = AsyncOpenAI(
            base_url=settings.wormsoft_api_base.rstrip("/"),
            api_key=self._api_key or "missing",
            http_client=self._http_client,
            max_retries=0,
        )

    async def close(self) -> None:
        await self._guard.close()
        await self._client.close()

    @property
    def is_available(self) -> bool:
        return bool(self._api_key)

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

    def _observe_rate_limit(self, exc: Exception, operation: str) -> None:
        if getattr(exc, "status_code", None) == 429:
            note_rate_limit_event(self._service_name, "wormsoft", operation)

    @staticmethod
    def _is_model_not_found_error(message: str) -> bool:
        lowered = str(message or "").lower()
        return (
            "unsupported model" in lowered
            or "unknown model" in lowered
            or "no such model" in lowered
            or "not found" in lowered
        )

    def normalize_error(self, exc: Exception, *, model: str) -> WormsoftTextError:
        response = getattr(exc, "response", None)
        status_code = getattr(exc, "status_code", None) or getattr(response, "status_code", None)
        message = str(exc)[:400]
        reason = "wormsoft_error"
        category = ERROR_UNKNOWN
        retryable = False
        if isinstance(exc, AuthenticationError) or status_code in {401, 403}:
            reason = "wormsoft_auth"
            category = ERROR_AUTH
        elif isinstance(exc, RateLimitError) or status_code == 429:
            reason = "wormsoft_429"
            category = ERROR_RATE_LIMITED
        elif status_code == 402:
            reason = "wormsoft_quota"
            category = ERROR_QUOTA_EXHAUSTED
        elif isinstance(exc, (APITimeoutError, httpx.TimeoutException)):
            reason = "wormsoft_timeout"
            category = ERROR_TIMEOUT
            retryable = True
        elif isinstance(exc, (APIConnectionError, httpx.TransportError)):
            reason = "wormsoft_connection"
            category = ERROR_CONNECTION
            retryable = True
        elif isinstance(exc, BadRequestError) or status_code == 400:
            if self._is_model_not_found_error(message):
                reason = "wormsoft_model_not_found"
                category = ERROR_MODEL_NOT_FOUND
            else:
                reason = "wormsoft_bad_request"
                category = ERROR_BAD_REQUEST
        elif isinstance(exc, APIStatusError) and status_code is not None and 500 <= status_code <= 599:
            reason = "wormsoft_5xx"
            category = ERROR_UPSTREAM_5XX
            retryable = status_code in {502, 503, 504}
        elif status_code == 503:
            reason = "wormsoft_unavailable"
            category = ERROR_PROVIDER_UNAVAILABLE
        elif status_code is not None:
            reason = f"wormsoft_{status_code}"

        return WormsoftTextError(
            f"wormsoft_request_failed model={model}: {message}",
            status_code=status_code,
            reason=reason,
            category=category,
            retryable=retryable,
        )

    async def _chat_once(
        self,
        *,
        system: str,
        user: str,
        model: str,
        max_tokens: int,
    ) -> Any:
        allowed, guard_reason = await self._guard.reserve_slot()
        if not allowed:
            raise WormsoftTextError(
                f"wormsoft_guard_blocked: {guard_reason}",
                reason=guard_reason,
                category=ERROR_PROVIDER_UNAVAILABLE,
                retryable=False,
            )

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
            await self._guard.record_success()
            return raw.parse()
        finally:
            self._release_request_slot()

    async def _chat_with_retry(
        self,
        *,
        system: str,
        user: str,
        model: str,
        max_tokens: int,
    ) -> Any:
        max_retries = max(0, min(2, int(self._settings.wormsoft_max_retries or 0)))
        last_error: WormsoftTextError | None = None
        for attempt in range(max_retries + 1):
            try:
                return await self._chat_once(
                    system=system,
                    user=user,
                    model=model,
                    max_tokens=max_tokens,
                )
            except WormsoftTextError as exc:
                last_error = exc
            except Exception as exc:
                last_error = self.normalize_error(exc, model=model)
            if last_error is None:
                break
            await self._guard.record_failure(
                status_code=last_error.status_code,
                reason=last_error.category,
                retryable=last_error.retryable,
            )
            if not last_error.retryable or attempt >= max_retries:
                raise last_error
            delay = min(3.0, 0.5 * (2**attempt) + random.uniform(0.0, 0.35))
            await asyncio.sleep(delay)
        raise last_error or WormsoftTextError("wormsoft_request_failed")

    async def count_tokens(self, model: str, text: str) -> int | None:
        del model, text
        return None

    async def budget_text(self, text: str, model: str, token_budget: int) -> BudgetedText:
        return await fit_text_to_token_budget(text, model, token_budget, self.count_tokens)

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
            raise WormsoftTextError(
                "wormsoft_api_key_missing",
                reason="wormsoft_api_key_missing",
                category=ERROR_PROVIDER_UNAVAILABLE,
                retryable=False,
            )
        model = str(model_override or "").strip()
        if not model:
            raise WormsoftTextError(
                f"wormsoft_model_missing task={task}",
                reason="wormsoft_model_missing",
                category=ERROR_BAD_REQUEST,
                retryable=False,
            )

        try:
            resp = await self._chat_with_retry(
                system=system,
                user=user,
                model=model,
                max_tokens=max_tokens,
            )
            usage = usage_from_openai_response(resp)
            actual_model = str(getattr(resp, "model", model) or model)
            note_llm_request(self._service_name, task, "wormsoft", model, actual_model, "ok")
            note_llm_usage(
                self._service_name,
                task,
                "wormsoft",
                model,
                actual_model,
                prompt_tokens=usage.prompt_tokens,
                completion_tokens=usage.completion_tokens,
                billable_tokens=usage.billable_tokens,
            )
            return GigaChatResponse(
                content=(resp.choices[0].message.content or ""),
                model=actual_model,
                requested_model=model,
                provider="wormsoft",
                usage=usage,
            )
        except WormsoftTextError as exc:
            self._observe_rate_limit(exc, task)
            note_llm_request(self._service_name, task, "wormsoft", model, "", "error")
            raise
        except Exception as exc:
            error = self.normalize_error(exc, model=model)
            self._observe_rate_limit(error, task)
            note_llm_request(self._service_name, task, "wormsoft", model, "", "error")
            raise error from exc
