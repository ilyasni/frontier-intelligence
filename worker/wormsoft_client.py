"""Wormsoft text-only client using the OpenAI-compatible API."""
from __future__ import annotations

import asyncio
import logging
import time

import httpx
from openai import AsyncOpenAI

from shared.config import get_settings
from shared.metrics import note_llm_request, note_llm_usage, note_rate_limit_event
from worker.llm_types import GigaChatResponse, usage_from_openai_response
from worker.token_budget import BudgetedText, fit_text_to_token_budget

logger = logging.getLogger(__name__)


class WormsoftTextClient:
    """OpenAI-style Wormsoft client for text tasks only."""

    def __init__(self, *, service_name: str = "worker"):
        settings = get_settings()
        self._settings = settings
        self._service_name = service_name
        self._request_sem = asyncio.Semaphore(
            max(1, int(settings.wormsoft_max_simultaneous_requests or 1))
        )
        self._min_request_interval_s = max(
            0.0,
            float(int(getattr(settings, "wormsoft_min_request_interval_ms", 2000) or 2000))
            / 1000.0,
        )
        self._request_gap_lock = asyncio.Lock()
        self._last_request_started_at = 0.0
        self._http_client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0))
        self._client = AsyncOpenAI(
            base_url=settings.wormsoft_api_base.rstrip("/"),
            api_key=settings.wormsoft_api_key or "missing",
            http_client=self._http_client,
            max_retries=max(0, int(settings.wormsoft_max_retries or 0)),
        )

    async def close(self) -> None:
        await self._client.close()

    @property
    def is_available(self) -> bool:
        return bool(self._settings.wormsoft_api_key)

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
    def _is_model_not_found_error(exc: Exception) -> bool:
        text = str(exc)
        lowered = text.lower()
        return "not found" in lowered or "unknown model" in lowered or "no such model" in lowered

    async def count_tokens(self, model: str, text: str) -> int | None:
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
        if not self.is_available:
            raise RuntimeError("wormsoft_api_key_missing")
        model = str(model_override or "").strip()
        if not model:
            raise ValueError(f"wormsoft_model_missing task={task}")

        try:
            await self._acquire_request_slot()
            try:
                resp = await self._client.chat.completions.create(
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
            usage = usage_from_openai_response(resp)
            actual_model = getattr(resp, "model", model)
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
                content=resp.choices[0].message.content or "",
                model=actual_model,
                requested_model=model,
                provider="wormsoft",
                usage=usage,
            )
        except Exception as exc:
            self._observe_rate_limit(exc, task)
            note_llm_request(self._service_name, task, "wormsoft", model, "", "error")
            raise
