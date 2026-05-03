"""Provider-aware LLM router that keeps Giga for embeddings/vision."""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from shared.config import get_settings
from shared.llm_routing import (
    PROVIDER_GIGACHAT,
    PROVIDER_WORMSOFT,
    ROUTABLE_LLM_TASKS,
    RUNTIME_LLM_ROUTING_REDIS_KEY,
    effective_llm_routing,
    normalize_provider,
)
from shared.metrics import note_llm_fallback
from worker.gigachat_client import GigaChatClient, GigaChatResponse
from worker.token_budget import fit_text_to_token_budget
from worker.wormsoft_client import WormsoftTextClient

logger = logging.getLogger(__name__)


class LLMRouterClient:
    """Facade that routes text tasks while preserving the Giga API surface."""

    def __init__(
        self,
        redis=None,
        *,
        service_name: str = "worker",
        gigachat_client: GigaChatClient | None = None,
        wormsoft_client: WormsoftTextClient | None = None,
    ):
        self._settings = get_settings()
        self._redis = redis
        self._service_name = service_name
        self._giga = gigachat_client or GigaChatClient(redis=redis, service_name=service_name)
        self._wormsoft = wormsoft_client or WormsoftTextClient(service_name=service_name)
        self._routing_overrides_loaded_at = 0.0
        self._routing_overrides_payload: dict[str, Any] | None = None
        self._runtime_redis = None

    async def close(self) -> None:
        if self._runtime_redis is not None:
            await self._runtime_redis.aclose()
        await self._wormsoft.close()
        await self._giga.close()

    @property
    def runtime_mode(self) -> str:
        return self._giga.runtime_mode

    @property
    def routing_settings(self):
        return effective_llm_routing(
            self._settings,
            self.runtime_mode,
            self._routing_overrides_payload,
        )

    async def refresh_runtime_overrides(self, *, force: bool = False) -> None:
        await self._giga.refresh_runtime_overrides(force=force)
        now = time.monotonic()
        if not force and now - self._routing_overrides_loaded_at < 15:
            return
        self._routing_overrides_loaded_at = now

        redis_client = self._redis
        if redis_client is None:
            try:
                import redis.asyncio as aioredis

                self._runtime_redis = self._runtime_redis or aioredis.from_url(
                    self._settings.redis_url,
                    decode_responses=True,
                )
                redis_client = self._runtime_redis
            except Exception as exc:
                logger.debug("llm_routing_redis_unavailable err=%s", exc)
                redis_client = None

        if redis_client is None:
            return

        try:
            raw_payload = await redis_client.get(RUNTIME_LLM_ROUTING_REDIS_KEY)
            if isinstance(raw_payload, bytes):
                raw_payload = raw_payload.decode("utf-8", errors="replace")
            if raw_payload:
                self._routing_overrides_payload = json.loads(str(raw_payload))
        except Exception as exc:
            logger.debug("llm_routing_refresh_failed err=%s", exc)

    def setting_value(self, name: str, default: Any = None) -> Any:
        return self._giga.setting_value(name, default)

    def setting_bool(self, name: str, default: bool = False) -> bool:
        return self._giga.setting_bool(name, default)

    def setting_str(self, name: str, default: str = "") -> str:
        return self._giga.setting_str(name, default)

    def setting_int(self, name: str, default: int) -> int:
        return self._giga.setting_int(name, default)

    def route_model_for_task(
        self,
        task: str,
        *,
        pro: bool = False,
        model_override: str | None = None,
    ) -> str:
        if model_override and model_override.strip():
            return model_override.strip()
        if task not in ROUTABLE_LLM_TASKS:
            return self._giga.route_model_for_task(task, pro=pro, model_override=model_override)
        route = self.routing_settings.route_for_task(task)
        return route.model or self._giga.route_model_for_task(task, pro=pro, model_override=model_override)

    def route_fallback_for_task(self, task: str) -> tuple[str, str]:
        if task not in ROUTABLE_LLM_TASKS:
            return self._giga.route_fallback_for_task(task)
        route = self.routing_settings.route_for_task(task)
        provider = normalize_provider(route.fallback_provider)
        model = route.fallback_model.strip()
        if not model:
            provider, model = self._giga.route_fallback_for_task(task)
        return provider, model

    async def count_tokens(self, model: str, text: str) -> int | None:
        normalized = str(model or "").strip()
        if normalized.startswith("GigaChat") or normalized.startswith("EmbeddingsGiga"):
            return await self._giga.count_tokens(normalized, text)
        return None

    async def budget_text(self, text: str, model: str, token_budget: int):
        return await fit_text_to_token_budget(text, model, token_budget, self.count_tokens)

    async def embed(self, text: str) -> list[float]:
        return await self._giga.embed(text)

    async def vision(self, image_bytes: bytes, prompt: str = "") -> GigaChatResponse:
        if prompt:
            return await self._giga.vision(image_bytes, prompt=prompt)
        return await self._giga.vision(image_bytes)

    async def _chat_gigachat(
        self,
        system: str,
        user: str,
        *,
        task: str,
        pro: bool,
        model_override: str | None,
        max_tokens: int,
    ) -> GigaChatResponse:
        return await self._giga.chat(
            system=system,
            user=user,
            task=task,
            pro=pro,
            model_override=model_override,
            provider_override=PROVIDER_GIGACHAT,
            max_tokens=max_tokens,
        )

    async def _chat_provider(
        self,
        provider: str,
        system: str,
        user: str,
        *,
        task: str,
        pro: bool,
        model_override: str | None,
        max_tokens: int,
    ) -> GigaChatResponse:
        normalized = normalize_provider(provider)
        if normalized == PROVIDER_WORMSOFT:
            return await self._wormsoft.chat(
                system=system,
                user=user,
                task=task,
                pro=pro,
                model_override=model_override,
                provider_override=PROVIDER_WORMSOFT,
                max_tokens=max_tokens,
            )
        return await self._chat_gigachat(
            system,
            user,
            task=task,
            pro=pro,
            model_override=model_override,
            max_tokens=max_tokens,
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
        await self.refresh_runtime_overrides()
        route = self.routing_settings.route_for_task(task) if task in ROUTABLE_LLM_TASKS else None
        provider = normalize_provider(provider_override or getattr(route, "provider", PROVIDER_GIGACHAT))
        if provider_override:
            if provider == PROVIDER_WORMSOFT:
                requested_model = (
                    str(model_override or "").strip()
                    or str(getattr(route, "model", "") or self._settings.wormsoft_model_default).strip()
                )
            else:
                requested_model = self._giga.route_model_for_task(task, pro=pro, model_override=model_override)
        else:
            requested_model = self.route_model_for_task(task, pro=pro, model_override=model_override)

        if provider != PROVIDER_WORMSOFT:
            return await self._chat_gigachat(
                system,
                user,
                task=task,
                pro=pro,
                model_override=requested_model,
                max_tokens=max_tokens,
            )

        if not self._wormsoft.is_available:
            fallback_provider, fallback_model = self.route_fallback_for_task(task)
            logger.warning(
                "wormsoft_unavailable_fallback task=%s fallback_provider=%s fallback_model=%s",
                task,
                fallback_provider,
                fallback_model,
            )
            note_llm_fallback(
                self._service_name,
                task,
                from_provider=PROVIDER_WORMSOFT,
                from_requested_model=requested_model,
                from_actual_model="",
                to_provider=fallback_provider,
                to_model=fallback_model,
                reason="provider_unavailable",
            )
            return await self._chat_provider(
                fallback_provider,
                system,
                user,
                task=task,
                pro=False,
                model_override=fallback_model,
                max_tokens=max_tokens,
            )

        try:
            return await self._wormsoft.chat(
                system=system,
                user=user,
                task=task,
                pro=pro,
                model_override=requested_model,
                provider_override=PROVIDER_WORMSOFT,
                max_tokens=max_tokens,
            )
        except Exception as exc:
            if provider_override:
                raise
            fallback_provider, fallback_model = self.route_fallback_for_task(task)
            logger.warning(
                "wormsoft_request_failed task=%s requested_model=%s fallback_provider=%s fallback_model=%s err=%s",
                task,
                requested_model,
                fallback_provider,
                fallback_model,
                exc,
            )
            note_llm_fallback(
                self._service_name,
                task,
                from_provider=PROVIDER_WORMSOFT,
                from_requested_model=requested_model,
                from_actual_model="",
                to_provider=fallback_provider,
                to_model=fallback_model,
                reason=type(exc).__name__[:64],
            )
            return await self._chat_provider(
                fallback_provider,
                system,
                user,
                task=task,
                pro=False,
                model_override=fallback_model,
                max_tokens=max_tokens,
            )
