"""Provider-aware LLM router that keeps Giga for embeddings/vision."""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from admin.backend.services.openrouter_picker import (
    pick_model,
    record_call_result,
    task_family_for_task,
    task_family_for_vision,
)
from shared.config import get_settings
from shared.llm_routing import (
    PROVIDER_GIGACHAT,
    PROVIDER_OPENROUTER,
    PROVIDER_POLZA,
    PROVIDER_WORMSOFT,
    ROUTABLE_LLM_TASKS,
    RUNTIME_LLM_ROUTING_REDIS_KEY,
    effective_llm_routing,
    normalize_provider,
)
from shared.metrics import note_llm_fallback, note_openrouter_vision_fallback
from worker.gigachat_client import GigaChatClient, GigaChatResponse
from worker.openrouter_client import OpenRouterVisionClient, OpenRouterVisionError
from worker.openrouter_guard import OpenRouterFreeGuard, OpenRouterVisionGuard
from worker.openrouter_text_client import OpenRouterTextClient, OpenRouterTextError
from worker.polza_client import PolzaVisionClient
from worker.polza_text_client import PolzaTextClient
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
        openrouter_client: OpenRouterVisionClient | None = None,
        polza_client: PolzaVisionClient | None = None,
        openrouter_text_client: OpenRouterTextClient | None = None,
        polza_text_client: PolzaTextClient | None = None,
        openrouter_guard: OpenRouterVisionGuard | None = None,
        openrouter_text_guard: OpenRouterFreeGuard | None = None,
    ):
        self._settings = get_settings()
        self._redis = redis
        self._service_name = service_name
        self._giga = gigachat_client or GigaChatClient(redis=redis, service_name=service_name)
        self._wormsoft = wormsoft_client or WormsoftTextClient(service_name=service_name)
        self._openrouter = openrouter_client or OpenRouterVisionClient(service_name=service_name)
        self._polza = polza_client or PolzaVisionClient(service_name=service_name)
        self._openrouter_text = openrouter_text_client or OpenRouterTextClient(
            service_name=service_name
        )
        self._polza_text = polza_text_client or PolzaTextClient(service_name=service_name)
        self._openrouter_guard = openrouter_guard or OpenRouterVisionGuard(
            redis=redis,
            service_name=service_name,
        )
        self._openrouter_text_guard = openrouter_text_guard or OpenRouterFreeGuard(
            redis=redis,
            service_name=service_name,
            publish_vision_metrics=False,
        )
        self._routing_overrides_loaded_at = 0.0
        self._routing_overrides_payload: dict[str, Any] | None = None
        self._runtime_redis = None

    async def close(self) -> None:
        if self._runtime_redis is not None:
            await self._runtime_redis.aclose()
        await self._openrouter_text_guard.close()
        await self._openrouter_guard.close()
        await self._polza_text.close()
        await self._openrouter_text.close()
        await self._polza.close()
        await self._openrouter.close()
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
        return route.model or self._giga.route_model_for_task(
            task,
            pro=pro,
            model_override=model_override,
        )

    def route_fallback_for_task(self, task: str) -> tuple[str, str]:
        fallbacks = self.route_fallbacks_for_task(task)
        if fallbacks:
            return fallbacks[0]
        return self._giga.route_fallback_for_task(task)

    def route_fallbacks_for_task(self, task: str) -> list[tuple[str, str]]:
        if task not in ROUTABLE_LLM_TASKS:
            provider, model = self._giga.route_fallback_for_task(task)
            return [(provider, model)]
        route = self.routing_settings.route_for_task(task)
        chain = route.fallback_chain()
        if chain:
            return chain
        provider, model = self._giga.route_fallback_for_task(task)
        return [(provider, model)]

    def _model_for_provider_override(
        self,
        task: str,
        provider: str,
        model_override: str | None,
    ) -> str:
        if model_override and model_override.strip():
            return model_override.strip()
        normalized = normalize_provider(provider)
        if task in ROUTABLE_LLM_TASKS:
            route = self.routing_settings.route_for_task(task)
            if normalize_provider(route.provider) == normalized and route.model.strip():
                return route.model.strip()
            for fallback_provider, fallback_model in self.route_fallbacks_for_task(task):
                if normalize_provider(fallback_provider) == normalized and fallback_model.strip():
                    return fallback_model.strip()
        if normalized == PROVIDER_WORMSOFT:
            return str(self._settings.wormsoft_model_default or "").strip()
        if normalized == PROVIDER_OPENROUTER:
            return str(getattr(self._openrouter_text, "default_model", "") or "").strip()
        if normalized == PROVIDER_POLZA:
            return str(getattr(self._polza_text, "default_model", "") or "").strip()
        return self._giga.route_model_for_task(task, pro=False, model_override=model_override)

    async def count_tokens(self, model: str, text: str) -> int | None:
        normalized = str(model or "").strip()
        if normalized.startswith("GigaChat") or normalized.startswith("EmbeddingsGiga"):
            return await self._giga.count_tokens(normalized, text)
        return None

    async def budget_text(self, text: str, model: str, token_budget: int):
        return await fit_text_to_token_budget(text, model, token_budget, self.count_tokens)

    async def embed(self, text: str) -> list[float]:
        return await self._giga.embed(text)

    def _vision_provider_for_quality_tier(self, quality_tier: str) -> str:
        tier = str(quality_tier or "standard").strip().lower() or "standard"
        if tier in {"trusted", "primary"}:
            return PROVIDER_GIGACHAT
        if not self.setting_bool("vision_routing_enabled", True):
            return PROVIDER_GIGACHAT
        return "openrouter"

    def _vision_gigachat_model(self) -> str:
        return (
            self.setting_str("gigachat_model_vision").strip()
            or self.setting_str("gigachat_model_pro", "GigaChat-2-Pro").strip()
            or "GigaChat-2-Pro"
        )

    def _note_vision_provider_fallback(self, to_provider: str, to_model: str, reason: str) -> None:
        note_llm_fallback(
            self._service_name,
            "vision",
            from_provider="openrouter",
            from_requested_model=getattr(self._openrouter, "default_model", "openrouter/free"),
            from_actual_model="",
            to_provider=to_provider,
            to_model=to_model,
            reason=reason,
        )

    @staticmethod
    def _uses_dynamic_openrouter(model: str) -> bool:
        return str(model or "").strip().lower() == "openrouter/free"

    async def _vision_gigachat(self, image_bytes: bytes, prompt: str) -> GigaChatResponse:
        if prompt:
            return await self._giga.vision(image_bytes, prompt=prompt)
        return await self._giga.vision(image_bytes)

    async def _vision_polza(
        self,
        image_bytes: bytes,
        prompt: str,
        fallback_reason: str,
    ) -> GigaChatResponse:
        if prompt:
            response = await self._polza.vision(image_bytes, prompt=prompt)
        else:
            response = await self._polza.vision(image_bytes)
        if fallback_reason:
            return GigaChatResponse(
                content=response.content,
                model=response.model,
                requested_model=response.requested_model,
                provider=response.provider,
                usage=response.usage,
                parsed=response.parsed,
                fallback_reason=fallback_reason,
            )
        return response

    async def _vision_gigachat_with_reason(
        self,
        image_bytes: bytes,
        prompt: str,
        fallback_reason: str,
    ) -> GigaChatResponse:
        response = await self._vision_gigachat(image_bytes, prompt)
        if fallback_reason:
            return GigaChatResponse(
                content=response.content,
                model=response.model,
                requested_model=response.requested_model,
                provider=response.provider,
                usage=response.usage,
                parsed=response.parsed,
                fallback_reason=fallback_reason,
            )
        return response

    async def _vision_fallback(
        self,
        image_bytes: bytes,
        prompt: str,
        reason: str,
    ) -> GigaChatResponse:
        if self._polza.is_available:
            self._note_vision_provider_fallback("polza", self._polza.default_model, reason)
            note_openrouter_vision_fallback(self._service_name, "polza", reason)
            try:
                return await self._vision_polza(image_bytes, prompt, reason)
            except Exception:
                self._note_vision_provider_fallback(
                    "gigachat",
                    self._vision_gigachat_model(),
                    "polza_error",
                )
                note_openrouter_vision_fallback(self._service_name, "gigachat", "polza_error")
                return await self._vision_gigachat_with_reason(image_bytes, prompt, "polza_error")
        self._note_vision_provider_fallback("gigachat", self._vision_gigachat_model(), reason)
        note_openrouter_vision_fallback(self._service_name, "gigachat", reason)
        return await self._vision_gigachat_with_reason(image_bytes, prompt, reason)

    async def vision(
        self,
        image_bytes: bytes,
        prompt: str = "",
        *,
        quality_tier: str = "standard",
    ) -> GigaChatResponse:
        if self._vision_provider_for_quality_tier(quality_tier) != "openrouter":
            return await self._vision_gigachat(image_bytes, prompt)

        model_override: str | None = None
        reserved_model_id: str | None = None
        if self._uses_dynamic_openrouter(getattr(self._openrouter, "default_model", "")):
            task_family = task_family_for_vision(quality_tier)
            decision = await pick_model(
                task_family or "vision_mass",
                service_name=self._service_name,
            )
            model_override = str(decision.get("model_id") or "").strip() or None
            reserved_model_id = model_override
            if not model_override:
                return await self._vision_fallback(
                    image_bytes,
                    prompt,
                    str(decision.get("reason") or "no_capable_model"),
                )

        started_at = time.monotonic()
        try:
            if prompt:
                response = await self._openrouter.vision(
                    image_bytes,
                    prompt=prompt,
                    model_override=model_override,
                )
            else:
                response = await self._openrouter.vision(
                    image_bytes,
                    model_override=model_override,
                )
            if reserved_model_id:
                await record_call_result(
                    reserved_model_id,
                    success=True,
                    latency_ms=(time.monotonic() - started_at) * 1000.0,
                    status_code=200,
                    service_name=self._service_name,
                )
            return response
        except OpenRouterVisionError as exc:
            if reserved_model_id:
                try:
                    await record_call_result(
                        reserved_model_id,
                        success=False,
                        latency_ms=(time.monotonic() - started_at) * 1000.0,
                        status_code=exc.status_code,
                        or_reset_at=exc.reset_at,
                        service_name=self._service_name,
                    )
                except Exception:
                    logger.debug("openrouter_guard_record_failure_failed", exc_info=True)
            return await self._vision_fallback(image_bytes, prompt, exc.reason)
        except Exception:
            if reserved_model_id:
                try:
                    await record_call_result(
                        reserved_model_id,
                        success=False,
                        latency_ms=(time.monotonic() - started_at) * 1000.0,
                        status_code=None,
                        service_name=self._service_name,
                    )
                except Exception:
                    logger.debug("openrouter_guard_record_failure_failed", exc_info=True)
            return await self._vision_fallback(image_bytes, prompt, "openrouter_error")

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
        if normalized == PROVIDER_OPENROUTER:
            return await self._openrouter_text.chat(
                system=system,
                user=user,
                task=task,
                pro=pro,
                model_override=model_override,
                provider_override=PROVIDER_OPENROUTER,
                max_tokens=max_tokens,
            )
        if normalized == PROVIDER_POLZA:
            return await self._polza_text.chat(
                system=system,
                user=user,
                task=task,
                pro=pro,
                model_override=model_override,
                provider_override=PROVIDER_POLZA,
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

    @staticmethod
    def _is_openrouter_free_model(model: str) -> bool:
        normalized = str(model or "").strip().lower()
        return normalized == "openrouter/free" or normalized.endswith(":free")

    def _provider_available(self, provider: str, model: str) -> tuple[bool, str]:
        normalized = normalize_provider(provider)
        if normalized == PROVIDER_WORMSOFT:
            return self._wormsoft.is_available, "provider_unavailable"
        if normalized == PROVIDER_OPENROUTER:
            if not self._openrouter_text.is_available:
                return False, "provider_unavailable"
            if not model.strip():
                return False, "model_missing"
        if normalized == PROVIDER_POLZA:
            if not self._polza_text.is_available:
                return False, "provider_unavailable"
            if not model.strip():
                return False, "model_missing"
        return True, "ok"

    def _route_attempts(
        self,
        task: str,
        *,
        provider_override: str | None,
        requested_model: str,
    ) -> list[tuple[str, str]]:
        if task not in ROUTABLE_LLM_TASKS:
            return [(PROVIDER_GIGACHAT, requested_model)]

        route = self.routing_settings.route_for_task(task)
        attempts: list[tuple[str, str]] = []
        if provider_override:
            attempts.append((normalize_provider(provider_override), requested_model))
        else:
            attempts.append((normalize_provider(route.provider), requested_model))
        attempts.extend(self.route_fallbacks_for_task(task))

        deduped: list[tuple[str, str]] = []
        for provider, model in attempts:
            pair = (normalize_provider(provider), str(model or "").strip())
            if not pair[1] or pair in deduped:
                continue
            deduped.append(pair)
        return deduped

    @staticmethod
    def _fallback_reason_from_error(exc: Exception) -> str:
        reason = getattr(exc, "reason", "")
        if reason:
            return str(reason)[:64]
        return type(exc).__name__[:64]

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
        provider = normalize_provider(
            provider_override or getattr(route, "provider", PROVIDER_GIGACHAT)
        )
        if provider_override:
            requested_model = self._model_for_provider_override(task, provider, model_override)
        else:
            requested_model = self.route_model_for_task(
                task,
                pro=pro,
                model_override=model_override,
            )

        attempts = self._route_attempts(
            task,
            provider_override=provider_override,
            requested_model=requested_model,
        )
        last_error: Exception | None = None

        for index, (attempt_provider, attempt_model) in enumerate(attempts):
            next_attempt = attempts[index + 1] if index + 1 < len(attempts) else None
            effective_model = attempt_model
            reserved_openrouter_model: str | None = None
            available, availability_reason = self._provider_available(
                attempt_provider,
                effective_model,
            )
            if not available:
                last_error = RuntimeError(availability_reason)
                if next_attempt:
                    note_llm_fallback(
                        self._service_name,
                        task,
                        from_provider=attempt_provider,
                        from_requested_model=attempt_model,
                        from_actual_model="",
                        to_provider=next_attempt[0],
                        to_model=next_attempt[1],
                        reason=availability_reason,
                    )
                    continue
                break

            if (
                attempt_provider == PROVIDER_OPENROUTER
                and self._uses_dynamic_openrouter(effective_model)
            ):
                decision = await pick_model(
                    task_family_for_task(task),
                    service_name=self._service_name,
                )
                picked_model = str(decision.get("model_id") or "").strip()
                if not picked_model:
                    last_error = RuntimeError(str(decision.get("reason") or "no_capable_model"))
                    if next_attempt:
                        note_llm_fallback(
                            self._service_name,
                            task,
                            from_provider=attempt_provider,
                            from_requested_model=effective_model,
                            from_actual_model="",
                            to_provider=next_attempt[0],
                            to_model=next_attempt[1],
                            reason=str(decision.get("reason") or "no_capable_model"),
                        )
                        continue
                    break
                effective_model = picked_model
                reserved_openrouter_model = picked_model

            started_at = time.monotonic()
            try:
                response = await self._chat_provider(
                    attempt_provider,
                    system,
                    user,
                    task=task,
                    pro=pro if index == 0 else False,
                    model_override=effective_model,
                    max_tokens=max_tokens,
                )
                if reserved_openrouter_model:
                    await record_call_result(
                        reserved_openrouter_model,
                        success=True,
                        latency_ms=(time.monotonic() - started_at) * 1000.0,
                        status_code=200,
                        service_name=self._service_name,
                    )
                return response
            except OpenRouterTextError as exc:
                last_error = exc
                if reserved_openrouter_model:
                    try:
                        await record_call_result(
                            reserved_openrouter_model,
                            success=False,
                            latency_ms=(time.monotonic() - started_at) * 1000.0,
                            status_code=exc.status_code,
                            or_reset_at=exc.reset_at,
                            service_name=self._service_name,
                        )
                    except Exception:
                        logger.debug("openrouter_text_guard_record_failure_failed", exc_info=True)
            except Exception as exc:
                last_error = exc
                if reserved_openrouter_model:
                    try:
                        await record_call_result(
                            reserved_openrouter_model,
                            success=False,
                            latency_ms=(time.monotonic() - started_at) * 1000.0,
                            status_code=None,
                            service_name=self._service_name,
                        )
                    except Exception:
                        logger.debug("openrouter_text_guard_record_failure_failed", exc_info=True)

            if next_attempt:
                note_llm_fallback(
                    self._service_name,
                    task,
                    from_provider=attempt_provider,
                    from_requested_model=effective_model,
                    from_actual_model="",
                    to_provider=next_attempt[0],
                    to_model=next_attempt[1],
                    reason=self._fallback_reason_from_error(last_error),
                )
                continue

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"no_llm_route_available task={task}")
