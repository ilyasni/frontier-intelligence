"""Provider-aware LLM router that keeps Giga for embeddings/vision."""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any

from shared.config import get_settings
from shared.llm_control_plane import (
    CONTROL_PLANE_POLICY_REDIS_KEY,
    EXECUTION_ROLE_PRIMARY,
    EXECUTION_ROLE_SHADOW,
    POLICY_MODE_DEGRADED,
    POLICY_MODE_SHADOW_EVAL,
    AttemptOutcome,
    ExecutionReceipt,
    ProviderError,
    ProviderExecutionRequest,
    RoutingCandidate,
    RoutingDecision,
    RoutingPolicyV2,
    ROUTING_EVENTS_MAXLEN,
    ROUTING_EVENTS_REDIS_KEY,
    ShadowComparison,
    ShadowEvaluationResult,
    TASK_FAMILY_EMBEDDINGS,
    TASK_FAMILY_TEXT_GENERATION,
    TASK_FAMILY_VISION_GENERATION,
    apply_execution_mode,
    default_routing_policy_v2,
    embedding_profile_for_model,
    fallback_allowed_for_mode,
    normalize_policy_mode,
    refresh_policy_candidate_models_from_derived,
    task_family_for_task as control_plane_family_for_task,
    ERROR_THROTTLED_LOCAL,
)
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
from shared.metrics import note_llm_cost, note_llm_fallback, note_llm_throttle_event, note_openrouter_vision_fallback
from worker.gigachat_client import GigaChatClient, GigaChatResponse
from worker.openrouter_client import OpenRouterVisionClient, OpenRouterVisionError
from worker.openrouter_guard import OpenRouterFreeGuard, OpenRouterVisionGuard
from worker.openrouter_text_client import OpenRouterTextClient, OpenRouterTextError
from worker.polza_client import PolzaVisionClient
from worker.polza_text_client import PolzaTextClient
from worker.provider_adapters import (
    GigaChatAdapter,
    OpenRouterAdapter,
    PolzaAdapter,
    WormsoftAdapter,
)
from worker.provider_budget_manager import ProviderBudgetManager
from worker.wormsoft_credit_guard import WormsoftCreditGuard
from worker.provider_circuit_breaker import ProviderCircuitBreaker
from worker.provider_quota_guard import ProviderPublishedQuotaGuard
from worker.token_budget import fit_text_to_token_budget
from worker.wormsoft_client import WormsoftTextClient, WormsoftTextError

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
        self._control_plane_policy_payload: dict[str, Any] | None = None
        self._control_plane_policy_source = "default_policy"
        self._runtime_redis = None
        self._circuit_breaker = ProviderCircuitBreaker(
            redis=redis,
            service_name=service_name,
            settings=self._settings,
        )
        self._budget_manager = ProviderBudgetManager(redis=redis, settings=self._settings)
        self._wormsoft_credit_guard = WormsoftCreditGuard(self._budget_manager, settings=self._settings)
        self._published_quota_guard = ProviderPublishedQuotaGuard(
            redis=redis,
            settings=self._settings,
        )
        self._shadow_eval_semaphore = asyncio.Semaphore(
            max(1, int(getattr(self._settings, "llm_shadow_eval_max_concurrency", 2) or 2))
        )
        self._shadow_tasks: set[asyncio.Task[Any]] = set()
        self._provider_adapters = {
            PROVIDER_WORMSOFT: WormsoftAdapter(
                service_name=service_name,
                settings=self._settings,
                redis=redis,
                text_client=self._wormsoft,
            ),
            PROVIDER_OPENROUTER: OpenRouterAdapter(
                service_name=service_name,
                settings=self._settings,
                text_client=self._openrouter_text,
                vision_client=self._openrouter,
                text_guard=self._openrouter_text_guard,
                vision_guard=self._openrouter_guard,
            ),
            PROVIDER_POLZA: PolzaAdapter(
                service_name=service_name,
                settings=self._settings,
                text_client=self._polza_text,
                vision_client=self._polza,
            ),
            PROVIDER_GIGACHAT: GigaChatAdapter(
                service_name=service_name,
                settings=self._settings,
                giga_client=self._giga,
            ),
        }
        self._last_routing_decision: RoutingDecision | None = None
        self._last_execution_receipt: ExecutionReceipt | None = None

    async def close(self) -> None:
        await self.drain_shadow_evaluations(cancel=True)
        if self._runtime_redis is not None:
            await self._runtime_redis.aclose()
        await self._budget_manager.close()
        await self._published_quota_guard.close()
        await self._circuit_breaker.close()
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
    def last_routing_decision(self) -> RoutingDecision | None:
        return self._last_routing_decision

    @property
    def last_execution_receipt(self) -> ExecutionReceipt | None:
        return self._last_execution_receipt

    async def drain_shadow_evaluations(self, *, cancel: bool = False) -> None:
        if not self._shadow_tasks:
            return
        pending = list(self._shadow_tasks)
        if cancel:
            for task in pending:
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    @property
    def routing_settings(self):
        return effective_llm_routing(
            self._settings,
            self.runtime_mode,
            self._routing_overrides_payload,
        )

    @property
    def routing_policy(self):
        fallback_policy = default_routing_policy_v2(
            self._settings,
            self.runtime_mode,
            self._routing_overrides_payload,
        )
        payload = self._control_plane_policy_payload
        if not isinstance(payload, dict):
            self._control_plane_policy_source = "default_policy"
            return fallback_policy
        try:
            policy = RoutingPolicyV2.model_validate(payload)
        except Exception:
            self._control_plane_policy_source = "invalid_policy_fallback"
            return fallback_policy
        self._control_plane_policy_source = "db_policy"
        return refresh_policy_candidate_models_from_derived(policy, fallback_policy)

    def _adapter_for_provider(self, provider: str):
        return self._provider_adapters[normalize_provider(provider)]

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
            raw_policy = await redis_client.get(CONTROL_PLANE_POLICY_REDIS_KEY)
            if isinstance(raw_policy, bytes):
                raw_policy = raw_policy.decode("utf-8", errors="replace")
            if raw_policy:
                self._control_plane_policy_payload = json.loads(str(raw_policy))
            else:
                self._control_plane_policy_payload = None
                self._control_plane_policy_source = "default_policy"
        except Exception as exc:
            logger.debug("llm_routing_refresh_failed err=%s", exc)
            self._control_plane_policy_payload = None
            self._control_plane_policy_source = "default_policy"

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

    async def _record_simple_execution(
        self,
        *,
        task: str,
        task_family: str,
        requested_provider: str,
        requested_model: str,
        response: GigaChatResponse | None,
        status: str,
        fallback_reason: str = "",
        latency_ms: float = 0.0,
    ) -> None:
        decision = RoutingDecision(
            task=task,
            task_family=task_family,
            mode=POLICY_MODE_DEGRADED,
            requested_provider=requested_provider,
            requested_model=requested_model,
            selected_provider=response.provider if response else requested_provider,
            selected_model=response.requested_model if response else requested_model,
            fallback_allowed=True,
            fallback_exception_only=True,
            considered_candidates=[
                RoutingCandidate(
                    provider=requested_provider,
                    model=requested_model,
                    capability_tags=[task_family],
                )
            ],
        )
        self._last_routing_decision = decision
        receipt = ExecutionReceipt(
            task=task,
            task_family=task_family,
            status=status,
            requested_provider=requested_provider,
            requested_model=requested_model,
            actual_provider=response.provider if response else requested_provider,
            actual_model=response.actual_model if response else requested_model,
            fallback_reason=fallback_reason or (response.fallback_reason if response else ""),
            latency_ms=latency_ms,
            decision=decision,
        )
        receipt = await self._record_finops_receipt(receipt)
        self._last_execution_receipt = receipt
        await self._append_routing_event("routing_decision_made", decision.model_dump())
        await self._append_routing_event("routing_execution_finished", receipt.model_dump())

    def _embedding_profile_reason(self, model: str) -> str:
        profile = embedding_profile_for_model(model)
        expected_dim = int(getattr(self._settings, "embed_dim", 0) or 0)
        profile_dim = int(profile.get("dimension") or 0)
        if expected_dim and profile_dim and expected_dim != profile_dim:
            return f"embedding_profile_mismatch_dim_{profile_dim}_vs_{expected_dim}"
        return ""

    @staticmethod
    def _note_skipped_candidate(
        skipped_candidates: list[dict[str, str]],
        attempt_outcomes: list[AttemptOutcome],
        *,
        provider: str,
        model: str,
        reason: str,
    ) -> None:
        """Record a candidate skipped by a guard into both routing-trace lists."""
        skipped_candidates.append({"provider": provider, "model": model, "reason": reason})
        attempt_outcomes.append(
            AttemptOutcome(provider=provider, model=model, status="skipped", reason=reason)
        )

    async def embed(self, text: str, *, purpose: str = "document") -> list[float]:
        await self.refresh_runtime_overrides()
        task = "embed"
        task_family = TASK_FAMILY_EMBEDDINGS
        candidates, policy_mode, fallback_exception_only = self._policy_candidates(
            task=task,
            task_family=task_family,
            provider_override=None,
            model_override=None,
        )
        attempts = [(candidate.provider, candidate.model) for candidate in candidates]
        last_error: Exception | None = None
        last_provider_error: ProviderError | None = None
        attempt_outcomes: list[AttemptOutcome] = []
        fallback_reason = ""
        skipped_candidates: list[dict[str, Any]] = []
        default_model = str(
            getattr(self._settings, "gigachat_embeddings_model", "EmbeddingsGigaR")
        )
        decision = self._routing_decision(
            task=task,
            task_family=task_family,
            attempts=attempts,
            selected_provider=attempts[0][0] if attempts else PROVIDER_GIGACHAT,
            selected_model=attempts[0][1] if attempts else default_model,
            skipped_candidates=skipped_candidates,
            mode=policy_mode,
            fallback_exception_only=fallback_exception_only,
        )
        self._last_routing_decision = decision
        self._last_execution_receipt = None
        await self._append_routing_event("routing_decision_made", decision.model_dump())

        for candidate in candidates:
            attempt_provider = candidate.provider
            requested_model = candidate.model
            adapter = self._adapter_for_provider(attempt_provider)
            request = ProviderExecutionRequest(
                system="",
                user="",
                text=text,
                task=task,
                task_family=task_family,
                model=requested_model,
                embedding_purpose=purpose,
            )
            effective_model, resolution_metadata = await self._resolve_provider_model(
                attempt_provider,
                request,
            )
            request = request.model_copy(
                update={
                    "model": effective_model,
                    "metadata": resolution_metadata,
                }
            )
            profile_reason = self._embedding_profile_reason(effective_model)
            if profile_reason:
                last_error = RuntimeError(profile_reason)
                fallback_reason = fallback_reason or profile_reason
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=profile_reason,
                )
                break

            allowed_by_circuit, circuit_reason = await self._circuit_breaker.reserve(
                attempt_provider,
                effective_model,
            )
            if not allowed_by_circuit:
                last_error = RuntimeError(circuit_reason)
                fallback_reason = fallback_reason or circuit_reason
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=circuit_reason,
                )
                break

            allowed_capacity, capacity_reason = await self._reserve_provider_capacity(
                attempt_provider,
                effective_model,
                task_family=task_family,
            )
            if not allowed_capacity:
                last_error = RuntimeError(capacity_reason)
                fallback_reason = fallback_reason or capacity_reason
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=capacity_reason,
                )
                break

            allowed_budget, budget_reason = await self._allow_budget_reservation(
                attempt_provider,
                effective_model,
                task_family=task_family,
                execution_role=EXECUTION_ROLE_PRIMARY,
            )
            if not allowed_budget:
                last_error = RuntimeError(budget_reason)
                fallback_reason = fallback_reason or budget_reason
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=budget_reason,
                )
                break

            allowed_quota, quota_reason = await self._allow_published_quota(
                attempt_provider,
                effective_model,
                execution_role=EXECUTION_ROLE_PRIMARY,
            )
            if not allowed_quota:
                last_error = RuntimeError(quota_reason)
                fallback_reason = fallback_reason or quota_reason
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=quota_reason,
                )
                break

            reservation = await self._budget_manager.reserve(
                provider=attempt_provider,
                model=effective_model,
                task_family=task_family,
            )
            started_at = time.monotonic()
            try:
                vector = await adapter.embed(request)
                latency_ms = (time.monotonic() - started_at) * 1000.0
                await self._record_provider_success(attempt_provider, effective_model)
                prompt_tokens = await self.count_tokens(effective_model, text) or 0
                if prompt_tokens <= 0:
                    # Fallback estimate when upstream token counter is unavailable.
                    prompt_tokens = max(1, len(text) // 4)
                billable_tokens = max(0, int(prompt_tokens))
                estimated_cost = float(billable_tokens)
                reservation = await self._budget_manager.commit(
                    reservation,
                    actual_units=1.0,
                    prompt_tokens=prompt_tokens,
                    billable_tokens=billable_tokens,
                )
                attempt_outcomes.append(
                    AttemptOutcome(
                        provider=attempt_provider,
                        model=effective_model,
                        status="ok",
                        actual_model=effective_model,
                    )
                )
                decision = self._routing_decision(
                    task=task,
                    task_family=task_family,
                    attempts=attempts,
                    selected_provider=attempt_provider,
                    selected_model=effective_model,
                    skipped_candidates=skipped_candidates,
                    mode=policy_mode,
                    fallback_exception_only=fallback_exception_only,
                )
                self._last_routing_decision = decision
                receipt = ExecutionReceipt(
                    task=task,
                    task_family=task_family,
                    status="ok",
                    requested_provider=attempts[0][0] if attempts else attempt_provider,
                    requested_model=attempts[0][1] if attempts else effective_model,
                    actual_provider=attempt_provider,
                    actual_model=effective_model,
                    fallback_reason=fallback_reason,
                    latency_ms=latency_ms,
                    cost_estimate=estimated_cost,
                    actual_cost=estimated_cost,
                    budget_attribution=reservation,
                    decision=decision,
                    attempts=attempt_outcomes,
                )
                receipt = await self._record_finops_receipt(receipt)
                self._last_execution_receipt = receipt
                await adapter.record_result(receipt=receipt, error=None)
                await self._append_routing_event(
                    "routing_execution_finished",
                    receipt.model_dump(),
                )
                await self._launch_shadow_evaluation(
                    task=task,
                    task_family=task_family,
                    mode=policy_mode,
                    attempts=attempts,
                    selected_provider=attempt_provider,
                    selected_model=effective_model,
                    skipped_candidates=skipped_candidates,
                    request=request.model_copy(update={"model": effective_model}),
                    primary_receipt=receipt,
                )
                return vector
            except Exception as exc:
                last_error = exc

            last_provider_error = adapter.normalize_error(last_error, model=effective_model)
            await self._record_provider_failure(
                attempt_provider,
                effective_model,
                last_provider_error,
            )
            await self._budget_manager.release(reservation)
            failed_receipt = ExecutionReceipt(
                task=task,
                task_family=task_family,
                status="error",
                requested_provider=attempts[0][0] if attempts else attempt_provider,
                requested_model=attempts[0][1] if attempts else effective_model,
                actual_provider=attempt_provider,
                actual_model=effective_model,
                fallback_reason=last_provider_error.reason,
                latency_ms=(time.monotonic() - started_at) * 1000.0,
                budget_attribution=reservation,
                provider_error=last_provider_error,
            )
            failed_receipt = await self._record_finops_receipt(failed_receipt)
            await adapter.record_result(receipt=failed_receipt, error=last_provider_error)
            fallback_reason = (
                last_provider_error.reason or self._fallback_reason_from_error(last_error)
            )
            attempt_outcomes.append(
                AttemptOutcome(
                    provider=attempt_provider,
                    model=effective_model,
                    status="error",
                    reason=fallback_reason,
                )
            )
            break

        if attempts:
            receipt = ExecutionReceipt(
                task=task,
                task_family=task_family,
                status="error",
                requested_provider=attempts[0][0],
                requested_model=attempts[0][1],
                actual_provider=normalize_provider(
                    getattr(last_provider_error, "provider", attempts[0][0])
                ),
                actual_model=attempts[0][1],
                fallback_reason=fallback_reason
                or self._fallback_reason_from_error(last_error or RuntimeError("embed_failed")),
                decision=self._routing_decision(
                    task=task,
                    task_family=task_family,
                    attempts=attempts,
                    selected_provider=attempts[0][0],
                    selected_model=attempts[0][1],
                    skipped_candidates=skipped_candidates,
                    mode=policy_mode,
                    fallback_exception_only=fallback_exception_only,
                ),
                attempts=attempt_outcomes,
                provider_error=last_provider_error,
            )
            receipt = await self._record_finops_receipt(receipt)
            self._last_execution_receipt = receipt
            await self._append_routing_event(
                "routing_execution_finished",
                receipt.model_dump(),
            )
        if last_error is not None:
            raise last_error
        raise RuntimeError("no_embedding_route_available")

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

    def _vision_provider_available(self, provider: str, model: str) -> tuple[bool, str]:
        normalized = normalize_provider(provider)
        if normalized == PROVIDER_WORMSOFT:
            if not bool(getattr(self._wormsoft, "is_available", False)):
                return False, "provider_unavailable"
            if not str(model or "").strip():
                return False, "model_missing"
        elif normalized == PROVIDER_OPENROUTER:
            if not bool(getattr(self._openrouter, "is_available", True)):
                return False, "provider_unavailable"
            if not str(model or "").strip():
                return False, "model_missing"
        elif normalized == PROVIDER_POLZA:
            if not self._polza.is_available:
                return False, "provider_unavailable"
            if not str(model or "").strip():
                return False, "model_missing"
        elif normalized == PROVIDER_GIGACHAT:
            if self._giga is None:
                return False, "provider_unavailable"
        return True, "ok"

    async def vision(
        self,
        image_bytes: bytes,
        prompt: str = "",
        *,
        quality_tier: str = "standard",
    ) -> GigaChatResponse:
        started_at = time.monotonic()
        candidates, policy_mode, fallback_exception_only = self._policy_candidates(
            task="vision",
            task_family=TASK_FAMILY_VISION_GENERATION,
            provider_override=None,
            model_override=None,
        )
        wormsoft_in = any(candidate.provider == PROVIDER_WORMSOFT for candidate in candidates)
        if wormsoft_in:
            order = {
                PROVIDER_WORMSOFT: 0,
                PROVIDER_OPENROUTER: 1,
                PROVIDER_POLZA: 2,
                PROVIDER_GIGACHAT: 3,
            }
            candidates.sort(key=lambda item: order.get(item.provider, 99))
        elif self._vision_provider_for_quality_tier(quality_tier) != "openrouter":
            candidates.sort(key=lambda item: 0 if item.provider == PROVIDER_GIGACHAT else 1)
        else:
            order = {
                PROVIDER_OPENROUTER: 0,
                PROVIDER_POLZA: 1,
                PROVIDER_GIGACHAT: 2,
            }
            candidates.sort(key=lambda item: order.get(item.provider, 99))
        attempts = [(candidate.provider, candidate.model) for candidate in candidates]
        skipped_candidates: list[dict[str, Any]] = []
        attempt_outcomes: list[AttemptOutcome] = []
        fallback_reason = ""
        initial_provider, initial_model = attempts[0] if attempts else (PROVIDER_GIGACHAT, self._vision_gigachat_model())
        decision = self._routing_decision(
            task="vision",
            task_family=TASK_FAMILY_VISION_GENERATION,
            attempts=attempts,
            selected_provider=initial_provider,
            selected_model=initial_model,
            skipped_candidates=skipped_candidates,
            mode=policy_mode,
            fallback_exception_only=fallback_exception_only,
        )
        self._last_routing_decision = decision
        self._last_execution_receipt = None
        await self._append_routing_event("routing_decision_made", decision.model_dump())

        last_error: Exception | None = None
        last_provider_error: ProviderError | None = None
        for index, candidate in enumerate(candidates):
            attempt_provider = candidate.provider
            effective_model = candidate.model
            next_attempt = attempts[index + 1] if index + 1 < len(attempts) else None
            available, availability_reason = self._vision_provider_available(attempt_provider, effective_model)
            if not available:
                last_error = RuntimeError(availability_reason)
                if not fallback_reason:
                    fallback_reason = availability_reason
                self._note_skipped_candidate(skipped_candidates, attempt_outcomes, provider=attempt_provider, model=effective_model, reason=availability_reason)
                if next_attempt:
                    if attempt_provider == PROVIDER_OPENROUTER:
                        self._note_vision_provider_fallback(next_attempt[0], next_attempt[1], availability_reason)
                        note_openrouter_vision_fallback(self._service_name, next_attempt[0], availability_reason)
                    continue
                break

            allowed_by_circuit, circuit_reason = await self._circuit_breaker.reserve(attempt_provider, effective_model)
            if not allowed_by_circuit:
                last_error = RuntimeError(circuit_reason)
                if not fallback_reason:
                    fallback_reason = circuit_reason
                self._note_skipped_candidate(skipped_candidates, attempt_outcomes, provider=attempt_provider, model=effective_model, reason=circuit_reason)
                if next_attempt:
                    if attempt_provider == PROVIDER_OPENROUTER:
                        self._note_vision_provider_fallback(next_attempt[0], next_attempt[1], circuit_reason)
                        note_openrouter_vision_fallback(self._service_name, next_attempt[0], circuit_reason)
                    continue
                break

            adapter = self._adapter_for_provider(attempt_provider)
            resolved_request = ProviderExecutionRequest(
                system="",
                user="",
                task="vision",
                task_family=TASK_FAMILY_VISION_GENERATION,
                model=effective_model,
                image_bytes=image_bytes,
                prompt=prompt,
                quality_tier=quality_tier,
            )
            effective_model, resolution_metadata = await self._resolve_provider_model(
                attempt_provider,
                resolved_request,
            )
            if not effective_model:
                reason = str(resolution_metadata.get("reason") or "no_capable_model")
                last_error = RuntimeError(reason)
                if not fallback_reason:
                    fallback_reason = reason
                self._note_skipped_candidate(skipped_candidates, attempt_outcomes, provider=attempt_provider, model=candidate.model, reason=reason)
                if next_attempt:
                    self._note_vision_provider_fallback(next_attempt[0], next_attempt[1], reason)
                    note_openrouter_vision_fallback(self._service_name, next_attempt[0], reason)
                    continue
                break
            resolved_request = resolved_request.model_copy(
                update={
                    "model": effective_model,
                    "metadata": resolution_metadata,
                }
            )

            allowed_capacity, capacity_reason = await self._reserve_provider_capacity(
                attempt_provider,
                effective_model,
                task_family=TASK_FAMILY_VISION_GENERATION,
            )
            if not allowed_capacity:
                last_error = RuntimeError(capacity_reason)
                if not fallback_reason:
                    fallback_reason = capacity_reason
                self._note_skipped_candidate(skipped_candidates, attempt_outcomes, provider=attempt_provider, model=effective_model, reason=capacity_reason)
                if next_attempt:
                    if attempt_provider == PROVIDER_OPENROUTER:
                        self._note_vision_provider_fallback(next_attempt[0], next_attempt[1], capacity_reason)
                        note_openrouter_vision_fallback(self._service_name, next_attempt[0], capacity_reason)
                    continue
                break

            allowed_budget, budget_reason = await self._allow_budget_reservation(
                attempt_provider,
                effective_model,
                task_family=TASK_FAMILY_VISION_GENERATION,
                execution_role=EXECUTION_ROLE_PRIMARY,
            )
            if not allowed_budget:
                last_error = RuntimeError(budget_reason)
                if not fallback_reason:
                    fallback_reason = budget_reason
                self._note_skipped_candidate(skipped_candidates, attempt_outcomes, provider=attempt_provider, model=effective_model, reason=budget_reason)
                if next_attempt:
                    if attempt_provider == PROVIDER_OPENROUTER:
                        self._note_vision_provider_fallback(next_attempt[0], next_attempt[1], budget_reason)
                        note_openrouter_vision_fallback(self._service_name, next_attempt[0], budget_reason)
                    continue
                break

            allowed_quota, quota_reason = await self._allow_published_quota(
                attempt_provider,
                effective_model,
                execution_role=EXECUTION_ROLE_PRIMARY,
            )
            if not allowed_quota:
                last_error = RuntimeError(quota_reason)
                if not fallback_reason:
                    fallback_reason = quota_reason
                self._note_skipped_candidate(skipped_candidates, attempt_outcomes, provider=attempt_provider, model=effective_model, reason=quota_reason)
                if next_attempt:
                    if attempt_provider == PROVIDER_OPENROUTER:
                        self._note_vision_provider_fallback(next_attempt[0], next_attempt[1], quota_reason)
                        note_openrouter_vision_fallback(self._service_name, next_attempt[0], quota_reason)
                    continue
                break

            reservation = await self._budget_manager.reserve(
                provider=attempt_provider,
                model=effective_model,
                task_family=TASK_FAMILY_VISION_GENERATION,
            )
            started_at = time.monotonic()
            try:
                response = await adapter.vision(resolved_request)
                latency_ms = (time.monotonic() - started_at) * 1000.0
                if fallback_reason and not response.fallback_reason:
                    response = GigaChatResponse(
                        content=response.content,
                        model=response.model,
                        requested_model=response.requested_model,
                        provider=response.provider,
                        usage=response.usage,
                        parsed=response.parsed,
                        fallback_reason=fallback_reason,
                    )
                await self._record_provider_success(attempt_provider, effective_model)
                attempt_outcomes.append(AttemptOutcome(provider=attempt_provider, model=effective_model, status="ok", actual_model=response.actual_model))
                decision = self._routing_decision(
                    task="vision",
                    task_family=TASK_FAMILY_VISION_GENERATION,
                    attempts=attempts,
                    selected_provider=attempt_provider,
                    selected_model=effective_model,
                    skipped_candidates=skipped_candidates,
                    mode=policy_mode,
                    fallback_exception_only=fallback_exception_only,
                )
                self._last_routing_decision = decision
                cost_estimate = adapter.estimate_cost(
                    response=response,
                    requested_model=effective_model,
                    actual_model=response.actual_model,
                )
                reservation = await self._budget_manager.commit(
                    reservation,
                    actual_units=cost_estimate if cost_estimate is not None else 1.0,
                    prompt_tokens=response.usage.prompt_tokens,
                    completion_tokens=response.usage.completion_tokens,
                    billable_tokens=response.usage.billable_tokens,
                )
                receipt = ExecutionReceipt(
                    task="vision",
                    task_family=TASK_FAMILY_VISION_GENERATION,
                    status="ok",
                    requested_provider=attempts[0][0] if attempts else attempt_provider,
                    requested_model=attempts[0][1] if attempts else effective_model,
                    actual_provider=response.provider,
                    actual_model=response.actual_model,
                    fallback_reason=response.fallback_reason,
                    latency_ms=latency_ms,
                    cost_estimate=cost_estimate,
                    budget_attribution=reservation,
                    decision=decision,
                    attempts=attempt_outcomes,
                )
                receipt = await self._record_finops_receipt(receipt)
                self._last_execution_receipt = receipt
                await adapter.record_result(receipt=receipt, error=None)
                await self._append_routing_event("routing_execution_finished", receipt.model_dump())
                await self._launch_shadow_evaluation(
                    task="vision",
                    task_family=TASK_FAMILY_VISION_GENERATION,
                    mode=policy_mode,
                    attempts=attempts,
                    selected_provider=attempt_provider,
                    selected_model=effective_model,
                    skipped_candidates=skipped_candidates,
                    request=resolved_request,
                    primary_receipt=receipt,
                )
                return response
            except Exception as exc:
                last_error = exc

            last_provider_error = adapter.normalize_error(last_error, model=effective_model)
            await self._record_provider_failure(attempt_provider, effective_model, last_provider_error)
            await self._budget_manager.release(reservation)
            failed_receipt = ExecutionReceipt(
                task="vision",
                task_family=TASK_FAMILY_VISION_GENERATION,
                status="error",
                requested_provider=attempts[0][0] if attempts else attempt_provider,
                requested_model=attempts[0][1] if attempts else effective_model,
                actual_provider=attempt_provider,
                actual_model=effective_model,
                fallback_reason=last_provider_error.reason,
                latency_ms=(time.monotonic() - started_at) * 1000.0,
                budget_attribution=reservation,
                provider_error=last_provider_error,
            )
            failed_receipt = await self._record_finops_receipt(failed_receipt)
            await adapter.record_result(receipt=failed_receipt, error=last_provider_error)
            fallback_reason = last_provider_error.reason or self._fallback_reason_from_error(last_error)
            attempt_outcomes.append(AttemptOutcome(provider=attempt_provider, model=effective_model, status="error", reason=fallback_reason))
            if next_attempt:
                if attempt_provider == PROVIDER_OPENROUTER:
                    self._note_vision_provider_fallback(next_attempt[0], next_attempt[1], fallback_reason)
                    note_openrouter_vision_fallback(self._service_name, next_attempt[0], fallback_reason)
                continue

        if attempts:
            receipt = ExecutionReceipt(
                task="vision",
                task_family=TASK_FAMILY_VISION_GENERATION,
                status="error",
                requested_provider=attempts[0][0],
                requested_model=attempts[0][1],
                actual_provider=normalize_provider(getattr(last_provider_error, "provider", attempts[-1][0])),
                actual_model=attempts[-1][1],
                fallback_reason=fallback_reason or self._fallback_reason_from_error(last_error or RuntimeError("vision_routing_failed")),
                decision=self._routing_decision(
                    task="vision",
                    task_family=TASK_FAMILY_VISION_GENERATION,
                    attempts=attempts,
                    selected_provider=attempts[-1][0],
                    selected_model=attempts[-1][1],
                    skipped_candidates=skipped_candidates,
                    mode=policy_mode,
                    fallback_exception_only=fallback_exception_only,
                ),
                attempts=attempt_outcomes,
                provider_error=last_provider_error,
            )
            receipt = await self._record_finops_receipt(receipt)
            self._last_execution_receipt = receipt
            await self._append_routing_event("routing_execution_finished", receipt.model_dump())
        if last_error is not None:
            raise last_error
        raise RuntimeError("no_vision_route_available")

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

    def _routing_decision(
        self,
        *,
        task: str,
        task_family: str | None = None,
        attempts: list[tuple[str, str]],
        selected_provider: str,
        selected_model: str,
        skipped_candidates: list[dict[str, Any]] | None = None,
        mode: str = POLICY_MODE_DEGRADED,
        fallback_exception_only: bool = True,
    ) -> RoutingDecision:
        candidates = [
            RoutingCandidate(provider=provider, model=model, capability_tags=["text"])
            for provider, model in attempts
        ]
        requested_provider, requested_model = attempts[0] if attempts else (selected_provider, selected_model)
        return RoutingDecision(
            task=task,
            task_family=task_family or control_plane_family_for_task(task),
            mode=normalize_policy_mode(mode),
            requested_provider=requested_provider,
            requested_model=requested_model,
            selected_provider=selected_provider,
            selected_model=selected_model,
            fallback_allowed=fallback_allowed_for_mode(mode, len(candidates)),
            fallback_exception_only=fallback_exception_only,
            considered_candidates=candidates,
            skipped_candidates=list(skipped_candidates or []),
        )

    def _shadow_candidates(
        self,
        attempts: list[tuple[str, str]],
        *,
        selected_provider: str,
        selected_model: str,
    ) -> list[tuple[str, str]]:
        selected_pair = (normalize_provider(selected_provider), str(selected_model or "").strip())
        for index, pair in enumerate(attempts):
            normalized = (normalize_provider(pair[0]), str(pair[1] or "").strip())
            if normalized == selected_pair:
                return attempts[index + 1 :]
        return []

    def _shadow_enabled(self, mode: str) -> bool:
        return bool(getattr(self._settings, "llm_shadow_eval_enabled", True)) and (
            normalize_policy_mode(mode) == POLICY_MODE_SHADOW_EVAL
        )

    @staticmethod
    def _shadow_comparison(
        primary_receipt: ExecutionReceipt,
        shadow_receipt: ExecutionReceipt,
    ) -> ShadowComparison:
        primary_cost = (
            primary_receipt.actual_cost
            if primary_receipt.actual_cost is not None
            else primary_receipt.cost_estimate
        )
        shadow_cost = (
            shadow_receipt.actual_cost
            if shadow_receipt.actual_cost is not None
            else shadow_receipt.cost_estimate
        )
        primary_budget = dict(primary_receipt.budget_attribution or {})
        shadow_budget = dict(shadow_receipt.budget_attribution or {})
        return ShadowComparison(
            status_match=primary_receipt.status == shadow_receipt.status,
            provider_match=primary_receipt.actual_provider == shadow_receipt.actual_provider,
            same_model=primary_receipt.actual_model == shadow_receipt.actual_model,
            latency_delta_ms=shadow_receipt.latency_ms - primary_receipt.latency_ms,
            cost_delta=(
                None
                if primary_cost is None or shadow_cost is None
                else float(shadow_cost) - float(primary_cost)
            ),
            prompt_tokens_delta=int(shadow_budget.get("prompt_tokens", 0) or 0)
            - int(primary_budget.get("prompt_tokens", 0) or 0),
            completion_tokens_delta=int(shadow_budget.get("completion_tokens", 0) or 0)
            - int(primary_budget.get("completion_tokens", 0) or 0),
            billable_tokens_delta=int(shadow_budget.get("billable_tokens", 0) or 0)
            - int(primary_budget.get("billable_tokens", 0) or 0),
        )

    def _finalize_execution_receipt(self, receipt: ExecutionReceipt) -> ExecutionReceipt:
        estimate = (
            float(receipt.cost_estimate)
            if receipt.cost_estimate is not None
            else None
        )
        actual = (
            float(receipt.actual_cost)
            if receipt.actual_cost is not None
            else estimate
        )
        drift = (
            float(receipt.cost_drift)
            if receipt.cost_drift is not None
            else (float(actual) - float(estimate) if actual is not None and estimate is not None else None)
        )
        budget = dict(receipt.budget_attribution or {})
        workspace_id = str(receipt.workspace_id or budget.get("workspace_id") or "")
        budget_class = str(
            receipt.budget_class
            or budget.get("budget_class")
            or getattr(receipt.decision, "budget_class", "")
            or ""
        )
        budget.setdefault("workspace_id", workspace_id)
        budget.setdefault("budget_class", budget_class)
        budget.setdefault("execution_role", receipt.execution_role)
        budget.setdefault("actual_provider", receipt.actual_provider)
        budget.setdefault("actual_model", receipt.actual_model)
        if estimate is not None:
            budget["cost_estimate"] = estimate
        if actual is not None:
            budget["actual_cost"] = actual
        if drift is not None:
            budget["cost_drift"] = drift
        return receipt.model_copy(
            update={
                "actual_cost": actual,
                "cost_drift": drift,
                "workspace_id": workspace_id,
                "budget_class": budget_class,
                "budget_attribution": budget,
            }
        )

    async def _record_finops_receipt(self, receipt: ExecutionReceipt) -> ExecutionReceipt:
        finalized = self._finalize_execution_receipt(receipt)
        note_llm_cost(
            self._service_name,
            finalized.actual_provider,
            finalized.task_family,
            finalized.execution_role,
            estimated_cost=finalized.cost_estimate,
            actual_cost=finalized.actual_cost,
            cost_drift=finalized.cost_drift,
        )
        await self._budget_manager.record_execution_receipt(finalized)
        await self._wormsoft_credit_guard.record(finalized)
        return finalized

    async def _publish_shadow_plan(
        self,
        *,
        task: str,
        task_family: str,
        mode: str,
        attempts: list[tuple[str, str]],
        selected_provider: str,
        selected_model: str,
        skipped_candidates: list[dict[str, Any]],
        shadow_group_id: str = "",
    ) -> None:
        if not self._shadow_enabled(mode):
            return
        shadow_attempts = [
            {"provider": provider, "model": model}
            for provider, model in self._shadow_candidates(
                attempts,
                selected_provider=selected_provider,
                selected_model=selected_model,
            )
        ]
        if not shadow_attempts:
            return
        await self._append_routing_event(
            "routing_shadow_plan",
            {
                "shadow_group_id": shadow_group_id,
                "task": task,
                "task_family": task_family,
                "mode": mode,
                "selected_provider": selected_provider,
                "selected_model": selected_model,
                "shadow_candidates": shadow_attempts,
                "skipped_candidates": list(skipped_candidates),
                "timestamp": time.time(),
            },
        )

    def _track_shadow_task(self, task: asyncio.Task[Any]) -> None:
        self._shadow_tasks.add(task)
        task.add_done_callback(self._shadow_tasks.discard)

    async def _launch_shadow_evaluation(
        self,
        *,
        task: str,
        task_family: str,
        mode: str,
        attempts: list[tuple[str, str]],
        selected_provider: str,
        selected_model: str,
        skipped_candidates: list[dict[str, Any]],
        request: ProviderExecutionRequest,
        primary_receipt: ExecutionReceipt,
    ) -> None:
        if not self._shadow_enabled(mode):
            return
        shadow_attempts = self._shadow_candidates(
            attempts,
            selected_provider=selected_provider,
            selected_model=selected_model,
        )
        if not shadow_attempts:
            return
        shadow_group_id = uuid.uuid4().hex
        await self._publish_shadow_plan(
            task=task,
            task_family=task_family,
            mode=mode,
            attempts=attempts,
            selected_provider=selected_provider,
            selected_model=selected_model,
            skipped_candidates=skipped_candidates,
            shadow_group_id=shadow_group_id,
        )
        shadow_request = request.model_copy(
            update={
                "metadata": {
                    **dict(request.metadata or {}),
                    "shadow_group_id": shadow_group_id,
                }
            }
        )
        task_obj = asyncio.create_task(
            self._run_shadow_evaluation(
                task=task,
                task_family=task_family,
                attempts=shadow_attempts,
                mode=mode,
                request=shadow_request,
                primary_receipt=primary_receipt,
                shadow_group_id=shadow_group_id,
            ),
            name=f"shadow-eval-{task}-{shadow_group_id[:8]}",
        )
        self._track_shadow_task(task_obj)

    async def _run_shadow_evaluation(
        self,
        *,
        task: str,
        task_family: str,
        attempts: list[tuple[str, str]],
        mode: str,
        request: ProviderExecutionRequest,
        primary_receipt: ExecutionReceipt,
        shadow_group_id: str,
    ) -> None:
        timeout = float(getattr(self._settings, "llm_shadow_eval_timeout_sec", 15.0) or 15.0)
        async with self._shadow_eval_semaphore:
            await self._append_routing_event(
                "routing_shadow_execution_started",
                {
                    "shadow_group_id": shadow_group_id,
                    "task": task,
                    "task_family": task_family,
                    "mode": mode,
                    "attempts": [
                        {"provider": provider, "model": model}
                        for provider, model in attempts
                    ],
                    "timestamp": time.time(),
                },
            )
            try:
                await asyncio.wait_for(
                    self._execute_shadow_chain(
                        task=task,
                        task_family=task_family,
                        attempts=attempts,
                        request=request,
                        primary_receipt=primary_receipt,
                        shadow_group_id=shadow_group_id,
                        mode=mode,
                    ),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                timeout_receipt = ExecutionReceipt(
                    task=task,
                    task_family=task_family,
                    status="error",
                    execution_role=EXECUTION_ROLE_SHADOW,
                    shadow_group_id=shadow_group_id,
                    requested_provider=attempts[0][0],
                    requested_model=attempts[0][1],
                    actual_provider=attempts[0][0],
                    actual_model=attempts[0][1],
                    fallback_reason="shadow_timeout",
                    provider_error=ProviderError(
                        provider=attempts[0][0],
                        category="timeout",
                        message="shadow evaluation timed out",
                        retryable=True,
                        reason="shadow_timeout",
                    ),
                    decision=self._routing_decision(
                        task=task,
                        task_family=task_family,
                        attempts=attempts,
                        selected_provider=attempts[0][0],
                        selected_model=attempts[0][1],
                        skipped_candidates=[],
                        mode=mode,
                    ),
                )
                timeout_receipt = await self._record_finops_receipt(timeout_receipt)
                result = ShadowEvaluationResult(
                    group_id=shadow_group_id,
                    primary_receipt=primary_receipt,
                    shadow_receipt=timeout_receipt,
                    comparison=self._shadow_comparison(primary_receipt, timeout_receipt),
                )
                await self._append_routing_event(
                    "routing_shadow_execution_finished",
                    result.model_dump(),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("shadow_evaluation_failed", exc_info=True)

    async def _execute_shadow_chain(
        self,
        *,
        task: str,
        task_family: str,
        attempts: list[tuple[str, str]],
        request: ProviderExecutionRequest,
        primary_receipt: ExecutionReceipt,
        shadow_group_id: str,
        mode: str,
    ) -> None:
        attempt_outcomes: list[AttemptOutcome] = []
        skipped_candidates: list[dict[str, Any]] = []
        last_receipt: ExecutionReceipt | None = None

        for provider, requested_model in attempts:
            effective_model = requested_model
            if task_family == TASK_FAMILY_VISION_GENERATION:
                available, reason = self._vision_provider_available(provider, effective_model)
            elif task_family == TASK_FAMILY_EMBEDDINGS:
                available, reason = True, "ok"
            else:
                available, reason = self._provider_available(provider, effective_model)
            if not available:
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=provider,
                    model=effective_model,
                    reason=reason,
                )
                continue

            allowed_by_circuit, circuit_reason = await self._circuit_breaker.reserve(
                provider,
                effective_model,
            )
            if not allowed_by_circuit:
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=provider,
                    model=effective_model,
                    reason=circuit_reason,
                )
                continue

            resolved_request = request.model_copy(update={"model": effective_model})
            effective_model, resolution_metadata = await self._resolve_provider_model(
                provider,
                resolved_request,
            )
            if not effective_model:
                reason = str(resolution_metadata.get("reason") or "no_capable_model")
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=provider,
                    model=requested_model,
                    reason=reason,
                )
                continue
            resolved_request = resolved_request.model_copy(
                update={
                    "model": effective_model,
                    "metadata": {
                        **dict(request.metadata or {}),
                        **dict(resolution_metadata or {}),
                    },
                }
            )

            if task_family == TASK_FAMILY_EMBEDDINGS:
                profile_reason = self._embedding_profile_reason(effective_model)
                if profile_reason:
                    self._note_skipped_candidate(
                        skipped_candidates,
                        attempt_outcomes,
                        provider=provider,
                        model=effective_model,
                        reason=profile_reason,
                    )
                    continue

            allowed_capacity, capacity_reason = await self._reserve_provider_capacity(
                provider,
                effective_model,
                task_family=task_family,
            )
            if not allowed_capacity:
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=provider,
                    model=effective_model,
                    reason=capacity_reason,
                )
                continue

            allowed_budget, budget_reason = await self._allow_budget_reservation(
                provider,
                effective_model,
                task_family=task_family,
                execution_role=EXECUTION_ROLE_SHADOW,
            )
            if not allowed_budget:
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=provider,
                    model=effective_model,
                    reason=budget_reason,
                )
                continue

            allowed_quota, quota_reason = await self._allow_published_quota(
                provider,
                effective_model,
                execution_role=EXECUTION_ROLE_SHADOW,
            )
            if not allowed_quota:
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=provider,
                    model=effective_model,
                    reason=quota_reason,
                )
                continue

            reservation = await self._budget_manager.reserve(
                provider=provider,
                model=effective_model,
                task_family=task_family,
                execution_role=EXECUTION_ROLE_SHADOW,
                metadata={"shadow_group_id": shadow_group_id},
            )
            adapter = self._adapter_for_provider(provider)
            started_at = time.monotonic()
            try:
                if task_family == TASK_FAMILY_VISION_GENERATION:
                    response = await adapter.vision(resolved_request)
                    cost_estimate = adapter.estimate_cost(
                        response=response,
                        requested_model=effective_model,
                        actual_model=response.actual_model,
                    )
                    reservation = await self._budget_manager.commit(
                        reservation,
                        actual_units=cost_estimate if cost_estimate is not None else 1.0,
                        prompt_tokens=response.usage.prompt_tokens,
                        completion_tokens=response.usage.completion_tokens,
                        billable_tokens=response.usage.billable_tokens,
                    )
                    attempt_outcomes.append(
                        AttemptOutcome(
                            provider=provider,
                            model=effective_model,
                            status="ok",
                            actual_model=response.actual_model,
                        )
                    )
                    shadow_receipt = ExecutionReceipt(
                        task=task,
                        task_family=task_family,
                        status="ok",
                        execution_role=EXECUTION_ROLE_SHADOW,
                        shadow_group_id=shadow_group_id,
                        requested_provider=attempts[0][0],
                        requested_model=attempts[0][1],
                        actual_provider=response.provider,
                        actual_model=response.actual_model,
                        fallback_reason=response.fallback_reason,
                        latency_ms=(time.monotonic() - started_at) * 1000.0,
                        cost_estimate=cost_estimate,
                        budget_attribution=reservation,
                        decision=self._routing_decision(
                            task=task,
                            task_family=task_family,
                            attempts=attempts,
                            selected_provider=provider,
                            selected_model=effective_model,
                            skipped_candidates=skipped_candidates,
                            mode=mode,
                        ),
                        attempts=attempt_outcomes,
                    )
                    shadow_receipt = await self._record_finops_receipt(shadow_receipt)
                elif task_family == TASK_FAMILY_EMBEDDINGS:
                    await adapter.embed(resolved_request)
                    reservation = await self._budget_manager.commit(
                        reservation,
                        actual_units=1.0,
                    )
                    attempt_outcomes.append(
                        AttemptOutcome(
                            provider=provider,
                            model=effective_model,
                            status="ok",
                            actual_model=effective_model,
                        )
                    )
                    shadow_receipt = ExecutionReceipt(
                        task=task,
                        task_family=task_family,
                        status="ok",
                        execution_role=EXECUTION_ROLE_SHADOW,
                        shadow_group_id=shadow_group_id,
                        requested_provider=attempts[0][0],
                        requested_model=attempts[0][1],
                        actual_provider=provider,
                        actual_model=effective_model,
                        latency_ms=(time.monotonic() - started_at) * 1000.0,
                        budget_attribution=reservation,
                        decision=self._routing_decision(
                            task=task,
                            task_family=task_family,
                            attempts=attempts,
                            selected_provider=provider,
                            selected_model=effective_model,
                            skipped_candidates=skipped_candidates,
                            mode=mode,
                        ),
                        attempts=attempt_outcomes,
                    )
                    shadow_receipt = await self._record_finops_receipt(shadow_receipt)
                else:
                    response = await adapter.execute(resolved_request)
                    cost_estimate = adapter.estimate_cost(
                        response=response,
                        requested_model=effective_model,
                        actual_model=response.actual_model,
                    )
                    reservation = await self._budget_manager.commit(
                        reservation,
                        actual_units=cost_estimate if cost_estimate is not None else 1.0,
                        prompt_tokens=response.usage.prompt_tokens,
                        completion_tokens=response.usage.completion_tokens,
                        billable_tokens=response.usage.billable_tokens,
                    )
                    attempt_outcomes.append(
                        AttemptOutcome(
                            provider=provider,
                            model=effective_model,
                            status="ok",
                            actual_model=response.actual_model,
                        )
                    )
                    shadow_receipt = ExecutionReceipt(
                        task=task,
                        task_family=task_family,
                        status="ok",
                        execution_role=EXECUTION_ROLE_SHADOW,
                        shadow_group_id=shadow_group_id,
                        requested_provider=attempts[0][0],
                        requested_model=attempts[0][1],
                        actual_provider=response.provider,
                        actual_model=response.actual_model,
                        fallback_reason=response.fallback_reason,
                        latency_ms=(time.monotonic() - started_at) * 1000.0,
                        cost_estimate=cost_estimate,
                        budget_attribution=reservation,
                        decision=self._routing_decision(
                            task=task,
                            task_family=task_family,
                            attempts=attempts,
                            selected_provider=provider,
                            selected_model=effective_model,
                            skipped_candidates=skipped_candidates,
                            mode=mode,
                        ),
                        attempts=attempt_outcomes,
                    )
                    shadow_receipt = await self._record_finops_receipt(shadow_receipt)

                result = ShadowEvaluationResult(
                    group_id=shadow_group_id,
                    primary_receipt=primary_receipt,
                    shadow_receipt=shadow_receipt,
                    comparison=self._shadow_comparison(primary_receipt, shadow_receipt),
                )
                await self._append_routing_event(
                    "routing_shadow_execution_finished",
                    result.model_dump(),
                )
                return
            except Exception as exc:
                await self._budget_manager.release(reservation)
                provider_error = adapter.normalize_error(exc, model=effective_model)
                attempt_outcomes.append(
                    AttemptOutcome(
                        provider=provider,
                        model=effective_model,
                        status="error",
                        reason=provider_error.reason or self._fallback_reason_from_error(exc),
                    )
                )
                last_receipt = ExecutionReceipt(
                    task=task,
                    task_family=task_family,
                    status="error",
                    execution_role=EXECUTION_ROLE_SHADOW,
                    shadow_group_id=shadow_group_id,
                    requested_provider=attempts[0][0],
                    requested_model=attempts[0][1],
                    actual_provider=provider,
                    actual_model=effective_model,
                    fallback_reason=provider_error.reason
                    or self._fallback_reason_from_error(exc),
                    latency_ms=(time.monotonic() - started_at) * 1000.0,
                    budget_attribution=reservation,
                    decision=self._routing_decision(
                        task=task,
                        task_family=task_family,
                        attempts=attempts,
                        selected_provider=provider,
                        selected_model=effective_model,
                        skipped_candidates=skipped_candidates,
                        mode=mode,
                    ),
                    attempts=attempt_outcomes,
                    provider_error=provider_error,
                )
                last_receipt = await self._record_finops_receipt(last_receipt)

        if last_receipt is None:
            last_receipt = ExecutionReceipt(
                task=task,
                task_family=task_family,
                status="error",
                execution_role=EXECUTION_ROLE_SHADOW,
                shadow_group_id=shadow_group_id,
                requested_provider=attempts[0][0],
                requested_model=attempts[0][1],
                actual_provider=attempts[-1][0],
                actual_model=attempts[-1][1],
                fallback_reason="no_shadow_route_available",
                decision=self._routing_decision(
                    task=task,
                    task_family=task_family,
                    attempts=attempts,
                    selected_provider=attempts[-1][0],
                    selected_model=attempts[-1][1],
                    skipped_candidates=skipped_candidates,
                    mode=mode,
                ),
                attempts=attempt_outcomes,
                provider_error=ProviderError(
                    provider=attempts[-1][0],
                    category="provider_unavailable",
                    message="no shadow route available",
                    retryable=False,
                    reason="no_shadow_route_available",
                ),
            )
            last_receipt = await self._record_finops_receipt(last_receipt)
        result = ShadowEvaluationResult(
            group_id=shadow_group_id,
            primary_receipt=primary_receipt,
            shadow_receipt=last_receipt,
            comparison=self._shadow_comparison(primary_receipt, last_receipt),
        )
        await self._append_routing_event(
            "routing_shadow_execution_finished",
            result.model_dump(),
        )

    async def _append_routing_event(self, event_type: str, payload: dict[str, Any]) -> None:
        redis_client = self._redis
        if redis_client is None:
            try:
                import redis.asyncio as aioredis

                self._runtime_redis = self._runtime_redis or aioredis.from_url(
                    self._settings.redis_url,
                    decode_responses=True,
                )
                redis_client = self._runtime_redis
            except Exception:
                return
        if redis_client is None:
            return
        event = {
            "event": event_type,
            "service": self._service_name,
            "payload": payload,
            "policy_source": self._control_plane_policy_source,
            "timestamp": time.time(),
        }
        try:
            await redis_client.lpush(ROUTING_EVENTS_REDIS_KEY, json.dumps(event))
            await redis_client.ltrim(ROUTING_EVENTS_REDIS_KEY, 0, ROUTING_EVENTS_MAXLEN - 1)
        except Exception:
            logger.debug("routing_event_publish_failed", exc_info=True)

    @staticmethod
    def _provider_error_from_exception(
        provider: str,
        exc: Exception,
    ) -> ProviderError:
        normalized_provider = normalize_provider(provider)
        if isinstance(exc, WormsoftTextError):
            return exc.as_provider_error()

        status_code = getattr(exc, "status_code", None)
        reason = str(getattr(exc, "reason", "") or "").strip() or type(exc).__name__.lower()
        category = "unknown"
        retryable = False

        if status_code in {401, 403}:
            category = "auth"
        elif status_code == 400:
            category = "bad_request"
        elif status_code == 402:
            category = "quota_exhausted"
        elif status_code == 429:
            category = "rate_limited"
        elif status_code is not None and 500 <= status_code <= 599:
            category = "upstream_5xx"
            retryable = status_code in {502, 503, 504}
        elif "timeout" in reason:
            category = "timeout"
            retryable = True
        elif "connection" in reason or "connect" in reason:
            category = "connection"
            retryable = True
        elif "guard_interval" in reason or "guard_quarantine" in reason:
            category = ERROR_THROTTLED_LOCAL
            retryable = "guard_interval" in reason
        elif "unavailable" in reason:
            category = "provider_unavailable"

        return ProviderError(
            provider=normalized_provider,
            category=category,
            message=str(exc)[:400],
            status_code=status_code,
            retryable=retryable,
            reset_at=getattr(exc, "reset_at", None),
            reason=reason[:64],
        )

    async def _reserve_provider_capacity(
        self,
        provider: str,
        model: str,
        task_family: str = TASK_FAMILY_TEXT_GENERATION,
    ) -> tuple[bool, str]:
        adapter = self._adapter_for_provider(provider)
        return await adapter.reserve_capacity(task_family=task_family, model=model)

    async def _allow_budget_reservation(
        self,
        provider: str,
        model: str,
        *,
        task_family: str,
        execution_role: str = EXECUTION_ROLE_PRIMARY,
    ) -> tuple[bool, str]:
        wormsoft_allowed, wormsoft_reason = await self._wormsoft_credit_guard.allow(
            provider=provider,
            execution_role=execution_role,
        )
        if not wormsoft_allowed:
            note_llm_throttle_event(self._service_name, normalize_provider(provider), wormsoft_reason)
            return False, wormsoft_reason
        allowed, reason, _ = await self._budget_manager.allow_reservation(
            provider=provider,
            model=model,
            task_family=task_family,
            execution_role=execution_role,
        )
        return allowed, reason

    async def _allow_published_quota(
        self,
        provider: str,
        model: str,
        *,
        execution_role: str = EXECUTION_ROLE_PRIMARY,
    ) -> tuple[bool, str]:
        return await self._published_quota_guard.allow(
            provider=provider,
            model=model,
            execution_role=execution_role,
        )

    async def _resolve_provider_model(
        self,
        provider: str,
        request: ProviderExecutionRequest,
    ) -> tuple[str, dict[str, Any]]:
        adapter = self._adapter_for_provider(provider)
        try:
            resolved_model, metadata = await adapter.resolve_model(request)
        except Exception:
            logger.debug("provider_model_resolution_failed", exc_info=True)
            return str(request.model or "").strip(), {}
        normalized = str(resolved_model or "").strip()
        if normalized:
            return normalized, dict(metadata or {})
        if metadata:
            return "", dict(metadata or {})
        return str(request.model or "").strip(), {}

    async def _record_provider_success(self, provider: str, model: str) -> None:
        try:
            await self._circuit_breaker.record_success(provider, model)
        except Exception:
            logger.debug("provider_circuit_success_record_failed", exc_info=True)

    async def _record_provider_failure(
        self,
        provider: str,
        model: str,
        error: ProviderError,
    ) -> None:
        try:
            await self._circuit_breaker.record_failure(provider, model, error)
        except Exception:
            logger.debug("provider_circuit_failure_record_failed", exc_info=True)
        if provider == PROVIDER_OPENROUTER and self._is_openrouter_free_model(model):
            try:
                await self._openrouter_text_guard.record_failure(
                    status_code=error.status_code,
                    reset_at=error.reset_at,
                )
            except Exception:
                logger.debug("openrouter_text_guard_record_failure_failed", exc_info=True)

    @staticmethod
    def _fallback_reason_from_error(exc: Exception) -> str:
        reason = getattr(exc, "reason", "")
        if reason:
            return str(reason)[:64]
        return type(exc).__name__[:64]

    def _policy_candidates(
        self,
        *,
        task: str,
        task_family: str,
        provider_override: str | None,
        model_override: str | None,
    ) -> tuple[list[RoutingCandidate], str, bool]:
        family_policy = self.routing_policy.family_policy(task_family)
        base_candidates = list(family_policy.candidates)
        candidates: list[RoutingCandidate] = []
        if provider_override:
            provider = normalize_provider(provider_override)
            candidates.append(
                RoutingCandidate(
                    provider=provider,
                    model=self._model_for_provider_override(task, provider, model_override),
                    capability_tags=[task_family],
                    notes="provider_override",
                )
            )
        elif model_override and base_candidates:
            # ГОЛЫЙ model_override (без provider_override) подменял модель
            # у ПЕРВОГО кандидата семейства, оставляя его провайдера. Для
            # mcp_synthesis первым идёт wormsoft, а override приходил с
            # идентификатором GigaChat — то есть wormsoft получал чужое имя
            # модели и отвечал 404. Замер: 8 вызовов синтеза за 60 дней,
            # 4 ошибки wormsoft и 4 успеха polza, ни одного успеха первого
            # кандидата. Каждый вызов гарантированно тратил круг впустую.
            wanted = model_override.strip()
            owner = next(
                (c for c in base_candidates if str(c.model or "").strip() == wanted),
                None,
            )
            if owner is not None:
                # Модель объявлена в политике — маршрутизируем к её ВЛАДЕЛЬЦУ,
                # а не к первому попавшемуся провайдеру. Остальные кандидаты
                # остаются фолбэком в прежнем порядке.
                candidates.append(owner)
                candidates.extend(c for c in base_candidates if c is not owner)
            else:
                # Владелец неизвестен: приписать модель произвольному провайдеру
                # значит снова получить 404. Override игнорируется, семейство
                # отрабатывает штатной цепочкой — это хуже намерения вызывающего,
                # но лучше гарантированной ошибки, и в логе видно, что случилось.
                logger.warning(
                    "bare model_override=%r does not belong to any candidate of family=%r "
                    "(providers: %s) — ignoring it; pass provider_override to pin a pair",
                    wanted,
                    task_family,
                    [c.provider for c in base_candidates],
                )
                candidates.extend(base_candidates)
        else:
            candidates.extend(base_candidates)

        deduped: list[RoutingCandidate] = []
        seen: set[tuple[str, str]] = set()
        for candidate in candidates:
            pair = (normalize_provider(candidate.provider), str(candidate.model or "").strip())
            if not pair[1] or pair in seen:
                continue
            seen.add(pair)
            deduped.append(candidate)
        if task_family in {TASK_FAMILY_TEXT_GENERATION, TASK_FAMILY_VISION_GENERATION}:
            priority = {
                PROVIDER_WORMSOFT: 0,
                PROVIDER_OPENROUTER: 1,
                PROVIDER_POLZA: 2,
                PROVIDER_GIGACHAT: 3,
            }
            deduped.sort(key=lambda item: priority.get(normalize_provider(item.provider), 99))
        if task_family == TASK_FAMILY_VISION_GENERATION:
            providers_present = {candidate.provider for candidate in deduped}
            polza_model = str(getattr(self._polza, "default_model", "") or "").strip()
            giga_model = self._vision_gigachat_model()
            if PROVIDER_POLZA not in providers_present and polza_model:
                deduped.append(
                    RoutingCandidate(
                        provider=PROVIDER_POLZA,
                        model=polza_model,
                        capability_tags=[task_family],
                        notes="runtime_polza_vision_fallback",
                    )
                )
            if PROVIDER_GIGACHAT not in providers_present and giga_model:
                deduped.append(
                    RoutingCandidate(
                        provider=PROVIDER_GIGACHAT,
                        model=giga_model,
                        capability_tags=[task_family],
                        notes="runtime_gigachat_vision_fallback",
                    )
                )
        effective_mode = normalize_policy_mode(
            family_policy.mode or self.routing_policy.default_mode
        )
        return (
            apply_execution_mode(
                deduped,
                mode=effective_mode,
                task_family=task_family,
            ),
            effective_mode,
            family_policy.fallback_exception_only,
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
        if task not in ROUTABLE_LLM_TASKS and not provider_override:
            return await self._chat_gigachat(
                system,
                user,
                task=task,
                pro=pro,
                model_override=model_override,
                max_tokens=max_tokens,
            )

        task_family = control_plane_family_for_task(task)
        candidates, policy_mode, fallback_exception_only = self._policy_candidates(
            task=task,
            task_family=task_family,
            provider_override=provider_override,
            model_override=model_override,
        )
        attempts = [(candidate.provider, candidate.model) for candidate in candidates]
        last_error: Exception | None = None
        last_provider_error: ProviderError | None = None
        attempt_outcomes: list[AttemptOutcome] = []
        fallback_reason = ""
        skipped_candidates: list[dict[str, Any]] = []
        decision = self._routing_decision(
            task=task,
            task_family=task_family,
            attempts=attempts,
            selected_provider=attempts[0][0] if attempts else PROVIDER_GIGACHAT,
            selected_model=attempts[0][1]
            if attempts
            else self.route_model_for_task(task, pro=pro, model_override=model_override),
            skipped_candidates=skipped_candidates,
            mode=policy_mode,
            fallback_exception_only=fallback_exception_only,
        )
        self._last_routing_decision = decision
        self._last_execution_receipt = None
        await self._append_routing_event("routing_decision_made", decision.model_dump())

        for index, candidate in enumerate(candidates):
            attempt_provider = candidate.provider
            attempt_model = candidate.model
            next_attempt = attempts[index + 1] if index + 1 < len(attempts) else None
            effective_model = attempt_model
            available, availability_reason = self._provider_available(
                attempt_provider,
                effective_model,
            )
            if not available:
                last_error = RuntimeError(availability_reason)
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=availability_reason,
                )
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

            allowed_by_circuit, circuit_reason = await self._circuit_breaker.reserve(
                attempt_provider,
                effective_model,
            )
            if not allowed_by_circuit:
                last_error = RuntimeError(circuit_reason)
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=circuit_reason,
                )
                if next_attempt:
                    note_llm_fallback(
                        self._service_name,
                        task,
                        from_provider=attempt_provider,
                        from_requested_model=effective_model,
                        from_actual_model="",
                        to_provider=next_attempt[0],
                        to_model=next_attempt[1],
                        reason=circuit_reason,
                    )
                    continue
                break

            adapter = self._adapter_for_provider(attempt_provider)
            resolved_request = ProviderExecutionRequest(
                system=system,
                user=user,
                task=task,
                task_family=task_family,
                model=effective_model,
                max_tokens=max_tokens,
                pro=pro if index == 0 else False,
            )
            effective_model, resolution_metadata = await self._resolve_provider_model(
                attempt_provider,
                resolved_request,
            )
            if not effective_model:
                reason = str(resolution_metadata.get("reason") or "no_capable_model")
                last_error = RuntimeError(reason)
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=candidate.model,
                    reason=reason,
                )
                if next_attempt:
                    note_llm_fallback(
                        self._service_name,
                        task,
                        from_provider=attempt_provider,
                        from_requested_model=candidate.model,
                        from_actual_model="",
                        to_provider=next_attempt[0],
                        to_model=next_attempt[1],
                        reason=reason,
                    )
                    continue
                break
            resolved_request = resolved_request.model_copy(
                update={
                    "model": effective_model,
                    "metadata": resolution_metadata,
                }
            )

            allowed_capacity, capacity_reason = await self._reserve_provider_capacity(
                attempt_provider,
                effective_model,
                task_family=task_family,
            )
            if not allowed_capacity:
                last_error = RuntimeError(capacity_reason)
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=capacity_reason,
                )
                if next_attempt:
                    note_llm_fallback(
                        self._service_name,
                        task,
                        from_provider=attempt_provider,
                        from_requested_model=effective_model,
                        from_actual_model="",
                        to_provider=next_attempt[0],
                        to_model=next_attempt[1],
                        reason=capacity_reason,
                    )
                    continue
                break

            allowed_budget, budget_reason = await self._allow_budget_reservation(
                attempt_provider,
                effective_model,
                task_family=task_family,
                execution_role=EXECUTION_ROLE_PRIMARY,
            )
            if not allowed_budget:
                last_error = RuntimeError(budget_reason)
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=budget_reason,
                )
                if next_attempt:
                    note_llm_fallback(
                        self._service_name,
                        task,
                        from_provider=attempt_provider,
                        from_requested_model=effective_model,
                        from_actual_model="",
                        to_provider=next_attempt[0],
                        to_model=next_attempt[1],
                        reason=budget_reason,
                    )
                    continue
                break

            allowed_quota, quota_reason = await self._allow_published_quota(
                attempt_provider,
                effective_model,
                execution_role=EXECUTION_ROLE_PRIMARY,
            )
            if not allowed_quota:
                last_error = RuntimeError(quota_reason)
                self._note_skipped_candidate(
                    skipped_candidates,
                    attempt_outcomes,
                    provider=attempt_provider,
                    model=effective_model,
                    reason=quota_reason,
                )
                if next_attempt:
                    note_llm_fallback(
                        self._service_name,
                        task,
                        from_provider=attempt_provider,
                        from_requested_model=effective_model,
                        from_actual_model="",
                        to_provider=next_attempt[0],
                        to_model=next_attempt[1],
                        reason=quota_reason,
                    )
                    continue
                break

            reservation = await self._budget_manager.reserve(
                provider=attempt_provider,
                model=effective_model,
                task_family=task_family,
            )
            started_at = time.monotonic()
            try:
                response = await adapter.execute(resolved_request)
                latency_ms = (time.monotonic() - started_at) * 1000.0
                await self._record_provider_success(attempt_provider, effective_model)
                attempt_outcomes.append(
                    AttemptOutcome(
                        provider=attempt_provider,
                        model=effective_model,
                        status="ok",
                        actual_model=response.actual_model,
                    )
                )
                if fallback_reason and not response.fallback_reason:
                    response = GigaChatResponse(
                        content=response.content,
                        model=response.model,
                        requested_model=response.requested_model,
                        provider=response.provider,
                        usage=response.usage,
                        parsed=response.parsed,
                        fallback_reason=fallback_reason,
                    )
                decision = self._routing_decision(
                    task=task,
                    task_family=task_family,
                    attempts=attempts,
                    selected_provider=attempt_provider,
                    selected_model=effective_model,
                    skipped_candidates=skipped_candidates,
                    mode=policy_mode,
                    fallback_exception_only=fallback_exception_only,
                )
                self._last_routing_decision = decision
                cost_estimate = adapter.estimate_cost(
                    response=response,
                    requested_model=effective_model,
                    actual_model=response.actual_model,
                )
                reservation = await self._budget_manager.commit(
                    reservation,
                    actual_units=cost_estimate if cost_estimate is not None else 1.0,
                    prompt_tokens=response.usage.prompt_tokens,
                    completion_tokens=response.usage.completion_tokens,
                    billable_tokens=response.usage.billable_tokens,
                )
                receipt = ExecutionReceipt(
                    task=task,
                    task_family=task_family,
                    status="ok",
                    requested_provider=attempts[0][0] if attempts else attempt_provider,
                    requested_model=attempts[0][1] if attempts else effective_model,
                    actual_provider=response.provider,
                    actual_model=response.actual_model,
                    fallback_reason=response.fallback_reason,
                    latency_ms=latency_ms,
                    cost_estimate=cost_estimate,
                    budget_attribution=reservation,
                    decision=decision,
                    attempts=attempt_outcomes,
                )
                receipt = await self._record_finops_receipt(receipt)
                self._last_execution_receipt = receipt
                await adapter.record_result(receipt=receipt, error=None)
                await self._append_routing_event(
                    "routing_execution_finished",
                    receipt.model_dump(),
                )
                await self._launch_shadow_evaluation(
                    task=task,
                    task_family=task_family,
                    mode=policy_mode,
                    attempts=attempts,
                    selected_provider=attempt_provider,
                    selected_model=effective_model,
                    skipped_candidates=skipped_candidates,
                    request=resolved_request,
                    primary_receipt=receipt,
                )
                return response
            except Exception as exc:
                last_error = exc

            last_provider_error = adapter.normalize_error(last_error, model=effective_model)
            await self._record_provider_failure(
                attempt_provider,
                effective_model,
                last_provider_error,
            )
            await self._budget_manager.release(reservation)
            failed_receipt = ExecutionReceipt(
                task=task,
                task_family=task_family,
                status="error",
                requested_provider=attempts[0][0] if attempts else attempt_provider,
                requested_model=attempts[0][1] if attempts else effective_model,
                actual_provider=attempt_provider,
                actual_model=effective_model,
                fallback_reason=last_provider_error.reason,
                latency_ms=(time.monotonic() - started_at) * 1000.0,
                budget_attribution=reservation,
                provider_error=last_provider_error,
            )
            failed_receipt = await self._record_finops_receipt(failed_receipt)
            await adapter.record_result(receipt=failed_receipt, error=last_provider_error)
            fallback_reason = (
                last_provider_error.reason or self._fallback_reason_from_error(last_error)
            )
            attempt_outcomes.append(
                AttemptOutcome(
                    provider=attempt_provider,
                    model=effective_model,
                    status="error",
                    reason=fallback_reason,
                )
            )
            if next_attempt:
                note_llm_fallback(
                    self._service_name,
                    task,
                    from_provider=attempt_provider,
                    from_requested_model=effective_model,
                    from_actual_model="",
                    to_provider=next_attempt[0],
                    to_model=next_attempt[1],
                    reason=fallback_reason,
                )
                continue

        if attempts:
            receipt = ExecutionReceipt(
                task=task,
                task_family=task_family,
                status="error",
                requested_provider=attempts[0][0],
                requested_model=attempts[0][1],
                actual_provider=normalize_provider(
                    getattr(last_provider_error, "provider", attempts[-1][0])
                ),
                actual_model=attempts[-1][1],
                fallback_reason=fallback_reason
                or self._fallback_reason_from_error(last_error or RuntimeError("routing_failed")),
                decision=self._routing_decision(
                    task=task,
                    task_family=task_family,
                    attempts=attempts,
                    selected_provider=attempts[-1][0],
                    selected_model=attempts[-1][1],
                    skipped_candidates=skipped_candidates,
                    mode=policy_mode,
                    fallback_exception_only=fallback_exception_only,
                ),
                attempts=attempt_outcomes,
                provider_error=last_provider_error,
            )
            receipt = await self._record_finops_receipt(receipt)
            self._last_execution_receipt = receipt
            await self._append_routing_event(
                "routing_execution_finished",
                receipt.model_dump(),
            )
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"no_llm_route_available task={task}")
