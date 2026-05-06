"""Provider adapter implementations for the LLM control plane."""
from __future__ import annotations

import time
from typing import Any

from admin.backend.services.openrouter_catalog import fetch_openrouter_catalog
from admin.backend.services.openrouter_health import fetch_openrouter_health_snapshot
from admin.backend.services.openrouter_key import fetch_openrouter_key
from admin.backend.services.openrouter_picker import (
    fetch_openrouter_runtime_state,
    pick_model,
    record_call_result,
    task_family_for_task as openrouter_task_family_for_task,
    task_family_for_vision,
)
from admin.backend.services.wormsoft_models import fetch_wormsoft_models
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
    BudgetWindowState,
    ModelCapabilitySnapshot,
    ModelCatalogSnapshot,
    ProviderError,
    ProviderExecutionRequest,
    ProviderStateSnapshot,
    embedding_profile_for_model,
    normalize_task_family,
    normalize_wormsoft_model_snapshot,
)
from shared.llm_routing import (
    PROVIDER_GIGACHAT,
    PROVIDER_OPENROUTER,
    PROVIDER_POLZA,
    PROVIDER_WORMSOFT,
    normalize_provider,
)
from worker.gigachat_client import GigaChatClient
from worker.llm_types import GigaChatResponse
from worker.openai_compat_text_client import OpenAICompatTextError
from worker.openrouter_client import OpenRouterVisionClient, OpenRouterVisionError
from worker.openrouter_guard import OpenRouterFreeGuard, OpenRouterVisionGuard
from worker.openrouter_text_client import OpenRouterTextClient
from worker.polza_client import PolzaVisionClient, PolzaVisionError
from worker.polza_text_client import PolzaTextClient
from worker.wormsoft_client import WormsoftTextClient, WormsoftTextError
from worker.wormsoft_guard import WormsoftSharedGuard


def _usage_cost_estimate(response: GigaChatResponse | None) -> float | None:
    if response is None:
        return None
    try:
        return float(response.usage.billable_tokens)
    except Exception:
        return None


def _synthetic_catalog(provider: str, models: list[dict[str, Any]]) -> ModelCatalogSnapshot:
    snapshots: list[ModelCapabilitySnapshot] = []
    for item in models:
        model = str(item.get("model") or "").strip()
        if not model:
            continue
        snapshots.append(
            ModelCapabilitySnapshot(
                provider=provider,
                model=model,
                modalities=list(item.get("modalities") or []),
                input_modalities=list(item.get("input_modalities") or []),
                output_modalities=list(item.get("output_modalities") or []),
                capabilities=list(item.get("capabilities") or []),
                supports_text_generation=bool(item.get("text")),
                supports_vision_generation=bool(item.get("vision")),
                supports_embeddings=bool(item.get("embeddings")),
                metadata=dict(item.get("metadata") or {}),
            )
        )
    return ModelCatalogSnapshot(
        provider=provider,
        available=bool(snapshots),
        fetched_at=time.time(),
        source="synthetic",
        status="ok" if snapshots else "empty",
        models=snapshots,
    )


def _normalize_generic_error(provider: str, exc: Exception, *, model: str) -> ProviderError:
    normalized_provider = normalize_provider(provider)
    status_code = getattr(exc, "status_code", None)
    reason = str(getattr(exc, "reason", "") or "").strip() or type(exc).__name__.lower()
    category = ERROR_UNKNOWN
    retryable = False

    if status_code in {401, 403}:
        category = ERROR_AUTH
    elif status_code == 400:
        category = ERROR_BAD_REQUEST
    elif status_code == 402:
        category = ERROR_QUOTA_EXHAUSTED
    elif status_code == 404 or "not found" in reason:
        category = ERROR_MODEL_NOT_FOUND
    elif status_code == 429:
        category = ERROR_RATE_LIMITED
    elif status_code is not None and 500 <= status_code <= 599:
        category = ERROR_UPSTREAM_5XX
        retryable = status_code in {502, 503, 504}
    elif "timeout" in reason:
        category = ERROR_TIMEOUT
        retryable = True
    elif "connection" in reason or "connect" in reason or "transport" in reason:
        category = ERROR_CONNECTION
        retryable = True
    elif "unavailable" in reason or "disabled" in reason or "guard_" in reason:
        category = ERROR_PROVIDER_UNAVAILABLE

    return ProviderError(
        provider=normalized_provider,
        category=category,
        message=f"{normalized_provider}_request_failed model={model}: {str(exc)[:300]}",
        status_code=status_code,
        retryable=retryable,
        reset_at=getattr(exc, "reset_at", None),
        reason=reason[:64],
    )


class WormsoftAdapter:
    provider_name = PROVIDER_WORMSOFT

    def __init__(
        self,
        *,
        service_name: str,
        settings=None,
        redis=None,
        text_client: WormsoftTextClient,
    ) -> None:
        self._service_name = service_name
        self._settings = settings or get_settings()
        self._guard = WormsoftSharedGuard(redis=redis, service_name=service_name, settings=self._settings)
        self._text = text_client

    async def discover_models(self) -> ModelCatalogSnapshot:
        payload = await fetch_wormsoft_models()
        return ModelCatalogSnapshot(
            provider=self.provider_name,
            available=bool(payload.get("available")),
            fetched_at=time.time(),
            source="wormsoft.models",
            status=str(payload.get("status") or "unknown"),
            models=normalize_wormsoft_model_snapshot(payload),
        )

    async def check_health(self) -> ProviderStateSnapshot:
        guard_state = await self._guard.get_state()
        available = bool(self._text.is_available)
        ready = available and not bool(guard_state.get("quarantined"))
        return ProviderStateSnapshot(
            provider=self.provider_name,
            available=available,
            ready=ready,
            health_status="quarantined" if guard_state.get("quarantined") else ("ok" if available else "missing_api_key"),
            key_present=bool(getattr(self._settings, "wormsoft_api_key", "")),
            quota_pressure="quarantined" if guard_state.get("quarantined") else "unknown",
            metadata={"guard": guard_state},
        )

    async def reserve_capacity(self, *, task_family: str, model: str) -> tuple[bool, str]:
        del task_family, model
        guard_state = await self._guard.get_state()
        if guard_state.get("quarantined"):
            return False, "guard_quarantine"
        return True, "ok"

    async def resolve_model(self, request: ProviderExecutionRequest) -> tuple[str, dict[str, Any]]:
        return str(request.model or "").strip(), {}

    async def execute(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        return await self._text.chat(
            system=request.system,
            user=request.user,
            task=request.task,
            pro=request.pro,
            model_override=request.model,
            provider_override=self.provider_name,
            max_tokens=request.max_tokens,
        )

    async def vision(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        raise WormsoftTextError(
            "wormsoft_vision_not_supported_in_runtime",
            reason="wormsoft_vision_unavailable",
            category=ERROR_PROVIDER_UNAVAILABLE,
            retryable=False,
        )

    async def embed(self, request: ProviderExecutionRequest) -> list[float]:
        del request
        raise WormsoftTextError(
            "wormsoft_embeddings_not_supported_in_runtime",
            reason="wormsoft_embeddings_unavailable",
            category=ERROR_PROVIDER_UNAVAILABLE,
            retryable=False,
        )

    async def record_result(
        self,
        *,
        receipt,
        error: ProviderError | None = None,
    ) -> None:
        del receipt, error

    def normalize_error(self, exc: Exception, *, model: str) -> ProviderError:
        if isinstance(exc, WormsoftTextError):
            return exc.as_provider_error()
        return self._text.normalize_error(exc, model=model).as_provider_error()

    def estimate_cost(
        self,
        *,
        response: GigaChatResponse | None,
        requested_model: str,
        actual_model: str,
    ) -> float | None:
        del requested_model, actual_model
        return _usage_cost_estimate(response)


class OpenRouterAdapter:
    provider_name = PROVIDER_OPENROUTER

    def __init__(
        self,
        *,
        service_name: str,
        settings=None,
        text_client: OpenRouterTextClient,
        vision_client: OpenRouterVisionClient,
        text_guard: OpenRouterFreeGuard,
        vision_guard: OpenRouterVisionGuard,
    ) -> None:
        self._service_name = service_name
        self._settings = settings or get_settings()
        self._text = text_client
        self._vision = vision_client
        self._text_guard = text_guard
        self._vision_guard = vision_guard

    async def discover_models(self) -> ModelCatalogSnapshot:
        payload = await fetch_openrouter_catalog()
        snapshots = [
            ModelCapabilitySnapshot(
                provider=self.provider_name,
                model=str(item.get("id") or ""),
                input_modalities=[str(value) for value in (item.get("input_modalities") or [])],
                output_modalities=[str(value) for value in (item.get("output_modalities") or [])],
                capabilities=[
                    capability
                    for capability, enabled in {
                        "vision": bool(item.get("supports_vision")),
                        "structured": bool(item.get("supports_structured")),
                        "tools": bool(item.get("supports_tools")),
                    }.items()
                    if enabled
                ],
                supports_text_generation=True,
                supports_vision_generation=bool(item.get("supports_vision")),
                supports_embeddings=False,
                metadata={
                    "context_length": int(item.get("context_length") or 0),
                    "supported_parameters": list(item.get("supported_parameters") or []),
                },
            )
            for item in (payload.get("models") or [])
            if str(item.get("id") or "").strip()
        ]
        return ModelCatalogSnapshot(
            provider=self.provider_name,
            available=bool(payload.get("available", payload.get("status") == "ok")),
            fetched_at=float(payload.get("fetched_at") or time.time()),
            source="openrouter.catalog",
            status=str(payload.get("status") or "unknown"),
            models=snapshots,
        )

    async def check_health(self) -> ProviderStateSnapshot:
        runtime_payload = await fetch_openrouter_runtime_state(service_name=self._service_name)
        health_payload = await fetch_openrouter_health_snapshot()
        key_payload = await fetch_openrouter_key()
        return ProviderStateSnapshot(
            provider=self.provider_name,
            available=bool(self._settings.openrouter_api_key),
            ready=bool(self._settings.openrouter_api_key) and int(runtime_payload.get("usable_model_count") or 0) > 0,
            health_status=str(health_payload.get("status") or "unknown"),
            key_present=bool(self._settings.openrouter_api_key),
            quota_pressure="low_credit" if float(key_payload.get("limit_remaining") or 0.0) <= 0.0 else "ok",
            budgets=[
                BudgetWindowState(
                    provider=self.provider_name,
                    scope="openrouter_key",
                    window_label="daily",
                    limit=float(key_payload.get("free_model_daily_limit") or 0.0),
                    remaining=float(key_payload.get("free_model_daily_limit") or 0.0)
                    - float(key_payload.get("usage_daily") or 0.0),
                    used=float(key_payload.get("usage_daily") or 0.0),
                    unit="requests",
                    status=str(key_payload.get("status") or "unknown"),
                    refreshed_at=float(key_payload.get("fetched_at") or time.time()),
                )
            ],
            metadata={
                "runtime": runtime_payload,
                "key": key_payload,
            },
        )

    async def resolve_model(self, request: ProviderExecutionRequest) -> tuple[str, dict[str, Any]]:
        normalized_family = normalize_task_family(request.task_family)
        requested = str(request.model or "").strip()
        if requested and requested != "openrouter/free" and not requested.endswith(":free"):
            return requested, {}
        if normalized_family == "vision_generation":
            decision = await pick_model(
                task_family_for_vision(request.quality_tier) or "vision_mass",
                service_name=self._service_name,
            )
        else:
            decision = await pick_model(
                openrouter_task_family_for_task(request.task),
                service_name=self._service_name,
            )
        picked_model = str(decision.get("model_id") or "").strip()
        return picked_model, decision

    async def reserve_capacity(self, *, task_family: str, model: str) -> tuple[bool, str]:
        if str(model or "").strip().lower() == "openrouter/free" or str(model or "").strip().lower().endswith(":free"):
            if normalize_task_family(task_family) == "vision_generation":
                return await self._vision_guard.reserve_slot()
            return await self._text_guard.reserve_slot()
        return True, "ok"

    async def execute(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        return await self._text.chat(
            system=request.system,
            user=request.user,
            task=request.task,
            pro=request.pro,
            model_override=request.model,
            provider_override=self.provider_name,
            max_tokens=request.max_tokens,
        )

    async def vision(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        if request.image_bytes is None:
            raise OpenRouterVisionError(
                "openrouter_vision_missing_image",
                reason="openrouter_bad_request",
                status_code=400,
            )
        return await self._vision.vision(
            request.image_bytes,
            prompt=request.prompt,
            model_override=request.model,
        )

    async def embed(self, request: ProviderExecutionRequest) -> list[float]:
        del request
        raise OpenAICompatTextError(
            "openrouter_embeddings_not_supported",
            reason="openrouter_embeddings_unavailable",
        )

    async def record_result(
        self,
        *,
        receipt,
        error: ProviderError | None = None,
    ) -> None:
        model_id = ""
        success = error is None and receipt is not None and getattr(receipt, "status", "") == "ok"
        if receipt is not None:
            model_id = str(receipt.actual_model or receipt.requested_model or "").strip()
        if not model_id:
            return
        if model_id == "openrouter/free":
            return
        if not model_id.endswith(":free"):
            return
        latency_ms = float(getattr(receipt, "latency_ms", 0.0) or 0.0) if receipt is not None else 0.0
        status_code = None if error is None else error.status_code
        try:
            await record_call_result(
                model_id,
                success=success,
                latency_ms=latency_ms,
                status_code=status_code,
                or_reset_at=None if error is None else error.reset_at,
                service_name=self._service_name,
            )
        except Exception:
            pass
        if error is not None:
            try:
                guard = (
                    self._vision_guard
                    if str(getattr(receipt, "task_family", "") or "") == "vision_generation"
                    else self._text_guard
                )
                await guard.record_failure(
                    status_code=error.status_code,
                    reset_at=error.reset_at,
                )
            except Exception:
                return

    def normalize_error(self, exc: Exception, *, model: str) -> ProviderError:
        return _normalize_generic_error(self.provider_name, exc, model=model)

    def estimate_cost(
        self,
        *,
        response: GigaChatResponse | None,
        requested_model: str,
        actual_model: str,
    ) -> float | None:
        del requested_model
        if str(actual_model or "").strip().lower().endswith(":free"):
            return 0.0
        return _usage_cost_estimate(response)


class PolzaAdapter:
    provider_name = PROVIDER_POLZA

    def __init__(
        self,
        *,
        service_name: str,
        settings=None,
        text_client: PolzaTextClient,
        vision_client: PolzaVisionClient,
    ) -> None:
        self._service_name = service_name
        self._settings = settings or get_settings()
        self._text = text_client
        self._vision = vision_client

    async def discover_models(self) -> ModelCatalogSnapshot:
        return _synthetic_catalog(
            self.provider_name,
            [
                {"model": str(self._settings.polza_text_model or "").strip(), "text": True},
                {"model": str(self._settings.polza_synthesis_model or "").strip(), "text": True, "metadata": {"kind": "synthesis"}},
                {
                    "model": str(self._settings.polza_vision_model or "").strip(),
                    "text": True,
                    "vision": bool(self._settings.polza_vision_model),
                    "input_modalities": ["text", "image"] if self._settings.polza_vision_model else [],
                    "output_modalities": ["text"] if self._settings.polza_vision_model else [],
                },
            ],
        )

    async def check_health(self) -> ProviderStateSnapshot:
        return ProviderStateSnapshot(
            provider=self.provider_name,
            available=bool(self._settings.polza_api_key),
            ready=bool(self._text.is_available),
            health_status="configured" if self._settings.polza_api_key else "missing_api_key",
            key_present=bool(self._settings.polza_api_key),
            quota_pressure="unknown",
            metadata={
                "text_model": self._text.default_model,
                "vision_model": self._vision.default_model,
            },
        )

    async def reserve_capacity(self, *, task_family: str, model: str) -> tuple[bool, str]:
        if not self._text.is_available and normalize_task_family(task_family) != "vision_generation":
            return False, "provider_unavailable"
        if not str(model or "").strip():
            return False, "model_missing"
        return True, "ok"

    async def resolve_model(self, request: ProviderExecutionRequest) -> tuple[str, dict[str, Any]]:
        return str(request.model or "").strip(), {}

    async def execute(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        return await self._text.chat(
            system=request.system,
            user=request.user,
            task=request.task,
            pro=request.pro,
            model_override=request.model,
            provider_override=self.provider_name,
            max_tokens=request.max_tokens,
        )

    async def vision(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        if request.image_bytes is None:
            raise PolzaVisionError(
                "polza_vision_missing_image",
                reason="polza_bad_request",
                status_code=400,
            )
        try:
            return await self._vision.vision(
                request.image_bytes,
                prompt=request.prompt,
                model_override=request.model,
            )
        except TypeError:
            return await self._vision.vision(
                request.image_bytes,
                prompt=request.prompt,
            )

    async def embed(self, request: ProviderExecutionRequest) -> list[float]:
        del request
        raise OpenAICompatTextError(
            "polza_embeddings_not_supported",
            reason="polza_embeddings_unavailable",
        )

    async def record_result(
        self,
        *,
        receipt,
        error: ProviderError | None = None,
    ) -> None:
        del receipt, error

    def normalize_error(self, exc: Exception, *, model: str) -> ProviderError:
        return _normalize_generic_error(self.provider_name, exc, model=model)

    def estimate_cost(
        self,
        *,
        response: GigaChatResponse | None,
        requested_model: str,
        actual_model: str,
    ) -> float | None:
        del requested_model, actual_model
        return _usage_cost_estimate(response)


class GigaChatAdapter:
    provider_name = PROVIDER_GIGACHAT

    def __init__(
        self,
        *,
        service_name: str,
        settings=None,
        giga_client: GigaChatClient,
    ) -> None:
        self._service_name = service_name
        self._settings = settings or get_settings()
        self._giga = giga_client

    async def discover_models(self) -> ModelCatalogSnapshot:
        embedding_model = str(self._settings.gigachat_embeddings_model or "").strip()
        return _synthetic_catalog(
            self.provider_name,
            [
                {"model": str(self._settings.gigachat_model or "").strip(), "text": True},
                {"model": str(self._settings.gigachat_model_lite or "").strip(), "text": True},
                {"model": str(self._settings.gigachat_model_pro or "").strip(), "text": True, "vision": True},
                {"model": str(self._settings.gigachat_model_max or "").strip(), "text": True, "vision": True},
                {"model": str(self._settings.gigachat_model_vision or "").strip(), "text": True, "vision": True},
                {
                    "model": embedding_model,
                    "embeddings": True,
                    "metadata": embedding_profile_for_model(embedding_model),
                },
            ],
        )

    async def check_health(self) -> ProviderStateSnapshot:
        available = bool(self._settings.gigachat_credentials)
        return ProviderStateSnapshot(
            provider=self.provider_name,
            available=available,
            ready=available,
            health_status="configured" if available else "missing_credentials",
            key_present=available,
            quota_pressure="unknown",
        )

    async def reserve_capacity(self, *, task_family: str, model: str) -> tuple[bool, str]:
        del task_family
        if not str(model or "").strip():
            return False, "model_missing"
        return True, "ok"

    async def resolve_model(self, request: ProviderExecutionRequest) -> tuple[str, dict[str, Any]]:
        return str(request.model or "").strip(), {}

    async def execute(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        return await self._giga.chat(
            system=request.system,
            user=request.user,
            task=request.task,
            pro=request.pro,
            model_override=request.model,
            provider_override=self.provider_name,
            max_tokens=request.max_tokens,
        )

    async def vision(self, request: ProviderExecutionRequest) -> GigaChatResponse:
        if request.image_bytes is None:
            raise RuntimeError("gigachat_vision_missing_image")
        if request.prompt:
            return await self._giga.vision(request.image_bytes, prompt=request.prompt)
        return await self._giga.vision(request.image_bytes)

    async def embed(self, request: ProviderExecutionRequest) -> list[float]:
        return await self._giga.embed(
            request.text,
            purpose=request.embedding_purpose,
        )

    async def record_result(
        self,
        *,
        receipt,
        error: ProviderError | None = None,
    ) -> None:
        del receipt, error

    def normalize_error(self, exc: Exception, *, model: str) -> ProviderError:
        return _normalize_generic_error(self.provider_name, exc, model=model)

    def estimate_cost(
        self,
        *,
        response: GigaChatResponse | None,
        requested_model: str,
        actual_model: str,
    ) -> float | None:
        del requested_model, actual_model
        return _usage_cost_estimate(response)
