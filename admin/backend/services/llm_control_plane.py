"""Admin-facing aggregation for the multi-provider LLM control plane."""
from __future__ import annotations

import json
import time
from typing import Any

from admin.backend.services.gigachat_balance import fetch_gigachat_balance
from admin.backend.services.llm_finops import fetch_llm_finops_snapshot
from admin.backend.services.openrouter_catalog import fetch_openrouter_catalog
from admin.backend.services.openrouter_health import fetch_openrouter_health_snapshot
from admin.backend.services.openrouter_key import fetch_openrouter_key
from admin.backend.services.openrouter_picker import fetch_openrouter_runtime_state
from admin.backend.services.wormsoft_limits import fetch_wormsoft_limits
from admin.backend.services.wormsoft_models import fetch_wormsoft_models
from shared.config import get_settings
from shared.llm_control_plane import (
    BudgetWindowState,
    CircuitState,
    CostAggregateState,
    ModelCapabilitySnapshot,
    ModelCatalogSnapshot,
    ProviderStateSnapshot,
    ROUTING_EVENTS_REDIS_KEY,
    RoutingPolicyV2,
    derive_provider_readiness,
    embedding_profile_for_model,
    normalize_wormsoft_model_snapshot,
    simulate_routing_decision,
)
from shared.llm_routing import PROVIDER_GIGACHAT, PROVIDER_OPENROUTER, PROVIDER_POLZA, PROVIDER_WORMSOFT
from shared.redis_client import get_client
from worker.provider_budget_manager import ProviderBudgetManager
from worker.provider_circuit_breaker import ProviderCircuitBreaker
from worker.wormsoft_guard import WormsoftSharedGuard


async def _wormsoft_provider_state() -> ProviderStateSnapshot:
    models_payload, limits_payload = await fetch_wormsoft_models(), await fetch_wormsoft_limits()
    catalog = ModelCatalogSnapshot(
        provider=PROVIDER_WORMSOFT,
        available=bool(models_payload.get("available")),
        fetched_at=models_payload.get("fetched_at"),
        source="wormsoft.models",
        status=str(models_payload.get("status") or "unknown"),
        models=normalize_wormsoft_model_snapshot(models_payload),
    )
    budgets: list[BudgetWindowState] = []
    for item in limits_payload.get("plans") or []:
        budgets.append(
            BudgetWindowState(
                provider=PROVIDER_WORMSOFT,
                scope="subscription_plan",
                window_label=str(item.get("id") or ""),
                limit=float(item.get("amount") or 0),
                remaining=None,
                used=None,
                unit="credits",
                status="published",
                refreshed_at=limits_payload.get("fetched_at"),
            )
        )
    available = bool(models_payload.get("available")) and bool(limits_payload.get("key_present"))
    ready = available
    health_status = "ok" if models_payload.get("available") else str(models_payload.get("status") or "unknown")
    quota_pressure = "unknown"
    return ProviderStateSnapshot(
        provider=PROVIDER_WORMSOFT,
        available=available,
        ready=ready,
        readiness_state=derive_provider_readiness(
            available=available,
            ready=ready,
            health_status=health_status,
            quota_pressure=quota_pressure,
        ),
        health_status=health_status,
        key_present=bool(limits_payload.get("key_present")),
        quota_pressure=quota_pressure,
        subscriptions=list(limits_payload.get("plans") or []),
        budgets=budgets,
        catalog=catalog,
        metadata={
            "limits": limits_payload,
            "pricing_models": int(limits_payload.get("pricing_model_count") or 0),
        },
    )


async def _openrouter_provider_state() -> ProviderStateSnapshot:
    catalog_payload = await fetch_openrouter_catalog()
    key_payload = await fetch_openrouter_key()
    health_payload = await fetch_openrouter_health_snapshot()
    runtime_payload = await fetch_openrouter_runtime_state(service_name="admin")
    catalog_models = [
        ModelCapabilitySnapshot(
            provider=PROVIDER_OPENROUTER,
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
        for item in (catalog_payload.get("models") or [])
        if str(item.get("id") or "").strip()
    ]
    available = bool(key_payload.get("available"))
    ready = available and int(health_payload.get("usable_model_count") or 0) > 0
    health_status = str(health_payload.get("status") or key_payload.get("status") or "unknown")
    quota_pressure = (
        "low_credit"
        if float(key_payload.get("limit_remaining") or 0.0) <= 0.0
        else "ok"
    )
    return ProviderStateSnapshot(
        provider=PROVIDER_OPENROUTER,
        available=available,
        ready=ready,
        readiness_state=derive_provider_readiness(
            available=available,
            ready=ready,
            health_status=health_status,
            quota_pressure=quota_pressure,
        ),
        health_status=health_status,
        key_present=bool(get_settings().openrouter_api_key),
        quota_pressure=quota_pressure,
        budgets=[
            BudgetWindowState(
                provider=PROVIDER_OPENROUTER,
                scope="api_key",
                window_label="daily",
                limit=float(key_payload.get("free_model_daily_limit") or 0.0),
                remaining=float(key_payload.get("free_model_daily_limit") or 0.0)
                - float(key_payload.get("usage_daily") or 0.0),
                used=float(key_payload.get("usage_daily") or 0.0),
                unit="requests",
                status=str(key_payload.get("status") or "unknown"),
                refreshed_at=key_payload.get("fetched_at"),
            )
        ],
        catalog=ModelCatalogSnapshot(
            provider=PROVIDER_OPENROUTER,
            available=bool(catalog_payload.get("available")),
            fetched_at=catalog_payload.get("fetched_at"),
            source="openrouter.catalog",
            status=str(catalog_payload.get("status") or "unknown"),
            models=catalog_models,
        ),
        metadata={
            "key": key_payload,
            "health": health_payload,
            "runtime": runtime_payload,
        },
    )


def _giga_provider_state(balance_payload: dict[str, Any]) -> ProviderStateSnapshot:
    balance_items = balance_payload.get("balance") or []
    total = sum(float(item.get("value") or 0.0) for item in balance_items)
    settings = get_settings()
    embedding_model = str(settings.gigachat_embeddings_model or "").strip()
    available = bool(settings.gigachat_credentials)
    ready = available
    health_status = str(balance_payload.get("status") or "unknown")
    quota_pressure = "low_balance" if total <= float(balance_payload.get("alert_threshold") or 0.0) else "ok"
    return ProviderStateSnapshot(
        provider=PROVIDER_GIGACHAT,
        available=available,
        ready=ready,
        readiness_state=derive_provider_readiness(
            available=available,
            ready=ready,
            health_status=health_status,
            quota_pressure=quota_pressure,
        ),
        health_status=health_status,
        key_present=available,
        quota_pressure=quota_pressure,
        budgets=[
            BudgetWindowState(
                provider=PROVIDER_GIGACHAT,
                scope="oauth_balance",
                window_label=str(item.get("usage") or "usage"),
                limit=None,
                remaining=float(item.get("value") or 0.0),
                unit="tokens",
                status=str(balance_payload.get("status") or "unknown"),
                refreshed_at=balance_payload.get("fetched_at"),
            )
            for item in balance_items
        ],
        catalog=ModelCatalogSnapshot(
            provider=PROVIDER_GIGACHAT,
            available=bool(settings.gigachat_credentials),
            fetched_at=balance_payload.get("fetched_at"),
            source="synthetic",
            status="configured" if settings.gigachat_credentials else "missing_credentials",
            models=[
                ModelCapabilitySnapshot(provider=PROVIDER_GIGACHAT, model=str(settings.gigachat_model or "").strip(), supports_text_generation=True),
                ModelCapabilitySnapshot(provider=PROVIDER_GIGACHAT, model=str(settings.gigachat_model_lite or "").strip(), supports_text_generation=True),
                ModelCapabilitySnapshot(provider=PROVIDER_GIGACHAT, model=str(settings.gigachat_model_pro or "").strip(), supports_text_generation=True, supports_vision_generation=True, input_modalities=["text", "image"], output_modalities=["text"]),
                ModelCapabilitySnapshot(provider=PROVIDER_GIGACHAT, model=str(settings.gigachat_model_max or "").strip(), supports_text_generation=True, supports_vision_generation=True, input_modalities=["text", "image"], output_modalities=["text"]),
                ModelCapabilitySnapshot(provider=PROVIDER_GIGACHAT, model=str(settings.gigachat_model_vision or "").strip(), supports_text_generation=True, supports_vision_generation=True, input_modalities=["text", "image"], output_modalities=["text"]),
                ModelCapabilitySnapshot(
                    provider=PROVIDER_GIGACHAT,
                    model=embedding_model,
                    supports_embeddings=True,
                    capabilities=["embeddings"],
                    metadata=embedding_profile_for_model(embedding_model),
                ),
            ],
        ),
        metadata={"balance": balance_payload},
    )


def _polza_synthetic_models(settings: Any) -> list[ModelCapabilitySnapshot]:
    """???????? ???????????? ???? ????????????: ???????? POLZA_VISION_MODEL ?????????????????? ?? ?????????????????? ??? ???????????????? vision ???? ?????? ???? ????????????.

    ?????????? `simulate_routing_decision` ?????????????? ???????????? ???????????? ???????????????? ???? ?????????? ???????????? ?????? vision ??? capability_mismatch.
    """
    text_model = str(settings.polza_text_model or "").strip()
    synthesis_model = str(settings.polza_synthesis_model or "").strip()
    vision_model = str(settings.polza_vision_model or "").strip()
    out: list[ModelCapabilitySnapshot] = []
    if text_model:
        vision_on_text = bool(vision_model and vision_model == text_model)
        out.append(
            ModelCapabilitySnapshot(
                provider=PROVIDER_POLZA,
                model=text_model,
                supports_text_generation=True,
                supports_vision_generation=vision_on_text,
                input_modalities=["text", "image"] if vision_on_text else ["text"],
                output_modalities=["text"],
            )
        )
    if synthesis_model and synthesis_model != text_model:
        out.append(
            ModelCapabilitySnapshot(
                provider=PROVIDER_POLZA,
                model=synthesis_model,
                supports_text_generation=True,
            )
        )
    if vision_model and vision_model != text_model:
        out.append(
            ModelCapabilitySnapshot(
                provider=PROVIDER_POLZA,
                model=vision_model,
                supports_text_generation=True,
                supports_vision_generation=True,
                input_modalities=["text", "image"],
                output_modalities=["text"],
            )
        )
    return out


def _polza_provider_state() -> ProviderStateSnapshot:
    settings = get_settings()
    available = bool(settings.polza_api_key)
    ready = bool(settings.polza_api_key and settings.polza_text_model)
    health_status = "configured" if settings.polza_api_key else "missing_api_key"
    return ProviderStateSnapshot(
        provider=PROVIDER_POLZA,
        available=available,
        ready=ready,
        readiness_state=derive_provider_readiness(
            available=available,
            ready=ready,
            health_status=health_status,
            quota_pressure="unknown",
        ),
        health_status=health_status,
        key_present=available,
        quota_pressure="unknown",
        catalog=ModelCatalogSnapshot(
            provider=PROVIDER_POLZA,
            available=bool(settings.polza_api_key and settings.polza_text_model),
            fetched_at=time.time(),
            source="synthetic",
            status="configured" if settings.polza_api_key else "missing_api_key",
            models=_polza_synthetic_models(settings),
        ),
        metadata={
            "text_model": settings.polza_text_model,
            "synthesis_model": settings.polza_synthesis_model,
            "vision_model": settings.polza_vision_model,
        },
    )


async def fetch_provider_state_snapshot(policy: RoutingPolicyV2 | None = None) -> dict[str, Any]:
    wormsoft_state = await _wormsoft_provider_state()
    openrouter_state = await _openrouter_provider_state()
    gigachat_balance = await fetch_gigachat_balance()
    gigachat_state = _giga_provider_state(gigachat_balance)
    polza_state = _polza_provider_state()
    states = [wormsoft_state, openrouter_state, polza_state, gigachat_state]
    models_by_provider: dict[str, list[str]] = {}
    for state in states:
        models_by_provider[state.provider] = [
            item.model
            for item in ((state.catalog.models if state.catalog else []) or [])
            if str(item.model or "").strip()
        ]
    budget_manager = ProviderBudgetManager(redis=get_client())
    try:
        runtime_budgets = await budget_manager.snapshot(
            [state.provider for state in states],
            models_by_provider=models_by_provider,
            task_families=["text_generation", "vision_generation", "embeddings"],
            execution_roles=["primary", "shadow"],
        )
    finally:
        await budget_manager.close()
    runtime_budget_map: dict[str, list[BudgetWindowState]] = {}
    for budget in runtime_budgets:
        runtime_budget_map.setdefault(budget.provider, []).append(budget)
    for state in states:
        state.budgets.extend(runtime_budget_map.get(state.provider, []))
        provider_runtime_budget = next(
            (
                item
                for item in state.budgets
                if item.scope == "runtime_usage"
            ),
            None,
        )
        if provider_runtime_budget and provider_runtime_budget.status == "hard_limited":
            state.quota_pressure = "runtime_hard_cap"
            state.readiness_state = derive_provider_readiness(
                available=state.available,
                ready=state.ready,
                health_status=state.health_status,
                quota_pressure=state.quota_pressure,
            )
        elif provider_runtime_budget and provider_runtime_budget.status == "soft_limited":
            if state.quota_pressure in {"ok", "unknown"}:
                state.quota_pressure = "runtime_soft_cap"
            state.readiness_state = derive_provider_readiness(
                available=state.available,
                ready=state.ready,
                health_status=state.health_status,
                quota_pressure=state.quota_pressure,
            )
    return {
        "status": "ok",
        "fetched_at": time.time(),
        "policy_family_count": len(policy.model_dump()) if policy else 0,
        "providers": [state.model_dump() for state in states],
    }


async def fetch_budget_state_snapshot() -> dict[str, Any]:
    provider_state = await fetch_provider_state_snapshot()
    budgets: list[dict[str, Any]] = []
    for provider in provider_state.get("providers", []):
        for budget in provider.get("budgets") or []:
            budgets.append(budget)
    finops = await fetch_llm_finops_snapshot()
    return {
        "status": "ok",
        "fetched_at": time.time(),
        "budgets": budgets,
        "costs": list(finops.get("costs") or []),
        "reconciliations": list(finops.get("reconciliations") or []),
        "summary": dict(finops.get("summary") or {}),
    }


async def fetch_capability_matrix_snapshot() -> dict[str, Any]:
    provider_state = await fetch_provider_state_snapshot()
    rows: list[dict[str, Any]] = []
    for provider in provider_state.get("providers", []):
        catalog = provider.get("catalog") or {}
        for model in catalog.get("models") or []:
            if not isinstance(model, dict):
                continue
            rows.append(
                {
                    "provider": str(provider.get("provider") or ""),
                    "model": str(model.get("model") or ""),
                    "text_generation": bool(model.get("supports_text_generation")),
                    "vision_generation": bool(model.get("supports_vision_generation")),
                    "embeddings": bool(model.get("supports_embeddings")),
                    "input_modalities": list(model.get("input_modalities") or []),
                    "output_modalities": list(model.get("output_modalities") or []),
                    "capabilities": list(model.get("capabilities") or []),
                    "dimension": (model.get("metadata") or {}).get("dimension"),
                    "context_tokens": (model.get("metadata") or {}).get("context_tokens"),
                    "distance_metric": (model.get("metadata") or {}).get("distance_metric"),
                    "index_family": (model.get("metadata") or {}).get("index_family"),
                    "query_prefix": (model.get("metadata") or {}).get("query_prefix"),
                    "document_prefix": (model.get("metadata") or {}).get("document_prefix"),
                }
            )
    rows.sort(key=lambda item: (item["provider"], item["model"]))
    return {
        "status": "ok",
        "fetched_at": time.time(),
        "rows": rows,
    }


async def fetch_circuit_state_snapshot(policy: RoutingPolicyV2) -> dict[str, Any]:
    providers = sorted(
        {
            candidate.provider
            for family in (
                policy.text_generation,
                policy.vision_generation,
                policy.embeddings,
            )
            for candidate in family.candidates
        }
    )
    models_by_provider: dict[str, list[str]] = {}
    for family in (policy.text_generation, policy.vision_generation, policy.embeddings):
        for candidate in family.candidates:
            models_by_provider.setdefault(candidate.provider, [])
            if candidate.model not in models_by_provider[candidate.provider]:
                models_by_provider[candidate.provider].append(candidate.model)

    redis = get_client()
    circuit_breaker = ProviderCircuitBreaker(redis=redis, service_name="admin")
    wormsoft_guard = WormsoftSharedGuard(redis=redis, service_name="admin")
    circuits = await circuit_breaker.snapshot(
        providers=providers,
        models_by_provider=models_by_provider,
    )
    wormsoft_state = await wormsoft_guard.get_state()
    return {
        "status": "ok",
        "fetched_at": time.time(),
        "circuits": [circuit.model_dump() for circuit in circuits],
        "wormsoft_guard": wormsoft_state,
    }


async def fetch_routing_events(limit: int = 50) -> dict[str, Any]:
    redis = get_client()
    raw_items = await redis.lrange(ROUTING_EVENTS_REDIS_KEY, 0, max(0, int(limit) - 1))
    events: list[dict[str, Any]] = []
    for raw in raw_items:
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            events.append(item)
    return {
        "status": "ok",
        "fetched_at": time.time(),
        "events": events,
    }


async def simulate_control_plane_route(
    policy: RoutingPolicyV2,
    *,
    task: str,
    task_family: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    provider_states_payload = await fetch_provider_state_snapshot(policy)
    circuits_payload = await fetch_circuit_state_snapshot(policy)
    state_index = {
        str(item.get("provider")): ProviderStateSnapshot.model_validate(item)
        for item in provider_states_payload.get("providers", [])
        if isinstance(item, dict)
    }
    decision = simulate_routing_decision(
        policy,
        task=task,
        task_family=task_family,
        provider_states=state_index,
        circuits=[
            CircuitState.model_validate(item)
            for item in (circuits_payload.get("circuits") or [])
            if isinstance(item, dict)
        ],
        mode=mode,
    )
    return {
        "status": "ok",
        "fetched_at": time.time(),
        "decision": decision.model_dump(),
        "provider_states": provider_states_payload.get("providers", []),
        "circuits": circuits_payload.get("circuits", []),
    }

