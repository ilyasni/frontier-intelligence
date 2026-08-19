"""Lightweight LLM finops reconciliation from cached provider snapshots + runtime aggregates."""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from admin.backend.services.gigachat_balance import _BALANCE_CACHE_KEY
from admin.backend.services.openrouter_key import _CACHE_KEY as OPENROUTER_KEY_CACHE_KEY
from admin.backend.services.wormsoft_limits import _CACHE_KEY as WORMSOFT_LIMITS_CACHE_KEY
from shared.config import get_settings
from shared.llm_control_plane import CostAggregateState, FinopsReconciliationState
from shared.llm_routing import (
    PROVIDER_GIGACHAT,
    PROVIDER_OPENROUTER,
    PROVIDER_POLZA,
    PROVIDER_WORMSOFT,
)
from shared.redis_client import get_client
from worker.provider_budget_manager import ProviderBudgetManager

logger = logging.getLogger(__name__)


async def _load_cached_json(key: str) -> dict[str, Any] | None:
    client = get_client()
    raw = await client.get(key)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _provider_cost_map(costs: list[CostAggregateState]) -> dict[str, CostAggregateState]:
    return {
        item.provider: item
        for item in costs
        if item.scope == "cost_provider"
    }


def _openrouter_free_requests(costs: list[CostAggregateState]) -> int:
    total = 0
    for item in costs:
        if item.provider != PROVIDER_OPENROUTER or item.scope != "cost_model":
            continue
        model = str(item.model or "").strip().lower()
        if model == "openrouter/free" or model.endswith(":free"):
            total += int(item.request_count or 0)
    return total


async def fetch_llm_finops_snapshot() -> dict[str, Any]:
    providers = [PROVIDER_WORMSOFT, PROVIDER_OPENROUTER, PROVIDER_POLZA, PROVIDER_GIGACHAT]
    settings = get_settings()
    budget_manager = ProviderBudgetManager(redis=get_client(), settings=settings)
    wormsoft_window_usage: float | None = None
    try:
        cost_windows = await budget_manager.snapshot_costs(
            providers,
            task_families=["text_generation", "vision_generation", "embeddings"],
            execution_roles=["primary", "shadow"],
        )
        try:
            wormsoft_window_usage = await budget_manager.credit_window_usage(
                provider=PROVIDER_WORMSOFT,
                window_seconds=int(settings.wormsoft_credit_window_seconds),
            )
        except Exception:
            logger.warning("wormsoft_finops_credit_window_read_failed", exc_info=True)
            wormsoft_window_usage = None
    finally:
        await budget_manager.close()

    provider_costs = _provider_cost_map(cost_windows)
    openrouter_key = await _load_cached_json(OPENROUTER_KEY_CACHE_KEY) or {}
    gigachat_balance = await _load_cached_json(_BALANCE_CACHE_KEY) or {}
    wormsoft_limits = await _load_cached_json(WORMSOFT_LIMITS_CACHE_KEY) or {}
    reconciliations: list[FinopsReconciliationState] = []

    for provider in providers:
        cost = provider_costs.get(provider) or CostAggregateState(
            provider=provider,
            scope="cost_provider",
            window_label=time.strftime("%Y%m%d", time.gmtime()),
            status="empty",
            refreshed_at=time.time(),
        )
        reconciliation = FinopsReconciliationState(
            provider=provider,
            status="ok" if cost.request_count or provider != PROVIDER_POLZA else "empty",
            runtime_request_count=cost.request_count,
            runtime_success_count=cost.success_count,
            runtime_error_count=cost.error_count,
            runtime_estimated_cost_total=cost.estimated_cost_total,
            runtime_actual_cost_total=cost.actual_cost_total,
            runtime_cost_drift_total=cost.cost_drift_total,
            runtime_billable_tokens=cost.billable_tokens,
            refreshed_at=max(float(cost.refreshed_at or 0.0), time.time()),
            metadata={},
        )

        if provider == PROVIDER_OPENROUTER:
            free_daily_limit = float(openrouter_key.get("free_model_daily_limit") or 0.0)
            published_usage = float(openrouter_key.get("usage_daily") or 0.0)
            runtime_free_requests = float(_openrouter_free_requests(cost_windows))
            gap = runtime_free_requests - published_usage
            status = "ok"
            if openrouter_key.get("stale"):
                status = "stale"
            elif free_daily_limit > 0 and published_usage >= free_daily_limit:
                status = "quota_exhausted"
            elif free_daily_limit > 0 and abs(gap) >= max(5.0, free_daily_limit * 0.2):
                status = "drift"
            elif free_daily_limit > 0 and published_usage >= free_daily_limit * 0.9:
                status = "near_cap"
            reconciliation = reconciliation.model_copy(
                update={
                    "status": status,
                    "published_limit": free_daily_limit or None,
                    "published_usage": published_usage,
                    "published_remaining": max(0.0, free_daily_limit - published_usage)
                    if free_daily_limit > 0
                    else float(openrouter_key.get("limit_remaining") or 0.0),
                    "gap_value": gap,
                    "gap_kind": "openrouter_free_daily_requests",
                    "unit": "requests",
                    "note": "Runtime free-request counts are reconciled against the cached OpenRouter key daily snapshot.",
                    "metadata": {
                        "runtime_free_requests": runtime_free_requests,
                        "limit_remaining_credits": float(openrouter_key.get("limit_remaining") or 0.0),
                        "is_free_tier": bool(openrouter_key.get("is_free_tier")),
                        "snapshot_status": str(openrouter_key.get("status") or ""),
                    },
                    "refreshed_at": float(openrouter_key.get("fetched_at") or reconciliation.refreshed_at or time.time()),
                }
            )
        elif provider == PROVIDER_GIGACHAT:
            balance_items = list(gigachat_balance.get("balance") or [])
            published_remaining = float(
                sum(int(item.get("value") or 0) for item in balance_items)
            )
            reconciliation = reconciliation.model_copy(
                update={
                    "status": "stale" if str(gigachat_balance.get("status") or "").startswith("stale_") else ("ok" if published_remaining > 0 else "low_balance"),
                    "published_remaining": published_remaining,
                    "unit": "tokens",
                    "gap_kind": "gigachat_balance_tokens",
                    "note": "GigaChat reconciliation tracks remaining package balance from the cached /balance snapshot.",
                    "metadata": {
                        "balance_items": balance_items,
                        "snapshot_status": str(gigachat_balance.get("status") or ""),
                    },
                    "refreshed_at": float(gigachat_balance.get("fetched_at") or reconciliation.refreshed_at or time.time()),
                }
            )
        elif provider == PROVIDER_WORMSOFT:
            plans = list(wormsoft_limits.get("plans") or [])
            published_limit = float(settings.wormsoft_credit_window_limit or 0.0)
            window_seconds = int(settings.wormsoft_credit_window_seconds or 0)
            active_plan = next(
                (
                    item
                    for item in plans
                    if int(item.get("amount") or 0) == int(published_limit)
                    and int(item.get("seconds") or 0) == window_seconds
                ),
                None,
            )
            estimated_usage = (
                float(wormsoft_window_usage) if wormsoft_window_usage is not None else None
            )
            utilization = (
                estimated_usage / published_limit
                if estimated_usage is not None and published_limit > 0.0
                else None
            )
            status = "stale" if wormsoft_limits.get("stale") else "ok"
            if published_limit <= 0.0 or estimated_usage is None:
                status = "unknown" if status == "ok" else status
            elif utilization is not None and utilization >= 0.98:
                status = "quota_exhausted"
            elif utilization is not None and utilization >= 0.8:
                status = "near_cap"
            reconciliation = reconciliation.model_copy(
                update={
                    "status": status,
                    "published_limit": published_limit or None,
                    "published_usage": estimated_usage,
                    "published_remaining": max(0.0, published_limit - estimated_usage)
                    if estimated_usage is not None and published_limit > 0.0
                    else None,
                    "gap_kind": "wormsoft_estimated_window_credits",
                    "unit": "credits",
                    "note": (
                        "Wormsoft does not publish live remaining credits; usage and remaining "
                        "are pricing-weighted estimates from the local trailing window."
                    ),
                    "metadata": {
                        "plans": plans,
                        "configured_window_seconds": window_seconds,
                        "configured_limit": published_limit,
                        "matched_active_plan": active_plan,
                        "estimated_utilization": utilization,
                        "snapshot_status": str(wormsoft_limits.get("status") or ""),
                    },
                    "refreshed_at": float(wormsoft_limits.get("fetched_at") or reconciliation.refreshed_at or time.time()),
                }
            )
        else:
            reconciliation = reconciliation.model_copy(
                update={
                    "status": "ok" if reconciliation.runtime_request_count > 0 else "empty",
                    "note": "Polza currently exposes runtime-only finops telemetry.",
                }
            )
        reconciliations.append(reconciliation)

    return {
        "status": "ok",
        "fetched_at": time.time(),
        "costs": [item.model_dump() for item in cost_windows],
        "reconciliations": [item.model_dump() for item in reconciliations],
        "summary": {
            "request_count": sum(item.runtime_request_count for item in reconciliations),
            "success_count": sum(item.runtime_success_count for item in reconciliations),
            "error_count": sum(item.runtime_error_count for item in reconciliations),
            "estimated_cost_total": sum(item.runtime_estimated_cost_total for item in reconciliations),
            "actual_cost_total": sum(item.runtime_actual_cost_total for item in reconciliations),
            "cost_drift_total": sum(item.runtime_cost_drift_total for item in reconciliations),
            "provider_count": len(reconciliations),
            "unit": "credits",
        },
    }
