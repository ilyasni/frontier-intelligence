"""Redis-backed runtime budget accounting for provider executions."""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from shared.config import get_settings
from shared.llm_control_plane import BudgetWindowState, CostAggregateState, ExecutionReceipt
from shared.llm_routing import PROVIDER_GIGACHAT, PROVIDER_OPENROUTER, PROVIDER_POLZA, PROVIDER_WORMSOFT, normalize_provider

logger = logging.getLogger(__name__)


def _day_key() -> str:
    return time.strftime("%Y%m%d", time.gmtime())


class ProviderBudgetManager:
    """Tracks runtime reservations/commits for provider requests."""

    def __init__(self, redis=None, *, settings=None) -> None:
        self._settings = settings or get_settings()
        self._redis = redis
        self._managed_redis = None

    async def close(self) -> None:
        if self._managed_redis is not None:
            await self._managed_redis.aclose()

    async def _client(self):
        if self._redis is not None:
            return self._redis
        if self._managed_redis is None:
            try:
                import redis.asyncio as aioredis

                redis_url = str(getattr(self._settings, "redis_url", "") or "").strip()
                if not redis_url:
                    return None
                self._managed_redis = aioredis.from_url(
                    redis_url,
                    decode_responses=True,
                )
            except Exception:
                logger.debug("budget_redis_client_init_failed", exc_info=True)
                return None
        return self._managed_redis

    @staticmethod
    def _credit_window_label(window_seconds: int) -> str:
        now = int(time.time())
        size = max(60, int(window_seconds or 3600))
        return str(now // size)

    @staticmethod
    def _credit_window_key(provider: str, *, window_seconds: int) -> str:
        normalized_provider = normalize_provider(provider)
        return f"llm:budget:credit_window:{normalized_provider}:{window_seconds}:{ProviderBudgetManager._credit_window_label(window_seconds)}"

    @staticmethod
    def _runtime_budget_key(
        provider: str,
        *,
        day: str | None = None,
        scope: str = "runtime_usage",
        model: str = "",
        task_family: str = "",
        execution_role: str = "",
    ) -> str:
        normalized_provider = normalize_provider(provider)
        suffix = ["llm:budget:runtime", normalized_provider, scope]
        if model:
            suffix.append(str(model).replace(":", "__").replace("/", "__"))
        if task_family:
            suffix.append(str(task_family))
        if execution_role:
            suffix.append(str(execution_role))
        suffix.append(day or _day_key())
        return ":".join(suffix)

    @staticmethod
    def _reservation_scopes(reservation: dict[str, Any]) -> list[dict[str, str]]:
        provider = normalize_provider(reservation.get("provider"))
        model = str(reservation.get("model") or "").strip()
        task_family = str(reservation.get("task_family") or "").strip()
        execution_role = str(reservation.get("execution_role") or "primary").strip()
        scopes = [
            {
                "scope": "runtime_usage",
                "provider": provider,
                "model": "",
                "task_family": "",
                "execution_role": "",
            },
        ]
        if model:
            scopes.append(
                {
                    "scope": "runtime_model",
                    "provider": provider,
                    "model": model,
                    "task_family": "",
                    "execution_role": "",
                }
            )
        if task_family:
            scopes.append(
                {
                    "scope": "runtime_task_family",
                    "provider": provider,
                    "model": "",
                    "task_family": task_family,
                    "execution_role": "",
                }
            )
        if execution_role:
            scopes.append(
                {
                    "scope": "runtime_execution_role",
                    "provider": provider,
                    "model": "",
                    "task_family": "",
                    "execution_role": execution_role,
                }
            )
        return scopes

    def _scope_limits(
        self,
        *,
        provider: str,
        scope: str,
        task_family: str = "",
        execution_role: str = "",
    ) -> tuple[float | None, float | None]:
        settings = self._settings
        normalized_provider = normalize_provider(provider)
        soft_limit: float | None = None
        hard_limit: float | None = None

        if scope == "runtime_usage":
            provider_soft_fields = {
                PROVIDER_OPENROUTER: getattr(settings, "llm_runtime_provider_openrouter_daily_request_soft_cap", 0),
                PROVIDER_WORMSOFT: getattr(settings, "llm_runtime_provider_wormsoft_daily_request_soft_cap", 0),
                PROVIDER_POLZA: getattr(settings, "llm_runtime_provider_polza_daily_request_soft_cap", 0),
                PROVIDER_GIGACHAT: getattr(settings, "llm_runtime_provider_gigachat_daily_request_soft_cap", 0),
            }
            provider_hard_fields = {
                PROVIDER_OPENROUTER: getattr(settings, "llm_runtime_provider_openrouter_daily_request_limit", 0),
                PROVIDER_WORMSOFT: getattr(settings, "llm_runtime_provider_wormsoft_daily_request_limit", 0),
                PROVIDER_POLZA: getattr(settings, "llm_runtime_provider_polza_daily_request_limit", 0),
                PROVIDER_GIGACHAT: getattr(settings, "llm_runtime_provider_gigachat_daily_request_limit", 0),
            }
            soft_limit = float(provider_soft_fields.get(normalized_provider) or 0.0) or None
            hard_limit = float(provider_hard_fields.get(normalized_provider) or 0.0) or None
            if normalized_provider == PROVIDER_OPENROUTER and soft_limit is None:
                fallback_cap = float(getattr(settings, "openrouter_free_rpd_soft_cap", 0) or 0.0)
                soft_limit = fallback_cap or None
        elif scope == "runtime_execution_role" and execution_role == "shadow":
            soft_limit = float(getattr(settings, "llm_runtime_shadow_daily_request_soft_cap", 0) or 0.0) or None
            hard_limit = float(getattr(settings, "llm_runtime_shadow_daily_request_limit", 0) or 0.0) or None
        elif scope == "runtime_task_family" and task_family == "embeddings":
            soft_limit = float(getattr(settings, "llm_runtime_embeddings_daily_request_soft_cap", 0) or 0.0) or None
            hard_limit = float(getattr(settings, "llm_runtime_embeddings_daily_request_limit", 0) or 0.0) or None

        return soft_limit, hard_limit

    @staticmethod
    def _finops_key(
        provider: str,
        *,
        day: str | None = None,
        scope: str = "cost_provider",
        model: str = "",
        task_family: str = "",
        execution_role: str = "",
        workspace_id: str = "",
    ) -> str:
        normalized_provider = normalize_provider(provider)
        suffix = ["llm:cost:runtime", normalized_provider, scope]
        if model:
            suffix.append(str(model).replace(":", "__").replace("/", "__"))
        if task_family:
            suffix.append(str(task_family))
        if execution_role:
            suffix.append(str(execution_role))
        if workspace_id:
            suffix.append(str(workspace_id).replace(":", "__").replace("/", "__"))
        suffix.append(day or _day_key())
        return ":".join(suffix)

    @staticmethod
    def _finops_scopes(payload: dict[str, Any]) -> list[dict[str, str]]:
        provider = normalize_provider(payload.get("provider"))
        model = str(payload.get("model") or "").strip()
        task_family = str(payload.get("task_family") or "").strip()
        execution_role = str(payload.get("execution_role") or "primary").strip()
        workspace_id = str(payload.get("workspace_id") or "").strip()
        scopes = [
            {
                "scope": "cost_provider",
                "provider": provider,
                "model": "",
                "task_family": "",
                "execution_role": "",
                "workspace_id": "",
            }
        ]
        if model:
            scopes.append(
                {
                    "scope": "cost_model",
                    "provider": provider,
                    "model": model,
                    "task_family": "",
                    "execution_role": "",
                    "workspace_id": "",
                }
            )
        if task_family:
            scopes.append(
                {
                    "scope": "cost_task_family",
                    "provider": provider,
                    "model": "",
                    "task_family": task_family,
                    "execution_role": "",
                    "workspace_id": "",
                }
            )
        if execution_role:
            scopes.append(
                {
                    "scope": "cost_execution_role",
                    "provider": provider,
                    "model": "",
                    "task_family": "",
                    "execution_role": execution_role,
                    "workspace_id": "",
                }
            )
        if workspace_id:
            scopes.append(
                {
                    "scope": "cost_workspace",
                    "provider": provider,
                    "model": "",
                    "task_family": "",
                    "execution_role": "",
                    "workspace_id": workspace_id,
                }
            )
        return scopes

    async def _write_many(self, commands: list[tuple[str, tuple[Any, ...]]]) -> None:
        redis = await self._client()
        if redis is None or not commands:
            return
        pipeline_factory = getattr(redis, "pipeline", None)
        if callable(pipeline_factory):
            try:
                async with redis.pipeline() as pipe:
                    for method_name, args in commands:
                        getattr(pipe, method_name)(*args)
                    await pipe.execute()
                return
            except Exception:
                logger.debug("budget_pipeline_write_failed_fallback_sequential", exc_info=True)
        for method_name, args in commands:
            await getattr(redis, method_name)(*args)

    async def add_credit_usage(
        self,
        *,
        provider: str,
        credits: float,
        window_seconds: int,
    ) -> float:
        redis = await self._client()
        if redis is None:
            return 0.0
        key = self._credit_window_key(provider, window_seconds=window_seconds)
        ttl = max(2 * max(60, int(window_seconds or 3600)), 120)
        try:
            total = await redis.hincrbyfloat(key, "used_credits", float(max(0.0, credits or 0.0)))
            await redis.hset(
                key,
                mapping={
                    "provider": normalize_provider(provider),
                    "window_seconds": str(max(60, int(window_seconds or 3600))),
                    "updated_at": str(time.time()),
                },
            )
            await redis.expire(key, ttl)
            return float(total or 0.0)
        except Exception:
            logger.warning("budget_add_credit_usage_failed", exc_info=True)
            return 0.0

    async def credit_window_usage(self, *, provider: str, window_seconds: int) -> float:
        redis = await self._client()
        if redis is None:
            return 0.0
        key = self._credit_window_key(provider, window_seconds=window_seconds)
        try:
            raw = await redis.hgetall(key)
        except Exception:
            logger.warning("budget_credit_window_read_failed", exc_info=True)
            return 0.0
        try:
            return float((raw or {}).get("used_credits") or 0.0)
        except (TypeError, ValueError):
            return 0.0

    async def _scope_snapshot(
        self,
        *,
        provider: str,
        scope: str,
        model: str = "",
        task_family: str = "",
        execution_role: str = "",
    ) -> BudgetWindowState:
        redis = await self._client()
        now = time.time()
        key = self._runtime_budget_key(
            provider,
            scope=scope,
            model=model,
            task_family=task_family,
            execution_role=execution_role,
        )
        try:
            raw = await redis.hgetall(key) if redis is not None else {}
        except Exception:
            logger.debug("budget_scope_snapshot_read_failed", exc_info=True)
            raw = {}

        def _num(name: str) -> float:
            try:
                return float(raw.get(name) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        reserved_units = _num("reserved_units")
        committed_units = _num("committed_units")
        active_requests = _num("active_requests")
        soft_limit, hard_limit = self._scope_limits(
            provider=provider,
            scope=scope,
            task_family=task_family,
            execution_role=execution_role,
        )
        outstanding = max(0.0, active_requests)
        current_load = committed_units + outstanding
        effective_limit = hard_limit if hard_limit is not None else soft_limit
        remaining = None if effective_limit is None else max(0.0, effective_limit - current_load)
        status = "ok" if raw else "empty"
        if hard_limit is not None and current_load >= hard_limit:
            status = "hard_limited"
        elif soft_limit is not None and current_load >= soft_limit:
            status = "soft_limited"
        return BudgetWindowState(
            provider=normalize_provider(provider),
            scope=scope,
            window_label=_day_key(),
            model=str(raw.get("model") or model or ""),
            task_family=str(raw.get("task_family") or task_family or ""),
            execution_role=str(raw.get("execution_role") or execution_role or ""),
            limit=hard_limit,
            soft_limit=soft_limit,
            remaining=remaining,
            used=committed_units,
            reserved=reserved_units,
            committed=committed_units,
            released=_num("released_requests"),
            outstanding=outstanding,
            unit=str(raw.get("unit") or "requests"),
            status=status,
            refreshed_at=_num("updated_at") or now,
        )

    async def allow_reservation(
        self,
        *,
        provider: str,
        model: str,
        task_family: str,
        execution_role: str = "primary",
    ) -> tuple[bool, str, list[BudgetWindowState]]:
        scopes = self._reservation_scopes(
            {
                "provider": provider,
                "model": model,
                "task_family": task_family,
                "execution_role": execution_role,
            }
        )
        snapshots: list[BudgetWindowState] = []
        for scope in scopes:
            snapshot = await self._scope_snapshot(
                provider=scope["provider"],
                scope=scope["scope"],
                model=scope["model"],
                task_family=scope["task_family"],
                execution_role=scope["execution_role"],
            )
            snapshots.append(snapshot)
            if snapshot.status == "hard_limited":
                reason_parts = [snapshot.scope]
                if snapshot.task_family:
                    reason_parts.append(snapshot.task_family)
                if snapshot.execution_role:
                    reason_parts.append(snapshot.execution_role)
                return False, f"runtime_hard_cap:{'/'.join(reason_parts)}", snapshots
            if snapshot.status == "soft_limited":
                reason_parts = [snapshot.scope]
                if snapshot.task_family:
                    reason_parts.append(snapshot.task_family)
                if snapshot.execution_role:
                    reason_parts.append(snapshot.execution_role)
                return False, f"runtime_soft_cap:{'/'.join(reason_parts)}", snapshots
        return True, "ok", snapshots

    async def reserve(
        self,
        *,
        provider: str,
        model: str,
        task_family: str,
        requested_units: float = 1.0,
        unit: str = "requests",
        execution_role: str = "primary",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        reservation = {
            "reservation_id": uuid.uuid4().hex,
            "provider": normalize_provider(provider),
            "model": str(model or "").strip(),
            "task_family": str(task_family or "").strip(),
            "requested_units": float(requested_units or 0.0),
            "unit": str(unit or "requests"),
            "execution_role": str(execution_role or "primary"),
            "reserved_at": time.time(),
            "day": _day_key(),
        }
        if metadata:
            reservation.update(dict(metadata))
        redis = await self._client()
        if redis is None:
            return reservation
        ttl = 3 * 24 * 3600
        try:
            for scope in self._reservation_scopes(reservation):
                key = self._runtime_budget_key(
                    scope["provider"],
                    day=reservation["day"],
                    scope=scope["scope"],
                    model=scope["model"],
                    task_family=scope["task_family"],
                    execution_role=scope["execution_role"],
                )
                await redis.hincrbyfloat(key, "reserved_units", reservation["requested_units"])
                await redis.hincrby(key, "reserved_requests", 1)
                await redis.hincrby(key, "active_requests", 1)
                await redis.hset(
                    key,
                    mapping={
                        "provider": reservation["provider"],
                        "unit": reservation["unit"],
                        "scope": scope["scope"],
                        "model": scope["model"],
                        "task_family": scope["task_family"],
                        "execution_role": scope["execution_role"],
                        "updated_at": str(time.time()),
                    },
                )
                await redis.expire(key, ttl)
        except Exception:
            logger.warning("budget_reserve_write_failed", exc_info=True)
            return reservation
        return reservation

    async def commit(
        self,
        reservation: dict[str, Any] | None,
        *,
        actual_units: float | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        billable_tokens: int = 0,
    ) -> dict[str, Any]:
        payload = dict(reservation or {})
        provider = normalize_provider(payload.get("provider"))
        if not provider:
            return payload
        payload["actual_units"] = float(
            actual_units
            if actual_units is not None
            else payload.get("requested_units") or 0.0
        )
        payload["prompt_tokens"] = int(prompt_tokens or 0)
        payload["completion_tokens"] = int(completion_tokens or 0)
        payload["billable_tokens"] = int(billable_tokens or 0)
        payload["committed_at"] = time.time()
        redis = await self._client()
        if redis is None:
            return payload
        ttl = 3 * 24 * 3600
        try:
            for scope in self._reservation_scopes(payload):
                key = self._runtime_budget_key(
                    scope["provider"],
                    day=payload.get("day"),
                    scope=scope["scope"],
                    model=scope["model"],
                    task_family=scope["task_family"],
                    execution_role=scope["execution_role"],
                )
                await redis.hincrbyfloat(key, "committed_units", payload["actual_units"])
                await redis.hincrby(key, "committed_requests", 1)
                await redis.hincrby(key, "active_requests", -1)
                await redis.hincrby(key, "prompt_tokens", payload["prompt_tokens"])
                await redis.hincrby(key, "completion_tokens", payload["completion_tokens"])
                await redis.hincrby(key, "billable_tokens", payload["billable_tokens"])
                await redis.hset(key, mapping={"updated_at": str(time.time())})
                await redis.expire(key, ttl)
        except Exception:
            logger.warning("budget_commit_write_failed", exc_info=True)
            return payload
        return payload

    async def record_execution_receipt(self, receipt: ExecutionReceipt | None) -> None:
        if receipt is None:
            return
        provider = normalize_provider(receipt.actual_provider or receipt.requested_provider)
        if not provider:
            return
        payload = {
            "provider": provider,
            "model": str(receipt.actual_model or receipt.requested_model or "").strip(),
            "task_family": str(receipt.task_family or "").strip(),
            "execution_role": str(receipt.execution_role or "primary").strip(),
            "workspace_id": str(receipt.workspace_id or "").strip(),
            "status": str(receipt.status or "").strip().lower(),
            "estimated_cost": float(receipt.cost_estimate or 0.0)
            if receipt.cost_estimate is not None
            else 0.0,
            "actual_cost": float(receipt.actual_cost or 0.0)
            if receipt.actual_cost is not None
            else 0.0,
            "cost_drift": float(receipt.cost_drift or 0.0)
            if receipt.cost_drift is not None
            else 0.0,
            "prompt_tokens": int(receipt.budget_attribution.get("prompt_tokens") or 0),
            "completion_tokens": int(receipt.budget_attribution.get("completion_tokens") or 0),
            "billable_tokens": int(receipt.budget_attribution.get("billable_tokens") or 0),
            "unit": str(receipt.cost_currency or "credits"),
            "day": _day_key(),
        }
        redis = await self._client()
        if redis is None:
            return
        now = time.time()
        ttl = 3 * 24 * 3600
        commands: list[tuple[str, tuple[Any, ...]]] = []
        for scope in self._finops_scopes(payload):
            key = self._finops_key(
                scope["provider"],
                day=payload["day"],
                scope=scope["scope"],
                model=scope["model"],
                task_family=scope["task_family"],
                execution_role=scope["execution_role"],
                workspace_id=scope["workspace_id"],
            )
            commands.extend(
                [
                    ("hincrby", (key, "request_count", 1)),
                    (
                        "hincrby",
                        (
                            key,
                            "success_count" if payload["status"] == "ok" else "error_count",
                            1,
                        ),
                    ),
                    ("hincrbyfloat", (key, "estimated_cost_total", payload["estimated_cost"])),
                    ("hincrbyfloat", (key, "actual_cost_total", payload["actual_cost"])),
                    ("hincrbyfloat", (key, "cost_drift_total", payload["cost_drift"])),
                    ("hincrby", (key, "prompt_tokens", payload["prompt_tokens"])),
                    ("hincrby", (key, "completion_tokens", payload["completion_tokens"])),
                    ("hincrby", (key, "billable_tokens", payload["billable_tokens"])),
                    (
                        "hset",
                        (
                            key,
                            {
                                "provider": payload["provider"],
                                "scope": scope["scope"],
                                "model": scope["model"],
                                "task_family": scope["task_family"],
                                "execution_role": scope["execution_role"],
                                "workspace_id": scope["workspace_id"],
                                "unit": payload["unit"],
                                "updated_at": str(now),
                                "last_status": payload["status"],
                            },
                        ),
                    ),
                    ("expire", (key, ttl)),
                ]
            )
        try:
            normalized_commands = []
            for method_name, args in commands:
                if method_name == "hset":
                    key, mapping = args
                    normalized_commands.append((method_name, (key,)))
                    await redis.hset(key, mapping=mapping)
                else:
                    normalized_commands.append((method_name, args))
            filtered = [item for item in normalized_commands if item[0] != "hset"]
            await self._write_many(filtered)
        except Exception:
            logger.warning("budget_receipt_write_failed", exc_info=True)
            return

    async def release(self, reservation: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(reservation or {})
        provider = normalize_provider(payload.get("provider"))
        if not provider:
            return payload
        payload["released_at"] = time.time()
        redis = await self._client()
        if redis is None:
            return payload
        ttl = 3 * 24 * 3600
        try:
            for scope in self._reservation_scopes(payload):
                key = self._runtime_budget_key(
                    scope["provider"],
                    day=payload.get("day"),
                    scope=scope["scope"],
                    model=scope["model"],
                    task_family=scope["task_family"],
                    execution_role=scope["execution_role"],
                )
                await redis.hincrby(key, "released_requests", 1)
                await redis.hincrby(key, "active_requests", -1)
                await redis.hset(key, mapping={"updated_at": str(time.time())})
                await redis.expire(key, ttl)
        except Exception:
            logger.warning("budget_release_write_failed", exc_info=True)
            return payload
        return payload

    async def snapshot(
        self,
        providers: list[str],
        *,
        models_by_provider: dict[str, list[str]] | None = None,
        task_families: list[str] | None = None,
        execution_roles: list[str] | None = None,
    ) -> list[BudgetWindowState]:
        redis = await self._client()
        now = time.time()
        if redis is None:
            return [
                BudgetWindowState(
                    provider=provider,
                    scope="runtime_usage",
                    window_label=_day_key(),
                    unit="requests",
                    status="unavailable",
                    refreshed_at=now,
                )
                for provider in providers
            ]

        result: list[BudgetWindowState] = []
        models_by_provider = models_by_provider or {}
        normalized_roles = sorted({str(role or "").strip() for role in (execution_roles or []) if str(role or "").strip()})
        normalized_families = sorted({str(family or "").strip() for family in (task_families or []) if str(family or "").strip()})
        for provider in sorted({normalize_provider(provider) for provider in providers}):
            scope_descriptors = [
                {"scope": "runtime_usage", "model": "", "task_family": "", "execution_role": ""}
            ]
            for model in sorted({str(item or "").strip() for item in models_by_provider.get(provider, []) if str(item or "").strip()}):
                scope_descriptors.append(
                    {
                        "scope": "runtime_model",
                        "model": model,
                        "task_family": "",
                        "execution_role": "",
                    }
                )
            for task_family in normalized_families:
                scope_descriptors.append(
                    {
                        "scope": "runtime_task_family",
                        "model": "",
                        "task_family": task_family,
                        "execution_role": "",
                    }
                )
            for execution_role in normalized_roles:
                scope_descriptors.append(
                    {
                        "scope": "runtime_execution_role",
                        "model": "",
                        "task_family": "",
                        "execution_role": execution_role,
                    }
                )

            for descriptor in scope_descriptors:
                key = self._runtime_budget_key(
                    provider,
                    scope=descriptor["scope"],
                    model=descriptor["model"],
                    task_family=descriptor["task_family"],
                    execution_role=descriptor["execution_role"],
                )
                try:
                    raw = await redis.hgetall(key)
                except Exception:
                    logger.debug("budget_snapshot_scope_read_failed", exc_info=True)
                    raw = {}

                result.append(
                    await self._scope_snapshot(
                        provider=provider,
                        scope=descriptor["scope"],
                        model=descriptor["model"],
                        task_family=descriptor["task_family"],
                        execution_role=descriptor["execution_role"],
                    )
                )
        return result

    async def _cost_snapshot(
        self,
        *,
        provider: str,
        scope: str,
        model: str = "",
        task_family: str = "",
        execution_role: str = "",
        workspace_id: str = "",
    ) -> CostAggregateState:
        redis = await self._client()
        now = time.time()
        key = self._finops_key(
            provider,
            scope=scope,
            model=model,
            task_family=task_family,
            execution_role=execution_role,
            workspace_id=workspace_id,
        )
        try:
            raw = await redis.hgetall(key) if redis is not None else {}
        except Exception:
            logger.debug("budget_cost_snapshot_read_failed", exc_info=True)
            raw = {}

        def _num_float(name: str) -> float:
            try:
                return float(raw.get(name) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        def _num_int(name: str) -> int:
            try:
                return int(float(raw.get(name) or 0.0))
            except (TypeError, ValueError):
                return 0

        return CostAggregateState(
            provider=normalize_provider(provider),
            scope=scope,
            window_label=_day_key(),
            model=str(raw.get("model") or model or ""),
            task_family=str(raw.get("task_family") or task_family or ""),
            execution_role=str(raw.get("execution_role") or execution_role or ""),
            workspace_id=str(raw.get("workspace_id") or workspace_id or ""),
            request_count=_num_int("request_count"),
            success_count=_num_int("success_count"),
            error_count=_num_int("error_count"),
            estimated_cost_total=_num_float("estimated_cost_total"),
            actual_cost_total=_num_float("actual_cost_total"),
            cost_drift_total=_num_float("cost_drift_total"),
            prompt_tokens=_num_int("prompt_tokens"),
            completion_tokens=_num_int("completion_tokens"),
            billable_tokens=_num_int("billable_tokens"),
            unit=str(raw.get("unit") or "credits"),
            status="ok" if raw else "empty",
            refreshed_at=_num_float("updated_at") or now,
        )

    async def _discover_cost_models(self, provider: str) -> list[str]:
        redis = await self._client()
        if redis is None:
            return []
        scan_iter = getattr(redis, "scan_iter", None)
        if not callable(scan_iter):
            return []
        pattern = f"llm:cost:runtime:{normalize_provider(provider)}:cost_model:*:{_day_key()}"
        models: set[str] = set()
        try:
            async for key in scan_iter(match=pattern):
                raw = await redis.hgetall(key)
                model = str(raw.get("model") or "").strip()
                if model:
                    models.add(model)
        except Exception:
            logger.debug("budget_discover_cost_models_failed", exc_info=True)
            return []
        return sorted(models)

    async def snapshot_costs(
        self,
        providers: list[str],
        *,
        models_by_provider: dict[str, list[str]] | None = None,
        task_families: list[str] | None = None,
        execution_roles: list[str] | None = None,
        workspace_ids: list[str] | None = None,
    ) -> list[CostAggregateState]:
        redis = await self._client()
        now = time.time()
        if redis is None:
            return [
                CostAggregateState(
                    provider=provider,
                    scope="cost_provider",
                    window_label=_day_key(),
                    unit="credits",
                    status="unavailable",
                    refreshed_at=now,
                )
                for provider in providers
            ]

        result: list[CostAggregateState] = []
        models_by_provider = models_by_provider or {}
        normalized_roles = sorted({str(role or "").strip() for role in (execution_roles or []) if str(role or "").strip()})
        normalized_families = sorted({str(family or "").strip() for family in (task_families or []) if str(family or "").strip()})
        normalized_workspaces = sorted({str(workspace_id or "").strip() for workspace_id in (workspace_ids or []) if str(workspace_id or "").strip()})
        for provider in sorted({normalize_provider(provider) for provider in providers}):
            discovered_models = set(
                str(item or "").strip()
                for item in models_by_provider.get(provider, [])
                if str(item or "").strip()
            )
            if not discovered_models:
                discovered_models.update(await self._discover_cost_models(provider))
            scope_descriptors = [
                {
                    "scope": "cost_provider",
                    "model": "",
                    "task_family": "",
                    "execution_role": "",
                    "workspace_id": "",
                }
            ]
            for model in sorted(discovered_models):
                scope_descriptors.append(
                    {
                        "scope": "cost_model",
                        "model": model,
                        "task_family": "",
                        "execution_role": "",
                        "workspace_id": "",
                    }
                )
            for task_family in normalized_families:
                scope_descriptors.append(
                    {
                        "scope": "cost_task_family",
                        "model": "",
                        "task_family": task_family,
                        "execution_role": "",
                        "workspace_id": "",
                    }
                )
            for execution_role in normalized_roles:
                scope_descriptors.append(
                    {
                        "scope": "cost_execution_role",
                        "model": "",
                        "task_family": "",
                        "execution_role": execution_role,
                        "workspace_id": "",
                    }
                )
            for workspace_id in normalized_workspaces:
                scope_descriptors.append(
                    {
                        "scope": "cost_workspace",
                        "model": "",
                        "task_family": "",
                        "execution_role": "",
                        "workspace_id": workspace_id,
                    }
                )

            for descriptor in scope_descriptors:
                result.append(
                    await self._cost_snapshot(
                        provider=provider,
                        scope=descriptor["scope"],
                        model=descriptor["model"],
                        task_family=descriptor["task_family"],
                        execution_role=descriptor["execution_role"],
                        workspace_id=descriptor["workspace_id"],
                    )
                )
        return result
