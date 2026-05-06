"""Operational settings and runtime mode controls for admin UI."""
from __future__ import annotations

import json
import logging

import redis.asyncio as aioredis
from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from admin.backend.services.gigachat_balance import fetch_gigachat_balance
from admin.backend.services.gigachat_weekly_report import fetch_gigachat_weekly_report
from admin.backend.services.llm_control_plane import (
    fetch_budget_state_snapshot,
    fetch_capability_matrix_snapshot,
    fetch_circuit_state_snapshot,
    fetch_provider_state_snapshot,
    fetch_routing_events,
    simulate_control_plane_route,
)
from admin.backend.services.openrouter_catalog import fetch_openrouter_catalog
from admin.backend.services.openrouter_health import fetch_openrouter_health_snapshot
from admin.backend.services.openrouter_key import fetch_openrouter_key
from admin.backend.services.openrouter_picker import fetch_openrouter_runtime_state
from admin.backend.services.wormsoft_limits import fetch_wormsoft_limits
from admin.backend.services.wormsoft_models import fetch_wormsoft_models
from shared.config import get_settings
from shared.llm_control_plane import (
    CONTROL_PLANE_POLICY_DB_KEY,
    CONTROL_PLANE_POLICY_REDIS_KEY,
    RoutingPolicyV2,
    default_routing_policy_v2,
)
from shared.llm_routing import (
    RUNTIME_LLM_ROUTING_DB_KEY,
    RUNTIME_LLM_ROUTING_REDIS_KEY,
    LLMRoutingSettings,
    effective_llm_routing,
    llm_provider_options,
)
from shared.runtime_modes import (
    RUNTIME_MODE_DB_KEY,
    RUNTIME_MODE_REDIS_KEY,
    effective_runtime_snapshot,
    normalize_runtime_mode,
    runtime_mode_options,
    runtime_overrides_for_mode,
)

router = APIRouter()
logger = logging.getLogger(__name__)


class RuntimeModeRequest(BaseModel):
    mode: str


class LLMRoutingRequest(LLMRoutingSettings):
    pass


class ControlPlanePolicyRequest(RoutingPolicyV2):
    pass


class RouteSimulationRequest(BaseModel):
    task: str
    task_family: str | None = None


async def _ensure_runtime_settings_table() -> None:
    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS admin_runtime_settings (
                    key TEXT PRIMARY KEY,
                    value JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        await session.commit()


async def _load_json_setting(key: str) -> tuple[dict | None, str | None]:
    await _ensure_runtime_settings_table()
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT value, updated_at FROM admin_runtime_settings WHERE key = :key"),
            {"key": key},
        )
        row = result.mappings().first()
        if not row:
            return None, None
        value = row["value"]
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                value = {}
        if not isinstance(value, dict):
            value = {}
        updated_at = row["updated_at"]
        updated_at_str = (
            updated_at.isoformat()
            if hasattr(updated_at, "isoformat")
            else str(updated_at)
        )
        return value, updated_at_str


async def _store_json_setting(key: str, payload: dict) -> str | None:
    await _ensure_runtime_settings_table()
    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                """
                INSERT INTO admin_runtime_settings (key, value, updated_at)
                VALUES (:key, CAST(:value AS jsonb), NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """
            ),
            {
                "key": key,
                "value": json.dumps(payload),
            },
        )
        await session.commit()

    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT updated_at FROM admin_runtime_settings WHERE key = :key"),
            {"key": key},
        )
        row = result.mappings().first()
        updated_at = row["updated_at"] if row else None
        return updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at or "")


async def _mirror_runtime_value(redis_key: str, value: str) -> None:
    settings = get_settings()
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await redis_client.set(redis_key, value)
    finally:
        await redis_client.aclose()


async def _load_runtime_mode_from_db() -> tuple[str | None, str | None]:
    value, updated_at = await _load_json_setting(RUNTIME_MODE_DB_KEY)
    if not value:
        return None, updated_at
    mode = normalize_runtime_mode((value or {}).get("mode"))
    return mode, updated_at


async def _runtime_mode_payload(source: str = "db") -> dict:
    settings = get_settings()
    db_mode, updated_at = await _load_runtime_mode_from_db()
    configured_mode = normalize_runtime_mode(settings.runtime_mode)
    mode = db_mode or configured_mode
    if db_mode:
        try:
            await _mirror_runtime_value(RUNTIME_MODE_REDIS_KEY, mode)
        except Exception as exc:
            logger.warning("runtime_mode_redis_mirror_failed mode=%s err=%s", mode, exc)
    overrides = runtime_overrides_for_mode(mode)
    return {
        "mode": mode,
        "configured_mode": configured_mode,
        "source": source if db_mode else "env",
        "updated_at": updated_at,
        "options": runtime_mode_options(),
        "overrides": overrides,
        "effective": effective_runtime_snapshot(settings, mode),
    }


async def _store_runtime_mode(mode: str) -> dict:
    normalized = normalize_runtime_mode(mode)
    await _store_json_setting(RUNTIME_MODE_DB_KEY, {"mode": normalized})
    try:
        await _mirror_runtime_value(RUNTIME_MODE_REDIS_KEY, normalized)
    except Exception as exc:
        logger.warning("runtime_mode_redis_mirror_failed mode=%s err=%s", normalized, exc)
    return await _runtime_mode_payload(source="db+redis")


async def _llm_routing_payload(source: str = "db") -> dict:
    settings = get_settings()
    runtime_mode = await _runtime_mode_payload()
    stored_payload, updated_at = await _load_json_setting(RUNTIME_LLM_ROUTING_DB_KEY)
    effective = effective_llm_routing(settings, runtime_mode["mode"], stored_payload)
    if stored_payload:
        try:
            await _mirror_runtime_value(
                RUNTIME_LLM_ROUTING_REDIS_KEY,
                json.dumps(stored_payload),
            )
        except Exception as exc:
            logger.warning("llm_routing_redis_mirror_failed err=%s", exc)
    return {
        "source": source if stored_payload else "env",
        "updated_at": updated_at,
        "providers": llm_provider_options(),
        "configured": stored_payload or {},
        "effective": effective.model_dump(),
    }


async def _store_llm_routing(request: LLMRoutingRequest) -> dict:
    payload = request.model_dump()
    await _store_json_setting(RUNTIME_LLM_ROUTING_DB_KEY, payload)
    try:
        await _mirror_runtime_value(RUNTIME_LLM_ROUTING_REDIS_KEY, json.dumps(payload))
    except Exception as exc:
        logger.warning("llm_routing_redis_mirror_failed err=%s", exc)
    return await _llm_routing_payload(source="db+redis")


async def _control_plane_policy_payload(source: str = "db") -> dict:
    settings = get_settings()
    runtime_mode = await _runtime_mode_payload()
    legacy_routing_payload, _ = await _load_json_setting(RUNTIME_LLM_ROUTING_DB_KEY)
    stored_payload, updated_at = await _load_json_setting(CONTROL_PLANE_POLICY_DB_KEY)
    effective = default_routing_policy_v2(settings, runtime_mode["mode"], legacy_routing_payload)
    if stored_payload:
        effective = RoutingPolicyV2.model_validate(stored_payload)
        try:
            await _mirror_runtime_value(
                CONTROL_PLANE_POLICY_REDIS_KEY,
                json.dumps(stored_payload),
            )
        except Exception as exc:
            logger.warning("control_plane_policy_redis_mirror_failed err=%s", exc)
    return {
        "source": source if stored_payload else "derived",
        "updated_at": updated_at,
        "effective": effective.model_dump(),
    }


async def _store_control_plane_policy(request: ControlPlanePolicyRequest) -> dict:
    payload = request.model_dump()
    await _store_json_setting(CONTROL_PLANE_POLICY_DB_KEY, payload)
    try:
        await _mirror_runtime_value(CONTROL_PLANE_POLICY_REDIS_KEY, json.dumps(payload))
    except Exception as exc:
        logger.warning("control_plane_policy_redis_mirror_failed err=%s", exc)
    return await _control_plane_policy_payload(source="db+redis")


@router.get("")
async def get_admin_settings():
    settings = get_settings()
    runtime_mode = await _runtime_mode_payload()
    llm_routing = await _llm_routing_payload()
    control_plane_policy = await _control_plane_policy_payload()
    effective = runtime_mode["effective"]
    return {
        "business": {
            "default_relevance_threshold": settings.default_relevance_threshold,
        },
        "runtime": {
            "runtime_mode": effective["runtime_mode"],
            "indexing_batch_size": settings.indexing_batch_size,
            "indexing_max_concurrency": effective["indexing_max_concurrency"],
            "indexing_max_retries": settings.indexing_max_retries,
            "indexing_backoff_ms": settings.indexing_backoff_ms,
            "indexing_claim_idle_ms": settings.indexing_claim_idle_ms,
            "indexing_consumer_cleanup_interval": settings.indexing_consumer_cleanup_interval,
            "vision_enabled": effective["vision_enabled"],
            "gpt2giga_enable_images": effective["gpt2giga_enable_images"],
            "vision_claim_idle_ms": settings.vision_claim_idle_ms,
            "vision_max_delivery_count": settings.vision_max_delivery_count,
            "vision_dlq_stream": settings.vision_dlq_stream,
            "gigachat_model": effective["gigachat_model"],
            "gigachat_model_lite": effective["gigachat_model_lite"],
            "gigachat_model_pro": effective["gigachat_model_pro"],
            "gigachat_model_max": effective["gigachat_model_max"],
            "gigachat_model_relevance": effective["gigachat_model_relevance"],
            "gigachat_model_concepts": effective["gigachat_model_concepts"],
            "gigachat_model_valence": effective["gigachat_model_valence"],
            "gigachat_model_mcp_synthesis": effective["gigachat_model_mcp_synthesis"],
            "gigachat_model_vision": effective["gigachat_model_vision"],
            "gigachat_escalation_enabled": effective["gigachat_escalation_enabled"],
            "redis_stream_lag_alert_threshold": settings.redis_stream_lag_alert_threshold,
            "redis_stream_pending_alert_threshold": settings.redis_stream_pending_alert_threshold,
            "redis_stream_oldest_pending_age_alert_seconds": (
                settings.redis_stream_oldest_pending_age_alert_seconds
            ),
            "gigachat_rc_joint_enabled": settings.gigachat_rc_joint_enabled,
            "gigachat_rc_joint_workspaces": settings.gigachat_rc_joint_workspaces,
            "gigachat_rc_joint_sources": settings.gigachat_rc_joint_sources,
            "admin_wormsoft_limits_refresh_cron": settings.admin_wormsoft_limits_refresh_cron,
            "admin_openrouter_catalog_refresh_cron": settings.admin_openrouter_catalog_refresh_cron,
            "admin_openrouter_key_refresh_cron": settings.admin_openrouter_key_refresh_cron,
            "admin_openrouter_health_refresh_cron": settings.admin_openrouter_health_refresh_cron,
            "admin_openrouter_reconcile_cron": settings.admin_openrouter_reconcile_cron,
            "sparse_vectors_enabled": settings.sparse_vectors_enabled,
            "searxng_enabled": settings.searxng_enabled,
            "searxng_timeout_seconds": settings.searxng_timeout_seconds,
            "searxng_max_results": settings.searxng_max_results,
            "missing_signals_enabled": settings.missing_signals_enabled,
            "missing_signals_topic_limit": settings.missing_signals_topic_limit,
            "missing_signals_min_gap_score": settings.missing_signals_min_gap_score,
            "embed_dim": settings.embed_dim,
        },
        "integrations": {
            "mcp_internal_url": settings.mcp_internal_url,
            "openai_api_base": settings.openai_api_base,
            "wormsoft_api_base": settings.wormsoft_api_base,
            "qdrant_url": settings.qdrant_url,
            "neo4j_url": settings.neo4j_url,
            "s3_endpoint_url": settings.s3_endpoint_url,
            "s3_bucket_name": settings.s3_bucket_name,
            "paddleocr_url": effective["paddleocr_url"],
            "prometheus_url": settings.prometheus_url,
        },
        "runtime_modes": runtime_mode,
        "llm_routing": llm_routing,
        "control_plane_policy": control_plane_policy,
        "providers": {
            "wormsoft": {
                "api_base": settings.wormsoft_api_base,
                "key_present": bool(settings.wormsoft_api_key),
            },
            "openrouter": {
                "api_base": settings.openrouter_base_url,
                "text_model": settings.openrouter_text_model,
                "vision_model": settings.openrouter_vision_model,
                "key_present": bool(settings.openrouter_api_key),
                "free_rpm_throttle": settings.openrouter_free_rpm_throttle,
                "free_rpd_soft_cap": settings.openrouter_free_rpd_soft_cap,
                "picker_sticky_sec": settings.openrouter_picker_sticky_sec,
                "health_probe_timeout_sec": settings.openrouter_health_probe_timeout_sec,
                "health_probe_max_tokens": settings.openrouter_health_probe_max_tokens,
            },
            "polza": {
                "api_base": settings.polza_base_url,
                "text_model": settings.polza_text_model,
                "synthesis_model": settings.polza_synthesis_model,
                "vision_model": settings.polza_vision_model,
                "key_present": bool(settings.polza_api_key),
            },
        },
        "secrets": {
            "database_url_set": bool(settings.database_url),
            "gigachat_credentials_set": bool(settings.gigachat_credentials),
            "wormsoft_api_key_set": bool(settings.wormsoft_api_key),
            "openrouter_api_key_set": bool(settings.openrouter_api_key),
            "polza_api_key_set": bool(settings.polza_api_key),
            "neo4j_password_set": bool(settings.neo4j_password),
            "s3_access_key_set": bool(settings.s3_access_key_id),
            "s3_secret_key_set": bool(settings.s3_secret_access_key),
            "telegram_bot_token_set": bool(settings.telegram_bot_token),
            "telegram_alert_chat_id_set": bool(settings.telegram_alert_chat_id),
            "telegram_account_0_set": bool(settings.tg_api_id_0 and settings.tg_api_hash_0),
            "telegram_account_1_set": bool(settings.tg_api_id_1 and settings.tg_api_hash_1),
        },
    }


@router.get("/runtime-mode")
async def get_runtime_mode():
    return await _runtime_mode_payload()


@router.post("/runtime-mode")
async def set_runtime_mode(request: RuntimeModeRequest):
    return await _store_runtime_mode(request.mode)


@router.get("/llm-routing")
async def get_llm_routing():
    return await _llm_routing_payload()


@router.post("/llm-routing")
async def set_llm_routing(request: LLMRoutingRequest):
    return await _store_llm_routing(request)


@router.get("/policy")
async def get_control_plane_policy():
    return await _control_plane_policy_payload()


@router.post("/policy")
async def set_control_plane_policy(request: ControlPlanePolicyRequest):
    return await _store_control_plane_policy(request)


@router.post("/simulate")
async def simulate_control_plane(request: RouteSimulationRequest):
    payload = await _control_plane_policy_payload()
    policy = RoutingPolicyV2.model_validate(payload["effective"])
    return await simulate_control_plane_route(
        policy,
        task=request.task,
        task_family=request.task_family,
    )


@router.get("/provider-state")
async def get_provider_state():
    payload = await _control_plane_policy_payload()
    policy = RoutingPolicyV2.model_validate(payload["effective"])
    return await fetch_provider_state_snapshot(policy)


@router.get("/budget-state")
async def get_budget_state():
    return await fetch_budget_state_snapshot()


@router.get("/capability-matrix")
async def get_capability_matrix():
    return await fetch_capability_matrix_snapshot()


@router.get("/circuits")
async def get_circuits():
    payload = await _control_plane_policy_payload()
    policy = RoutingPolicyV2.model_validate(payload["effective"])
    return await fetch_circuit_state_snapshot(policy)


@router.get("/routing-events")
async def get_routing_events(limit: int = 50):
    return await fetch_routing_events(limit=limit)


@router.get("/providers/wormsoft/models")
async def get_wormsoft_models():
    return await fetch_wormsoft_models()


@router.get("/providers/wormsoft/limits")
async def get_wormsoft_limits():
    return await fetch_wormsoft_limits()


@router.get("/providers/openrouter/catalog")
async def get_openrouter_catalog():
    return await fetch_openrouter_catalog()


@router.get("/providers/openrouter/key")
async def get_openrouter_key():
    return await fetch_openrouter_key()


@router.get("/providers/openrouter/health")
async def get_openrouter_health():
    return await fetch_openrouter_health_snapshot()


@router.get("/providers/openrouter/runtime")
async def get_openrouter_runtime():
    return await fetch_openrouter_runtime_state(service_name="admin")


@router.get("/gigachat-balance")
async def get_gigachat_balance():
    return await fetch_gigachat_balance()


@router.get("/gigachat-weekly-report")
async def get_gigachat_weekly_report():
    return await fetch_gigachat_weekly_report()
