"""OpenRouter dynamic per-model picker and runtime state helpers."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any

from admin.backend.services.openrouter_catalog import list_free_models
from shared.config import get_settings
from shared.metrics import (
    note_openrouter_picker_decision,
    note_openrouter_picker_skip,
    set_openrouter_model_health,
    set_openrouter_model_usage,
)
from shared.openrouter_limits import clamp_quarantine_until
from shared.redis_client import get_client

logger = logging.getLogger(__name__)

TASK_FAMILY_VISION_MASS = "vision_mass"
TASK_FAMILY_TEXT_STRUCTURED = "text_structured"
TASK_FAMILY_TEXT_TOOLS = "text_tools"
TASK_FAMILY_TEXT_BASIC = "text_basic"

HEALTH_KEY = "or:health:{model_id}"
RPD_KEY = "or:rpd:{model_id}:{date}"
RPM_KEY = "or:rpm:{model_id}:{minute}"
DECISION_KEY = "or:picker:decision:{task_family}"
FIVE_XX_BURST_KEY = "or:5xx:{model_id}"

DEFAULT_RPD_TTL_SEC = 90_000
DEFAULT_RPM_TTL_SEC = 120
DEFAULT_HEALTH_TTL_SEC = 86_400

TASK_FAMILY_REQUIREMENTS: dict[str, dict[str, Any]] = {
    TASK_FAMILY_VISION_MASS: {"capability": "vision", "min_context": 16_000},
    TASK_FAMILY_TEXT_STRUCTURED: {"capability": "structured", "min_context": 8_000},
    TASK_FAMILY_TEXT_TOOLS: {"capability": "tools", "min_context": 32_000},
    TASK_FAMILY_TEXT_BASIC: {"capability": None, "min_context": 4_000},
}

_RESERVE_SCRIPT = None

_RESERVE_SLOT_LUA = """
local health_key = KEYS[1]
local rpm_key = KEYS[2]
local rpd_key = KEYS[3]

local now_ts = tonumber(ARGV[1])
local rpm_limit = tonumber(ARGV[2])
local rpd_limit = tonumber(ARGV[3])
local rpd_ttl = tonumber(ARGV[4])
local rpm_ttl = tonumber(ARGV[5])

local quarantine_until = tonumber(redis.call('HGET', health_key, 'in_quarantine_until') or '0')
if quarantine_until > now_ts then
  return {
    'guard_quarantine',
    quarantine_until,
    tonumber(redis.call('GET', rpd_key) or '0'),
    tonumber(redis.call('GET', rpm_key) or '0')
  }
end

local rpm_used = tonumber(redis.call('GET', rpm_key) or '0')
if rpm_used >= rpm_limit then
  return {
    'guard_rpm_throttle',
    quarantine_until,
    tonumber(redis.call('GET', rpd_key) or '0'),
    rpm_used
  }
end

local rpd_used = tonumber(redis.call('GET', rpd_key) or '0')
if rpd_used >= rpd_limit then
  return {'guard_rpd_soft_cap', quarantine_until, rpd_used, rpm_used}
end

rpd_used = tonumber(redis.call('INCR', rpd_key) or '0')
if rpd_used == 1 then
  redis.call('EXPIRE', rpd_key, rpd_ttl)
end

local rpm_used_after = tonumber(redis.call('INCR', rpm_key) or '0')
if rpm_used_after == 1 then
  redis.call('EXPIRE', rpm_key, rpm_ttl)
end

return {'ok', quarantine_until, rpd_used, rpm_used_after}
"""


@dataclass
class ModelHealth:
    success: int = 0
    fail: int = 0
    latency_ms: float = 0.0
    last_check: float = 0.0
    in_quarantine_until: float = 0.0
    last_status_code: int = 0
    last_probe_profile: str = ""

    @property
    def total(self) -> int:
        return self.success + self.fail

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.5
        return self.success / self.total


def _utc_date_key() -> str:
    return time.strftime("%Y%m%d", time.gmtime())


def _utc_minute_key() -> str:
    return time.strftime("%Y%m%d%H%M", time.gmtime())


def task_family_for_task(task: str) -> str:
    normalized = str(task or "").strip().lower()
    if normalized in {"relevance_concepts", "concepts"}:
        return TASK_FAMILY_TEXT_STRUCTURED
    if normalized == "mcp_synthesis":
        return TASK_FAMILY_TEXT_TOOLS
    return TASK_FAMILY_TEXT_BASIC


def task_family_for_vision(quality_tier: str) -> str | None:
    tier = str(quality_tier or "").strip().lower()
    if tier in {"trusted", "primary"}:
        return None
    return TASK_FAMILY_VISION_MASS


def _capability_pass(model: dict[str, Any], req: dict[str, Any]) -> bool:
    capability = req.get("capability")
    if capability == "vision" and not model.get("supports_vision"):
        return False
    if capability == "structured" and not model.get("supports_structured"):
        return False
    if capability == "tools" and not model.get("supports_tools"):
        return False
    if int(model.get("context_length") or 0) < int(req.get("min_context") or 0):
        return False
    return True


def _score(
    model: dict[str, Any],
    health: ModelHealth,
    *,
    rpd_used: int,
    rpd_limit: int,
    safety_buffer: float,
    now: float,
) -> float:
    score = 100.0
    score += 50.0 * health.success_rate
    score += 30.0 * max(0.0, (rpd_limit - rpd_used) / max(1, rpd_limit))
    if health.latency_ms > 0:
        score -= 20.0 * min(1.0, health.latency_ms / 30_000.0)
    if health.in_quarantine_until > now:
        score -= 1000.0
    if rpd_used >= int(rpd_limit * (1.0 - safety_buffer)):
        score -= 500.0
    return score


async def _iter_keys(redis, pattern: str) -> list[str]:
    if hasattr(redis, "scan_iter"):
        keys: list[str] = []
        async for key in redis.scan_iter(match=pattern):
            keys.append(str(key))
        return keys
    store = getattr(redis, "store", {})
    return [str(key) for key in store if fnmatch(str(key), pattern)]


async def _load_health(redis, model_id: str) -> ModelHealth:
    raw = await redis.hgetall(HEALTH_KEY.format(model_id=model_id))
    if not raw:
        return ModelHealth()

    def _int(name: str) -> int:
        try:
            return int(raw.get(name) or 0)
        except (TypeError, ValueError):
            return 0

    def _float(name: str) -> float:
        try:
            return float(raw.get(name) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    return ModelHealth(
        success=_int("success"),
        fail=_int("fail"),
        latency_ms=_float("latency_ms"),
        last_check=_float("last_check"),
        # Потолок применяем и на ЧТЕНИИ: в Redis могут лежать отравленные
        # значения, записанные до исправления парсера (карантин до 58554 года).
        # Без этого они провисели бы до истечения TTL ключа.
        in_quarantine_until=clamp_quarantine_until(_float("in_quarantine_until")),
        last_status_code=_int("last_status_code"),
        last_probe_profile=str(raw.get("last_probe_profile") or ""),
    )


async def _store_health(redis, model_id: str, health: ModelHealth) -> None:
    key = HEALTH_KEY.format(model_id=model_id)
    await redis.hset(
        key,
        mapping={
            "success": health.success,
            "fail": health.fail,
            "latency_ms": health.latency_ms,
            "last_check": health.last_check,
            "in_quarantine_until": health.in_quarantine_until,
            "last_status_code": health.last_status_code,
            "last_probe_profile": health.last_probe_profile,
        },
    )
    await redis.expire(key, DEFAULT_HEALTH_TTL_SEC)


async def _load_usage(redis, model_id: str) -> tuple[int, int]:
    rpd_key = RPD_KEY.format(model_id=model_id, date=_utc_date_key())
    rpm_key = RPM_KEY.format(model_id=model_id, minute=_utc_minute_key())
    return int(await redis.get(rpd_key) or 0), int(await redis.get(rpm_key) or 0)


async def _reserve_slot_fallback(redis, model_id: str) -> tuple[bool, str, int, int]:
    settings = get_settings()
    health = await _load_health(redis, model_id)
    now = time.time()
    if health.in_quarantine_until > now:
        rpd_used, rpm_used = await _load_usage(redis, model_id)
        return False, "guard_quarantine", rpd_used, rpm_used

    rpm_key = RPM_KEY.format(model_id=model_id, minute=_utc_minute_key())
    rpd_key = RPD_KEY.format(model_id=model_id, date=_utc_date_key())
    rpm_used = int(await redis.get(rpm_key) or 0)
    if rpm_used >= int(settings.openrouter_free_rpm_throttle or 18):
        rpd_used = int(await redis.get(rpd_key) or 0)
        return False, "guard_rpm_throttle", rpd_used, rpm_used

    rpd_used = int(await redis.get(rpd_key) or 0)
    if rpd_used >= int(settings.openrouter_free_rpd_soft_cap or 850):
        return False, "guard_rpd_soft_cap", rpd_used, rpm_used

    rpd_used = int(await redis.incr(rpd_key) or 0)
    await redis.expire(rpd_key, DEFAULT_RPD_TTL_SEC)
    rpm_used = int(await redis.incr(rpm_key) or 0)
    await redis.expire(rpm_key, DEFAULT_RPM_TTL_SEC)
    return True, "ok", rpd_used, rpm_used


async def reserve_model_slot(model_id: str, *, service_name: str = "worker") -> tuple[bool, str]:
    global _RESERVE_SCRIPT

    settings = get_settings()
    redis = get_client()
    rpd_key = RPD_KEY.format(model_id=model_id, date=_utc_date_key())
    rpm_key = RPM_KEY.format(model_id=model_id, minute=_utc_minute_key())
    health_key = HEALTH_KEY.format(model_id=model_id)

    if hasattr(redis, "register_script"):
        if _RESERVE_SCRIPT is None:
            _RESERVE_SCRIPT = redis.register_script(_RESERVE_SLOT_LUA)
        try:
            raw = await _RESERVE_SCRIPT(
                keys=[health_key, rpm_key, rpd_key],
                args=[
                    str(time.time()),
                    str(int(settings.openrouter_free_rpm_throttle or 18)),
                    str(int(settings.openrouter_free_rpd_soft_cap or 850)),
                    str(DEFAULT_RPD_TTL_SEC),
                    str(DEFAULT_RPM_TTL_SEC),
                ],
                client=redis,
            )
            result = list(raw or [])
            ok = str(result[0] if result else "guard_unavailable") == "ok"
            reason = str(result[0] if result else "guard_unavailable")
            rpd_used = int(result[2] if len(result) > 2 else 0)
            rpm_used = int(result[3] if len(result) > 3 else 0)
            set_openrouter_model_usage(
                service_name,
                model_id,
                rpd_used=rpd_used,
                rpm_used=rpm_used,
            )
            return ok, reason
        except Exception:
            logger.debug("openrouter_reserve_slot_lua_failed_fallback", exc_info=True)

    ok, reason, rpd_used, rpm_used = await _reserve_slot_fallback(redis, model_id)
    set_openrouter_model_usage(service_name, model_id, rpd_used=rpd_used, rpm_used=rpm_used)
    return ok, reason


async def pick_model(
    task_family: str,
    *,
    force_refresh: bool = False,
    service_name: str = "worker",
) -> dict[str, Any]:
    settings = get_settings()
    redis = get_client()
    now = time.time()
    sticky_ttl = int(settings.openrouter_picker_sticky_sec or 600)
    decision_key = DECISION_KEY.format(task_family=task_family)

    if not force_refresh:
        cached_raw = await redis.get(decision_key)
        if cached_raw:
            try:
                cached = json.loads(str(cached_raw))
            except json.JSONDecodeError:
                cached = None
            if isinstance(cached, dict) and float(cached.get("sticky_until") or 0.0) > now:
                model_id = str(cached.get("model_id") or "")
                if model_id:
                    allowed, reason = await reserve_model_slot(model_id, service_name=service_name)
                    if allowed:
                        cached["source"] = "sticky"
                        note_openrouter_picker_decision(service_name, task_family, model_id)
                        return cached
                    note_openrouter_picker_skip(service_name, task_family, reason)

    req = (
        TASK_FAMILY_REQUIREMENTS.get(task_family)
        or TASK_FAMILY_REQUIREMENTS[TASK_FAMILY_TEXT_BASIC]
    )
    models = await list_free_models()
    candidates: list[dict[str, Any]] = []
    rpd_limit = int(settings.openrouter_free_rpd_soft_cap or 850)
    safety_buffer = float(settings.openrouter_rpd_safety_buffer or 0.1)

    for model in models:
        if not _capability_pass(model, req):
            continue
        health = await _load_health(redis, model["id"])
        rpd_used, rpm_used = await _load_usage(redis, model["id"])
        set_openrouter_model_health(
            service_name,
            model["id"],
            success_rate=health.success_rate,
            latency_ms=health.latency_ms,
            is_quarantined=health.in_quarantine_until > now,
        )
        set_openrouter_model_usage(
            service_name,
            model["id"],
            rpd_used=rpd_used,
            rpm_used=rpm_used,
        )
        candidate = {
            "model_id": model["id"],
            "health_rate": health.success_rate,
            "p95_ms": health.latency_ms,
            "rpd_used": rpd_used,
            "rpm_used": rpm_used,
            "in_quarantine": health.in_quarantine_until > now,
            "near_cap": rpd_used >= int(rpd_limit * (1.0 - safety_buffer)),
            "score": _score(
                model,
                health,
                rpd_used=rpd_used,
                rpd_limit=rpd_limit,
                safety_buffer=safety_buffer,
                now=now,
            ),
        }
        candidates.append(candidate)

    if not candidates:
        note_openrouter_picker_skip(service_name, task_family, "no_capable_model")
        return {
            "task_family": task_family,
            "model_id": None,
            "reason": "no_capable_model",
            "decided_at": now,
            "candidates": [],
        }

    candidates.sort(key=lambda item: item["score"], reverse=True)
    last_reason = "all_quarantined"
    for candidate in candidates:
        model_id = str(candidate["model_id"])
        if candidate["in_quarantine"]:
            continue
        if candidate["near_cap"]:
            last_reason = "near_cap"
            note_openrouter_picker_skip(service_name, task_family, "near_cap")
            continue
        allowed, reason = await reserve_model_slot(model_id, service_name=service_name)
        if not allowed:
            last_reason = reason
            note_openrouter_picker_skip(service_name, task_family, reason)
            continue
        decision = {
            "task_family": task_family,
            "model_id": model_id,
            "score": candidate["score"],
            "decided_at": now,
            "sticky_until": now + sticky_ttl,
            "source": "fresh",
            "candidates": candidates[:5],
        }
        await redis.set(decision_key, json.dumps(decision), ex=sticky_ttl)
        note_openrouter_picker_decision(service_name, task_family, model_id)
        return decision

    note_openrouter_picker_skip(service_name, task_family, last_reason)
    return {
        "task_family": task_family,
        "model_id": None,
        "reason": last_reason,
        "decided_at": now,
        "candidates": candidates[:5],
    }


async def record_call_result(
    model_id: str,
    *,
    success: bool,
    latency_ms: float,
    status_code: int | None = None,
    or_reset_at: float | None = None,
    probe_profile: str = "",
    service_name: str = "worker",
) -> dict[str, Any]:
    settings = get_settings()
    redis = get_client()
    health = await _load_health(redis, model_id)
    if success:
        health.success += 1
    else:
        health.fail += 1
    if latency_ms > 0:
        health.latency_ms = (
            latency_ms
            if health.latency_ms <= 0
            else (0.7 * health.latency_ms + 0.3 * latency_ms)
        )
    health.last_check = time.time()
    effective_status = int(status_code or 200) if success else int(status_code or 0)
    health.last_status_code = effective_status
    if probe_profile:
        health.last_probe_profile = probe_profile

    now = time.time()
    if not success:
        if status_code == 429:
            # Даже с исправленным парсером не доверяем значению безоговорочно:
            # потолок в сутки делает худший случай самовосстанавливающимся.
            health.in_quarantine_until = max(
                health.in_quarantine_until,
                max(now + 60.0, clamp_quarantine_until(or_reset_at, now=now)),
            )
        elif status_code == 402:
            health.in_quarantine_until = max(
                health.in_quarantine_until,
                now + float(settings.openrouter_free_quarantine_5xx_sec or 900),
            )
        elif status_code is not None and 500 <= status_code <= 599:
            burst_key = FIVE_XX_BURST_KEY.format(model_id=model_id)
            burst = int(await redis.incr(burst_key) or 0)
            await redis.expire(
                burst_key,
                int(settings.openrouter_quarantine_5xx_window_sec or 300),
            )
            if burst >= int(settings.openrouter_quarantine_5xx_threshold or 3):
                health.in_quarantine_until = max(
                    health.in_quarantine_until,
                    now + float(settings.openrouter_free_quarantine_5xx_sec or 900),
                )

    await _store_health(redis, model_id, health)
    rpd_used, rpm_used = await _load_usage(redis, model_id)
    set_openrouter_model_health(
        service_name,
        model_id,
        success_rate=health.success_rate,
        latency_ms=health.latency_ms,
        is_quarantined=health.in_quarantine_until > now,
    )
    set_openrouter_model_usage(
        service_name,
        model_id,
        rpd_used=rpd_used,
        rpm_used=rpm_used,
    )
    return {
        "model_id": model_id,
        "success_rate": health.success_rate,
        "latency_ms": health.latency_ms,
        "quarantine_until": health.in_quarantine_until,
        "rpd_used": rpd_used,
        "rpm_used": rpm_used,
    }


async def list_picker_decisions() -> list[dict[str, Any]]:
    redis = get_client()
    now = time.time()
    decisions: list[dict[str, Any]] = []
    for key in await _iter_keys(redis, DECISION_KEY.format(task_family="*")):
        raw = await redis.get(key)
        if not raw:
            continue
        try:
            payload = json.loads(str(raw))
        except json.JSONDecodeError:
            continue
        if float(payload.get("sticky_until") or 0.0) <= now:
            await redis.delete(key)
            continue
        decisions.append(payload)
    decisions.sort(key=lambda item: str(item.get("task_family") or ""))
    return decisions


async def fetch_openrouter_runtime_state(*, service_name: str = "admin") -> dict[str, Any]:
    redis = get_client()
    models = await list_free_models()
    decisions = await list_picker_decisions()
    now = time.time()
    snapshots: list[dict[str, Any]] = []
    usable = 0
    quarantined = 0
    near_cap = 0
    settings = get_settings()
    rpd_limit = int(settings.openrouter_free_rpd_soft_cap or 850)
    safety_buffer = float(settings.openrouter_rpd_safety_buffer or 0.1)

    for model in models:
        health = await _load_health(redis, model["id"])
        rpd_used, rpm_used = await _load_usage(redis, model["id"])
        is_quarantined = health.in_quarantine_until > now
        is_near_cap = rpd_used >= int(rpd_limit * (1.0 - safety_buffer))
        if is_quarantined:
            quarantined += 1
        elif is_near_cap:
            near_cap += 1
        else:
            usable += 1
        set_openrouter_model_health(
            service_name,
            model["id"],
            success_rate=health.success_rate,
            latency_ms=health.latency_ms,
            is_quarantined=is_quarantined,
        )
        set_openrouter_model_usage(
            service_name,
            model["id"],
            rpd_used=rpd_used,
            rpm_used=rpm_used,
        )
        snapshots.append(
            {
                "model_id": model["id"],
                "supports_vision": bool(model.get("supports_vision")),
                "supports_structured": bool(model.get("supports_structured")),
                "supports_tools": bool(model.get("supports_tools")),
                "context_length": int(model.get("context_length") or 0),
                "success_rate": health.success_rate,
                "latency_ms": health.latency_ms,
                "last_check": health.last_check,
                "quarantine_until": health.in_quarantine_until,
                "rpd_used": rpd_used,
                "rpm_used": rpm_used,
                "near_cap": is_near_cap,
            }
        )

    return {
        "status": "ok",
        "fetched_at": now,
        "usable_model_count": usable,
        "quarantined_model_count": quarantined,
        "near_cap_model_count": near_cap,
        "model_count": len(models),
        "decision_count": len(decisions),
        "decisions": decisions,
        "models": snapshots,
    }


async def reconcile_openrouter_state(*, service_name: str = "admin") -> dict[str, Any]:
    state = await fetch_openrouter_runtime_state(service_name=service_name)
    state["status"] = "ok"
    state["reconciled_at"] = time.time()
    return state
