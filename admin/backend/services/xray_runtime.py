from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis

from shared.config import get_settings
from shared.xray_profile_registry import (
    enabled_profiles,
    find_profile,
    profile_runtime_summary,
    read_active_profile_name,
    resolve_active_profile,
    touch_file,
    write_text_atomic,
)

logger = logging.getLogger(__name__)

_XRAY_REMEDIATION_HISTORY_KEY = "admin:xray:remediation:history"
_XRAY_REMEDIATION_HISTORY_MAX_ITEMS = 100
_runtime_lock = asyncio.Lock()


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _paths() -> dict[str, str]:
    settings = get_settings()
    return {
        "registry": settings.xray_profile_registry_path,
        "active": settings.xray_active_profile_path,
        "previous": settings.xray_previous_profile_path,
        "trigger": settings.xray_reload_trigger_path,
    }


def get_runtime_state() -> dict[str, Any]:
    paths = _paths()
    active_name, active_profile, profiles = resolve_active_profile(paths["registry"], paths["active"])
    previous_name = read_active_profile_name(paths["previous"])
    return {
        "mode": "registry" if active_profile else "legacy",
        "active_profile": active_name or None,
        "previous_profile": previous_name or None,
        "profiles": [
            profile_runtime_summary(item, is_active=item.get("name") == active_name)
            for item in profiles
        ],
    }


async def _record_remediation_event(payload: dict[str, Any]) -> None:
    settings = get_settings()
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        encoded = json.dumps(payload, ensure_ascii=False)
        await redis.lpush(_XRAY_REMEDIATION_HISTORY_KEY, encoded)
        await redis.ltrim(_XRAY_REMEDIATION_HISTORY_KEY, 0, _XRAY_REMEDIATION_HISTORY_MAX_ITEMS - 1)
    finally:
        await redis.aclose()


async def get_remediation_history(*, limit: int = 20) -> list[dict[str, Any]]:
    settings = get_settings()
    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        rows = await redis.lrange(_XRAY_REMEDIATION_HISTORY_KEY, 0, max(1, min(limit, 100)) - 1)
    finally:
        await redis.aclose()
    history: list[dict[str, Any]] = []
    for raw in rows:
        try:
            history.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return history


def _candidate_next_profile_name(current_name: str) -> str | None:
    paths = _paths()
    candidates = enabled_profiles(paths["registry"])
    if not candidates:
        return None
    if not current_name:
        return str(candidates[0]["name"])
    current_index = next(
        (index for index, item in enumerate(candidates) if item.get("name") == current_name),
        None,
    )
    if current_index is None:
        return str(candidates[0]["name"])
    next_index = current_index + 1
    if next_index >= len(candidates):
        return None
    return str(candidates[next_index]["name"])


async def switch_profile(
    *,
    target_name: str,
    reason: str,
    trigger: str,
    metadata: dict[str, Any] | None = None,
    allow_post_switch_remediation: bool = False,
) -> dict[str, Any]:
    target = str(target_name or "").strip()
    if not target:
        raise ValueError("target_profile_required")

    async with _runtime_lock:
        paths = _paths()
        current_name, _active_profile, profiles = resolve_active_profile(paths["registry"], paths["active"])
        wanted = find_profile(profiles, target)
        if not wanted:
            raise ValueError("unknown_target_profile")
        if not wanted.get("enabled"):
            raise ValueError("target_profile_disabled")
        if current_name == target:
            return {
                "status": "noop",
                "reason": "already_active",
                "active_profile": current_name,
                "target_profile": target,
                "runtime": get_runtime_state(),
            }

        if current_name:
            write_text_atomic(paths["previous"], current_name)
        write_text_atomic(paths["active"], target)
        touch_file(paths["trigger"])
        await asyncio.sleep(4)

    from admin.backend.services.xray_health import run_xray_health_check

    post_switch = await run_xray_health_check(allow_remediation=allow_post_switch_remediation)
    payload = {
        "status": "switched",
        "trigger": trigger,
        "reason": reason,
        "previous_profile": current_name or None,
        "active_profile": target,
        "checked_at": _utcnow_iso(),
        "metadata": metadata or {},
        "post_switch": {
            "status": post_switch.get("status"),
            "targets_failed": post_switch.get("targets_failed"),
            "targets_total": post_switch.get("targets_total"),
            "failure_ratio": post_switch.get("failure_ratio"),
            "source_smoke_status": (post_switch.get("source_smoke") or {}).get("status"),
        },
    }
    await _record_remediation_event(payload)
    logger.info("xray_profile_switched %s", json.dumps(payload, ensure_ascii=False))
    return {
        **payload,
        "runtime": get_runtime_state(),
        "health": post_switch,
    }


async def failover_to_next_profile(
    *,
    reason: str,
    trigger: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_name = get_runtime_state().get("active_profile") or ""
    target_name = _candidate_next_profile_name(str(current_name))
    if not target_name:
        payload = {
            "status": "unavailable",
            "trigger": trigger,
            "reason": reason,
            "active_profile": current_name or None,
            "checked_at": _utcnow_iso(),
            "metadata": metadata or {},
        }
        await _record_remediation_event(payload)
        return payload
    return await switch_profile(
        target_name=target_name,
        reason=reason,
        trigger=trigger,
        metadata=metadata,
        allow_post_switch_remediation=False,
    )


async def rollback_profile(*, reason: str, trigger: str) -> dict[str, Any]:
    previous_name = read_active_profile_name(_paths()["previous"])
    if not previous_name:
        payload = {
            "status": "unavailable",
            "trigger": trigger,
            "reason": reason,
            "active_profile": get_runtime_state().get("active_profile"),
            "checked_at": _utcnow_iso(),
            "metadata": {"rollback": True},
        }
        await _record_remediation_event(payload)
        return payload
    return await switch_profile(
        target_name=previous_name,
        reason=reason,
        trigger=trigger,
        metadata={"rollback": True},
        allow_post_switch_remediation=False,
    )
