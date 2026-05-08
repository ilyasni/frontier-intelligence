from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class SourceQualityBreakdown:
    authority: float
    runtime_health: float
    signal_yield: float
    freshness: float
    composite: float


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def normalize_source_authority(raw: Any, default: float = 0.5) -> float:
    try:
        return clamp01(float(raw if raw is not None else default))
    except (TypeError, ValueError):
        return clamp01(default)


def normalize_optional_bool(raw: Any) -> bool | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        value = raw.strip().lower()
        if value in {"true", "1", "yes", "on"}:
            return True
        if value in {"false", "0", "no", "off"}:
            return False
    return bool(raw)


def _parse_dt(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        return raw
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _compute_operational_status(
    *,
    source_type: str,
    recent_success_count: int,
    recent_error_count: int,
    last_run_status: str | None,
    last_error_text: str,
    last_seen_published_at: Any,
) -> dict[str, Any]:
    total_runs = recent_success_count + recent_error_count
    error_rate = recent_error_count / max(total_runs, 1)
    state = "healthy"
    reason = "stable"
    review_level = "none"
    last_run_is_error = (last_run_status or "").lower() == "error"
    text = (last_error_text or "").lower()

    if recent_error_count >= 3 and (error_rate >= 0.25 or last_run_is_error):
        state = "critical"
        reason = "repeated_errors"
        review_level = "urgent"
    elif recent_error_count >= 1 and (error_rate >= 0.1 or last_run_is_error):
        state = "degraded"
        reason = "intermittent_errors"
        review_level = "soon"

    if any(token in text for token in ("telegram_username_unresolved", "resolveusernamerequest")):
        state = "critical"
        reason = "invalid_source_handle"
        review_level = "urgent"
    elif any(token in text for token in ("connection attempts failed", "connecttimeout", "name or service not known")):
        if state != "critical":
            state = "degraded"
            reason = "network_or_dns"
            review_level = "soon"

    # Для HTTP-источников отдельно помечаем stale по дате последней публикации.
    stale_days: int | None = None
    seen_dt = _parse_dt(last_seen_published_at)
    if seen_dt and source_type in {"rss", "web", "api"}:
        if seen_dt.tzinfo is None:
            seen_dt = seen_dt.replace(tzinfo=UTC)
        stale_days = max(0, int((datetime.now(UTC) - seen_dt).total_seconds() // 86400))
        if stale_days >= 120 and state in {"healthy", "degraded"}:
            state = "stale"
            reason = "no_new_items_long_time"
            review_level = "planned"

    return {
        "state": state,
        "reason": reason,
        "review_level": review_level,
        "error_rate_14d": round(error_rate, 4),
        "recent_success_count": recent_success_count,
        "recent_error_count": recent_error_count,
        "last_seen_published_at": seen_dt.isoformat() if seen_dt else None,
        "stale_days": stale_days,
    }


def recommend_content_mode(
    *,
    source_type: str,
    last_run_status: str | None,
    last_run_error_text: str | None,
    fetched_count: int,
    emitted_count: int,
    error_rate: float,
    parse_full_content: bool | None = None,
) -> str:
    if source_type in {"telegram", "email"}:
        return "native"
    if source_type == "web":
        if parse_full_content is False:
            return "listing-only"
        if (last_run_status or "") == "error" or error_rate >= 0.4:
            return "listing-only"
        return "full-content"
    if source_type in {"rss", "api"}:
        text = (last_run_error_text or "").lower()
        if "403" in text or "429" in text or error_rate >= 0.45:
            return "summary-only"
        if fetched_count > 0 and emitted_count == 0:
            return "summary-only"
        return "full-content"
    return "summary-only"


def compute_source_quality(
    *,
    authority: float,
    success_count: int,
    error_count: int,
    fetched_count: int,
    emitted_count: int,
    relevant_ratio: float,
    avg_tag_count: float,
    linked_ratio: float,
    freshness_hours: float | None,
) -> SourceQualityBreakdown:
    authority_score = normalize_source_authority(authority)

    total_runs = max(success_count + error_count, 1)
    success_ratio = success_count / total_runs
    emission_ratio = emitted_count / max(fetched_count, 1)
    runtime_health = clamp01(success_ratio * 0.7 + emission_ratio * 0.3)

    signal_yield = clamp01(
        clamp01(relevant_ratio) * 0.6
        + clamp01(avg_tag_count / 5.0) * 0.2
        + clamp01(linked_ratio) * 0.2
    )

    if freshness_hours is None:
        freshness = 0.2
    elif freshness_hours <= 24:
        freshness = 1.0
    elif freshness_hours <= 72:
        freshness = 0.8
    elif freshness_hours <= 168:
        freshness = 0.55
    elif freshness_hours <= 720:
        freshness = 0.3
    else:
        freshness = 0.15

    composite = clamp01(
        authority_score * 0.35
        + runtime_health * 0.30
        + signal_yield * 0.20
        + freshness * 0.15
    )
    return SourceQualityBreakdown(
        authority=authority_score,
        runtime_health=runtime_health,
        signal_yield=signal_yield,
        freshness=freshness,
        composite=composite,
    )


def source_quality_payload(row: dict[str, Any]) -> dict[str, Any]:
    authority = normalize_source_authority(
        row.get("source_authority")
        if row.get("source_authority") is not None
        else (row.get("extra") or {}).get("source_authority")
        if isinstance(row.get("extra"), dict)
        else None
    )
    emitted = int(row.get("last_run_emitted_count") or 0)
    fetched = int(row.get("last_run_fetched_count") or 0)
    success_count = int(row.get("recent_success_count") or 0)
    error_count = int(row.get("recent_error_count") or 0)
    relevant_ratio = float(row.get("relevant_ratio") or 0.0)
    avg_tag_count = float(row.get("avg_tag_count") or 0.0)
    linked_ratio = float(row.get("linked_ratio") or 0.0)
    freshness_hours = row.get("freshness_hours")
    freshness_hours = None if freshness_hours is None else float(freshness_hours)
    breakdown = compute_source_quality(
        authority=authority,
        success_count=success_count,
        error_count=error_count,
        fetched_count=fetched,
        emitted_count=emitted,
        relevant_ratio=relevant_ratio,
        avg_tag_count=avg_tag_count,
        linked_ratio=linked_ratio,
        freshness_hours=freshness_hours,
    )
    recommendation = recommend_content_mode(
        source_type=str(row.get("source_type") or ""),
        last_run_status=row.get("last_run_status"),
        last_run_error_text=row.get("last_run_error_text") or row.get("last_error"),
        fetched_count=fetched,
        emitted_count=emitted,
        error_rate=error_count / max(success_count + error_count, 1),
        parse_full_content=normalize_optional_bool(
            ((row.get("extra") or {}).get("parse") or {}).get("full_content")
            if isinstance(row.get("extra"), dict)
            else None
        ),
    )
    operational_status = _compute_operational_status(
        source_type=str(row.get("source_type") or ""),
        recent_success_count=success_count,
        recent_error_count=error_count,
        last_run_status=row.get("last_run_status"),
        last_error_text=str(row.get("last_error") or row.get("last_run_error_text") or ""),
        last_seen_published_at=row.get("last_seen_published_at"),
    )
    return {
        "source_authority": breakdown.authority,
        "source_score": breakdown.composite,
        "source_score_updated_at": datetime.now(UTC).isoformat(),
        "score_breakdown": {
            "authority": round(breakdown.authority, 4),
            "runtime_health": round(breakdown.runtime_health, 4),
            "signal_yield": round(breakdown.signal_yield, 4),
            "freshness": round(breakdown.freshness, 4),
        },
        "recommended_content_mode": recommendation,
        "operational_status": operational_status,
        "quality_tier": (
            row.get("quality_tier")
            or ((row.get("extra") or {}).get("quality_tier") if isinstance(row.get("extra"), dict) else None)
            or "standard"
        ),
    }
