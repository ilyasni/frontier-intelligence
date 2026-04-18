"""Urgent Telegram alerts for confirmed trend clusters."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from admin.backend.services.telegram_alerts import (
    send_telegram_alert_message,
    telegram_alerts_enabled,
)
from shared.config import Settings, get_settings

logger = logging.getLogger(__name__)

ALERT_KIND_URGENT_STABLE = "urgent_stable_trend"
_TELEGRAM_MESSAGE_LIMIT = 3900
_ALERT_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS trend_alerts (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    trend_cluster_id TEXT NOT NULL,
    cluster_key TEXT NOT NULL,
    alert_kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'error')),
    score FLOAT DEFAULT 0.0,
    message TEXT,
    attempts INTEGER DEFAULT 1,
    last_error TEXT,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workspace_id, cluster_key, alert_kind)
)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trend_alerts_sent_at
    ON trend_alerts(sent_at DESC)
    WHERE status = 'sent'
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trend_alerts_cluster
    ON trend_alerts(workspace_id, trend_cluster_id, alert_kind)
    """,
)

_schema_ready = False


async def ensure_trend_alerts_table() -> None:
    global _schema_ready
    if _schema_ready:
        return

    engine = get_engine()
    async with engine.begin() as conn:
        for statement in _ALERT_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))
    _schema_ready = True


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _list_value(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _truncate(value: Any, limit: int) -> str:
    text_value = str(value or "").strip()
    if len(text_value) <= limit:
        return text_value
    return f"{text_value[: max(0, limit - 3)].rstrip()}..."


def _format_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "").strip()


def _candidate_reason(row: dict[str, Any], settings: Settings) -> str | None:
    doc_count = _int_value(row.get("doc_count"))
    source_count = _int_value(row.get("source_count"))
    if doc_count < settings.trend_alert_min_doc_count:
        return None
    if source_count < settings.trend_alert_min_source_count:
        return None

    signal_score = _float_value(row.get("signal_score"))
    if signal_score >= settings.trend_alert_min_signal_score:
        return "high_score"

    has_change_point = _bool_value(row.get("has_recent_change_point"))
    change_point_strength = _float_value(row.get("change_point_strength"))
    if (
        has_change_point
        and signal_score >= settings.trend_alert_change_point_min_signal_score
        and change_point_strength >= settings.trend_alert_min_change_point_strength
    ):
        return "change_point"
    return None


def _reason_label(reason: str) -> str:
    if reason == "change_point":
        return "recent change point with strong confirmation"
    return "high stable trend score"


def _build_alert_message(row: dict[str, Any], reason: str) -> str:
    keywords = [
        str(item).strip()
        for item in _list_value(row.get("keywords"))
        if str(item).strip()
    ][:8]

    lines = [
        "Frontier urgent trend alert",
        f"workspace: {row.get('workspace_id')}",
        f"title: {_truncate(row.get('title'), 220)}",
        (
            f"score: {_float_value(row.get('signal_score')):.3f}"
            f" | docs: {_int_value(row.get('doc_count'))}"
            f" | sources: {_int_value(row.get('source_count'))}"
        ),
        (
            f"reason: {_reason_label(reason)}"
            f" | change_point: {_float_value(row.get('change_point_strength')):.3f}"
        ),
    ]

    detected_at = _format_datetime(row.get("detected_at"))
    if detected_at:
        lines.append(f"detected_at: {detected_at}")

    insight = _truncate(row.get("insight"), 700)
    opportunity = _truncate(row.get("opportunity"), 700)
    if insight:
        lines.append(f"insight: {insight}")
    if opportunity:
        lines.append(f"opportunity: {opportunity}")
    if keywords:
        lines.append(f"keywords: {', '.join(keywords)}")

    message = "\n".join(lines)
    return _truncate(message, _TELEGRAM_MESSAGE_LIMIT)


def _remaining_weekly_capacity(sent_count: int, settings: Settings) -> int:
    return max(0, settings.trend_alert_max_per_7d - sent_count)


async def _sent_alert_count_7d(session: AsyncSession) -> int:
    result = await session.execute(
        text(
            """
            SELECT COUNT(*) AS cnt
            FROM trend_alerts
            WHERE alert_kind = :alert_kind
              AND status = 'sent'
              AND COALESCE(sent_at, created_at) >= NOW() - INTERVAL '7 days'
            """
        ),
        {"alert_kind": ALERT_KIND_URGENT_STABLE},
    )
    return int(result.scalar() or 0)


async def _fetch_candidates(
    session: AsyncSession,
    *,
    settings: Settings,
    limit: int,
) -> list[dict[str, Any]]:
    result = await session.execute(
        text(
            """
            SELECT
                tc.id,
                tc.workspace_id,
                tc.cluster_key,
                tc.title,
                tc.insight,
                tc.opportunity,
                tc.signal_score,
                tc.signal_stage,
                tc.doc_count,
                tc.source_count,
                tc.keywords,
                tc.has_recent_change_point,
                tc.change_point_strength,
                tc.detected_at
            FROM trend_clusters tc
            JOIN workspaces w ON w.id = tc.workspace_id
            WHERE w.is_active IS TRUE
              AND tc.signal_stage = 'stable'
              AND tc.detected_at >= NOW() - (:window_hours * INTERVAL '1 hour')
              AND tc.doc_count >= :min_doc_count
              AND tc.source_count >= :min_source_count
              AND (
                    tc.signal_score >= :min_signal_score
                    OR (
                        tc.has_recent_change_point IS TRUE
                        AND tc.signal_score >= :change_point_min_signal_score
                        AND tc.change_point_strength >= :min_change_point_strength
                    )
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM trend_alerts ta
                    WHERE ta.workspace_id = tc.workspace_id
                      AND ta.cluster_key = tc.cluster_key
                      AND ta.alert_kind = :alert_kind
                      AND ta.status = 'sent'
              )
            ORDER BY
                tc.signal_score DESC,
                tc.has_recent_change_point DESC,
                tc.source_count DESC,
                tc.doc_count DESC,
                tc.detected_at DESC
            LIMIT :limit
            """
        ),
        {
            "window_hours": settings.trend_alert_window_hours,
            "min_doc_count": settings.trend_alert_min_doc_count,
            "min_source_count": settings.trend_alert_min_source_count,
            "min_signal_score": settings.trend_alert_min_signal_score,
            "change_point_min_signal_score": (
                settings.trend_alert_change_point_min_signal_score
            ),
            "min_change_point_strength": settings.trend_alert_min_change_point_strength,
            "alert_kind": ALERT_KIND_URGENT_STABLE,
            "limit": limit,
        },
    )
    rows = [dict(row) for row in result.mappings().all()]
    return [row for row in rows if _candidate_reason(row, settings)]


async def _claim_alert(
    session: AsyncSession,
    *,
    row: dict[str, Any],
    reason: str,
    message: str,
) -> str | None:
    alert_id = f"trend-alert:{uuid4().hex}"
    result = await session.execute(
        text(
            """
            INSERT INTO trend_alerts (
                id,
                workspace_id,
                trend_cluster_id,
                cluster_key,
                alert_kind,
                reason,
                status,
                score,
                message,
                attempts,
                created_at,
                updated_at
            )
            VALUES (
                :id,
                :workspace_id,
                :trend_cluster_id,
                :cluster_key,
                :alert_kind,
                :reason,
                'pending',
                :score,
                :message,
                1,
                NOW(),
                NOW()
            )
            ON CONFLICT (workspace_id, cluster_key, alert_kind)
            DO UPDATE SET
                trend_cluster_id = EXCLUDED.trend_cluster_id,
                reason = EXCLUDED.reason,
                status = 'pending',
                score = EXCLUDED.score,
                message = EXCLUDED.message,
                attempts = trend_alerts.attempts + 1,
                last_error = NULL,
                updated_at = NOW()
            WHERE trend_alerts.status = 'error'
               OR (
                    trend_alerts.status = 'pending'
                    AND trend_alerts.updated_at < NOW() - INTERVAL '1 hour'
               )
            RETURNING id
            """
        ),
        {
            "id": alert_id,
            "workspace_id": row["workspace_id"],
            "trend_cluster_id": row["id"],
            "cluster_key": row["cluster_key"],
            "alert_kind": ALERT_KIND_URGENT_STABLE,
            "reason": reason,
            "score": _float_value(row.get("signal_score")),
            "message": message,
        },
    )
    claimed_id = result.scalar_one_or_none()
    return str(claimed_id) if claimed_id else None


async def _mark_alert_sent(session: AsyncSession, alert_id: str) -> None:
    await session.execute(
        text(
            """
            UPDATE trend_alerts
            SET status = 'sent',
                sent_at = NOW(),
                updated_at = NOW(),
                last_error = NULL
            WHERE id = :id
            """
        ),
        {"id": alert_id},
    )


async def _mark_alert_error(session: AsyncSession, alert_id: str, error: str) -> None:
    await session.execute(
        text(
            """
            UPDATE trend_alerts
            SET status = 'error',
                last_error = :error,
                updated_at = NOW()
            WHERE id = :id
            """
        ),
        {"id": alert_id, "error": _truncate(error, 1000)},
    )


async def run_urgent_trend_alerts(*, dry_run: bool = False) -> dict[str, Any]:
    settings = get_settings()
    if not settings.trend_alerts_enabled:
        return {
            "status": "disabled",
            "job_name": "urgent_trend_alerts",
            "dry_run": dry_run,
        }

    await ensure_trend_alerts_table()

    if not dry_run and not telegram_alerts_enabled():
        return {
            "status": "not_configured",
            "job_name": "urgent_trend_alerts",
            "reason": "telegram_alert_transport_missing",
            "dry_run": dry_run,
        }

    engine = get_engine()
    async with AsyncSession(engine) as session:
        sent_count_7d = await _sent_alert_count_7d(session)
        remaining_weekly = _remaining_weekly_capacity(sent_count_7d, settings)
        run_limit = min(settings.trend_alert_max_per_run, remaining_weekly)
        if run_limit <= 0:
            return {
                "status": "ok",
                "job_name": "urgent_trend_alerts",
                "dry_run": dry_run,
                "sent": 0,
                "candidates": 0,
                "skipped": 0,
                "weekly_limit_reached": True,
                "sent_count_7d": sent_count_7d,
                "max_per_7d": settings.trend_alert_max_per_7d,
            }

        candidates = await _fetch_candidates(
            session,
            settings=settings,
            limit=max(run_limit * 3, run_limit),
        )
        candidates = candidates[:run_limit]

        if dry_run:
            return {
                "status": "ok",
                "job_name": "urgent_trend_alerts",
                "dry_run": True,
                "sent": 0,
                "candidates": len(candidates),
                "sent_count_7d": sent_count_7d,
                "max_per_7d": settings.trend_alert_max_per_7d,
                "alerts": [
                    {
                        "trend_cluster_id": row["id"],
                        "workspace_id": row["workspace_id"],
                        "title": row["title"],
                        "reason": _candidate_reason(row, settings),
                        "signal_score": _float_value(row.get("signal_score")),
                        "doc_count": _int_value(row.get("doc_count")),
                        "source_count": _int_value(row.get("source_count")),
                    }
                    for row in candidates
                ],
            }

        sent = 0
        skipped = 0
        errors: list[dict[str, str]] = []
        for row in candidates:
            reason = _candidate_reason(row, settings)
            if not reason:
                skipped += 1
                continue

            message = _build_alert_message(row, reason)
            alert_id = await _claim_alert(session, row=row, reason=reason, message=message)
            await session.commit()
            if not alert_id:
                skipped += 1
                continue

            try:
                delivered = await send_telegram_alert_message(message)
                if not delivered:
                    raise RuntimeError("telegram_alert_transport_missing")
            except Exception as exc:
                logger.exception(
                    "Urgent trend alert delivery failed trend_cluster_id=%s",
                    row.get("id"),
                )
                await _mark_alert_error(session, alert_id, str(exc))
                await session.commit()
                errors.append({"trend_cluster_id": str(row.get("id")), "error": str(exc)})
                continue

            await _mark_alert_sent(session, alert_id)
            await session.commit()
            sent += 1

        return {
            "status": "ok" if not errors else "partial_error",
            "job_name": "urgent_trend_alerts",
            "dry_run": False,
            "sent": sent,
            "candidates": len(candidates),
            "skipped": skipped,
            "errors": errors,
            "sent_count_7d": sent_count_7d,
            "max_per_7d": settings.trend_alert_max_per_7d,
        }
