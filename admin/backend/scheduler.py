from __future__ import annotations

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from admin.backend.services.gigachat_balance import fetch_gigachat_balance
from admin.backend.services.pipeline_jobs import (
    list_active_workspace_ids,
    refresh_source_scores,
    run_semantic_cluster_job,
    run_signal_analysis_job,
)
from admin.backend.services.trend_alerts import run_urgent_trend_alerts
from shared.config import get_settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None
_source_score_lock = asyncio.Lock()
_cluster_lock = asyncio.Lock()
_gigachat_balance_lock = asyncio.Lock()
_trend_alert_lock = asyncio.Lock()
_manual_jobs_table_ready = False


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _manual_job_lock(job_name: str) -> asyncio.Lock | None:
    if job_name == "refresh_source_scores":
        return _source_score_lock
    if job_name in {"run_semantic_clusters", "run_signal_analysis"}:
        return _cluster_lock
    if job_name == "refresh_gigachat_balance":
        return _gigachat_balance_lock
    return None


async def ensure_manual_jobs_table() -> None:
    global _manual_jobs_table_ready
    if _manual_jobs_table_ready:
        return
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS admin_manual_jobs (
                    id TEXT PRIMARY KEY,
                    job_name TEXT NOT NULL,
                    workspace_id TEXT,
                    status TEXT NOT NULL,
                    trigger TEXT DEFAULT 'manual',
                    summary JSONB DEFAULT '{}',
                    result JSONB,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_admin_manual_jobs_lookup
                    ON admin_manual_jobs(job_name, workspace_id, created_at DESC)
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_admin_manual_jobs_status
                    ON admin_manual_jobs(status, created_at DESC)
                """
            )
        )
    _manual_jobs_table_ready = True


async def reconcile_running_manual_jobs() -> None:
    await ensure_manual_jobs_table()
    engine = get_engine()
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                """
                UPDATE admin_manual_jobs
                SET status = 'error',
                    error = COALESCE(error, 'interrupted_by_admin_restart'),
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE status = 'running'
                """
            )
        )
        await session.commit()


def _serialize_manual_job(job: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "id": job["id"],
        "job_name": job["job_name"],
        "workspace_id": job.get("workspace_id"),
        "status": job["status"],
        "created_at": _isoformat(job.get("created_at")),
        "started_at": _isoformat(job.get("started_at")),
        "finished_at": _isoformat(job.get("finished_at")),
        "trigger": job.get("trigger", "manual"),
        "summary": job.get("summary") or {},
        "error": job.get("error"),
    }
    if job.get("result") is not None:
        payload["result"] = job["result"]
    return payload


async def list_manual_jobs(
    *,
    job_name: str | None = None,
    workspace_id: str | None = None,
    only_running: bool = False,
    limit: int = 20,
) -> list[dict[str, Any]]:
    await ensure_manual_jobs_table()
    clauses = ["1=1"]
    params: dict[str, Any] = {"limit": max(1, limit)}
    if job_name is not None:
        clauses.append("job_name = :job_name")
        params["job_name"] = job_name
    if workspace_id is not None:
        clauses.append("workspace_id = :workspace_id")
        params["workspace_id"] = workspace_id
    if only_running:
        clauses.append("status = 'running'")
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                f"""
                SELECT *
                FROM admin_manual_jobs
                WHERE {' AND '.join(clauses)}
                ORDER BY COALESCE(started_at, created_at) DESC
                LIMIT :limit
                """
            ),
            params,
        )
        return [_serialize_manual_job(dict(row)) for row in result.mappings().all()]


async def get_manual_job(job_id: str) -> dict[str, Any] | None:
    await ensure_manual_jobs_table()
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT * FROM admin_manual_jobs WHERE id = :id"),
            {"id": job_id},
        )
        row = result.mappings().first()
        return _serialize_manual_job(dict(row)) if row else None


async def manual_job_metrics_snapshot(
    *,
    failure_window_minutes: int = 60,
) -> list[dict[str, Any]]:
    await ensure_manual_jobs_table()
    engine = get_engine()
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                """
                SELECT
                    job_name,
                    COUNT(*) FILTER (WHERE status = 'running') AS running,
                    COALESCE(
                        MAX(EXTRACT(EPOCH FROM (NOW() - started_at)))
                            FILTER (WHERE status = 'running' AND started_at IS NOT NULL),
                        0
                    ) AS oldest_running_age_seconds,
                    COUNT(*) FILTER (
                        WHERE status = 'error'
                          AND COALESCE(finished_at, updated_at, created_at)
                              >= NOW() - make_interval(mins => :failure_window_minutes)
                    ) AS recent_failures
                FROM admin_manual_jobs
                GROUP BY job_name
                ORDER BY job_name
                """
            ),
            {"failure_window_minutes": max(1, int(failure_window_minutes or 60))},
        )
        return [
            {
                "job_name": str(row["job_name"]),
                "running": int(row["running"] or 0),
                "oldest_running_age_seconds": float(row["oldest_running_age_seconds"] or 0.0),
                "recent_failures": int(row["recent_failures"] or 0),
            }
            for row in result.mappings().all()
        ]


async def launch_manual_job(
    *,
    job_name: str,
    workspace_id: str | None,
    runner,
) -> dict[str, Any]:
    await ensure_manual_jobs_table()
    lock = _manual_job_lock(job_name)
    engine = get_engine()
    async with AsyncSession(engine) as session:
        existing_result = await session.execute(
            text(
                """
                SELECT *
                FROM admin_manual_jobs
                WHERE job_name = :job_name
                  AND (
                    (workspace_id IS NULL AND CAST(:workspace_id AS text) IS NULL)
                    OR workspace_id = CAST(:workspace_id AS text)
                  )
                  AND status = 'running'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"job_name": job_name, "workspace_id": workspace_id},
        )
        existing = existing_result.mappings().first()
        if existing:
            payload = _serialize_manual_job(dict(existing))
            payload["status"] = "already_running"
            return payload
    if lock and lock.locked():
        return {
            "id": None,
            "job_name": job_name,
            "workspace_id": workspace_id,
            "status": "already_running",
            "created_at": _isoformat(_utcnow()),
            "started_at": None,
            "finished_at": None,
            "trigger": "manual",
            "summary": {"reason": "lock_held"},
            "error": None,
        }

    job_id = f"manual:{job_name}:{uuid4().hex[:12]}"
    job = {
        "id": job_id,
        "job_name": job_name,
        "workspace_id": workspace_id,
        "status": "queued",
        "created_at": _utcnow(),
        "started_at": None,
        "finished_at": None,
        "trigger": "manual",
        "summary": {},
        "error": None,
        "result": None,
    }
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                """
                INSERT INTO admin_manual_jobs (
                    id, job_name, workspace_id, status, trigger, summary,
                    result, error, created_at, started_at, finished_at, updated_at
                )
                VALUES (
                    :id, :job_name, :workspace_id, :status, :trigger, CAST(:summary AS jsonb),
                    NULL, :error, :created_at, NULL, NULL, :created_at
                )
                """
            ),
            {
                "id": job_id,
                "job_name": job_name,
                "workspace_id": workspace_id,
                "status": "queued",
                "trigger": "manual",
                "summary": json.dumps({}),
                "error": None,
                "created_at": job["created_at"],
            },
        )
        await session.commit()

    async def _runner() -> None:
        started_at = _utcnow()
        async with AsyncSession(engine) as session:
            await session.execute(
                text(
                    """
                    UPDATE admin_manual_jobs
                    SET status = 'running', started_at = :started_at, updated_at = :started_at
                    WHERE id = :id
                    """
                ),
                {"id": job_id, "started_at": started_at},
            )
            await session.commit()
        try:
            if lock is not None:
                async with lock:
                    process = await asyncio.create_subprocess_exec(
                        sys.executable,
                        "-m",
                        "admin.backend.manual_jobs",
                        job_name,
                        workspace_id or "__all__",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    stdout, stderr = await process.communicate()
            else:
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "admin.backend.manual_jobs",
                    job_name,
                    workspace_id or "__all__",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await process.communicate()
            if process.returncode != 0:
                raise RuntimeError(
                    (stderr or stdout or b"manual_job_subprocess_failed").decode("utf-8", errors="replace").strip()
                )
            result = json.loads((stdout or b"{}").decode("utf-8", errors="replace"))
            status = str(result.get("status") or "ok")
            summary = {
                key: result.get(key)
                for key in (
                    "workspace_id",
                    "semantic_clusters",
                    "trend_clusters",
                    "emerging_signals",
                    "missing_signals",
                    "workspace_count",
                    "job_name",
                )
                if key in result
            }
            async with AsyncSession(engine) as session:
                await session.execute(
                    text(
                        """
                        UPDATE admin_manual_jobs
                        SET status = :status,
                            summary = CAST(:summary AS jsonb),
                            result = CAST(:result AS jsonb),
                            finished_at = NOW(),
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": job_id,
                        "status": status,
                        "summary": json.dumps(summary, ensure_ascii=False),
                        "result": json.dumps(result, ensure_ascii=False),
                    },
                )
                await session.commit()
        except Exception as exc:
            logger.exception("Manual job failed job_name=%s workspace_id=%s", job_name, workspace_id)
            async with AsyncSession(engine) as session:
                await session.execute(
                    text(
                        """
                        UPDATE admin_manual_jobs
                        SET status = 'error',
                            error = :error,
                            summary = CAST(:summary AS jsonb),
                            finished_at = NOW(),
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": job_id,
                        "error": str(exc),
                        "summary": json.dumps(
                            {"job_name": job_name, "workspace_id": workspace_id},
                            ensure_ascii=False,
                        ),
                    },
                )
                await session.commit()

    asyncio.create_task(_runner())
    return _serialize_manual_job(job)


async def _run_for_active_workspaces(
    *,
    job_name: str,
    lock: asyncio.Lock,
    runner,
) -> dict[str, Any]:
    if lock.locked():
        logger.warning("Skipping %s: previous run is still in progress", job_name)
        return {"status": "skipped", "reason": "already_running", "job_name": job_name}

    async with lock:
        workspace_ids = await list_active_workspace_ids()
        if not workspace_ids:
            logger.info("Skipping %s: no active workspaces", job_name)
            return {
                "status": "ok",
                "job_name": job_name,
                "workspace_count": 0,
                "results": [],
            }

        results = []
        for workspace_id in workspace_ids:
            try:
                results.append(await runner(workspace_id))
            except Exception:
                logger.exception("%s failed for workspace=%s", job_name, workspace_id)
                results.append(
                    {
                        "status": "error",
                        "workspace_id": workspace_id,
                        "job_name": job_name,
                    }
                )
        logger.info(
            "Completed %s for %d active workspaces",
            job_name,
            len(workspace_ids),
        )
        return {
            "status": "ok",
            "job_name": job_name,
            "workspace_count": len(workspace_ids),
            "results": results,
        }


async def scheduled_refresh_source_scores() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="refresh_source_scores",
        lock=_source_score_lock,
        runner=refresh_source_scores,
    )


async def scheduled_semantic_clustering() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_semantic_clusters",
        lock=_cluster_lock,
        runner=run_semantic_cluster_job,
    )


async def scheduled_signal_analysis() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_signal_analysis",
        lock=_cluster_lock,
        runner=run_signal_analysis_job,
    )


async def scheduled_refresh_gigachat_balance() -> dict[str, Any]:
    if _gigachat_balance_lock.locked():
        logger.warning("Skipping refresh_gigachat_balance: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "refresh_gigachat_balance",
        }

    async with _gigachat_balance_lock:
        result = await fetch_gigachat_balance()
        logger.info(
            "Completed refresh_gigachat_balance status=%s available=%s balance_items=%d",
            result.get("status"),
            result.get("available"),
            len(result.get("balance") or []),
        )
        return {
            "status": "ok",
            "job_name": "refresh_gigachat_balance",
            "result": result,
        }


async def scheduled_urgent_trend_alerts() -> dict[str, Any]:
    if _trend_alert_lock.locked():
        logger.warning("Skipping urgent_trend_alerts: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "urgent_trend_alerts",
        }

    async with _trend_alert_lock:
        result = await run_urgent_trend_alerts()
        logger.info(
            "Completed urgent_trend_alerts status=%s sent=%s candidates=%s skipped=%s",
            result.get("status"),
            result.get("sent"),
            result.get("candidates"),
            result.get("skipped"),
        )
        return result


def get_scheduler() -> AsyncIOScheduler | None:
    return _scheduler


def scheduler_status() -> dict[str, Any]:
    settings = get_settings()
    scheduler = get_scheduler()
    jobs = []
    if scheduler:
        for job in scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "next_run_time": (
                        job.next_run_time.astimezone(UTC).isoformat()
                        if job.next_run_time
                        else None
                    ),
                    "trigger": str(job.trigger),
                }
            )
    return {
        "enabled": settings.admin_scheduler_enabled,
        "timezone": settings.admin_scheduler_timezone,
        "running": bool(scheduler and scheduler.running),
        "jobs": jobs,
    }


def _build_scheduler() -> AsyncIOScheduler:
    settings = get_settings()
    timezone = settings.admin_scheduler_timezone
    scheduler = AsyncIOScheduler(timezone=timezone)

    common_kwargs = {
        "replace_existing": True,
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": settings.admin_scheduler_misfire_grace_seconds,
    }

    scheduler.add_job(
        scheduled_refresh_source_scores,
        CronTrigger.from_crontab(
            settings.admin_source_score_refresh_cron,
            timezone=timezone,
        ),
        id="refresh_source_scores",
        jitter=settings.admin_scheduler_max_jitter_seconds,
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_semantic_clustering,
        CronTrigger.from_crontab(
            settings.admin_semantic_cluster_cron,
            timezone=timezone,
        ),
        id="run_semantic_clusters",
        jitter=settings.admin_scheduler_max_jitter_seconds,
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_signal_analysis,
        CronTrigger.from_crontab(
            settings.admin_signal_cluster_cron,
            timezone=timezone,
        ),
        id="run_signal_analysis",
        jitter=settings.admin_scheduler_max_jitter_seconds,
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_refresh_gigachat_balance,
        CronTrigger.from_crontab(
            settings.admin_gigachat_balance_refresh_cron,
            timezone=timezone,
        ),
        id="refresh_gigachat_balance",
        jitter=min(10, settings.admin_scheduler_max_jitter_seconds),
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_urgent_trend_alerts,
        CronTrigger.from_crontab(
            settings.admin_trend_alert_cron,
            timezone=timezone,
        ),
        id="urgent_trend_alerts",
        jitter=min(60, settings.admin_scheduler_max_jitter_seconds),
        **common_kwargs,
    )
    return scheduler


@asynccontextmanager
async def scheduler_lifespan():
    global _scheduler

    settings = get_settings()
    if not settings.admin_scheduler_enabled:
        yield
        return

    _scheduler = _build_scheduler()
    _scheduler.start()
    await reconcile_running_manual_jobs()
    await scheduled_refresh_gigachat_balance()
    logger.info(
        "Admin scheduler started with timezone=%s, refresh_cron=%s, cluster_cron=%s, signal_cron=%s, gigachat_balance_cron=%s, trend_alert_cron=%s",
        settings.admin_scheduler_timezone,
        settings.admin_source_score_refresh_cron,
        settings.admin_semantic_cluster_cron,
        settings.admin_signal_cluster_cron,
        settings.admin_gigachat_balance_refresh_cron,
        settings.admin_trend_alert_cron,
    )
    try:
        yield
    finally:
        if _scheduler:
            _scheduler.shutdown(wait=False)
            logger.info("Admin scheduler stopped")
            _scheduler = None
