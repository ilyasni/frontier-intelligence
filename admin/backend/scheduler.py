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
from admin.backend.services.openrouter_catalog import fetch_openrouter_catalog
from admin.backend.services.openrouter_health import probe_openrouter_health
from admin.backend.services.openrouter_key import fetch_openrouter_key
from admin.backend.services.openrouter_picker import reconcile_openrouter_state
from admin.backend.services.pipeline_jobs import list_active_workspace_ids
from admin.backend.services.trend_alerts import run_urgent_trend_alerts
from admin.backend.services.wormsoft_limits import fetch_wormsoft_limits
from admin.backend.services.xray_health import run_xray_health_check
from shared.config import get_settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None
_source_score_lock = asyncio.Lock()
_cluster_lock = asyncio.Lock()
_gigachat_balance_lock = asyncio.Lock()
_wormsoft_limits_lock = asyncio.Lock()
_openrouter_catalog_lock = asyncio.Lock()
_openrouter_key_lock = asyncio.Lock()
_openrouter_health_lock = asyncio.Lock()
_openrouter_reconcile_lock = asyncio.Lock()
_trend_alert_lock = asyncio.Lock()
_xray_health_lock = asyncio.Lock()
_retrospective_lock = asyncio.Lock()
_novelty_judge_lock = asyncio.Lock()
_relevance_audit_lock = asyncio.Lock()
_graph_maintenance_lock = asyncio.Lock()
_entity_resolution_lock = asyncio.Lock()
_manual_jobs_table_ready = False


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


async def _run_startup_step(
    job_name: str,
    runner,
    *,
    timeout_seconds: float = 30.0,
) -> None:
    try:
        await asyncio.wait_for(runner(), timeout=timeout_seconds)
    except TimeoutError:
        logger.warning(
            "Startup warmup timed out for %s after %.1fs; continuing admin startup",
            job_name,
            timeout_seconds,
        )
    except Exception:
        logger.exception("Startup warmup failed for %s; continuing admin startup", job_name)


def _manual_job_lock(job_name: str) -> asyncio.Lock | None:
    if job_name == "refresh_source_scores":
        return _source_score_lock
    if job_name in {"run_semantic_clusters", "run_signal_analysis", "run_missing_signals"}:
        return _cluster_lock
    if job_name == "run_retrospective_review":
        return _retrospective_lock
    if job_name == "run_novelty_judge":
        return _novelty_judge_lock
    if job_name == "run_relevance_audit":
        return _relevance_audit_lock
    if job_name in {"run_graph_maintenance", "run_graph_resolution"}:
        return _graph_maintenance_lock
    if job_name == "run_entity_resolution":
        return _entity_resolution_lock
    if job_name == "refresh_gigachat_balance":
        return _gigachat_balance_lock
    if job_name == "refresh_wormsoft_limits":
        return _wormsoft_limits_lock
    if job_name == "refresh_openrouter_catalog":
        return _openrouter_catalog_lock
    if job_name == "refresh_openrouter_key":
        return _openrouter_key_lock
    if job_name == "probe_openrouter_health":
        return _openrouter_health_lock
    if job_name == "reconcile_openrouter_state":
        return _openrouter_reconcile_lock
    return None


async def _run_job_subprocess(job_name: str, workspace_id: str | None) -> dict[str, Any]:
    """Execute a pipeline job in a separate OS process and return its JSON result.

    Heavy jobs (semantic clustering, signal analysis, …) do CPU-bound, pure-Python
    work that would otherwise block the admin asyncio event loop — freezing
    ``/api/health`` and ``/metrics`` and starving every other scheduled job. Running
    them in a child process (via the existing ``admin.backend.manual_jobs``
    stdout-protocol entrypoint) keeps the loop responsive and isolates the job's own
    DB/Redis connections from the admin process. Raises ``RuntimeError`` on non-zero
    exit, with the child's stderr/stdout as the message.
    """
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
        err_text = (
            stderr or stdout or b"job_subprocess_failed"
        ).decode("utf-8", errors="replace").strip()
        raise RuntimeError(err_text)
    return json.loads((stdout or b"{}").decode("utf-8", errors="replace"))


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
                    result = await _run_job_subprocess(job_name, workspace_id)
            else:
                result = await _run_job_subprocess(job_name, workspace_id)
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
            logger.exception(
                "Manual job failed job_name=%s workspace_id=%s",
                job_name,
                workspace_id,
            )
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

        # Each workspace runs in its own child process so the CPU-bound work never
        # blocks the admin event loop; per-workspace try/except keeps a single
        # workspace failure from aborting the rest of the run.
        results = []
        for workspace_id in workspace_ids:
            try:
                results.append(await _run_job_subprocess(job_name, workspace_id))
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
    )


async def scheduled_semantic_clustering() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_semantic_clusters",
        lock=_cluster_lock,
    )


async def scheduled_signal_analysis() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_signal_analysis",
        lock=_cluster_lock,
    )


async def scheduled_retrospective_review() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_retrospective_review",
        lock=_retrospective_lock,
    )


async def scheduled_novelty_judge() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_novelty_judge",
        lock=_novelty_judge_lock,
    )


async def scheduled_relevance_audit() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_relevance_audit",
        lock=_relevance_audit_lock,
    )


async def scheduled_graph_maintenance() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_graph_maintenance",
        lock=_graph_maintenance_lock,
    )


async def scheduled_entity_resolution() -> dict[str, Any]:
    return await _run_for_active_workspaces(
        job_name="run_entity_resolution",
        lock=_entity_resolution_lock,
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


async def scheduled_refresh_wormsoft_limits() -> dict[str, Any]:
    if _wormsoft_limits_lock.locked():
        logger.warning("Skipping refresh_wormsoft_limits: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "refresh_wormsoft_limits",
        }

    async with _wormsoft_limits_lock:
        result = await fetch_wormsoft_limits()
        logger.info(
            "Completed refresh_wormsoft_limits status=%s available=%s plans=%d pricing_models=%d",
            result.get("status"),
            result.get("available"),
            len(result.get("plans") or []),
            len(result.get("pricing") or {}),
        )
        return {
            "status": "ok",
            "job_name": "refresh_wormsoft_limits",
            "result": result,
        }


async def scheduled_refresh_openrouter_catalog() -> dict[str, Any]:
    if _openrouter_catalog_lock.locked():
        logger.warning("Skipping refresh_openrouter_catalog: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "refresh_openrouter_catalog",
        }

    async with _openrouter_catalog_lock:
        result = await fetch_openrouter_catalog()
        logger.info(
            "Completed refresh_openrouter_catalog status=%s model_count=%d",
            result.get("status"),
            int(result.get("model_count") or 0),
        )
        return {
            "status": "ok",
            "job_name": "refresh_openrouter_catalog",
            "result": result,
        }


async def scheduled_refresh_openrouter_key() -> dict[str, Any]:
    if _openrouter_key_lock.locked():
        logger.warning("Skipping refresh_openrouter_key: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "refresh_openrouter_key",
        }

    async with _openrouter_key_lock:
        result = await fetch_openrouter_key()
        logger.info(
            "Completed refresh_openrouter_key status=%s free_tier=%s usage_daily=%.2f",
            result.get("status"),
            result.get("is_free_tier"),
            float(result.get("usage_daily") or 0.0),
        )
        return {
            "status": "ok",
            "job_name": "refresh_openrouter_key",
            "result": result,
        }


async def scheduled_probe_openrouter_health() -> dict[str, Any]:
    if _openrouter_health_lock.locked():
        logger.warning("Skipping probe_openrouter_health: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "probe_openrouter_health",
        }

    async with _openrouter_health_lock:
        result = await probe_openrouter_health()
        logger.info(
            "Completed probe_openrouter_health status=%s probed=%d ok=%d skipped=%d",
            result.get("status"),
            int(result.get("probed") or 0),
            int(result.get("ok") or 0),
            int(result.get("skipped") or 0),
        )
        return {
            "status": "ok",
            "job_name": "probe_openrouter_health",
            "result": result,
        }


async def scheduled_reconcile_openrouter_state() -> dict[str, Any]:
    if _openrouter_reconcile_lock.locked():
        logger.warning("Skipping reconcile_openrouter_state: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "reconcile_openrouter_state",
        }

    async with _openrouter_reconcile_lock:
        result = await reconcile_openrouter_state(service_name="admin")
        logger.info(
            "Completed reconcile_openrouter_state usable=%d quarantined=%d near_cap=%d",
            int(result.get("usable_model_count") or 0),
            int(result.get("quarantined_model_count") or 0),
            int(result.get("near_cap_model_count") or 0),
        )
        return {
            "status": "ok",
            "job_name": "reconcile_openrouter_state",
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


async def scheduled_xray_health_check() -> dict[str, Any]:
    if _xray_health_lock.locked():
        logger.warning("Skipping xray_health_check: previous run is still in progress")
        return {
            "status": "skipped",
            "reason": "already_running",
            "job_name": "xray_health_check",
        }
    async with _xray_health_lock:
        result = await run_xray_health_check()
        logger.info(
            "Completed xray_health_check status=%s failed=%s/%s streak=%s",
            result.get("status"),
            result.get("targets_failed"),
            result.get("targets_total"),
            result.get("streak"),
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
        scheduled_retrospective_review,
        CronTrigger.from_crontab(
            settings.admin_retrospective_review_cron,
            timezone=timezone,
        ),
        id="run_retrospective_review",
        jitter=settings.admin_scheduler_max_jitter_seconds,
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_novelty_judge,
        CronTrigger.from_crontab(
            settings.admin_novelty_judge_cron,
            timezone=timezone,
        ),
        id="run_novelty_judge",
        jitter=settings.admin_scheduler_max_jitter_seconds,
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_relevance_audit,
        CronTrigger.from_crontab(
            settings.admin_relevance_audit_cron,
            timezone=timezone,
        ),
        id="run_relevance_audit",
        jitter=settings.admin_scheduler_max_jitter_seconds,
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_graph_maintenance,
        CronTrigger.from_crontab(
            settings.admin_graph_maintenance_cron,
            timezone=timezone,
        ),
        id="run_graph_maintenance",
        jitter=settings.admin_scheduler_max_jitter_seconds,
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_entity_resolution,
        CronTrigger.from_crontab(
            settings.admin_entity_resolution_cron,
            timezone=timezone,
        ),
        id="run_entity_resolution",
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
        scheduled_refresh_wormsoft_limits,
        CronTrigger.from_crontab(
            settings.admin_wormsoft_limits_refresh_cron,
            timezone=timezone,
        ),
        id="refresh_wormsoft_limits",
        jitter=min(20, settings.admin_scheduler_max_jitter_seconds),
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_refresh_openrouter_catalog,
        CronTrigger.from_crontab(
            settings.admin_openrouter_catalog_refresh_cron,
            timezone=timezone,
        ),
        id="refresh_openrouter_catalog",
        jitter=min(30, settings.admin_scheduler_max_jitter_seconds),
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_refresh_openrouter_key,
        CronTrigger.from_crontab(
            settings.admin_openrouter_key_refresh_cron,
            timezone=timezone,
        ),
        id="refresh_openrouter_key",
        jitter=min(15, settings.admin_scheduler_max_jitter_seconds),
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_probe_openrouter_health,
        CronTrigger.from_crontab(
            settings.admin_openrouter_health_refresh_cron,
            timezone=timezone,
        ),
        id="probe_openrouter_health",
        jitter=min(10, settings.admin_scheduler_max_jitter_seconds),
        **common_kwargs,
    )
    scheduler.add_job(
        scheduled_reconcile_openrouter_state,
        CronTrigger.from_crontab(
            settings.admin_openrouter_reconcile_cron,
            timezone=timezone,
        ),
        id="reconcile_openrouter_state",
        jitter=min(5, settings.admin_scheduler_max_jitter_seconds),
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
    scheduler.add_job(
        scheduled_xray_health_check,
        CronTrigger.from_crontab(
            settings.admin_xray_health_cron,
            timezone=timezone,
        ),
        id="xray_health_check",
        jitter=min(10, settings.admin_scheduler_max_jitter_seconds),
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
    await _run_startup_step("refresh_gigachat_balance", scheduled_refresh_gigachat_balance)
    await _run_startup_step("refresh_wormsoft_limits", scheduled_refresh_wormsoft_limits)
    await _run_startup_step("refresh_openrouter_catalog", scheduled_refresh_openrouter_catalog)
    await _run_startup_step("refresh_openrouter_key", scheduled_refresh_openrouter_key)
    await _run_startup_step("probe_openrouter_health", scheduled_probe_openrouter_health)
    await _run_startup_step("xray_health_check", scheduled_xray_health_check)
    await _run_startup_step(
        "reconcile_openrouter_state",
        scheduled_reconcile_openrouter_state,
    )
    logger.info(
        (
            "Admin scheduler started with timezone=%s, refresh_cron=%s, "
            "cluster_cron=%s, signal_cron=%s, gigachat_balance_cron=%s, "
            "wormsoft_limits_cron=%s, openrouter_catalog_cron=%s, "
            "openrouter_key_cron=%s, openrouter_health_cron=%s, "
            "openrouter_reconcile_cron=%s, trend_alert_cron=%s, "
            "xray_health_cron=%s"
        ),
        settings.admin_scheduler_timezone,
        settings.admin_source_score_refresh_cron,
        settings.admin_semantic_cluster_cron,
        settings.admin_signal_cluster_cron,
        settings.admin_gigachat_balance_refresh_cron,
        settings.admin_wormsoft_limits_refresh_cron,
        settings.admin_openrouter_catalog_refresh_cron,
        settings.admin_openrouter_key_refresh_cron,
        settings.admin_openrouter_health_refresh_cron,
        settings.admin_openrouter_reconcile_cron,
        settings.admin_trend_alert_cron,
        settings.admin_xray_health_cron,
    )
    try:
        yield
    finally:
        if _scheduler:
            _scheduler.shutdown(wait=False)
            logger.info("Admin scheduler stopped")
            _scheduler = None
