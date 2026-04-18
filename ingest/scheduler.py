"""Scheduler — reads sources from DB and runs them on interval."""
import logging
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


async def load_sources(database_url: str) -> list[dict[str, Any]]:
    """Load enabled sources from PostgreSQL."""
    url = database_url.replace("postgresql+asyncpg://", "postgresql://")
    try:
        conn = await asyncpg.connect(url)
        rows = await conn.fetch("""
            SELECT
                s.id, s.workspace_id, s.source_type, s.name,
                s.url, s.tg_channel, s.tg_account_idx,
                s.schedule_cron, s.extra, s.proxy_config,
                w.name as workspace_name,
                w.relevance_weights
            FROM sources s
            JOIN workspaces w ON w.id = s.workspace_id
            WHERE s.is_enabled = TRUE AND w.is_active = TRUE
        """)
        await conn.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.error("Failed to load sources: %s", exc)
        return []


def cron_to_minutes(cron: str) -> int:
    """Calculate interval in minutes from cron expression using croniter."""
    try:
        from datetime import datetime

        from croniter import croniter
        it = croniter(cron, datetime.now())
        next1 = it.get_next(datetime)
        next2 = it.get_next(datetime)
        return max(1, round((next2 - next1).total_seconds() / 60))
    except Exception:
        pass
    return 60  # default
