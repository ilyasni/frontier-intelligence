from __future__ import annotations

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine
from shared.config import get_settings


async def _query_prometheus(query: str) -> list[dict]:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0)) as client:
        response = await client.get(
            f"{settings.prometheus_url.rstrip('/')}/api/v1/query",
            params={"query": query},
        )
        response.raise_for_status()
        payload = response.json()
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    result = data.get("result", []) if isinstance(data, dict) else []
    return result if isinstance(result, list) else []


def _vector_rows(result: list[dict], *, default_metric: dict | None = None) -> list[dict]:
    rows = []
    for item in result or []:
        metric = item.get("metric") if isinstance(item, dict) else {}
        value = item.get("value") if isinstance(item, dict) else None
        numeric = 0.0
        if isinstance(value, list) and len(value) >= 2:
            try:
                numeric = float(value[1] or 0.0)
            except (TypeError, ValueError):
                numeric = 0.0
        rows.append({**(default_metric or {}), **(metric or {}), "value": numeric})
    return rows


async def fetch_gigachat_weekly_report() -> dict:
    requests_rows = _vector_rows(
        await _query_prometheus(
            "sum by (task, model, status) (increase(frontier_gigachat_requests_total[7d]))"
        )
    )
    tokens_rows = _vector_rows(
        await _query_prometheus(
            "sum by (task, model) (increase(frontier_gigachat_billable_tokens_total[7d]))"
        )
    )
    escalation_rows = _vector_rows(
        await _query_prometheus(
            "sum by (task, from_model, to_model) (increase(frontier_gigachat_escalations_total[7d]))"
        )
    )
    rate_limit_rows = _vector_rows(
        await _query_prometheus(
            "sum by (service, operation) (increase(frontier_rate_limit_events_total{upstream=\"gigachat\"}[7d]))"
        )
    )

    async with AsyncSession(get_engine()) as session:
        status_result = await session.execute(
            text(
                """
                SELECT
                    COALESCE(i.embedding_status, 'pending') AS status,
                    COUNT(*) AS count
                FROM posts p
                LEFT JOIN indexing_status i ON i.post_id = p.id
                WHERE p.created_at >= NOW() - INTERVAL '7 days'
                GROUP BY COALESCE(i.embedding_status, 'pending')
                ORDER BY status ASC
                """
            )
        )
        status_rows = [dict(row) for row in status_result.mappings().all()]

    status_total = sum(int(row.get("count") or 0) for row in status_rows) or 1
    pipeline_status = [
        {
            "status": row["status"],
            "count": int(row["count"] or 0),
            "share": round(int(row["count"] or 0) / status_total, 4),
        }
        for row in status_rows
    ]

    return {
        "window": "7d",
        "requests": requests_rows,
        "billable_tokens": tokens_rows,
        "escalations": escalation_rows,
        "rate_limits": rate_limit_rows,
        "pipeline_status": pipeline_status,
    }
