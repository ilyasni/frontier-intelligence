"""Контур C (RSI): аудит тихих false-negative Relevance Filter.

Relevance Filter режет посты на входе со статусом dropped — молча. Лог решений
(relevance_decisions) уже копится с Фазы 0. Здесь: считаем метрики отклонений и
подтверждённых человеком false-negative (на Grafana), и если их доля высока —
предлагаем ПОНИЗИТЬ порог релевантности через тот же MCP-гейт, что и Контур A
(approve пишет в workspaces.relevance_weights->threshold). Семплинг и вердикт —
через MCP (list_relevance_audit_sample / mark_relevance_audit).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.metrics import set_relevance_audit_metric
from worker.services.clustering_math import digest as _digest
from worker.services.retrospective import percentile

logger = logging.getLogger(__name__)

REL_AUDIT_WINDOW_DAYS = 30
REL_MIN_AUDITED = 15
REL_MIN_FN = 5
REL_FN_RATE_PROPOSE = 0.15   # доля FN среди отсуженных, выше которой имеет смысл понижать порог
REL_RETAIN_PCT = 20          # порог-кандидат удерживает ~80% подтверждённых false-negative
REL_MAX_STEP = 0.2           # не двигаем порог более чем на 0.2 за шаг
REL_MIN_CHANGE = 0.02


def propose_relevance_threshold(
    current_threshold: float,
    audited: list[dict[str, Any]],
    *,
    min_audited: int = REL_MIN_AUDITED,
    min_fn: int = REL_MIN_FN,
    fn_rate: float = REL_FN_RATE_PROPOSE,
) -> dict[str, Any] | None:
    """Чистый: по отсуженным reject'ам предложить понижение порога релевантности."""
    fns = [
        float(a["score"])
        for a in audited
        if a.get("verdict") == "false_negative" and a.get("score") is not None
    ]
    n = len(audited)
    if n < min_audited or len(fns) < min_fn or (len(fns) / n) < fn_rate:
        return None

    candidate = percentile(fns, REL_RETAIN_PCT)
    candidate = min(candidate, current_threshold)               # только понижаем
    candidate = max(current_threshold - REL_MAX_STEP, candidate)  # ограничиваем шаг
    candidate = max(0.0, round(candidate, 4))
    if (current_threshold - candidate) < REL_MIN_CHANGE:
        return None

    recovered = sum(1 for s in fns if s >= candidate)
    return {
        "threshold_key": "relevance_threshold",
        "current_value": round(float(current_threshold), 4),
        "proposed_value": candidate,
        "direction": "lower",
        "evidence": {
            "n_audited": n,
            "n_false_negative": len(fns),
            "fn_rate": round(len(fns) / n, 3),
            "recovered_at_proposed": recovered,
        },
    }


async def _current_relevance_threshold(session: AsyncSession, workspace_id: str) -> float:
    row = (
        await session.execute(
            text(
                "SELECT COALESCE(relevance_weights->>'threshold', '0.6') AS thr "
                "FROM workspaces WHERE id = :ws"
            ),
            {"ws": workspace_id},
        )
    ).mappings().first()
    try:
        return float((row or {}).get("thr") or 0.6)
    except (TypeError, ValueError):
        return 0.6


async def run_relevance_audit_metrics(workspace_id: str) -> dict[str, Any]:
    """Считает метрики аудита, обновляет Grafana-гейджи и (при достаточных данных) предлагает порог."""
    from shared.db import get_session_factory

    run_id = _digest(f"{workspace_id}|relaudit", "relaudit")
    async with get_session_factory()() as session:
        counts = (
            await session.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS rejected,
                        COUNT(*) FILTER (WHERE audit_status IS NOT NULL) AS audited,
                        COUNT(*) FILTER (WHERE audit_status = 'false_negative') AS false_negatives,
                        COUNT(*) FILTER (WHERE audit_status = 'correct_reject') AS correct
                    FROM relevance_decisions
                    WHERE workspace_id = :ws AND relevant = false
                      AND decided_at >= NOW() - make_interval(days => :days)
                    """
                ),
                {"ws": workspace_id, "days": REL_AUDIT_WINDOW_DAYS},
            )
        ).mappings().first() or {}

        rejected = int(counts.get("rejected") or 0)
        audited = int(counts.get("audited") or 0)
        fn = int(counts.get("false_negatives") or 0)
        fn_rate = round(fn / audited, 4) if audited else 0.0

        set_relevance_audit_metric("worker", workspace_id, "rejected_30d", rejected)
        set_relevance_audit_metric("worker", workspace_id, "audited_30d", audited)
        set_relevance_audit_metric("worker", workspace_id, "false_negatives_30d", fn)
        set_relevance_audit_metric("worker", workspace_id, "false_negative_rate", fn_rate)

        proposal = None
        if audited >= REL_MIN_AUDITED:
            rows = await session.execute(
                text(
                    """
                    SELECT score, audit_status AS verdict
                    FROM relevance_decisions
                    WHERE workspace_id = :ws AND relevant = false AND audit_status IS NOT NULL
                      AND decided_at >= NOW() - make_interval(days => :days)
                    """
                ),
                {"ws": workspace_id, "days": REL_AUDIT_WINDOW_DAYS},
            )
            audited_rows = [dict(r) for r in rows.mappings().all()]
            current_thr = await _current_relevance_threshold(session, workspace_id)
            prop = propose_relevance_threshold(current_thr, audited_rows)
            if prop:
                await _upsert_relevance_proposal(session, workspace_id=workspace_id, run_id=run_id, prop=prop)
                proposal = prop
            await session.commit()

    logger.info(
        "relevance_audit workspace=%s rejected=%d audited=%d fn=%d fn_rate=%.3f proposal=%s",
        workspace_id, rejected, audited, fn, fn_rate, bool(proposal),
    )
    return {
        "workspace_id": workspace_id,
        "status": "ok",
        "rejected_30d": rejected,
        "audited_30d": audited,
        "false_negatives_30d": fn,
        "false_negative_rate": fn_rate,
        "proposal": proposal,
    }


async def _upsert_relevance_proposal(
    session: AsyncSession, *, workspace_id: str, run_id: str, prop: dict[str, Any]
) -> None:
    proposal_id = _digest(f"{workspace_id}|relevance_threshold|pending", "thrprop")
    rationale = (
        f"Понизить relevance_threshold: {prop['current_value']} → {prop['proposed_value']}. "
        f"Из {prop['evidence']['n_audited']} отсуженных reject'ов "
        f"{prop['evidence']['n_false_negative']} оказались ошибочными "
        f"(false-negative rate {prop['evidence']['fn_rate']}); новый порог вернул бы "
        f"{prop['evidence']['recovered_at_proposed']} из них."
    )
    await session.execute(
        text(
            """
            INSERT INTO threshold_proposals (
                id, workspace_id, threshold_key, current_value, proposed_value, status,
                cohort_days, direction, rationale, evidence, run_id, created_by, created_at, updated_at
            ) VALUES (
                :id, :ws, 'relevance_threshold', :cur, :prop, 'pending',
                :cohort, :direction, :rationale, CAST(:evidence AS jsonb), :run_id,
                'relevance_audit', NOW(), NOW()
            )
            ON CONFLICT (workspace_id, threshold_key) WHERE status = 'pending'
            DO UPDATE SET
                current_value = EXCLUDED.current_value, proposed_value = EXCLUDED.proposed_value,
                direction = EXCLUDED.direction, rationale = EXCLUDED.rationale,
                evidence = EXCLUDED.evidence, run_id = EXCLUDED.run_id, updated_at = NOW()
            """
        ),
        {
            "id": proposal_id,
            "ws": workspace_id,
            "cur": prop["current_value"],
            "prop": prop["proposed_value"],
            "cohort": REL_AUDIT_WINDOW_DAYS,
            "direction": prop["direction"],
            "rationale": rationale,
            "evidence": json.dumps(prop["evidence"]),
            "run_id": run_id,
        },
    )
