"""Контур B (RSI): прогон кросс-семейного novelty-judge по weak-снимкам.

Берёт малый набор недавних weak-снимков без вердикта, судит их другой семьёй
(DeepSeek через wormsoft → polza), пишет вердикт в weak_signal_snapshots.judge_verdict
и помечает «underrated» там, где детектор сказал weak, а судья — high novelty.
Это пойманная слепота primary-семьи: топливо для ретро-петли и сигнал оператору.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text

from shared.config import get_settings
from shared.metrics import note_novelty_judge
from worker.chains.novelty_judge_chain import NoveltyJudgeChain
from worker.llm_router_client import LLMRouterClient

logger = logging.getLogger(__name__)


async def _aclose(client: Any) -> None:
    close = getattr(client, "close", None) or getattr(client, "aclose", None)
    if close:
        try:
            await close()
        except Exception:
            logger.debug("novelty_judge client close failed", exc_info=True)


async def run_novelty_judge(workspace_id: str, cap: int | None = None) -> dict[str, Any]:
    settings = get_settings()
    if not bool(getattr(settings, "novelty_judge_enabled", True)):
        return {"workspace_id": workspace_id, "status": "disabled", "judged": 0}

    cap = int(cap or settings.novelty_judge_max_per_run)
    threshold = float(settings.novelty_judge_threshold)
    from shared.db import get_session_factory

    router = LLMRouterClient(service_name="worker")
    chain = NoveltyJudgeChain(
        None,
        None,
        model=settings.novelty_judge_model,
        fallback_model=settings.novelty_judge_fallback_model,
        token_budget=int(settings.novelty_judge_token_budget),
        router_client=router,
    )

    judged = underrated = failed = 0
    try:
        async with get_session_factory()() as session:
            rows = await session.execute(
                text(
                    """
                    SELECT id, signal_key, title, keywords, source_count,
                           source_diversity, burst_score, signal_score, stage
                    FROM weak_signal_snapshots
                    WHERE workspace_id = :ws AND judge_verdict IS NULL
                    ORDER BY detected_at DESC
                    LIMIT :cap
                    """
                ),
                {"ws": workspace_id, "cap": cap},
            )
            candidates = [dict(r) for r in rows.mappings().all()]

            for cand in candidates:
                verdict = await chain.run(cand)
                if verdict is None:
                    failed += 1
                    note_novelty_judge("worker", "failed")
                    continue
                verdict["underrated"] = bool(verdict["novelty_score"] >= threshold)
                await session.execute(
                    text(
                        """
                        UPDATE weak_signal_snapshots
                        SET judge_verdict = CAST(:verdict AS jsonb), updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": cand["id"], "verdict": json.dumps(verdict, ensure_ascii=False)},
                )
                judged += 1
                if verdict["underrated"]:
                    underrated += 1
                    note_novelty_judge("worker", "underrated")
                else:
                    note_novelty_judge("worker", "confirmed_weak")
            await session.commit()
    finally:
        await _aclose(router)

    logger.info(
        "novelty_judge workspace=%s judged=%d underrated=%d failed=%d",
        workspace_id,
        judged,
        underrated,
        failed,
    )
    return {
        "workspace_id": workspace_id,
        "status": "ok",
        "judged": judged,
        "underrated": underrated,
        "failed": failed,
        "cap": cap,
    }
