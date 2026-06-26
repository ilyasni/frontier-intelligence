"""Контур B (RSI): кросс-семейный novelty-judge.

Основной поток (экстракция/кластеризация) идёт через Gemma+GigaChat — у этой связки
системная слепота к тому, что вне её распределения, а слабые сигналы живут именно там.
Судья другой семьи (DeepSeek через wormsoft, polza как резерв) независимо оценивает
малый набор weak-кандидатов: «это правда зарождающийся сигнал, который статистический
детектор недооценил?» Где детектор сказал weak, а судья — high novelty, это пойманная
слепота: признак для ретро-петли (Контур A) и сигнал оператору.

build_prompt и normalize_verdict — чистые (тестируются без LLM).
"""

from __future__ import annotations

import logging
from typing import Any

from worker.llm_json import parse_llm_json_object

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Ты — судья новизны сигналов из ДРУГОЙ модельной семьи, чем основной детектор. "
    "Основной статистический детектор пометил кандидата как слабый (weak). Твоя задача — "
    "независимо оценить, является ли это потенциально новым/зарождающимся сигналом, который "
    "конвенциональные детекторы систематически недооценивают (вне их распределения). "
    "Будь скептичен к хайпу, но внимателен к раннему, ещё малозаметному. "
    "Верни ТОЛЬКО валидный JSON без пояснений: "
    '{"novelty_score": <0..1>, "out_of_distribution": <true|false>, "reasoning": "<кратко, до 200 симв>"}'
)


def build_prompt(candidate: dict[str, Any]) -> str:
    """Сводка weak-кандидата для судьи (из снимка): заголовок + концепты + метрики."""
    title = str(candidate.get("title") or "").strip()[:200]
    keywords = candidate.get("keywords") or []
    if isinstance(keywords, str):
        kw = keywords
    else:
        kw = ", ".join(str(k) for k in keywords[:12])
    lines = [
        f"Заголовок: {title}",
        f"Концепты: {kw}",
        f"Источников: {candidate.get('source_count', 0)}; "
        f"разнообразие источников: {round(float(candidate.get('source_diversity', 0) or 0), 3)}; "
        f"burst: {round(float(candidate.get('burst_score', 0) or 0), 3)}; "
        f"signal_score детектора: {round(float(candidate.get('signal_score', 0) or 0), 3)}",
    ]
    return "\n".join(lines)


def normalize_verdict(raw: dict[str, Any]) -> dict[str, Any]:
    """Приводим ответ судьи к {novelty_score: 0..1, out_of_distribution: bool, reasoning: str}."""
    try:
        novelty = float(raw.get("novelty_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        novelty = 0.0
    novelty = max(0.0, min(1.0, novelty))
    ood = bool(raw.get("out_of_distribution", False))
    reasoning = str(raw.get("reasoning") or "").strip()[:200]
    return {"novelty_score": round(novelty, 4), "out_of_distribution": ood, "reasoning": reasoning}


class NoveltyJudgeChain:
    """Вызывает судью: wormsoft (DeepSeek) → polza (DeepSeek) как резерв."""

    def __init__(
        self,
        wormsoft_client: Any,
        polza_client: Any,
        *,
        model: str,
        fallback_model: str,
        token_budget: int = 1200,
    ) -> None:
        self.wormsoft = wormsoft_client
        self.polza = polza_client
        self.model = model
        self.fallback_model = fallback_model
        self.token_budget = token_budget
        self.last_meta: dict[str, Any] = {}

    async def run(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        prompt = build_prompt(candidate)

        # Primary: wormsoft / DeepSeek-v4-pro.
        if getattr(self.wormsoft, "is_available", False) and self.model:
            try:
                resp = await self.wormsoft.chat(
                    system=SYSTEM_PROMPT,
                    user=prompt,
                    task="novelty_judge",
                    model_override=self.model,
                    max_tokens=self.token_budget,
                )
                verdict = normalize_verdict(parse_llm_json_object(resp.content))
                verdict["_provider"] = "wormsoft"
                verdict["_model"] = getattr(resp, "actual_model", self.model) or self.model
                self.last_meta = {"status": "ok", "provider": "wormsoft", "escalated": False}
                return verdict
            except Exception as exc:
                logger.info("novelty_judge wormsoft failed, falling back to polza: %s", exc)

        # Fallback: polza / DeepSeek-v3.2.
        if getattr(self.polza, "is_available", False) and self.fallback_model:
            try:
                resp = await self.polza.chat(
                    system=SYSTEM_PROMPT,
                    user=prompt,
                    task="novelty_judge",
                    model_override=self.fallback_model,
                    max_tokens=self.token_budget,
                )
                verdict = normalize_verdict(parse_llm_json_object(resp.content))
                verdict["_provider"] = "polza"
                verdict["_model"] = getattr(resp, "actual_model", self.fallback_model) or self.fallback_model
                self.last_meta = {"status": "ok", "provider": "polza", "escalated": True}
                return verdict
            except Exception as exc:
                logger.warning("novelty_judge polza fallback failed: %s", exc)

        self.last_meta = {"status": "failed", "provider": None, "escalated": True}
        return None
