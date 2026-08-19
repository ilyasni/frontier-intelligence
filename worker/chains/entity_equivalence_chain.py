"""Контур D+ (RSI): LLM-судья эквивалентности сущностей (другая семья — DeepSeek).

Кандидаты-пары (акроним ↔ расшифровка, со-встречающиеся в документах) приходят из
entity_resolution. Судья подтверждает, что это ОДНА сущность (аббревиатура/алиас), а не
просто связанные термины («ИИ» ↔ «ИИ-инструменты» — НЕ одно и то же). Reuse паттерна
novelty_judge_chain: wormsoft (DeepSeek) → polza fallback.
"""

from __future__ import annotations

import logging
from typing import Any

from worker.llm_json import parse_llm_json_object

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Ты эксперт по разрешению сущностей в графе знаний. Тебе дают два термина-концепта, "
    "которые со-встречаются в документах и где один может быть аббревиатурой другого. "
    "Реши, обозначают ли они ОДНУ И ТУ ЖЕ сущность (один — аббревиатура/алиас/иное написание "
    "другого), или это РАЗНЫЕ, пусть и связанные понятия. "
    "Примеры одной сущности: «LLM» и «Large Language Model»; «AWS» и «Amazon Web Services». "
    "Примеры РАЗНЫХ: «AI» и «AI infrastructure»; «ИИ» и «ИИ-инструменты» (инструменты — не сам ИИ). "
    "canonical — какой термин оставить главным (обычно полная форма): 'a' или 'b'. "
    "Верни ТОЛЬКО валидный JSON: "
    '{"equivalent": <true|false>, "canonical": "a"|"b", "confidence": <0..1>, "reasoning": "<до 200 симв>"}'
)


def build_prompt(name_a: str, name_b: str) -> str:
    return f"Термин a: {str(name_a)[:160]}\nТермин b: {str(name_b)[:160]}"


def normalize_verdict(raw: dict[str, Any]) -> dict[str, Any]:
    equivalent = bool(raw.get("equivalent", False))
    canonical = str(raw.get("canonical") or "b").strip().lower()
    if canonical not in {"a", "b"}:
        canonical = "b"
    try:
        confidence = float(raw.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    reasoning = str(raw.get("reasoning") or "").strip()[:200]
    return {
        "equivalent": equivalent,
        "canonical": canonical,
        "confidence": round(confidence, 4),
        "reasoning": reasoning,
    }


class EntityEquivalenceChain:
    def __init__(
        self,
        wormsoft_client: Any,
        polza_client: Any,
        *,
        model: str,
        fallback_model: str,
        token_budget: int = 2000,
        router_client: Any | None = None,
    ) -> None:
        self.wormsoft = wormsoft_client
        self.polza = polza_client
        self.router = router_client
        self.model = model
        self.fallback_model = fallback_model
        self.token_budget = token_budget

    async def run(self, name_a: str, name_b: str) -> dict[str, Any] | None:
        prompt = build_prompt(name_a, name_b)
        if (self.router is not None or getattr(self.wormsoft, "is_available", False)) and self.model:
            try:
                if self.router is not None:
                    resp = await self.router.chat(
                        system=SYSTEM_PROMPT,
                        user=prompt,
                        task="entity_equivalence",
                        provider_override="wormsoft",
                        model_override=self.model,
                        max_tokens=self.token_budget,
                    )
                else:
                    resp = await self.wormsoft.chat(
                        system=SYSTEM_PROMPT,
                        user=prompt,
                        task="entity_equivalence",
                        model_override=self.model,
                        max_tokens=self.token_budget,
                    )
                v = normalize_verdict(parse_llm_json_object(resp.content))
                v["_provider"] = "wormsoft"
                return v
            except Exception as exc:
                logger.info("entity_equivalence wormsoft failed, fallback to polza: %s", exc)
        if (self.router is not None or getattr(self.polza, "is_available", False)) and self.fallback_model:
            try:
                if self.router is not None:
                    resp = await self.router.chat(
                        system=SYSTEM_PROMPT,
                        user=prompt,
                        task="entity_equivalence",
                        provider_override="polza",
                        model_override=self.fallback_model,
                        max_tokens=self.token_budget,
                    )
                else:
                    resp = await self.polza.chat(
                        system=SYSTEM_PROMPT,
                        user=prompt,
                        task="entity_equivalence",
                        model_override=self.fallback_model,
                        max_tokens=self.token_budget,
                    )
                v = normalize_verdict(parse_llm_json_object(resp.content))
                v["_provider"] = "polza"
                return v
            except Exception as exc:
                logger.warning("entity_equivalence polza fallback failed: %s", exc)
        return None
