from __future__ import annotations

import json
import logging
from pathlib import Path

from pydantic.fields import FieldInfo

from shared.config import get_settings
from shared.metrics import note_gigachat_escalation
from worker.chains.concept_chain import ConceptChain
from worker.chains.relevance_chain import normalize_relevance_category
from worker.gigachat_client import GigaChatClient
from worker.llm_json import parse_llm_json_object

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "relevance_concepts.txt"


class RelevanceConceptsChain:
    def __init__(self, client: GigaChatClient):
        self.client = client
        self._template = PROMPT_PATH.read_text(encoding="utf-8")
        self._settings = get_settings()
        self._system = "Ты аналитик трендов. Верни только валидный JSON."
        self.last_meta: dict = {}

    def _setting_str(self, name: str, default: str = "") -> str:
        value = getattr(self._settings, name, default)
        if isinstance(value, FieldInfo) or value is None:
            return default
        return str(value)

    def _setting_bool(self, name: str, default: bool = False) -> bool:
        value = getattr(self._settings, name, default)
        if isinstance(value, FieldInfo):
            return default
        return bool(value)

    def _setting_int(self, name: str, default: int) -> int:
        value = getattr(self._settings, name, default)
        if isinstance(value, FieldInfo):
            return default
        return int(value or default)

    def _setting_float(self, name: str, default: float) -> float:
        value = getattr(self._settings, name, default)
        if isinstance(value, FieldInfo) or value is None:
            return default
        return float(value)

    async def _call(self, prompt: str, *, model_override: str | None = None):
        return await self.client.chat(
            system=self._system,
            user=prompt,
            task="relevance",
            model_override=model_override,
        )

    async def run(
        self,
        content: str,
        workspace_name: str,
        categories: list[str],
        threshold: float,
    ) -> tuple[dict, list[dict]]:
        if not content.strip():
            return (
                {"score": 0.0, "category": "other", "reasoning": "empty content", "relevant": False},
                [],
            )

        prompt_model = (
            self._setting_str("gigachat_model_relevance").strip()
            or self._setting_str("gigachat_model_lite", "GigaChat-2")
        )
        budgeted = await self.client.budget_text(
            content,
            prompt_model,
            self._setting_int("gigachat_token_budget_relevance_concepts", 1800),
        )
        prompt = (
            self._template
            .replace("{{workspace_name}}", workspace_name)
            .replace("{{categories_json}}", json.dumps(categories, ensure_ascii=False))
            .replace("{{content}}", budgeted.text)
        )

        try:
            response = await self._call(prompt)
            parsed = parse_llm_json_object(response.content)
            rel, concepts = self._normalize_payload(parsed, categories, threshold)
            self.last_meta = {
                "model": response.model,
                "usage": response.usage,
                "escalated": False,
                "budget_truncated": budgeted.truncated,
            }
            if not concepts and rel["score"] >= max(0.0, threshold - self._settings.gigachat_relevance_gray_zone):
                raise ValueError("empty_concepts_in_relevant_candidate")
            return rel, concepts
        except Exception as exc:
            logger.info("Relevance+Concepts joint chain primary attempt failed, escalating: %s", exc)
            if not self._setting_bool("gigachat_escalation_enabled", True):
                self.last_meta = {}
                return (
                    {"score": 0.0, "category": "other", "reasoning": str(exc), "relevant": False},
                    [],
                )

        fallback_model = self._setting_str("gigachat_model_pro", "GigaChat-2-Pro")
        note_gigachat_escalation("worker", "relevance_concepts", prompt_model, fallback_model)
        try:
            response = await self._call(prompt, model_override=fallback_model)
            parsed = parse_llm_json_object(response.content)
            rel, concepts = self._normalize_payload(parsed, categories, threshold)
            self.last_meta = {
                "model": response.model,
                "usage": response.usage,
                "escalated": True,
                "budget_truncated": budgeted.truncated,
            }
            return rel, concepts
        except Exception as exc:
            logger.warning("Relevance+Concepts joint chain failed after escalation: %s", exc)
            self.last_meta = {}
            return (
                {"score": 0.0, "category": "other", "reasoning": str(exc), "relevant": False},
                [],
            )

    def _normalize_payload(
        self,
        payload: dict,
        categories: list[str],
        threshold: float,
    ) -> tuple[dict, list[dict]]:
        score = max(0.0, min(1.0, float(payload.get("score", 0.0) or 0.0)))
        category, _ = normalize_relevance_category(payload.get("category", "other"), categories)
        gray_zone = max(0.0, self._setting_float("gigachat_relevance_gray_zone", 0.1))
        relevant = score >= threshold or (
            category != "other" and score >= max(0.0, threshold - gray_zone)
        )
        rel = {
            "score": score,
            "category": category,
            "reasoning": str(payload.get("reasoning") or "")[:500],
            "relevant": relevant,
        }
        concepts = ConceptChain._validate_concepts(payload.get("concepts", []))
        return rel, concepts
