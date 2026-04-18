"""Relevance chain - score a post against workspace categories."""
from __future__ import annotations

import json
import logging
import re
from difflib import get_close_matches
from pathlib import Path
from typing import Any

from pydantic.fields import FieldInfo

from shared.config import get_settings
from shared.metrics import note_gigachat_escalation
from worker.gigachat_client import GigaChatClient, GigaChatResponse
from worker.llm_json import parse_llm_json_object

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "relevance.txt"

_OTHER_SLUGS = frozenset(
    {"other", "другое", "прочее", "misc", "none", "неизвестно", "не_определено", "иное"}
)

_ALIAS_TO_SLUG: dict[str, str] = {
    "tech": "technology",
    "it": "technology",
    "technologies": "technology",
    "технологии": "technology",
    "техника": "technology",
    "информационные_технологии": "technology",
    "business": "business_models",
    "бизнес": "business_models",
    "biz": "business_models",
    "стартап": "business_models",
    "стартапы": "business_models",
    "модель": "business_models",
    "дизайн": "design",
    "ux": "design",
    "ui": "design",
    "наука": "science",
    "исследования": "science",
    "общество": "society",
    "социум": "society",
    "social": "society",
}


def _slug_key(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^a-z0-9а-яё_]+", "", s, flags=re.IGNORECASE)
    return s.strip("_") or ""


def normalize_relevance_category(raw: Any, allowed: list[str]) -> tuple[str, str | None]:
    """Normalize LLM category to one of the workspace slugs or `other`."""
    allowed_clean = [str(a).strip() for a in allowed if str(a).strip()]
    if not allowed_clean:
        return "other", None

    by_key: dict[str, str] = {}
    for cat in allowed_clean:
        by_key[_slug_key(cat)] = cat

    if raw is None:
        return "other", None

    raw_str = str(raw).strip()
    if not raw_str:
        return "other", None

    key = _slug_key(raw_str)
    if key in _OTHER_SLUGS:
        return "other", None

    if key in by_key:
        return by_key[key], None

    if key in _ALIAS_TO_SLUG:
        target = _ALIAS_TO_SLUG[key]
        if target in by_key:
            return by_key[target], raw_str
        for cat in allowed_clean:
            if _slug_key(cat) == target:
                return cat, raw_str

    keys = list(by_key.keys())
    if key and keys:
        match = get_close_matches(key, keys, n=1, cutoff=0.76)
        if match:
            return by_key[match[0]], raw_str

    logger.warning(
        "relevance_category_unknown raw=%r allowed=%s",
        raw_str[:120],
        allowed_clean,
    )
    return "other", raw_str


class RelevanceChain:
    def __init__(self, client: GigaChatClient):
        self.client = client
        self._template = PROMPT_PATH.read_text(encoding="utf-8")
        self._settings = get_settings()
        self._system = "Ты аналитик трендов. Верни только валидный JSON."

    def _setting_str(self, name: str, default: str = "") -> str:
        value = self.client.setting_value(name, default) if hasattr(self.client, "setting_value") else getattr(
            self._settings,
            name,
            default,
        )
        if isinstance(value, FieldInfo) or value is None:
            return default
        return str(value)

    def _setting_bool(self, name: str, default: bool = False) -> bool:
        value = self.client.setting_value(name, default) if hasattr(self.client, "setting_value") else getattr(
            self._settings,
            name,
            default,
        )
        if isinstance(value, FieldInfo):
            return default
        return bool(value)

    def _setting_int(self, name: str, default: int) -> int:
        value = self.client.setting_value(name, default) if hasattr(self.client, "setting_value") else getattr(
            self._settings,
            name,
            default,
        )
        if isinstance(value, FieldInfo):
            return default
        return int(value or default)

    def _setting_float(self, name: str, default: float) -> float:
        value = self.client.setting_value(name, default) if hasattr(self.client, "setting_value") else getattr(
            self._settings,
            name,
            default,
        )
        if isinstance(value, FieldInfo) or value is None:
            return default
        return float(value)

    async def _call(
        self,
        prompt: str,
        *,
        model_override: str | None = None,
        pro: bool = False,
    ) -> GigaChatResponse:
        return await self.client.chat(
            system=self._system,
            user=prompt,
            task="relevance",
            pro=pro,
            model_override=model_override,
        )

    def _needs_escalation(
        self,
        result: dict[str, Any],
        category: str,
        threshold: float,
        coerced_from: str | None,
    ) -> bool:
        if not self._setting_bool("gigachat_escalation_enabled", True):
            return False
        score = float(result.get("score", 0))
        gray_zone = max(0.0, self._setting_float("gigachat_relevance_gray_zone", 0.1))
        if abs(score - threshold) <= gray_zone:
            return True
        if category == "other" and coerced_from:
            return True
        return False

    def _is_relevant(self, score: float, category: str, threshold: float) -> bool:
        if score >= threshold:
            return True
        if category == "other":
            return False
        gray_zone = max(0.0, self._setting_float("gigachat_relevance_gray_zone", 0.1))
        return score >= max(0.0, threshold - gray_zone)

    async def run(
        self,
        content: str,
        workspace_name: str,
        categories: list[str],
        threshold: float = 0.6,
    ) -> dict:
        """Returns {"score": float, "category": str, "reasoning": str, "relevant": bool}."""
        if not content.strip():
            return {"score": 0.0, "category": "other", "reasoning": "empty content", "relevant": False}
        if hasattr(self.client, "refresh_runtime_overrides"):
            await self.client.refresh_runtime_overrides()

        prompt_model = (
            self._setting_str("gigachat_model_relevance").strip()
            or self._setting_str("gigachat_model_lite", "GigaChat-2")
        )
        budgeted = await self.client.budget_text(
            content,
            prompt_model,
            self._setting_int("gigachat_token_budget_relevance", 1500),
        )
        cats_json = json.dumps(categories, ensure_ascii=False)
        cats_str = ", ".join(categories)
        prompt = (
            self._template
            .replace("{{workspace_name}}", workspace_name)
            .replace("{{categories}}", cats_str)
            .replace("{{categories_json}}", cats_json)
            .replace("{{content}}", budgeted.text)
        )

        raw = ""
        response: GigaChatResponse | None = None
        try:
            response = await self._call(prompt)
            raw = response.content
            result = parse_llm_json_object(raw)

            score = float(result.get("score", 0))
            score = max(0.0, min(1.0, score))
            reasoning = str(result.get("reasoning", ""))[:500]
            category, coerced_from = normalize_relevance_category(result.get("category", "other"), categories)

            if self._needs_escalation(result, category, threshold, coerced_from):
                raise ValueError("gray_zone_or_ambiguous_category")

            return {
                "score": score,
                "category": category,
                "reasoning": reasoning,
                "relevant": self._is_relevant(score, category, threshold),
                "_usage": response.usage,
                "_model": response.model,
                "_budget_truncated": budgeted.truncated,
            }
        except Exception as exc:
            snippet = (raw or "")[:800].replace("\n", " ")
            logger.info(
                "Relevance chain primary attempt failed, escalation=%s response_snippet=%r",
                exc,
                snippet,
            )
            if not self._setting_bool("gigachat_escalation_enabled", True):
                return {"score": 0.0, "category": "other", "reasoning": str(exc), "relevant": False}

        fallback_model = self._setting_str("gigachat_model_pro", "GigaChat-2-Pro")
        primary_model = response.model if response else prompt_model
        note_gigachat_escalation("worker", "relevance", primary_model, fallback_model)

        try:
            response = await self._call(prompt, model_override=fallback_model)
            result = parse_llm_json_object(response.content)
            score = float(result.get("score", 0))
            score = max(0.0, min(1.0, score))
            reasoning = str(result.get("reasoning", ""))[:500]
            category, _ = normalize_relevance_category(result.get("category", "other"), categories)
            return {
                "score": score,
                "category": category,
                "reasoning": reasoning,
                "relevant": self._is_relevant(score, category, threshold),
                "_usage": response.usage,
                "_model": response.model,
                "_budget_truncated": budgeted.truncated,
                "_escalated": True,
            }
        except Exception as exc:
            logger.warning("Relevance chain failed after escalation: %s", exc)
            return {"score": 0.0, "category": "other", "reasoning": str(exc), "relevant": False}
