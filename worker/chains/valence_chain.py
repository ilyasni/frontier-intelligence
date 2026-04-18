from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pydantic.fields import FieldInfo

from shared.config import get_settings
from shared.metrics import note_gigachat_escalation
from worker.gigachat_client import GigaChatClient, GigaChatResponse
from worker.llm_json import parse_llm_json_object

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "valence.txt"
ALLOWED_VALENCE = {"positive", "neutral", "negative"}
ALLOWED_SIGNAL_TYPES = {
    "growth",
    "failure",
    "launch",
    "closure",
    "regulation",
    "adoption",
    "rejection",
    "delay",
    "recall",
    "lawsuit",
    "partnership",
    "investment",
    "policy",
    "other",
}


class ValenceChain:
    def __init__(self, client: GigaChatClient):
        self.client = client
        self._template = PROMPT_PATH.read_text(encoding="utf-8")
        self._settings = get_settings()
        self._system = "Ты аналитик сигналов. Верни только валидный JSON."
        self.last_meta: dict[str, Any] = {}

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

    async def _call(self, prompt: str, *, model_override: str | None = None) -> GigaChatResponse:
        return await self.client.chat(
            system=self._system,
            user=prompt,
            task="valence",
            model_override=model_override,
        )

    @staticmethod
    def _normalize_result(result: dict[str, Any]) -> dict[str, Any]:
        valence = str(result.get("valence") or "neutral").strip().lower()
        if valence not in ALLOWED_VALENCE:
            valence = "neutral"
        signal_type = str(result.get("signal_type") or "other").strip().lower()
        if signal_type not in ALLOWED_SIGNAL_TYPES:
            signal_type = "other"
        try:
            confidence = float(result.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        reasoning = str(result.get("reasoning") or "").strip()[:160]
        return {
            "valence": valence,
            "signal_type": signal_type,
            "confidence": confidence,
            "reasoning": reasoning,
        }

    async def run(self, content: str) -> dict[str, Any]:
        if not content.strip():
            return {
                "valence": "neutral",
                "signal_type": "other",
                "confidence": 0.0,
                "reasoning": "empty content",
            }
        if hasattr(self.client, "refresh_runtime_overrides"):
            await self.client.refresh_runtime_overrides()

        prompt_model = (
            self._setting_str("gigachat_model_valence").strip()
            or self._setting_str("gigachat_model_lite", "GigaChat-2")
        )
        budgeted = await self.client.budget_text(
            content,
            prompt_model,
            self._setting_int("gigachat_token_budget_valence", 1200),
        )
        prompt = self._template.replace("{{content}}", budgeted.text)

        try:
            response = await self._call(prompt)
            result = self._normalize_result(parse_llm_json_object(response.content))
            self.last_meta = {
                "model": response.model,
                "usage": response.usage,
                "escalated": False,
                "budget_truncated": budgeted.truncated,
            }
            return result
        except Exception as exc:
            logger.info("Valence chain primary attempt failed, escalating: %s", exc)
            if not self._setting_bool("gigachat_escalation_enabled", True):
                self.last_meta = {}
                return {
                    "valence": "neutral",
                    "signal_type": "other",
                    "confidence": 0.0,
                    "reasoning": str(exc)[:160],
                }

        fallback_model = self._setting_str("gigachat_model_pro", "GigaChat-2-Pro")
        note_gigachat_escalation("worker", "valence", prompt_model, fallback_model)
        try:
            response = await self._call(prompt, model_override=fallback_model)
            result = self._normalize_result(parse_llm_json_object(response.content))
            self.last_meta = {
                "model": response.model,
                "usage": response.usage,
                "escalated": True,
                "budget_truncated": budgeted.truncated,
            }
            return result
        except Exception as exc:
            logger.warning("Valence chain failed after escalation: %s", exc)
            self.last_meta = {}
            return {
                "valence": "neutral",
                "signal_type": "other",
                "confidence": 0.0,
                "reasoning": str(exc)[:160],
            }
