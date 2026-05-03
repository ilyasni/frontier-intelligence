"""Concept extraction chain - extract entities and concepts from text."""
from __future__ import annotations

import logging
from pathlib import Path

from pydantic.fields import FieldInfo

from shared.config import get_settings
from shared.metrics import note_llm_fallback
from worker.gigachat_client import GigaChatClient
from worker.llm_json import parse_llm_json_object

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "concepts.txt"


class ConceptChain:
    def __init__(self, client: GigaChatClient):
        self.client = client
        self._template = PROMPT_PATH.read_text(encoding="utf-8")
        self._settings = get_settings()
        self._system = "Ты аналитик. Извлеки концепции и верни только валидный JSON."
        self.last_meta: dict = {}

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

    @staticmethod
    def _validate_concepts(concepts: list) -> list[dict]:
        valid = []
        for item in concepts:
            if isinstance(item, dict) and "name" in item:
                valid.append({
                    "name": str(item["name"])[:100],
                    "category": str(item.get("category", "other"))[:50],
                    "weight": min(5, max(1, int(item.get("weight", 1)))),
                })
        return valid[:10]

    async def _call(
        self,
        prompt: str,
        *,
        model_override: str | None = None,
        provider_override: str | None = None,
    ):
        return await self.client.chat(
            system=self._system,
            user=prompt,
            task="concepts",
            model_override=model_override,
            provider_override=provider_override,
        )

    async def run(self, content: str) -> list[dict]:
        """Returns list of {name, category, weight} dicts."""
        prompt_model = (
            self.client.route_model_for_task("concepts")
            if hasattr(self.client, "route_model_for_task")
            else (
                self._setting_str("gigachat_model_concepts").strip()
                or self._setting_str("gigachat_model_lite", "GigaChat-2")
            )
        )
        prompt_provider = "gigachat"
        if hasattr(self.client, "routing_settings"):
            prompt_provider = self.client.routing_settings.route_for_task("concepts").provider
        if not content.strip():
            self.last_meta = {
                "provider": prompt_provider,
                "requested_model": prompt_model,
                "actual_model": "",
                "usage": None,
                "escalated": False,
                "budget_truncated": False,
                "status": "not_called",
                "skip_reason": "empty_content",
                "error": "",
            }
            return []
        if hasattr(self.client, "refresh_runtime_overrides"):
            await self.client.refresh_runtime_overrides()
        budgeted = await self.client.budget_text(
            content,
            prompt_model,
            self._setting_int("gigachat_token_budget_concepts", 1500),
        )
        prompt = self._template.replace("{{content}}", budgeted.text)

        try:
            response = await self._call(prompt)
            result = parse_llm_json_object(response.content)
            valid = self._validate_concepts(result.get("concepts", []))
            if not valid:
                raise ValueError("empty_or_invalid_concepts")
            self.last_meta = {
                "model": response.model,
                "provider": response.provider,
                "requested_model": response.requested_model or prompt_model,
                "actual_model": response.actual_model,
                "usage": response.usage,
                "escalated": False,
                "budget_truncated": budgeted.truncated,
                "status": "ok",
                "skip_reason": "",
                "error": "",
            }
            return valid
        except Exception as exc:
            logger.info("Concept chain primary attempt failed, escalating: %s", exc)
            if not self._setting_bool("gigachat_escalation_enabled", True):
                self.last_meta = {
                    "provider": prompt_provider,
                    "requested_model": prompt_model,
                    "actual_model": "",
                    "usage": None,
                    "escalated": False,
                    "budget_truncated": budgeted.truncated,
                    "status": "failed",
                    "skip_reason": "",
                    "error": str(exc)[:200],
                }
                return []

        fallback_provider, fallback_model = (
            self.client.route_fallback_for_task("concepts")
            if hasattr(self.client, "route_fallback_for_task")
            else ("gigachat", self._setting_str("gigachat_model_pro", "GigaChat-2-Pro"))
        )
        note_llm_fallback(
            "worker",
            "concepts",
            from_provider=prompt_provider,
            from_requested_model=prompt_model,
            from_actual_model="",
            to_provider=fallback_provider,
            to_model=fallback_model,
            reason="chain_escalation",
        )

        try:
            response = await self._call(
                prompt,
                model_override=fallback_model,
                provider_override=fallback_provider,
            )
            result = parse_llm_json_object(response.content)
            valid = self._validate_concepts(result.get("concepts", []))
            self.last_meta = {
                "model": response.model,
                "provider": response.provider,
                "requested_model": response.requested_model or fallback_model,
                "actual_model": response.actual_model,
                "usage": response.usage,
                "escalated": True,
                "budget_truncated": budgeted.truncated,
                "status": "ok",
                "skip_reason": "",
                "error": "",
            }
            return valid
        except Exception as exc:
            logger.warning("Concept chain failed after escalation: %s", exc)
            self.last_meta = {
                "provider": fallback_provider,
                "requested_model": fallback_model,
                "actual_model": "",
                "usage": None,
                "escalated": True,
                "budget_truncated": budgeted.truncated,
                "status": "failed",
                "skip_reason": "",
                "error": str(exc)[:200],
            }
            return []
