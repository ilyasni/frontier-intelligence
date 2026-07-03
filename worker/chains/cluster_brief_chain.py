"""Russian brief chain — rephrase a trend cluster into title_ru/insight/opportunity.

Routes through the ``mcp_synthesis`` task so the LLM router uses wormsoft as the
primary provider (with its configured fallbacks and credit guard). The output is
presentation text for Telegram alerts and MCP, never used for cluster matching.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from worker.llm_json import parse_llm_json_object

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "cluster_brief.txt"

_TASK = "mcp_synthesis"
_TITLE_RU_MAX = 120
_TEXT_MAX = 600


class ClusterBriefChain:
    def __init__(self, client: Any):
        self.client = client
        self._template = PROMPT_PATH.read_text(encoding="utf-8")
        self._system = (
            "Ты аналитик трендов. Пиши кратко по-русски и возвращай только валидный JSON."
        )
        self.last_meta: dict[str, Any] = {}

    @staticmethod
    def _clip(value: Any, limit: int) -> str:
        text = str(value or "").strip().strip('"').strip()
        if len(text) <= limit:
            return text
        return f"{text[: max(0, limit - 1)].rstrip()}…"

    def _build_prompt(
        self,
        *,
        title: str,
        keywords: list[str],
        evidence_titles: list[str],
    ) -> str:
        keywords_text = ", ".join(keywords) if keywords else "—"
        evidence_text = (
            "\n".join(f"- {item}" for item in evidence_titles[:8]) if evidence_titles else "—"
        )
        return (
            self._template.replace("{{title}}", title)
            .replace("{{keywords}}", keywords_text)
            .replace("{{evidence}}", evidence_text)
        )

    async def run(
        self,
        *,
        title: str,
        keywords: list[str] | None = None,
        evidence_titles: list[str] | None = None,
    ) -> dict[str, str] | None:
        """Return ``{title_ru, insight, opportunity}`` or ``None`` on failure."""
        title = str(title or "").strip()
        if not title:
            self.last_meta = {"status": "not_called", "skip_reason": "empty_title"}
            return None

        prompt = self._build_prompt(
            title=title,
            keywords=[str(k).strip() for k in (keywords or []) if str(k).strip()],
            evidence_titles=[str(t).strip() for t in (evidence_titles or []) if str(t).strip()],
        )

        try:
            response = await self.client.chat(
                system=self._system,
                user=prompt,
                task=_TASK,
                max_tokens=512,
            )
            result = parse_llm_json_object(response.content)
            title_ru = self._clip(result.get("title_ru"), _TITLE_RU_MAX)
            insight = self._clip(result.get("insight"), _TEXT_MAX)
            opportunity = self._clip(result.get("opportunity"), _TEXT_MAX)
            if not title_ru and not insight:
                raise ValueError("empty_brief")
            self.last_meta = {
                "status": "ok",
                "provider": getattr(response, "provider", ""),
                "model": getattr(response, "model", ""),
                "usage": getattr(response, "usage", None),
            }
            return {
                "title_ru": title_ru or title,
                "insight": insight,
                "opportunity": opportunity,
            }
        except Exception as exc:
            logger.warning("Cluster brief chain failed: %s", exc)
            self.last_meta = {"status": "failed", "error": str(exc)[:200]}
            return None
