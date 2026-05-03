"""Runtime LLM routing shared by admin UI, workers, and MCP tools."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

RUNTIME_LLM_ROUTING_REDIS_KEY = "frontier:runtime:llm_routing"
RUNTIME_LLM_ROUTING_DB_KEY = "llm_routing_v1"

PROVIDER_GIGACHAT = "gigachat"
PROVIDER_WORMSOFT = "wormsoft"
LLM_PROVIDERS = (PROVIDER_GIGACHAT, PROVIDER_WORMSOFT)

TASK_RELEVANCE_CONCEPTS = "relevance_concepts"
TASK_RELEVANCE = "relevance"
TASK_CONCEPTS = "concepts"
TASK_VALENCE = "valence"
TASK_MCP_SYNTHESIS = "mcp_synthesis"
TASK_CHAT = "chat"

ROUTABLE_LLM_TASKS = (
    TASK_RELEVANCE_CONCEPTS,
    TASK_RELEVANCE,
    TASK_CONCEPTS,
    TASK_VALENCE,
    TASK_MCP_SYNTHESIS,
)

DEFAULT_WORMSOFT_TEXT_MODEL = "wormsoft/agent/medium"


def normalize_provider(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "giga": PROVIDER_GIGACHAT,
        "gigachat": PROVIDER_GIGACHAT,
        "gpt2giga": PROVIDER_GIGACHAT,
        "wormsoft": PROVIDER_WORMSOFT,
    }
    normalized = aliases.get(raw, raw)
    if normalized in LLM_PROVIDERS:
        return normalized
    return PROVIDER_GIGACHAT


class LLMRoute(BaseModel):
    provider: str = PROVIDER_GIGACHAT
    model: str = ""
    fallback_provider: str = PROVIDER_GIGACHAT
    fallback_model: str = ""

    @field_validator("provider", "fallback_provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)

    @field_validator("model", "fallback_model", mode="before")
    @classmethod
    def _validate_model(cls, value: Any) -> str:
        return str(value or "").strip()


class LLMRoutingSettings(BaseModel):
    relevance_concepts: LLMRoute = Field(default_factory=LLMRoute)
    relevance: LLMRoute = Field(default_factory=LLMRoute)
    concepts: LLMRoute = Field(default_factory=LLMRoute)
    valence: LLMRoute = Field(default_factory=LLMRoute)
    mcp_synthesis: LLMRoute = Field(default_factory=LLMRoute)

    def route_for_task(self, task: str) -> LLMRoute:
        key = task if task in ROUTABLE_LLM_TASKS else TASK_MCP_SYNTHESIS
        return getattr(self, key)


def _gigachat_primary_model(settings: Any, task: str) -> str:
    if task == TASK_RELEVANCE_CONCEPTS:
        return str(
            getattr(settings, "gigachat_model_relevance", "")
            or getattr(settings, "gigachat_model_lite", "GigaChat-2")
        ).strip()
    if task == TASK_RELEVANCE:
        return str(
            getattr(settings, "gigachat_model_relevance", "")
            or getattr(settings, "gigachat_model_lite", "GigaChat-2")
        ).strip()
    if task == TASK_CONCEPTS:
        return str(
            getattr(settings, "gigachat_model_concepts", "")
            or getattr(settings, "gigachat_model_lite", "GigaChat-2")
        ).strip()
    if task == TASK_VALENCE:
        return str(
            getattr(settings, "gigachat_model_valence", "")
            or getattr(settings, "gigachat_model_lite", "GigaChat-2")
        ).strip()
    if task == TASK_MCP_SYNTHESIS:
        return str(
            getattr(settings, "gigachat_model_mcp_synthesis", "")
            or getattr(settings, "gigachat_model_lite", "GigaChat-2")
        ).strip()
    return str(getattr(settings, "gigachat_model", "GigaChat-2")).strip()


def _gigachat_fallback_model(settings: Any, task: str) -> str:
    if task in {TASK_RELEVANCE_CONCEPTS, TASK_RELEVANCE, TASK_CONCEPTS, TASK_VALENCE, TASK_MCP_SYNTHESIS}:
        return str(getattr(settings, "gigachat_model_pro", "GigaChat-2-Pro")).strip()
    return str(getattr(settings, "gigachat_model_pro", "GigaChat-2-Pro")).strip()


def default_llm_routing(settings: Any) -> LLMRoutingSettings:
    wormsoft_model = str(
        getattr(settings, "wormsoft_model_default", DEFAULT_WORMSOFT_TEXT_MODEL)
        or DEFAULT_WORMSOFT_TEXT_MODEL
    ).strip()
    return LLMRoutingSettings(
        relevance_concepts=LLMRoute(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallback_provider=PROVIDER_GIGACHAT,
            fallback_model=_gigachat_primary_model(settings, TASK_RELEVANCE_CONCEPTS),
        ),
        relevance=LLMRoute(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallback_provider=PROVIDER_GIGACHAT,
            fallback_model=_gigachat_primary_model(settings, TASK_RELEVANCE),
        ),
        concepts=LLMRoute(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallback_provider=PROVIDER_GIGACHAT,
            fallback_model=_gigachat_primary_model(settings, TASK_CONCEPTS),
        ),
        valence=LLMRoute(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallback_provider=PROVIDER_GIGACHAT,
            fallback_model=_gigachat_primary_model(settings, TASK_VALENCE),
        ),
        mcp_synthesis=LLMRoute(
            provider=PROVIDER_GIGACHAT,
            model=_gigachat_primary_model(settings, TASK_MCP_SYNTHESIS),
            fallback_provider=PROVIDER_GIGACHAT,
            fallback_model=_gigachat_fallback_model(settings, TASK_MCP_SYNTHESIS),
        ),
    )


def effective_llm_routing(
    settings: Any,
    runtime_mode: str | None,
    payload: dict[str, Any] | None = None,
) -> LLMRoutingSettings:
    routing = default_llm_routing(settings)
    if payload:
        base = routing.model_dump()
        for task in ROUTABLE_LLM_TASKS:
            if isinstance(payload.get(task), dict):
                base[task] = {**base[task], **payload[task]}
        routing = LLMRoutingSettings.model_validate(base)

    if str(runtime_mode or "").strip().lower() == "gigachat-2-only":
        forced = routing.model_dump()
        for task in ROUTABLE_LLM_TASKS:
            forced[task] = {
                "provider": PROVIDER_GIGACHAT,
                "model": _gigachat_primary_model(settings, task),
                "fallback_provider": PROVIDER_GIGACHAT,
                "fallback_model": _gigachat_fallback_model(settings, task),
            }
        routing = LLMRoutingSettings.model_validate(forced)

    return routing


def llm_provider_options() -> list[dict[str, str]]:
    return [
        {
            "id": PROVIDER_GIGACHAT,
            "label": "GigaChat",
            "description": "Use GigaChat via gpt2giga-proxy for this task.",
        },
        {
            "id": PROVIDER_WORMSOFT,
            "label": "Wormsoft",
            "description": "Use Wormsoft as the primary text provider for this task.",
        },
    ]
