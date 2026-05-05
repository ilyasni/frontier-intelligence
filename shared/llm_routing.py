"""Runtime LLM routing shared by admin UI, workers, and MCP tools."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

RUNTIME_LLM_ROUTING_REDIS_KEY = "frontier:runtime:llm_routing"
RUNTIME_LLM_ROUTING_DB_KEY = "llm_routing_v1"

PROVIDER_GIGACHAT = "gigachat"
PROVIDER_WORMSOFT = "wormsoft"
PROVIDER_OPENROUTER = "openrouter"
PROVIDER_POLZA = "polza"
LLM_PROVIDERS = (
    PROVIDER_GIGACHAT,
    PROVIDER_WORMSOFT,
    PROVIDER_OPENROUTER,
    PROVIDER_POLZA,
)

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
DEFAULT_WORMSOFT_SYNTHESIS_MODEL = "wormsoft/agent/large"
DEFAULT_OPENROUTER_TEXT_MODEL = "openrouter/free"
DEFAULT_POLZA_TEXT_MODEL = "google/gemma-3-12b-it"
DEFAULT_POLZA_SYNTHESIS_MODEL = "mistralai/mistral-small-3.1-24b-instruct"


def normalize_provider(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "giga": PROVIDER_GIGACHAT,
        "gigachat": PROVIDER_GIGACHAT,
        "gpt2giga": PROVIDER_GIGACHAT,
        "wormsoft": PROVIDER_WORMSOFT,
        "openrouter": PROVIDER_OPENROUTER,
        "or": PROVIDER_OPENROUTER,
        "polza": PROVIDER_POLZA,
    }
    normalized = aliases.get(raw, raw)
    if normalized in LLM_PROVIDERS:
        return normalized
    return PROVIDER_GIGACHAT


class LLMFallback(BaseModel):
    provider: str = PROVIDER_GIGACHAT
    model: str = ""

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)

    @field_validator("model", mode="before")
    @classmethod
    def _validate_model(cls, value: Any) -> str:
        return str(value or "").strip()


class LLMRoute(BaseModel):
    provider: str = PROVIDER_GIGACHAT
    model: str = ""
    fallback_provider: str = PROVIDER_GIGACHAT
    fallback_model: str = ""
    fallbacks: list[LLMFallback] = Field(default_factory=list)

    @field_validator("provider", "fallback_provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)

    @field_validator("model", "fallback_model", mode="before")
    @classmethod
    def _validate_model(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("fallbacks", mode="before")
    @classmethod
    def _validate_fallbacks(cls, value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return []

    @classmethod
    def with_fallbacks(
        cls,
        *,
        provider: str,
        model: str,
        fallbacks: list[LLMFallback],
    ) -> LLMRoute:
        first = fallbacks[0] if fallbacks else LLMFallback()
        return cls(
            provider=provider,
            model=model,
            fallback_provider=first.provider,
            fallback_model=first.model,
            fallbacks=fallbacks,
        )

    def model_post_init(self, __context: Any) -> None:
        if not self.fallbacks:
            fallback_model = self.fallback_model.strip()
            if fallback_model:
                self.fallbacks = [
                    LLMFallback(
                        provider=self.fallback_provider,
                        model=fallback_model,
                    )
                ]
        elif self.fallback_model.strip():
            first = self.fallbacks[0] if self.fallbacks else None
            if (
                first is None
                or first.provider != normalize_provider(self.fallback_provider)
                or first.model != self.fallback_model.strip()
            ):
                self.fallbacks = [
                    LLMFallback(
                        provider=self.fallback_provider,
                        model=self.fallback_model.strip(),
                    ),
                    *self.fallbacks[1:],
                ]
        if self.fallbacks:
            self.fallback_provider = self.fallbacks[0].provider
            self.fallback_model = self.fallbacks[0].model

    def fallback_chain(self) -> list[tuple[str, str]]:
        chain: list[tuple[str, str]] = []
        for fallback in self.fallbacks:
            provider = normalize_provider(fallback.provider)
            model = fallback.model.strip()
            if not model:
                continue
            if (provider, model) not in chain:
                chain.append((provider, model))
        return chain


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
    if task in {
        TASK_RELEVANCE_CONCEPTS,
        TASK_RELEVANCE,
        TASK_CONCEPTS,
        TASK_VALENCE,
        TASK_MCP_SYNTHESIS,
    }:
        return str(getattr(settings, "gigachat_model_pro", "GigaChat-2-Pro")).strip()
    return str(getattr(settings, "gigachat_model_pro", "GigaChat-2-Pro")).strip()


def _wormsoft_synthesis_model(settings: Any) -> str:
    return str(
        getattr(settings, "wormsoft_model_mcp_synthesis", "")
        or DEFAULT_WORMSOFT_SYNTHESIS_MODEL
        or DEFAULT_WORMSOFT_SYNTHESIS_MODEL
    ).strip()


def _openrouter_text_model(settings: Any) -> str:
    return str(
        getattr(settings, "openrouter_text_model", DEFAULT_OPENROUTER_TEXT_MODEL)
        or DEFAULT_OPENROUTER_TEXT_MODEL
    ).strip()


def _polza_text_model(settings: Any, task: str) -> str:
    if task == TASK_MCP_SYNTHESIS:
        return str(
            getattr(settings, "polza_synthesis_model", "")
            or getattr(settings, "polza_text_model", DEFAULT_POLZA_SYNTHESIS_MODEL)
            or DEFAULT_POLZA_SYNTHESIS_MODEL
        ).strip()
    return str(
        getattr(settings, "polza_text_model", DEFAULT_POLZA_TEXT_MODEL)
        or DEFAULT_POLZA_TEXT_MODEL
    ).strip()


def _default_text_fallbacks(settings: Any, task: str) -> list[LLMFallback]:
    return [
        LLMFallback(provider=PROVIDER_OPENROUTER, model=_openrouter_text_model(settings)),
        LLMFallback(provider=PROVIDER_POLZA, model=_polza_text_model(settings, task)),
        LLMFallback(provider=PROVIDER_GIGACHAT, model=_gigachat_primary_model(settings, task)),
    ]


def default_llm_routing(settings: Any) -> LLMRoutingSettings:
    wormsoft_model = str(
        getattr(settings, "wormsoft_model_default", DEFAULT_WORMSOFT_TEXT_MODEL)
        or DEFAULT_WORMSOFT_TEXT_MODEL
    ).strip()
    return LLMRoutingSettings(
        relevance_concepts=LLMRoute.with_fallbacks(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallbacks=_default_text_fallbacks(settings, TASK_RELEVANCE_CONCEPTS),
        ),
        relevance=LLMRoute.with_fallbacks(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallbacks=_default_text_fallbacks(settings, TASK_RELEVANCE),
        ),
        concepts=LLMRoute.with_fallbacks(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallbacks=_default_text_fallbacks(settings, TASK_CONCEPTS),
        ),
        valence=LLMRoute.with_fallbacks(
            provider=PROVIDER_WORMSOFT,
            model=wormsoft_model,
            fallbacks=_default_text_fallbacks(settings, TASK_VALENCE),
        ),
        mcp_synthesis=LLMRoute.with_fallbacks(
            provider=PROVIDER_WORMSOFT,
            model=_wormsoft_synthesis_model(settings),
            fallbacks=_default_text_fallbacks(settings, TASK_MCP_SYNTHESIS),
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
                "fallbacks": [
                    {
                        "provider": PROVIDER_GIGACHAT,
                        "model": _gigachat_fallback_model(settings, task),
                    }
                ],
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
        {
            "id": PROVIDER_OPENROUTER,
            "label": "OpenRouter",
            "description": "Use OpenRouter as a low-cost fallback for text tasks.",
        },
        {
            "id": PROVIDER_POLZA,
            "label": "Polza",
            "description": "Use Polza as a paid RUB fallback before GigaChat.",
        },
    ]
