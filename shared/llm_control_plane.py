"""Shared control-plane models and helpers for multi-provider LLM routing."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal, Protocol

from pydantic import BaseModel, Field, field_validator

from shared.embedding_models import embedding_profile
from shared.llm_routing import (
    DEFAULT_OPENROUTER_TEXT_MODEL,
    DEFAULT_POLZA_SYNTHESIS_MODEL,
    DEFAULT_POLZA_TEXT_MODEL,
    DEFAULT_WORMSOFT_SYNTHESIS_MODEL,
    DEFAULT_WORMSOFT_TEXT_MODEL,
    PROVIDER_GIGACHAT,
    PROVIDER_OPENROUTER,
    PROVIDER_POLZA,
    PROVIDER_WORMSOFT,
    ROUTABLE_LLM_TASKS,
    TASK_CHAT,
    TASK_CONCEPTS,
    TASK_MCP_SYNTHESIS,
    TASK_RELEVANCE,
    TASK_RELEVANCE_CONCEPTS,
    TASK_VALENCE,
    effective_llm_routing,
    normalize_provider,
)

if TYPE_CHECKING:
    from worker.llm_types import GigaChatResponse


CONTROL_PLANE_POLICY_DB_KEY = "llm_control_plane_policy_v2"
CONTROL_PLANE_POLICY_REDIS_KEY = "frontier:runtime:llm_control_plane_policy_v2"
ROUTING_EVENTS_REDIS_KEY = "frontier:runtime:routing_events"
ROUTING_EVENTS_MAXLEN = 250

TASK_FAMILY_TEXT_GENERATION = "text_generation"
TASK_FAMILY_VISION_GENERATION = "vision_generation"
TASK_FAMILY_EMBEDDINGS = "embeddings"
TASK_FAMILY_UNKNOWN = "unknown"
TASK_FAMILIES = (
    TASK_FAMILY_TEXT_GENERATION,
    TASK_FAMILY_VISION_GENERATION,
    TASK_FAMILY_EMBEDDINGS,
)

POLICY_MODE_STRICT = "strict"
POLICY_MODE_DEGRADED = "degraded"
POLICY_MODE_MAINTENANCE = "maintenance"
POLICY_MODE_SHADOW_EVAL = "shadow-eval"
POLICY_MODES = (
    POLICY_MODE_STRICT,
    POLICY_MODE_DEGRADED,
    POLICY_MODE_MAINTENANCE,
    POLICY_MODE_SHADOW_EVAL,
)

ERROR_AUTH = "auth"
ERROR_BAD_REQUEST = "bad_request"
ERROR_MODEL_NOT_FOUND = "model_not_found"
ERROR_RATE_LIMITED = "rate_limited"
ERROR_QUOTA_EXHAUSTED = "quota_exhausted"
ERROR_TIMEOUT = "timeout"
ERROR_CONNECTION = "connection"
ERROR_UPSTREAM_5XX = "upstream_5xx"
ERROR_PROVIDER_UNAVAILABLE = "provider_unavailable"
ERROR_UNKNOWN = "unknown"
ERROR_CATEGORIES = (
    ERROR_AUTH,
    ERROR_BAD_REQUEST,
    ERROR_MODEL_NOT_FOUND,
    ERROR_RATE_LIMITED,
    ERROR_QUOTA_EXHAUSTED,
    ERROR_TIMEOUT,
    ERROR_CONNECTION,
    ERROR_UPSTREAM_5XX,
    ERROR_PROVIDER_UNAVAILABLE,
    ERROR_UNKNOWN,
)

CIRCUIT_STATE_CLOSED = "closed"
CIRCUIT_STATE_OPEN = "open"
CIRCUIT_LEVEL_PROVIDER = "provider"
CIRCUIT_LEVEL_MODEL = "model"
EXECUTION_ROLE_PRIMARY = "primary"
EXECUTION_ROLE_SHADOW = "shadow"


def normalize_task_family(value: Any) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "text": TASK_FAMILY_TEXT_GENERATION,
        "generation": TASK_FAMILY_TEXT_GENERATION,
        "text_generation": TASK_FAMILY_TEXT_GENERATION,
        "vision": TASK_FAMILY_VISION_GENERATION,
        "vision_generation": TASK_FAMILY_VISION_GENERATION,
        "image": TASK_FAMILY_VISION_GENERATION,
        "embeddings": TASK_FAMILY_EMBEDDINGS,
        "embedding": TASK_FAMILY_EMBEDDINGS,
        "embed": TASK_FAMILY_EMBEDDINGS,
    }
    normalized = aliases.get(raw, raw)
    if normalized in TASK_FAMILIES:
        return normalized
    return TASK_FAMILY_UNKNOWN


def task_family_for_task(task: str) -> str:
    normalized = str(task or "").strip().lower()
    if normalized in ROUTABLE_LLM_TASKS or normalized == TASK_CHAT:
        return TASK_FAMILY_TEXT_GENERATION
    if normalized in {"vision", "image", "ocr"}:
        return TASK_FAMILY_VISION_GENERATION
    if normalized in {"embed", "embedding", "embeddings"}:
        return TASK_FAMILY_EMBEDDINGS
    return TASK_FAMILY_UNKNOWN


def now_ts() -> float:
    return time.time()


def normalize_policy_mode(value: Any) -> str:
    raw = str(value or POLICY_MODE_DEGRADED).strip().lower()
    return raw if raw in POLICY_MODES else POLICY_MODE_DEGRADED


class ProviderError(BaseModel):
    provider: str
    category: str = ERROR_UNKNOWN
    message: str = ""
    status_code: int | None = None
    retryable: bool = False
    reset_at: float | None = None
    reason: str = ""

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)

    @field_validator("category", mode="before")
    @classmethod
    def _validate_category(cls, value: Any) -> str:
        raw = str(value or ERROR_UNKNOWN).strip().lower()
        return raw if raw in ERROR_CATEGORIES else ERROR_UNKNOWN


class CircuitState(BaseModel):
    level: Literal["provider", "model"] = CIRCUIT_LEVEL_PROVIDER
    provider: str
    model: str = ""
    state: Literal["open", "closed"] = CIRCUIT_STATE_CLOSED
    opened_at: float | None = None
    opened_until: float | None = None
    reason: str = ""
    failure_count: int = 0
    last_error_category: str = ERROR_UNKNOWN

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)

    @field_validator("last_error_category", mode="before")
    @classmethod
    def _validate_last_error_category(cls, value: Any) -> str:
        raw = str(value or ERROR_UNKNOWN).strip().lower()
        return raw if raw in ERROR_CATEGORIES else ERROR_UNKNOWN


class BudgetWindowState(BaseModel):
    provider: str
    scope: str = "provider"
    window_label: str = ""
    limit: float | None = None
    remaining: float | None = None
    used: float | None = None
    reserved: float | None = None
    committed: float | None = None
    released: float | None = None
    outstanding: float | None = None
    unit: str = "credits"
    status: str = "unknown"
    refreshed_at: float | None = None

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)


class ModelCapabilitySnapshot(BaseModel):
    provider: str
    model: str
    modalities: list[str] = Field(default_factory=list)
    input_modalities: list[str] = Field(default_factory=list)
    output_modalities: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    supports_text_generation: bool = False
    supports_vision_generation: bool = False
    supports_embeddings: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)


class ModelCatalogSnapshot(BaseModel):
    provider: str
    available: bool = False
    fetched_at: float | None = None
    models: list[ModelCapabilitySnapshot] = Field(default_factory=list)
    source: str = ""
    status: str = "unknown"

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)


class ProviderStateSnapshot(BaseModel):
    provider: str
    available: bool = False
    ready: bool = False
    health_status: str = "unknown"
    latency_ms: float | None = None
    key_present: bool = False
    quota_pressure: str = "unknown"
    subscriptions: list[dict[str, Any]] = Field(default_factory=list)
    budgets: list[BudgetWindowState] = Field(default_factory=list)
    circuits: list[CircuitState] = Field(default_factory=list)
    catalog: ModelCatalogSnapshot | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)


class RoutingCandidate(BaseModel):
    provider: str
    model: str
    enabled: bool = True
    capability_tags: list[str] = Field(default_factory=list)
    budget_class: str = ""
    notes: str = ""

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)

    @field_validator("model", mode="before")
    @classmethod
    def _validate_model(cls, value: Any) -> str:
        return str(value or "").strip()


class TaskFamilyPolicy(BaseModel):
    family: str
    mode: str = POLICY_MODE_DEGRADED
    fallback_exception_only: bool = True
    candidates: list[RoutingCandidate] = Field(default_factory=list)

    @field_validator("family", mode="before")
    @classmethod
    def _validate_family(cls, value: Any) -> str:
        normalized = normalize_task_family(value)
        return normalized if normalized != TASK_FAMILY_UNKNOWN else TASK_FAMILY_TEXT_GENERATION

    @field_validator("mode", mode="before")
    @classmethod
    def _validate_mode(cls, value: Any) -> str:
        return normalize_policy_mode(value)


class RoutingPolicyV2(BaseModel):
    version: Literal["v2"] = "v2"
    default_mode: str = POLICY_MODE_DEGRADED
    text_generation: TaskFamilyPolicy
    vision_generation: TaskFamilyPolicy
    embeddings: TaskFamilyPolicy
    updated_at: float = Field(default_factory=now_ts)

    @field_validator("default_mode", mode="before")
    @classmethod
    def _validate_default_mode(cls, value: Any) -> str:
        return normalize_policy_mode(value)

    def family_policy(self, family: str) -> TaskFamilyPolicy:
        normalized = normalize_task_family(family)
        if normalized == TASK_FAMILY_VISION_GENERATION:
            return self.vision_generation
        if normalized == TASK_FAMILY_EMBEDDINGS:
            return self.embeddings
        return self.text_generation


class AttemptOutcome(BaseModel):
    provider: str
    model: str
    status: str
    reason: str = ""
    actual_model: str = ""

    @field_validator("provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)


class RoutingDecision(BaseModel):
    task: str
    task_family: str
    mode: str = POLICY_MODE_DEGRADED
    requested_provider: str
    requested_model: str
    selected_provider: str
    selected_model: str
    fallback_allowed: bool = True
    fallback_exception_only: bool = True
    considered_candidates: list[RoutingCandidate] = Field(default_factory=list)
    skipped_candidates: list[dict[str, Any]] = Field(default_factory=list)
    decided_at: float = Field(default_factory=now_ts)
    workspace_id: str = ""
    budget_class: str = ""

    @field_validator("task_family", mode="before")
    @classmethod
    def _validate_task_family(cls, value: Any) -> str:
        normalized = normalize_task_family(value)
        return normalized if normalized != TASK_FAMILY_UNKNOWN else TASK_FAMILY_TEXT_GENERATION

    @field_validator("requested_provider", "selected_provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)


class ExecutionReceipt(BaseModel):
    task: str
    task_family: str
    status: str
    execution_role: str = EXECUTION_ROLE_PRIMARY
    shadow_group_id: str = ""
    requested_provider: str
    requested_model: str
    actual_provider: str
    actual_model: str
    fallback_reason: str = ""
    latency_ms: float = 0.0
    cost_estimate: float | None = None
    budget_attribution: dict[str, Any] = Field(default_factory=dict)
    decision: RoutingDecision | None = None
    attempts: list[AttemptOutcome] = Field(default_factory=list)
    provider_error: ProviderError | None = None
    completed_at: float = Field(default_factory=now_ts)

    @field_validator("task_family", mode="before")
    @classmethod
    def _validate_task_family(cls, value: Any) -> str:
        normalized = normalize_task_family(value)
        return normalized if normalized != TASK_FAMILY_UNKNOWN else TASK_FAMILY_TEXT_GENERATION

    @field_validator("requested_provider", "actual_provider", mode="before")
    @classmethod
    def _validate_provider(cls, value: Any) -> str:
        return normalize_provider(value)

    @field_validator("execution_role", mode="before")
    @classmethod
    def _validate_execution_role(cls, value: Any) -> str:
        raw = str(value or EXECUTION_ROLE_PRIMARY).strip().lower()
        if raw in {EXECUTION_ROLE_PRIMARY, EXECUTION_ROLE_SHADOW}:
            return raw
        return EXECUTION_ROLE_PRIMARY


class ShadowComparison(BaseModel):
    status_match: bool
    provider_match: bool
    same_model: bool
    latency_delta_ms: float | None = None
    cost_delta: float | None = None
    prompt_tokens_delta: int | None = None
    completion_tokens_delta: int | None = None
    billable_tokens_delta: int | None = None


class ShadowEvaluationResult(BaseModel):
    group_id: str
    primary_receipt: ExecutionReceipt
    shadow_receipt: ExecutionReceipt
    comparison: ShadowComparison


class ProviderExecutionRequest(BaseModel):
    system: str
    user: str
    task: str
    task_family: str
    model: str
    max_tokens: int = 1024
    pro: bool = False
    prompt: str = ""
    text: str = ""
    image_bytes: bytes | None = None
    quality_tier: str = "standard"
    embedding_purpose: str = "document"
    workspace_id: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("task_family", mode="before")
    @classmethod
    def _validate_task_family(cls, value: Any) -> str:
        normalized = normalize_task_family(value)
        return normalized if normalized != TASK_FAMILY_UNKNOWN else TASK_FAMILY_TEXT_GENERATION


class ProviderAdapter(Protocol):
    provider_name: str

    async def discover_models(self) -> ModelCatalogSnapshot: ...

    async def check_health(self) -> ProviderStateSnapshot: ...

    async def reserve_capacity(
        self,
        *,
        task_family: str,
        model: str,
    ) -> tuple[bool, str]: ...

    async def resolve_model(
        self,
        request: ProviderExecutionRequest,
    ) -> tuple[str, dict[str, Any]]: ...

    async def execute(self, request: ProviderExecutionRequest) -> GigaChatResponse: ...

    async def vision(self, request: ProviderExecutionRequest) -> GigaChatResponse: ...

    async def embed(self, request: ProviderExecutionRequest) -> list[float]: ...

    async def record_result(
        self,
        *,
        receipt: ExecutionReceipt | None,
        error: ProviderError | None = None,
    ) -> None: ...

    def normalize_error(self, exc: Exception, *, model: str) -> ProviderError: ...

    def estimate_cost(
        self,
        *,
        response: GigaChatResponse | None,
        requested_model: str,
        actual_model: str,
    ) -> float | None: ...


def default_routing_policy_v2(
    settings: Any,
    runtime_mode: str | None = None,
    legacy_payload: dict[str, Any] | None = None,
) -> RoutingPolicyV2:
    effective = effective_llm_routing(settings, runtime_mode, legacy_payload)
    text_candidates: list[RoutingCandidate] = []
    seen: set[tuple[str, str]] = set()
    for task in (
        TASK_RELEVANCE_CONCEPTS,
        TASK_RELEVANCE,
        TASK_CONCEPTS,
        TASK_VALENCE,
        TASK_MCP_SYNTHESIS,
    ):
        route = effective.route_for_task(task)
        pairs = [(route.provider, route.model), *route.fallback_chain()]
        for provider, model in pairs:
            pair = (normalize_provider(provider), str(model or "").strip())
            if not pair[1] or pair in seen:
                continue
            seen.add(pair)
            text_candidates.append(
                RoutingCandidate(
                    provider=pair[0],
                    model=pair[1],
                    capability_tags=["text"],
                    budget_class="default",
                )
            )

    vision_candidates = [
        RoutingCandidate(
            provider=PROVIDER_OPENROUTER,
            model=str(getattr(settings, "openrouter_vision_model", "") or DEFAULT_OPENROUTER_TEXT_MODEL),
            capability_tags=["vision", "text"],
            budget_class="exploratory",
        ),
        RoutingCandidate(
            provider=PROVIDER_GIGACHAT,
            model=str(
                getattr(settings, "gigachat_model_vision", "")
                or getattr(settings, "gigachat_model_pro", "GigaChat-2-Pro")
            ).strip(),
            capability_tags=["vision", "text"],
            budget_class="trusted",
        ),
    ]
    polza_vision = str(getattr(settings, "polza_vision_model", "") or "").strip()
    if polza_vision:
        vision_candidates.append(
            RoutingCandidate(
                provider=PROVIDER_POLZA,
                model=polza_vision,
                capability_tags=["vision", "text"],
                budget_class="fallback",
            )
        )

    embeddings_candidates = [
        RoutingCandidate(
            provider=PROVIDER_GIGACHAT,
            model=str(
                getattr(settings, "gigachat_embeddings_model", "EmbeddingsGigaR")
                or "EmbeddingsGigaR"
            ).strip(),
            capability_tags=["embeddings"],
            budget_class="core",
        )
    ]

    return RoutingPolicyV2(
        default_mode=POLICY_MODE_DEGRADED,
        text_generation=TaskFamilyPolicy(
            family=TASK_FAMILY_TEXT_GENERATION,
            mode=POLICY_MODE_DEGRADED,
            fallback_exception_only=True,
            candidates=text_candidates
            or [
                RoutingCandidate(
                    provider=PROVIDER_WORMSOFT,
                    model=DEFAULT_WORMSOFT_TEXT_MODEL,
                    capability_tags=["text"],
                ),
                RoutingCandidate(
                    provider=PROVIDER_OPENROUTER,
                    model=DEFAULT_OPENROUTER_TEXT_MODEL,
                    capability_tags=["text"],
                ),
                RoutingCandidate(
                    provider=PROVIDER_POLZA,
                    model=DEFAULT_POLZA_TEXT_MODEL,
                    capability_tags=["text"],
                ),
                RoutingCandidate(
                    provider=PROVIDER_GIGACHAT,
                    model="GigaChat-2",
                    capability_tags=["text"],
                ),
            ],
        ),
        vision_generation=TaskFamilyPolicy(
            family=TASK_FAMILY_VISION_GENERATION,
            mode=POLICY_MODE_DEGRADED,
            fallback_exception_only=True,
            candidates=[candidate for candidate in vision_candidates if candidate.model],
        ),
        embeddings=TaskFamilyPolicy(
            family=TASK_FAMILY_EMBEDDINGS,
            mode=POLICY_MODE_STRICT,
            fallback_exception_only=True,
            candidates=embeddings_candidates,
        ),
    )


def policy_candidates_for_task(
    policy: RoutingPolicyV2,
    *,
    task: str,
    task_family: str | None = None,
) -> list[RoutingCandidate]:
    family = normalize_task_family(task_family or task_family_for_task(task))
    return list(policy.family_policy(family).candidates)


def model_supports_family(model: ModelCapabilitySnapshot, family: str) -> bool:
    normalized = normalize_task_family(family)
    if normalized == TASK_FAMILY_TEXT_GENERATION:
        return bool(model.supports_text_generation)
    if normalized == TASK_FAMILY_VISION_GENERATION:
        return bool(model.supports_vision_generation)
    if normalized == TASK_FAMILY_EMBEDDINGS:
        return bool(model.supports_embeddings)
    return True


def fallback_allowed_for_mode(mode: str, candidate_count: int) -> bool:
    normalized = normalize_policy_mode(mode)
    if normalized in {POLICY_MODE_STRICT, POLICY_MODE_MAINTENANCE}:
        return False
    return candidate_count > 1


def apply_execution_mode(
    candidates: list[RoutingCandidate],
    *,
    mode: str,
    task_family: str,
) -> list[RoutingCandidate]:
    normalized_mode = normalize_policy_mode(mode)
    normalized_family = normalize_task_family(task_family)
    enabled = [candidate for candidate in candidates if candidate.enabled]
    if not enabled:
        return []
    if normalized_mode == POLICY_MODE_STRICT:
        return enabled[:1]
    if normalized_mode == POLICY_MODE_MAINTENANCE:
        preferred_provider = PROVIDER_GIGACHAT
        if normalized_family == TASK_FAMILY_VISION_GENERATION:
            preferred_provider = PROVIDER_GIGACHAT
        elif normalized_family == TASK_FAMILY_EMBEDDINGS:
            preferred_provider = PROVIDER_GIGACHAT
        for candidate in enabled:
            if candidate.provider == preferred_provider:
                return [candidate]
        return enabled[:1]
    return enabled


def embedding_profile_for_model(model: str) -> dict[str, Any]:
    return embedding_profile(model)


def simulate_routing_decision(
    policy: RoutingPolicyV2,
    *,
    task: str,
    task_family: str | None = None,
    provider_states: dict[str, ProviderStateSnapshot] | None = None,
    circuits: list[CircuitState] | None = None,
    mode: str | None = None,
) -> RoutingDecision:
    family = normalize_task_family(task_family or task_family_for_task(task))
    family_policy = policy.family_policy(family)
    effective_mode = normalize_policy_mode(mode or family_policy.mode or policy.default_mode)
    states = provider_states or {}
    circuit_index: dict[tuple[str, str], CircuitState] = {}
    provider_circuit_index: dict[str, CircuitState] = {}
    for circuit in circuits or []:
        if circuit.level == CIRCUIT_LEVEL_PROVIDER:
            provider_circuit_index[circuit.provider] = circuit
        else:
            circuit_index[(circuit.provider, circuit.model)] = circuit

    considered = apply_execution_mode(
        list(family_policy.candidates),
        mode=effective_mode,
        task_family=family,
    )
    skipped: list[dict[str, Any]] = []
    selected = considered[0] if considered else RoutingCandidate(provider=PROVIDER_GIGACHAT, model="")
    for candidate in considered:
        state = states.get(candidate.provider)
        if state and state.catalog and state.catalog.models:
            matching_model = next(
                (
                    item
                    for item in state.catalog.models
                    if item.model == candidate.model or candidate.model == "openrouter/free"
                ),
                None,
            )
            if matching_model is not None and not model_supports_family(matching_model, family):
                skipped.append(
                    {
                        "provider": candidate.provider,
                        "model": candidate.model,
                        "reason": "capability_mismatch",
                    }
                )
                continue
        provider_circuit = provider_circuit_index.get(candidate.provider)
        model_circuit = circuit_index.get((candidate.provider, candidate.model))
        if state and not state.ready:
            skipped.append(
                {
                    "provider": candidate.provider,
                    "model": candidate.model,
                    "reason": "provider_not_ready",
                }
            )
            continue
        if provider_circuit and provider_circuit.state == CIRCUIT_STATE_OPEN:
            skipped.append(
                {
                    "provider": candidate.provider,
                    "model": candidate.model,
                    "reason": provider_circuit.reason or "provider_circuit_open",
                }
            )
            continue
        if model_circuit and model_circuit.state == CIRCUIT_STATE_OPEN:
            skipped.append(
                {
                    "provider": candidate.provider,
                    "model": candidate.model,
                    "reason": model_circuit.reason or "model_circuit_open",
                }
            )
            continue
        selected = candidate
        break

    requested = considered[0] if considered else selected
    return RoutingDecision(
        task=task,
        task_family=family,
        mode=effective_mode,
        requested_provider=requested.provider,
        requested_model=requested.model,
        selected_provider=selected.provider,
        selected_model=selected.model,
        fallback_allowed=fallback_allowed_for_mode(effective_mode, len(considered)),
        fallback_exception_only=family_policy.fallback_exception_only,
        considered_candidates=considered,
        skipped_candidates=skipped,
    )


def normalize_wormsoft_model_snapshot(payload: dict[str, Any]) -> list[ModelCapabilitySnapshot]:
    items = payload.get("models")
    if not isinstance(items, list):
        items = payload.get("data")
    if not isinstance(items, list):
        items = []

    normalized: list[ModelCapabilitySnapshot] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        model_id = str(item.get("id") or item.get("name") or item.get("label") or "").strip()
        if not model_id:
            continue
        modalities = [str(value).strip().lower() for value in (item.get("modalities") or []) if str(value).strip()]
        input_modalities = [
            str(value).strip().lower()
            for value in (item.get("input_modalities") or [])
            if str(value).strip()
        ]
        output_modalities = [
            str(value).strip().lower()
            for value in (item.get("output_modalities") or [])
            if str(value).strip()
        ]
        capabilities = [
            str(value).strip().lower()
            for value in (item.get("capabilities") or [])
            if str(value).strip()
        ]
        tags = set(modalities + input_modalities + output_modalities + capabilities)
        supports_embeddings = "embedding" in tags or "embeddings" in tags
        supports_vision = "image" in tags or "vision" in tags
        supports_text = (
            "text" in tags
            or not supports_embeddings
            or model_id.startswith("wormsoft/agent/")
        )
        normalized.append(
            ModelCapabilitySnapshot(
                provider=PROVIDER_WORMSOFT,
                model=model_id,
                modalities=modalities,
                input_modalities=input_modalities,
                output_modalities=output_modalities,
                capabilities=capabilities,
                supports_text_generation=supports_text,
                supports_vision_generation=supports_vision,
                supports_embeddings=supports_embeddings,
                metadata={
                    "source_payload": {
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "type": item.get("type"),
                    }
                },
            )
        )
    return normalized
