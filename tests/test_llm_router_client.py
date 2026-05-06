import json
from types import SimpleNamespace

import pytest

from shared.llm_control_plane import (
    POLICY_MODE_MAINTENANCE,
    POLICY_MODE_SHADOW_EVAL,
    POLICY_MODE_STRICT,
    ROUTING_EVENTS_REDIS_KEY,
    RoutingCandidate,
    RoutingPolicyV2,
    TaskFamilyPolicy,
)
from worker.llm_router_client import LLMRouterClient
from worker.llm_types import GigaChatResponse
from worker.openrouter_client import OpenRouterVisionError
from worker.provider_budget_manager import ProviderBudgetManager


class _FakeGiga:
    def __init__(self):
        self.runtime_mode = "custom"
        self.calls = []
        self.embed_calls = []

    async def refresh_runtime_overrides(self, *, force: bool = False) -> None:
        return None

    def setting_value(self, name, default=None):
        return default

    def setting_bool(self, name, default=False):
        return default

    def setting_str(self, name, default=""):
        return default

    def setting_int(self, name, default=0):
        return default

    def route_model_for_task(
        self,
        task: str,
        *,
        pro: bool = False,
        model_override: str | None = None,
    ) -> str:
        return model_override or "GigaChat-2"

    def route_fallback_for_task(self, task: str) -> tuple[str, str]:
        return ("gigachat", "GigaChat-2")

    async def count_tokens(self, model: str, text: str) -> int | None:
        return 1

    async def budget_text(self, text: str, model: str, token_budget: int):
        raise AssertionError(
            "router budget_text should not call raw giga budget helper in this test"
        )

    async def embed(self, text: str, *, purpose: str = "document"):
        self.embed_calls.append({"text": text, "purpose": purpose})
        return [0.1]

    async def vision(self, image_bytes: bytes, prompt: str = ""):
        return GigaChatResponse(
            content="{}",
            model="GigaChat-2-Pro",
            requested_model="GigaChat-2-Pro",
        )

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        model = kwargs.get("model_override") or "GigaChat-2"
        return GigaChatResponse(
            content="{}",
            model=model,
            requested_model=model,
            provider="gigachat",
        )

    async def close(self) -> None:
        return None


class _FakeWormsoft:
    def __init__(self, *, available: bool, raise_error: bool = False):
        self._available = available
        self._raise_error = raise_error
        self.calls = []

    @property
    def is_available(self) -> bool:
        return self._available

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self._raise_error:
            raise RuntimeError("wormsoft_failed")
        model = kwargs.get("model_override") or "wormsoft/agent/medium"
        return GigaChatResponse(
            content="{}",
            model="gemma4:31b-cloud",
            requested_model=model,
            provider="wormsoft",
        )

    async def close(self) -> None:
        return None


class _FakeOpenRouter:
    def __init__(self, *, response: GigaChatResponse | None = None, exc: Exception | None = None):
        self._response = response or GigaChatResponse(
            content="{}",
            model="qwen/qwen2.5-vl-7b-instruct:free",
            requested_model="qwen/qwen2.5-vl-7b-instruct:free",
            provider="openrouter",
        )
        self._exc = exc
        self.calls = []

    @property
    def default_model(self) -> str:
        return "openrouter/free"

    async def vision(
        self,
        image_bytes: bytes,
        *,
        prompt: str = "",
        model_override: str | None = None,
    ):
        self.calls.append(
            {
                "size": len(image_bytes),
                "prompt": prompt,
                "model_override": model_override,
            }
        )
        if self._exc:
            raise self._exc
        if model_override:
            return GigaChatResponse(
                content="{}",
                model=model_override,
                requested_model=model_override,
                provider="openrouter",
            )
        return self._response

    async def close(self) -> None:
        return None


class _FakePolza:
    def __init__(self, *, available: bool = True):
        self._available = available
        self.calls = []

    @property
    def default_model(self) -> str:
        return "qwen3-vl-30b"

    @property
    def is_available(self) -> bool:
        return self._available

    async def vision(self, image_bytes: bytes, *, prompt: str = ""):
        self.calls.append({"size": len(image_bytes), "prompt": prompt})
        return GigaChatResponse(
            content="{}",
            model="qwen3-vl-30b",
            requested_model="qwen3-vl-30b",
            provider="polza",
        )

    async def close(self) -> None:
        return None


class _FakeOpenRouterText:
    def __init__(self, *, available: bool = True, raise_error: bool = False):
        self._available = available
        self._raise_error = raise_error
        self.calls = []

    @property
    def default_model(self) -> str:
        return "openrouter/free"

    @property
    def is_available(self) -> bool:
        return self._available

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self._raise_error:
            raise RuntimeError("openrouter_text_failed")
        model = kwargs.get("model_override") or self.default_model
        return GigaChatResponse(
            content="{}",
            model=model,
            requested_model=model,
            provider="openrouter",
        )

    async def close(self) -> None:
        return None


def _install_dynamic_openrouter(
    monkeypatch,
    *,
    model_id: str | None = "qwen/qwen2.5-7b-instruct:free",
):
    async def _fake_pick_model(
        task_family: str,
        *,
        force_refresh: bool = False,
        service_name: str = "worker",
    ):
        del force_refresh, service_name
        if model_id:
            return {
                "task_family": task_family,
                "model_id": model_id,
                "sticky_until": 9_999_999_999.0,
                "decided_at": 1_700_000_000.0,
                "candidates": [],
            }
        return {
            "task_family": task_family,
            "model_id": None,
            "reason": "no_capable_model",
            "decided_at": 1_700_000_000.0,
            "candidates": [],
        }

    async def _fake_record_call_result(*args, **kwargs):
        return {"status": "ok"}

    monkeypatch.setattr("worker.provider_adapters.pick_model", _fake_pick_model)
    monkeypatch.setattr("worker.provider_adapters.record_call_result", _fake_record_call_result)


def _policy_override(
    *,
    text_mode: str = "degraded",
    embeddings_mode: str = "strict",
    text_candidates: list[RoutingCandidate] | None = None,
    embeddings_candidates: list[RoutingCandidate] | None = None,
) -> RoutingPolicyV2:
    return RoutingPolicyV2(
        text_generation=TaskFamilyPolicy(
            family="text_generation",
            mode=text_mode,
            candidates=text_candidates
            or [
                RoutingCandidate(provider="wormsoft", model="wormsoft/agent/medium"),
                RoutingCandidate(provider="openrouter", model="openrouter/free"),
                RoutingCandidate(provider="polza", model="google/gemma-3-12b-it"),
                RoutingCandidate(provider="gigachat", model="GigaChat-2"),
            ],
        ),
        vision_generation=TaskFamilyPolicy(
            family="vision_generation",
            mode="degraded",
            candidates=[
                RoutingCandidate(provider="openrouter", model="openrouter/free"),
                RoutingCandidate(provider="polza", model="qwen3-vl-30b"),
                RoutingCandidate(provider="gigachat", model="GigaChat-2-Pro"),
            ],
        ),
        embeddings=TaskFamilyPolicy(
            family="embeddings",
            mode=embeddings_mode,
            candidates=embeddings_candidates
            or [RoutingCandidate(provider="gigachat", model="EmbeddingsGigaR")],
        ),
    )


class _FakePolzaText:
    def __init__(self, *, available: bool = True, raise_error: bool = False):
        self._available = available
        self._raise_error = raise_error
        self.calls = []

    @property
    def default_model(self) -> str:
        return "google/gemma-3-12b-it"

    @property
    def is_available(self) -> bool:
        return self._available

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self._raise_error:
            raise RuntimeError("polza_text_failed")
        model = kwargs.get("model_override") or self.default_model
        return GigaChatResponse(
            content="{}",
            model=model,
            requested_model=model,
            provider="polza",
        )

    async def close(self) -> None:
        return None


class _FakeGuard:
    def __init__(self, *responses: tuple[bool, str]):
        self._responses = list(responses) or [(True, "ok")]
        self.failures = []

    async def reserve_slot(self) -> tuple[bool, str]:
        if len(self._responses) > 1:
            return self._responses.pop(0)
        return self._responses[0]

    async def record_failure(
        self,
        *,
        status_code: int | None,
        reset_at: float | None = None,
    ) -> None:
        self.failures.append({"status_code": status_code, "reset_at": reset_at})

    async def close(self) -> None:
        return None


class _ExplodingGuard(_FakeGuard):
    async def record_failure(
        self,
        *,
        status_code: int | None,
        reset_at: float | None = None,
    ) -> None:
        raise RuntimeError("guard_write_failed")


class _FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.values = {}
        self.lists = {}

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hincrbyfloat(self, key, field, amount):
        bucket = self.hashes.setdefault(key, {})
        bucket[field] = float(bucket.get(field, 0.0) or 0.0) + float(amount)
        return bucket[field]

    async def hincrby(self, key, field, amount):
        bucket = self.hashes.setdefault(key, {})
        bucket[field] = int(bucket.get(field, 0) or 0) + int(amount)
        return bucket[field]

    async def hset(self, key, mapping=None, *args):
        bucket = self.hashes.setdefault(key, {})
        if mapping:
            bucket.update(mapping)
        elif len(args) == 2:
            bucket[args[0]] = args[1]
        return True

    async def expire(self, key, ttl):
        return True

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ex=None):
        self.values[key] = value
        return True

    async def delete(self, key):
        self.values.pop(key, None)
        self.hashes.pop(key, None)
        return 1

    async def incr(self, key):
        value = int(self.values.get(key, 0) or 0) + 1
        self.values[key] = value
        return value

    async def lpush(self, key, value):
        bucket = self.lists.setdefault(key, [])
        bucket.insert(0, value)
        return len(bucket)

    async def ltrim(self, key, start, stop):
        bucket = self.lists.setdefault(key, [])
        if stop >= 0:
            self.lists[key] = bucket[start : stop + 1]
        else:
            self.lists[key] = bucket[start:]
        return True


def _settings(embed_dim: int = 2560, **overrides):
    values = dict(
        redis_url="redis://redis:6379",
        embed_dim=embed_dim,
        gigachat_model="GigaChat-2",
        gigachat_model_lite="GigaChat-2",
        gigachat_model_pro="GigaChat-2-Pro",
        gigachat_model_max="GigaChat-2-Max",
        gigachat_model_relevance="GigaChat-2",
        gigachat_model_concepts="GigaChat-2",
        gigachat_model_valence="GigaChat-2",
        gigachat_model_mcp_synthesis="GigaChat-2-Pro",
        wormsoft_model_default="wormsoft/agent/medium",
        wormsoft_model_mcp_synthesis="wormsoft/agent/large",
        wormsoft_api_key="secret",
        openrouter_text_model="openrouter/free",
        openrouter_api_key="or-key",
        openrouter_referrer="https://frontier-intelligence.local",
        polza_text_model="google/gemma-3-12b-it",
        polza_synthesis_model="mistralai/mistral-small-3.1-24b-instruct",
        polza_api_key="polza-key",
        openrouter_free_rpd_soft_cap=850,
        llm_runtime_provider_openrouter_daily_request_soft_cap=0,
        llm_runtime_provider_openrouter_daily_request_limit=0,
        llm_runtime_shadow_daily_request_soft_cap=250,
        llm_runtime_shadow_daily_request_limit=0,
        llm_runtime_embeddings_daily_request_soft_cap=0,
        llm_runtime_embeddings_daily_request_limit=0,
        llm_runtime_provider_wormsoft_daily_request_soft_cap=0,
        llm_runtime_provider_wormsoft_daily_request_limit=0,
        llm_runtime_provider_polza_daily_request_soft_cap=0,
        llm_runtime_provider_polza_daily_request_limit=0,
        llm_runtime_provider_gigachat_daily_request_soft_cap=0,
        llm_runtime_provider_gigachat_daily_request_limit=0,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_llm_router_uses_wormsoft_for_relevance(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch)
    giga = _FakeGiga()
    wormsoft = _FakeWormsoft(available=True)
    client = LLMRouterClient(
        gigachat_client=giga,
        wormsoft_client=wormsoft,
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=False),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(available=False),
        openrouter_guard=_FakeGuard(),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "wormsoft"
    assert response.requested_model == "wormsoft/agent/medium"
    assert wormsoft.calls
    assert not giga.calls


@pytest.mark.asyncio
async def test_llm_router_falls_back_to_giga_when_wormsoft_unavailable(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch)
    giga = _FakeGiga()
    wormsoft = _FakeWormsoft(available=False)
    client = LLMRouterClient(
        gigachat_client=giga,
        wormsoft_client=wormsoft,
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=False),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(available=False),
        openrouter_guard=_FakeGuard(),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "openrouter"


@pytest.mark.asyncio
async def test_llm_router_falls_back_to_polza_when_openrouter_text_fails(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch)
    client = LLMRouterClient(
        gigachat_client=_FakeGiga(),
        wormsoft_client=_FakeWormsoft(available=False),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=False),
        openrouter_text_client=_FakeOpenRouterText(raise_error=True),
        polza_text_client=_FakePolzaText(available=True),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "polza"


@pytest.mark.asyncio
async def test_llm_router_falls_back_to_giga_when_polza_text_unavailable(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch, model_id=None)
    giga = _FakeGiga()
    client = LLMRouterClient(
        gigachat_client=giga,
        wormsoft_client=_FakeWormsoft(available=False),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=False),
        openrouter_text_client=_FakeOpenRouterText(available=False),
        polza_text_client=_FakePolzaText(available=False),
        openrouter_guard=_FakeGuard((False, "guard_rpd_soft_cap")),
        openrouter_text_guard=_FakeGuard((False, "guard_rpd_soft_cap")),
    )

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "gigachat"
    assert giga.calls
    assert giga.calls[0]["model_override"] == "GigaChat-2"


@pytest.mark.asyncio
async def test_llm_router_routes_mass_vision_to_openrouter(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch, model_id="qwen/qwen2.5-vl-7b-instruct:free")
    giga = _FakeGiga()
    client = LLMRouterClient(
        gigachat_client=giga,
        wormsoft_client=_FakeWormsoft(available=True),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.vision(b"\xff\xd8\xffjpeg", quality_tier="exploratory")

    assert response.provider == "openrouter"
    assert response.requested_model == "qwen/qwen2.5-vl-7b-instruct:free"
    assert client._openrouter.calls[0]["model_override"] == "qwen/qwen2.5-vl-7b-instruct:free"


@pytest.mark.asyncio
async def test_llm_router_falls_back_to_polza_on_openrouter_429(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch, model_id="qwen/qwen2.5-vl-7b-instruct:free")
    guard = _FakeGuard((True, "ok"))
    client = LLMRouterClient(
        gigachat_client=_FakeGiga(),
        wormsoft_client=_FakeWormsoft(available=True),
        openrouter_client=_FakeOpenRouter(
            exc=OpenRouterVisionError(
                "rate limited",
                status_code=429,
                reset_at=1_700_000_000.0,
                reason="openrouter_429",
            )
        ),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=guard,
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.vision(b"\xff\xd8\xffjpeg", quality_tier="research")

    assert response.provider == "polza"
    assert response.fallback_reason == "openrouter_429"


@pytest.mark.asyncio
async def test_llm_router_falls_back_to_giga_when_polza_disabled(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch, model_id=None)
    giga = _FakeGiga()
    client = LLMRouterClient(
        gigachat_client=giga,
        wormsoft_client=_FakeWormsoft(available=True),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=False),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(available=False),
        openrouter_guard=_FakeGuard((False, "guard_rpd_soft_cap")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.vision(b"\xff\xd8\xffjpeg", quality_tier="standard")

    assert response.provider == "gigachat"
    assert response.fallback_reason == "no_capable_model"


@pytest.mark.asyncio
async def test_llm_router_still_falls_back_when_guard_record_failure_breaks(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch, model_id="qwen/qwen2.5-vl-7b-instruct:free")
    client = LLMRouterClient(
        gigachat_client=_FakeGiga(),
        wormsoft_client=_FakeWormsoft(available=True),
        openrouter_client=_FakeOpenRouter(
            exc=OpenRouterVisionError(
                "rate limited",
                status_code=429,
                reset_at=1_700_000_000.0,
                reason="openrouter_429",
            )
        ),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=_ExplodingGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.vision(b"\xff\xd8\xffjpeg", quality_tier="exploratory")

    assert response.provider == "polza"
    assert response.fallback_reason == "openrouter_429"


@pytest.mark.asyncio
async def test_llm_router_embed_uses_control_plane_path(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch)
    giga = _FakeGiga()
    client = LLMRouterClient(
        gigachat_client=giga,
        wormsoft_client=_FakeWormsoft(available=True),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    vector = await client.embed("hello", purpose="query")

    assert vector == [0.1]
    assert giga.embed_calls[0]["purpose"] == "query"
    assert client.last_routing_decision is not None
    assert client.last_routing_decision.task_family == "embeddings"
    assert client.last_execution_receipt is not None
    assert client.last_execution_receipt.status == "ok"
    assert client.last_execution_receipt.actual_provider == "gigachat"


@pytest.mark.asyncio
async def test_llm_router_embed_strict_mode_does_not_fallback(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch)
    monkeypatch.setattr(
        LLMRouterClient,
        "routing_policy",
        property(
            lambda self: _policy_override(
                embeddings_mode=POLICY_MODE_STRICT,
                embeddings_candidates=[
                    RoutingCandidate(provider="openrouter", model="openrouter/free"),
                    RoutingCandidate(provider="gigachat", model="EmbeddingsGigaR"),
                ],
            )
        ),
    )
    client = LLMRouterClient(
        gigachat_client=_FakeGiga(),
        wormsoft_client=_FakeWormsoft(available=True),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    with pytest.raises(Exception):
        await client.embed("hello")

    assert client.last_routing_decision is not None
    assert client.last_routing_decision.mode == POLICY_MODE_STRICT
    assert len(client.last_routing_decision.considered_candidates) == 1
    assert client.last_execution_receipt is not None
    assert client.last_execution_receipt.actual_provider == "openrouter"


@pytest.mark.asyncio
async def test_llm_router_embed_rejects_incompatible_embedding_profile(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", lambda: _settings(embed_dim=1024))
    _install_dynamic_openrouter(monkeypatch)
    client = LLMRouterClient(
        gigachat_client=_FakeGiga(),
        wormsoft_client=_FakeWormsoft(available=True),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    with pytest.raises(RuntimeError, match="embedding_profile_mismatch_dim_2560_vs_1024"):
        await client.embed("hello")

    assert client.last_execution_receipt is not None
    assert "embedding_profile_mismatch" in client.last_execution_receipt.fallback_reason


@pytest.mark.asyncio
async def test_llm_router_maintenance_mode_prefers_gigachat(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch)
    monkeypatch.setattr(
        LLMRouterClient,
        "routing_policy",
        property(
            lambda self: _policy_override(
                text_mode=POLICY_MODE_MAINTENANCE,
                text_candidates=[
                    RoutingCandidate(provider="wormsoft", model="wormsoft/agent/medium"),
                    RoutingCandidate(provider="openrouter", model="openrouter/free"),
                    RoutingCandidate(provider="gigachat", model="GigaChat-2"),
                ],
            )
        ),
    )
    giga = _FakeGiga()
    wormsoft = _FakeWormsoft(available=True)
    client = LLMRouterClient(
        gigachat_client=giga,
        wormsoft_client=wormsoft,
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "gigachat"
    assert not wormsoft.calls
    assert giga.calls
    assert client.last_routing_decision is not None
    assert client.last_routing_decision.mode == POLICY_MODE_MAINTENANCE
    assert len(client.last_routing_decision.considered_candidates) == 1


@pytest.mark.asyncio
async def test_llm_router_shadow_eval_runs_alternative_chain_and_publishes_result(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    _install_dynamic_openrouter(monkeypatch)
    monkeypatch.setattr(
        LLMRouterClient,
        "routing_policy",
        property(
            lambda self: _policy_override(
                text_mode=POLICY_MODE_SHADOW_EVAL,
                text_candidates=[
                    RoutingCandidate(provider="wormsoft", model="wormsoft/agent/medium"),
                    RoutingCandidate(provider="gigachat", model="GigaChat-2"),
                ],
            )
        ),
    )
    redis = _FakeRedis()
    giga = _FakeGiga()
    wormsoft = _FakeWormsoft(available=True)
    client = LLMRouterClient(
        redis=redis,
        gigachat_client=giga,
        wormsoft_client=wormsoft,
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=True),
        openrouter_text_client=_FakeOpenRouterText(),
        polza_text_client=_FakePolzaText(),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.chat(system="s", user="u", task="relevance")
    await client.drain_shadow_evaluations()

    assert response.provider == "wormsoft"
    assert giga.calls
    events = [
        json.loads(item)
        for item in redis.lists.get(ROUTING_EVENTS_REDIS_KEY, [])
    ]
    shadow_finished = next(
        event for event in events if event.get("event") == "routing_shadow_execution_finished"
    )
    payload = shadow_finished["payload"]
    assert payload["primary_receipt"]["status"] == "ok"
    assert payload["shadow_receipt"]["execution_role"] == "shadow"
    assert payload["shadow_receipt"]["status"] == "ok"
    assert payload["shadow_receipt"]["actual_provider"] == "gigachat"
    assert payload["comparison"]["status_match"] is True
    assert payload["comparison"]["provider_match"] is False


@pytest.mark.asyncio
async def test_llm_router_skips_openrouter_when_runtime_budget_soft_cap_reached(monkeypatch) -> None:
    redis = _FakeRedis()
    settings = _settings(llm_runtime_provider_openrouter_daily_request_soft_cap=1)
    monkeypatch.setattr("worker.llm_router_client.get_settings", lambda: settings)
    _install_dynamic_openrouter(monkeypatch, model_id="openrouter/free")
    manager = ProviderBudgetManager(redis=redis, settings=settings)
    reservation = await manager.reserve(
        provider="openrouter",
        model="openrouter/free",
        task_family="text_generation",
    )
    await manager.commit(reservation, actual_units=1.0)
    client = LLMRouterClient(
        redis=redis,
        gigachat_client=_FakeGiga(),
        wormsoft_client=_FakeWormsoft(available=False),
        openrouter_client=_FakeOpenRouter(),
        polza_client=_FakePolza(available=False),
        openrouter_text_client=_FakeOpenRouterText(available=True),
        polza_text_client=_FakePolzaText(available=True),
        openrouter_guard=_FakeGuard((True, "ok")),
        openrouter_text_guard=_FakeGuard((True, "ok")),
    )

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "polza"
    assert client.last_routing_decision is not None
    assert any(
        item.get("reason") == "runtime_soft_cap:runtime_usage"
        for item in client.last_routing_decision.skipped_candidates
    )
