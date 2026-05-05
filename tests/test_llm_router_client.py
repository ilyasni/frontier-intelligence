from types import SimpleNamespace

import pytest

from worker.llm_router_client import LLMRouterClient
from worker.llm_types import GigaChatResponse
from worker.openrouter_client import OpenRouterVisionError


class _FakeGiga:
    def __init__(self):
        self.runtime_mode = "custom"
        self.calls = []

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

    async def embed(self, text: str):
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

    monkeypatch.setattr("worker.llm_router_client.pick_model", _fake_pick_model)
    monkeypatch.setattr("worker.llm_router_client.record_call_result", _fake_record_call_result)


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


def _settings():
    return SimpleNamespace(
        redis_url="redis://redis:6379",
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
    )


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
