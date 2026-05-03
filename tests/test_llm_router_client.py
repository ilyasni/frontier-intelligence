from types import SimpleNamespace

import pytest

from worker.llm_router_client import LLMRouterClient
from worker.llm_types import GigaChatResponse


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

    def route_model_for_task(self, task: str, *, pro: bool = False, model_override: str | None = None) -> str:
        return model_override or "GigaChat-2"

    def route_fallback_for_task(self, task: str) -> tuple[str, str]:
        return ("gigachat", "GigaChat-2")

    async def count_tokens(self, model: str, text: str) -> int | None:
        return 1

    async def budget_text(self, text: str, model: str, token_budget: int):
        raise AssertionError("router budget_text should not call raw giga budget helper in this test")

    async def embed(self, text: str):
        return [0.1]

    async def vision(self, image_bytes: bytes, prompt: str = ""):
        return GigaChatResponse(content="{}", model="GigaChat-2-Pro", requested_model="GigaChat-2-Pro")

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
        wormsoft_api_key="secret",
    )


@pytest.mark.asyncio
async def test_llm_router_uses_wormsoft_for_relevance(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    giga = _FakeGiga()
    wormsoft = _FakeWormsoft(available=True)
    client = LLMRouterClient(gigachat_client=giga, wormsoft_client=wormsoft)

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "wormsoft"
    assert response.requested_model == "wormsoft/agent/medium"
    assert wormsoft.calls
    assert not giga.calls


@pytest.mark.asyncio
async def test_llm_router_falls_back_to_giga_when_wormsoft_unavailable(monkeypatch) -> None:
    monkeypatch.setattr("worker.llm_router_client.get_settings", _settings)
    giga = _FakeGiga()
    wormsoft = _FakeWormsoft(available=False)
    client = LLMRouterClient(gigachat_client=giga, wormsoft_client=wormsoft)

    response = await client.chat(system="s", user="u", task="relevance")

    assert response.provider == "gigachat"
    assert giga.calls
    assert giga.calls[0]["model_override"] == "GigaChat-2"
