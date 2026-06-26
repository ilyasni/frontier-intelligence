from types import SimpleNamespace

import pytest

from shared.llm_control_plane import ProviderExecutionRequest
from worker.provider_adapters import OpenRouterAdapter


class _FakeRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}

    async def get(self, key: str):
        return self.store.get(key)


def _settings(*, fail_safe_enabled: bool = True):
    return SimpleNamespace(
        openrouter_api_key="test-key",
        openrouter_referrer="https://frontier-intelligence.local",
        openrouter_fail_safe_enabled=fail_safe_enabled,
        openrouter_fail_safe_stale_sec=1800,
    )


def _adapter(settings):
    return OpenRouterAdapter(
        service_name="worker",
        settings=settings,
        text_client=SimpleNamespace(is_available=True),
        vision_client=SimpleNamespace(is_available=True),
        text_guard=SimpleNamespace(),
        vision_guard=SimpleNamespace(),
    )


@pytest.mark.asyncio
async def test_openrouter_adapter_blocks_non_free_model(monkeypatch) -> None:
    adapter = _adapter(_settings(fail_safe_enabled=False))
    request = ProviderExecutionRequest(
        system="",
        user="u",
        task="concepts",
        task_family="text_generation",
        model="openai/gpt-4o",
    )

    resolved, meta = await adapter.resolve_model(request)

    assert resolved == ""
    assert meta["reason"] == "non_free_model_blocked"


@pytest.mark.asyncio
async def test_openrouter_adapter_fail_safe_blocks_stale_key(monkeypatch) -> None:
    stale_key = '{"status":"stale_request_error","fetched_at":1}'
    good_catalog = '{"status":"ok","fetched_at":9999999999,"model_count":3}'
    redis = _FakeRedis(
        {
            "admin:openrouter_key:last_ok": stale_key,
            "or:catalog:snapshot": good_catalog,
        }
    )
    monkeypatch.setattr("worker.provider_adapters.get_client", lambda: redis)
    async def _unexpected_pick_model(*args, **kwargs):
        raise AssertionError("pick_model should not be called")

    monkeypatch.setattr("worker.provider_adapters.pick_model", _unexpected_pick_model)

    adapter = _adapter(_settings(fail_safe_enabled=True))
    request = ProviderExecutionRequest(
        system="",
        user="u",
        task="vision",
        task_family="vision_generation",
        model="openrouter/free",
    )

    resolved, meta = await adapter.resolve_model(request)

    assert resolved == ""
    assert meta["reason"] == "openrouter_fail_safe_key_stale"


@pytest.mark.asyncio
async def test_openrouter_adapter_reserve_capacity_free_noop() -> None:
    adapter = _adapter(_settings(fail_safe_enabled=False))

    allowed, reason = await adapter.reserve_capacity(
        task_family="vision_generation",
        model="qwen/qwen2.5-vl-7b-instruct:free",
    )

    assert allowed is True
    assert reason == "ok"
