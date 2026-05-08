from types import SimpleNamespace

import pytest

from shared.llm_control_plane import ERROR_THROTTLED_LOCAL
from worker.wormsoft_client import WormsoftTextClient


class _FakeAsyncClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class _FakeOpenAIClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def close(self) -> None:
        return None


class _GuardAlwaysBlocked:
    async def reserve_slot(self):
        return False, "guard_interval"

    async def record_failure(self, **kwargs) -> None:
        return None

    async def close(self) -> None:
        return None


def test_wormsoft_client_uses_dedicated_pacing_settings(monkeypatch) -> None:
    settings = SimpleNamespace(
        wormsoft_api_base="https://ai.wormsoft.ru/api/gpt",
        wormsoft_api_key="secret",
        wormsoft_max_simultaneous_requests=2,
        wormsoft_min_request_interval_ms=3500,
        wormsoft_max_retries=0,
    )
    monkeypatch.setattr("worker.wormsoft_client.get_settings", lambda: settings)
    monkeypatch.setattr("worker.wormsoft_client.httpx.AsyncClient", _FakeAsyncClient)
    monkeypatch.setattr("worker.wormsoft_client.AsyncOpenAI", _FakeOpenAIClient)

    client = WormsoftTextClient()

    assert client._request_sem._value == 2
    assert client._min_request_interval_s == 3.5
    assert client._client.kwargs["max_retries"] == 0


@pytest.mark.asyncio
async def test_wormsoft_guard_interval_maps_to_local_throttle(monkeypatch) -> None:
    settings = SimpleNamespace(
        wormsoft_api_base="https://ai.wormsoft.ru/api/gpt",
        wormsoft_api_key="secret",
        wormsoft_max_simultaneous_requests=1,
        wormsoft_min_request_interval_ms=0,
        wormsoft_max_retries=0,
        wormsoft_shared_guard_enabled=True,
        wormsoft_read_timeout_sec=45.0,
        wormsoft_connect_timeout_sec=5.0,
        wormsoft_write_timeout_sec=45.0,
        wormsoft_pool_timeout_sec=10.0,
        wormsoft_max_connections=20,
        wormsoft_max_keepalive_connections=5,
    )
    monkeypatch.setattr("worker.wormsoft_client.get_settings", lambda: settings)
    monkeypatch.setattr("worker.wormsoft_client.httpx.AsyncClient", _FakeAsyncClient)
    monkeypatch.setattr("worker.wormsoft_client.AsyncOpenAI", _FakeOpenAIClient)

    client = WormsoftTextClient()
    client._guard = _GuardAlwaysBlocked()

    with pytest.raises(Exception) as exc:
        await client.chat(system="s", user="u", task="relevance", model_override="wormsoft/agent/medium")
    err = exc.value
    assert getattr(err, "category", None) == ERROR_THROTTLED_LOCAL
    assert getattr(err, "retryable", None) is True
