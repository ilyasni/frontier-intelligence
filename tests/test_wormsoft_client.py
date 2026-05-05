from types import SimpleNamespace

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
