from types import SimpleNamespace

import pytest

from admin.backend.services.openrouter_health import probe_openrouter_health


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.store[key] = value


def _settings():
    return SimpleNamespace(
        openrouter_api_key="test-key",
        openrouter_health_probe_timeout_sec=1.0,
        openrouter_health_probe_max_tokens=1,
        openrouter_health_probe_batch_size=2,
    )


@pytest.mark.asyncio
async def test_probe_openrouter_health_rotates_cursor(monkeypatch) -> None:
    redis = _FakeRedis()
    seen: list[str] = []

    async def _fake_runtime_state(service_name: str = "admin"):
        return {
            "models": [{"model_id": "m1:free"}, {"model_id": "m2:free"}, {"model_id": "m3:free"}],
        }

    async def _fake_list_free_models():
        return [{"id": "m1:free"}, {"id": "m2:free"}, {"id": "m3:free"}]

    async def _fake_probe_model(client, model):
        seen.append(model["id"])
        return {"model_id": model["id"], "status": "ok"}

    class _DummyAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("admin.backend.services.openrouter_health.get_settings", _settings)
    monkeypatch.setattr("admin.backend.services.openrouter_health.get_client", lambda: redis)
    monkeypatch.setattr(
        "admin.backend.services.openrouter_health.fetch_openrouter_runtime_state",
        _fake_runtime_state,
    )
    monkeypatch.setattr(
        "admin.backend.services.openrouter_health.list_free_models",
        _fake_list_free_models,
    )
    monkeypatch.setattr("admin.backend.services.openrouter_health._probe_model", _fake_probe_model)
    monkeypatch.setattr(
        "admin.backend.services.openrouter_health.httpx.AsyncClient",
        lambda *args, **kwargs: _DummyAsyncClient(),
    )

    first = await probe_openrouter_health()
    second = await probe_openrouter_health()

    assert first["probed"] == 2
    assert second["probed"] == 2
    assert seen[:2] == ["m1:free", "m2:free"]
    assert seen[2:] == ["m3:free", "m1:free"]
