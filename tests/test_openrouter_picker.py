from types import SimpleNamespace

import pytest

from admin.backend.services.openrouter_picker import (
    DECISION_KEY,
    HEALTH_KEY,
    pick_model,
    record_call_result,
    task_family_for_task,
)


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.expiry: dict[str, int] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.store[key] = value
        if ex is not None:
            self.expiry[key] = ex

    async def incr(self, key: str):
        value = int(self.store.get(key, "0") or 0) + 1
        self.store[key] = str(value)
        return value

    async def expire(self, key: str, ttl: int):
        self.expiry[key] = ttl

    async def delete(self, key: str):
        self.store.pop(key, None)
        self.hashes.pop(key, None)

    async def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    async def hset(self, key: str, mapping: dict[str, object]):
        payload = self.hashes.setdefault(key, {})
        for name, value in mapping.items():
            payload[str(name)] = str(value)


def _settings():
    return SimpleNamespace(
        openrouter_free_rpm_throttle=18,
        openrouter_free_rpd_soft_cap=850,
        openrouter_free_quarantine_5xx_sec=900,
        openrouter_picker_sticky_sec=600,
        openrouter_rpd_safety_buffer=0.1,
        openrouter_quarantine_5xx_threshold=3,
        openrouter_quarantine_5xx_window_sec=300,
    )


def _structured_models():
    return [
        {
            "id": "qwen/qwen2.5-7b-instruct:free",
            "context_length": 32768,
            "supports_vision": False,
            "supports_structured": True,
            "supports_tools": False,
        }
    ]


async def _list_structured_models():
    return _structured_models()


@pytest.mark.asyncio
async def test_task_family_mapping() -> None:
    assert task_family_for_task("relevance_concepts") == "text_structured"
    assert task_family_for_task("mcp_synthesis") == "text_tools"
    assert task_family_for_task("valence") == "text_basic"


@pytest.mark.asyncio
async def test_picker_reuses_sticky_decision(monkeypatch) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_settings", _settings)
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_client", lambda: redis)
    monkeypatch.setattr(
        "admin.backend.services.openrouter_picker.list_free_models",
        _list_structured_models,
    )

    first = await pick_model("text_structured", service_name="worker")
    second = await pick_model("text_structured", service_name="worker")

    assert first["model_id"] == "qwen/qwen2.5-7b-instruct:free"
    assert second["model_id"] == "qwen/qwen2.5-7b-instruct:free"
    assert second["source"] == "sticky"
    assert DECISION_KEY.format(task_family="text_structured") in redis.store


@pytest.mark.asyncio
async def test_picker_skips_quarantined_model(monkeypatch) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_settings", _settings)
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_client", lambda: redis)
    monkeypatch.setattr(
        "admin.backend.services.openrouter_picker.list_free_models",
        _list_structured_models,
    )

    await record_call_result(
        "qwen/qwen2.5-7b-instruct:free",
        success=False,
        latency_ms=100,
        status_code=429,
        or_reset_at=time_future(),
        service_name="worker",
    )
    decision = await pick_model("text_structured", service_name="worker")

    assert decision["model_id"] is None
    assert decision["reason"] == "all_quarantined"


@pytest.mark.asyncio
async def test_picker_skips_near_cap(monkeypatch) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_settings", _settings)
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_client", lambda: redis)
    monkeypatch.setattr(
        "admin.backend.services.openrouter_picker.list_free_models",
        _list_structured_models,
    )
    redis.store["or:rpd:qwen/qwen2.5-7b-instruct:free:20990101"] = "800"
    monkeypatch.setattr(
        "admin.backend.services.openrouter_picker._utc_date_key",
        lambda: "20990101",
    )

    decision = await pick_model("text_structured", service_name="worker")

    assert decision["model_id"] is None
    assert decision["reason"] == "near_cap"


@pytest.mark.asyncio
async def test_record_call_result_writes_health_hash(monkeypatch) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_settings", _settings)
    monkeypatch.setattr("admin.backend.services.openrouter_picker.get_client", lambda: redis)

    await record_call_result(
        "qwen/qwen2.5-7b-instruct:free",
        success=True,
        latency_ms=120.0,
        status_code=200,
        service_name="worker",
    )

    health_key = HEALTH_KEY.format(model_id="qwen/qwen2.5-7b-instruct:free")
    assert redis.hashes[health_key]["success"] == "1"
    assert redis.hashes[health_key]["last_status_code"] == "200"


def time_future() -> float:
    import time

    return time.time() + 600.0
