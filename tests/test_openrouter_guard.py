from types import SimpleNamespace

import pytest

from worker.openrouter_guard import QUARANTINE_KEY, RPD_KEY, RPM_KEY, OpenRouterVisionGuard


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.expiry = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.store[key] = value
        if ex is not None:
            self.expiry[key] = ex

    async def incr(self, key: str):
        value = int(self.store.get(key, 0) or 0) + 1
        self.store[key] = str(value)
        return value

    async def expire(self, key: str, ttl: int):
        self.expiry[key] = ttl


def _settings():
    return SimpleNamespace(
        redis_url="redis://redis:6379",
        openrouter_free_rpm_throttle=18,
        openrouter_free_rpd_soft_cap=850,
        openrouter_free_quarantine_5xx_sec=900,
    )


@pytest.mark.asyncio
async def test_openrouter_guard_reserves_slot_below_caps(monkeypatch) -> None:
    monkeypatch.setattr("worker.openrouter_guard.get_settings", _settings)
    redis = _FakeRedis()
    guard = OpenRouterVisionGuard(redis=redis)

    allowed, reason = await guard.reserve_slot()

    assert allowed is True
    assert reason == "ok"
    assert len([key for key in redis.store if key.startswith(RPD_KEY.split("{", 1)[0])]) == 1
    assert len([key for key in redis.store if key.startswith(RPM_KEY.split("{", 1)[0])]) == 1


@pytest.mark.asyncio
async def test_openrouter_guard_blocks_when_rpd_soft_cap_reached(monkeypatch) -> None:
    monkeypatch.setattr("worker.openrouter_guard.get_settings", _settings)
    redis = _FakeRedis()
    rpd_key = RPD_KEY.format(date="20990101")
    redis.store[rpd_key] = "850"
    guard = OpenRouterVisionGuard(redis=redis)

    monkeypatch.setattr("worker.openrouter_guard._utc_date_key", lambda: "20990101")
    allowed, reason = await guard.reserve_slot()

    assert allowed is False
    assert reason == "guard_rpd_soft_cap"


@pytest.mark.asyncio
async def test_openrouter_guard_sets_quarantine_on_429(monkeypatch) -> None:
    monkeypatch.setattr("worker.openrouter_guard.get_settings", _settings)
    redis = _FakeRedis()
    guard = OpenRouterVisionGuard(redis=redis)

    await guard.record_failure(status_code=429, reset_at=2_000_000_000.0)

    assert float(redis.store[QUARANTINE_KEY]) == 2_000_000_000.0


@pytest.mark.asyncio
async def test_openrouter_guard_quarantines_after_5xx_burst(monkeypatch) -> None:
    monkeypatch.setattr("worker.openrouter_guard.get_settings", _settings)
    redis = _FakeRedis()
    guard = OpenRouterVisionGuard(redis=redis)

    await guard.record_failure(status_code=500, reset_at=None)
    await guard.record_failure(status_code=502, reset_at=None)
    await guard.record_failure(status_code=503, reset_at=None)

    assert QUARANTINE_KEY in redis.store


@pytest.mark.asyncio
async def test_openrouter_guard_does_not_shorten_existing_quarantine(monkeypatch) -> None:
    monkeypatch.setattr("worker.openrouter_guard.get_settings", _settings)
    redis = _FakeRedis()
    redis.store[QUARANTINE_KEY] = "300.0"
    guard = OpenRouterVisionGuard(redis=redis)

    await guard._set_quarantine_until(120.0)

    assert float(redis.store[QUARANTINE_KEY]) == 300.0
