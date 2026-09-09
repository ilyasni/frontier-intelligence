import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from admin.backend.routers import monitoring as monitoring_module


class _DummyRequest:
    def __init__(self, payload):
        self._payload = payload
        self.headers = {}
        self.query_params = {}

    async def json(self):
        return self._payload


class _DeliveryRedis:
    def __init__(self, *, release_failure=False, mark_failure=False):
        self.values = {}
        self.expiry = {}
        self.closed = 0
        self.deletes = []
        self.release_failure = release_failure
        self.mark_failure = mark_failure

    async def set(self, key, value, *, ex, nx=False):
        if nx and key in self.values:
            return False
        if value == "delivered" and self.mark_failure:
            raise RuntimeError("private-mark-response")
        self.values[key] = value
        self.expiry[key] = ex
        return True

    async def get(self, key):
        return self.values.get(key)

    async def delete(self, key):
        self.deletes.append(key)
        if self.release_failure:
            raise RuntimeError("private-redis-response")
        self.values.pop(key, None)

    async def aclose(self):
        self.closed += 1


def _use_delivery_redis(monkeypatch, redis):
    monkeypatch.setattr(monitoring_module, "get_settings", lambda: SimpleNamespace(redis_url="redis://fake"))
    monkeypatch.setattr(monitoring_module.aioredis, "from_url", lambda *a, **kw: redis)


def test_alertmanager_webhook_ignores_empty_alert_groups(monkeypatch) -> None:
    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda request: None)

    called = {"sent": False}

    async def _fake_send(_message: str) -> bool:
        called["sent"] = True
        return True

    monkeypatch.setattr(monitoring_module, "send_ntfy_alert_message", _fake_send)

    result = asyncio.run(
        monitoring_module.alertmanager_webhook(
            _DummyRequest({"status": "firing", "alerts": []})
        )
    )

    assert result["status"] == "ignored"
    assert result["reason"] == "empty_alert_group"
    assert called["sent"] is False


def test_alertmanager_webhook_ignores_duplicate_groups(monkeypatch) -> None:
    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda request: None)

    async def _claim(_payload):
        return False

    monkeypatch.setattr(monitoring_module, "_claim_alert_delivery", _claim)

    result = asyncio.run(
        monitoring_module.alertmanager_webhook(
            _DummyRequest(
                {
                    "status": "firing",
                    "alerts": [{"fingerprint": "abc"}],
                    "commonLabels": {"alertname": "FrontierCoreServiceDown"},
                }
            )
        )
    )

    assert result["status"] == "ignored"
    assert result["reason"] == "duplicate_alert_group"


def test_alertmanager_webhook_accepts_only_after_delivery(monkeypatch) -> None:
    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda request: None)
    redis = _DeliveryRedis()
    _use_delivery_redis(monkeypatch, redis)

    delivered = []

    async def _send(message: str, **kwargs) -> bool:
        assert list(redis.values.values()) == ["pending"]
        assert list(redis.expiry.values()) == [120]
        delivered.append(message)
        return True

    monkeypatch.setattr(monitoring_module, "send_ntfy_alert_message", _send)

    result = asyncio.run(
        monitoring_module.alertmanager_webhook(
            _DummyRequest(
                {
                    "status": "firing",
                    "alerts": [{"fingerprint": "abc"}],
                    "commonLabels": {
                        "alertname": "FrontierCoreServiceDown",
                        "severity": "critical",
                        "service": "admin",
                    },
                    "commonAnnotations": {
                        "summary": "Core Frontier service is down",
                    },
                }
            )
        )
    )

    assert result["status"] == "accepted"
    assert result["delivered"] is True
    assert len(delivered) == 1
    assert list(redis.values.values()) == ["delivered"]
    assert list(redis.expiry.values()) == [1800]


def test_alertmanager_delivery_retries_ntfy_with_existing_backoff(monkeypatch) -> None:
    attempts = 0
    delays: list[float] = []

    async def _fake_send(_message: str, **_kwargs: str) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("delivery failed")
        return True

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(monitoring_module, "send_ntfy_alert_message", _fake_send)
    monkeypatch.setattr(monitoring_module.asyncio, "sleep", _fake_sleep)

    delivered = asyncio.run(
        monitoring_module._deliver_alert_message(
            {
                "status": "firing",
                "commonLabels": {"alertname": "FrontierCoreServiceDown"},
            },
            "worker is down",
        )
    )

    assert attempts == 3
    assert delays == [2.0, 4.0]
    assert delivered is True


@pytest.mark.parametrize("failure", [False, RuntimeError("private-response")])
@pytest.mark.parametrize("release_failure", [False, True])
def test_alertmanager_failed_delivery_releases_claim_and_returns_503(
    monkeypatch, caplog, failure, release_failure
) -> None:
    redis = _DeliveryRedis(release_failure=release_failure)
    attempts = []
    delays = []

    async def send(message, **kwargs):
        attempts.append(message)
        if len(attempts) <= 3:
            if isinstance(failure, Exception):
                raise failure
            return failure
        return True

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda r: None)
    _use_delivery_redis(monkeypatch, redis)
    monkeypatch.setattr(monitoring_module, "send_ntfy_alert_message", send)
    monkeypatch.setattr(monitoring_module.asyncio, "sleep", sleep)
    payload = {"status": "firing", "alerts": [{"fingerprint": "abc"}]}

    async def scenario():
        with pytest.raises(HTTPException) as exc:
            await monitoring_module.alertmanager_webhook(_DummyRequest(payload))
        assert exc.value.status_code == 503
        assert len(attempts) == 3
        assert delays == [2.0, 4.0]
        assert redis.deletes == [monitoring_module._alert_delivery_key(payload)]
        assert redis.closed == 2
        if release_failure:
            with pytest.raises(HTTPException) as retry_exc:
                await monitoring_module.alertmanager_webhook(_DummyRequest(payload))
            assert retry_exc.value.status_code == 503
            assert list(redis.values.values()) == ["pending"]
            assert list(redis.expiry.values()) == [120]
            assert len(attempts) == 3
            # Моделируем истечение короткого lease после недоступности Redis delete.
            redis.values.clear()
            result = await monitoring_module.alertmanager_webhook(_DummyRequest(payload))
            assert result["delivered"] is True
            assert len(attempts) == 4
        else:
            result = await monitoring_module.alertmanager_webhook(_DummyRequest(payload))
            assert result["delivered"] is True
            assert len(attempts) == 4
            duplicate = await monitoring_module.alertmanager_webhook(_DummyRequest(payload))
            assert duplicate["reason"] == "duplicate_alert_group"
            assert len(attempts) == 4

    asyncio.run(scenario())
    assert "private-response" not in caplog.text
    assert "private-redis-response" not in caplog.text


@pytest.mark.parametrize("state", ["pending", "1", "unknown"])
def test_alertmanager_unconfirmed_claim_never_acknowledges_duplicate(monkeypatch, state) -> None:
    redis = _DeliveryRedis()
    payload = {"status": "firing", "alerts": [{"fingerprint": "abc"}]}
    redis.values[monitoring_module._alert_delivery_key(payload)] = state
    _use_delivery_redis(monkeypatch, redis)
    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda r: None)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(monitoring_module.alertmanager_webhook(_DummyRequest(payload)))
    assert exc.value.status_code == 503


def test_alertmanager_mark_failure_preserves_pending_and_returns_503(monkeypatch, caplog) -> None:
    redis = _DeliveryRedis(mark_failure=True)
    payload = {"status": "firing", "alerts": [{"fingerprint": "abc"}]}
    _use_delivery_redis(monkeypatch, redis)
    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda r: None)

    async def send(*args, **kwargs):
        return True

    monkeypatch.setattr(monitoring_module, "send_ntfy_alert_message", send)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(monitoring_module.alertmanager_webhook(_DummyRequest(payload)))
    assert exc.value.status_code == 503
    assert list(redis.values.values()) == ["pending"]
    assert list(redis.expiry.values()) == [120]
    assert "private-mark-response" not in caplog.text


def test_alertmanager_health_reports_ntfy_configuration(monkeypatch) -> None:
    monkeypatch.setattr(
        monitoring_module,
        "get_settings",
        lambda: SimpleNamespace(
            alertmanager_webhook_token="configured",
            ntfy_url="https://ntfy.example/frontier-alerts",
            ntfy_credential_file="/run/credentials/ntfy-publisher",
        ),
    )
    monkeypatch.setattr(monitoring_module, "ntfy_alerts_enabled", lambda: True)

    result = asyncio.run(monitoring_module.alertmanager_health())

    assert result == {
        "status": "ok",
        "ntfy_enabled": True,
        "alertmanager_token_configured": True,
        "alertmanager_basic_auth_username": "alertmanager",
        "ntfy_url_configured": True,
        "ntfy_credential_file_configured": True,
    }
