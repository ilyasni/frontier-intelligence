import asyncio

from admin.backend.routers import monitoring as monitoring_module


class _DummyRequest:
    def __init__(self, payload):
        self._payload = payload
        self.headers = {}
        self.query_params = {}

    async def json(self):
        return self._payload


def test_alertmanager_webhook_ignores_empty_alert_groups(monkeypatch) -> None:
    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda request: None)

    called = {"sent": False}

    async def _fake_send(_message: str) -> bool:
        called["sent"] = True
        return True

    monkeypatch.setattr(monitoring_module, "send_telegram_alert_message", _fake_send)

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


def test_alertmanager_webhook_accepts_and_schedules_delivery(monkeypatch) -> None:
    monkeypatch.setattr(monitoring_module, "_assert_alertmanager_token", lambda request: None)

    async def _claim(_payload):
        return True

    scheduled = {"called": False}

    async def _deliver(_payload, _message: str) -> None:
        scheduled["called"] = True

    class _Task:
        def __init__(self, coro) -> None:
            self.coro = coro

    def _fake_create_task(coro):
        scheduled["called"] = True
        coro.close()
        return _Task(coro)

    monkeypatch.setattr(monitoring_module, "_claim_alert_delivery", _claim)
    monkeypatch.setattr(monitoring_module, "_deliver_alert_message", _deliver)
    monkeypatch.setattr(monitoring_module.asyncio, "create_task", _fake_create_task)

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
    assert scheduled["called"] is True
