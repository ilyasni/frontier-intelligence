import asyncio
from types import SimpleNamespace

import pytest

from admin.backend.services import gigachat_balance, xray_health
from admin.backend.services.ntfy_alerts import format_alertmanager_message


def test_format_alertmanager_message_includes_core_context() -> None:
    message = format_alertmanager_message(
        {
            "status": "firing",
            "commonLabels": {
                "alertname": "FrontierCoreServiceDown",
                "severity": "critical",
                "service": "worker",
            },
            "commonAnnotations": {
                "summary": "Core Frontier service is down",
                "description": "worker is unreachable",
            },
            "alerts": [
                {
                    "labels": {
                        "job": "worker",
                        "instance": "worker:9090",
                    }
                }
            ],
        }
    )

    assert "Frontier FIRING: FrontierCoreServiceDown" in message
    assert "severity: CRITICAL" in message
    assert "service: worker" in message
    assert "summary: Core Frontier service is down" in message
    assert "job=worker instance=worker:9090" in message


def test_format_alertmanager_message_fits_ntfy_utf8_limit() -> None:
    message = format_alertmanager_message(
        {
            "commonAnnotations": {"description": "ошибка 🟠" * 1000},
            "alerts": [{}],
        }
    )

    assert len(message.encode("utf-8")) < 4096  # ровно 4096 = вложение для ntfy
    assert message.endswith("\n… message truncated")


def test_gigachat_low_balance_uses_ntfy_and_keeps_signature_dedupe(
    monkeypatch,
) -> None:
    sent: list[tuple[str, dict[str, str]]] = []

    async def _fake_send(text: str, **kwargs: str) -> bool:
        sent.append((text, kwargs))
        return True

    monkeypatch.setattr(gigachat_balance, "ntfy_alerts_enabled", lambda: True)
    monkeypatch.setattr(gigachat_balance, "send_ntfy_alert_message", _fake_send)
    monkeypatch.setattr(gigachat_balance, "_LAST_LOW_BALANCE_SIGNATURE", "")
    settings = SimpleNamespace(gigachat_balance_alert_threshold=100_000)
    items = [{"usage": "GigaChat-Pro", "value": 42_000}]

    asyncio.run(gigachat_balance._notify_low_balance_if_needed(settings, items))
    asyncio.run(gigachat_balance._notify_low_balance_if_needed(settings, items))

    assert len(sent) == 1
    assert "GigaChat-Pro: 42000" in sent[0][0]
    assert sent[0][1]["title"] == "Frontier: low GigaChat balance"


def test_gigachat_low_balance_bounds_ntfy_message_by_utf8_bytes(monkeypatch) -> None:
    sent: list[str] = []

    async def _fake_send(text: str, **_kwargs: str) -> bool:
        sent.append(text)
        return True

    monkeypatch.setattr(gigachat_balance, "ntfy_alerts_enabled", lambda: True)
    monkeypatch.setattr(gigachat_balance, "send_ntfy_alert_message", _fake_send)
    monkeypatch.setattr(gigachat_balance, "_LAST_LOW_BALANCE_SIGNATURE", "")
    settings = SimpleNamespace(gigachat_balance_alert_threshold=100_000)

    asyncio.run(
        gigachat_balance._notify_low_balance_if_needed(
            settings,
            [{"usage": "модель🟠" * 1000, "value": 1}],
        )
    )

    assert len(sent) == 1
    assert len(sent[0].encode("utf-8")) < 4096
    assert sent[0].endswith("\n… message truncated")


def test_xray_degradation_uses_ntfy(monkeypatch) -> None:
    sent: list[tuple[str, dict[str, str]]] = []

    async def _fake_send(text: str, **kwargs: str) -> bool:
        sent.append((text, kwargs))
        return True

    monkeypatch.setattr(xray_health, "ntfy_alerts_enabled", lambda: True)
    monkeypatch.setattr(xray_health, "send_ntfy_alert_message", _fake_send)

    asyncio.run(
        xray_health._send_xray_alert(
            3,
            [
                xray_health.ProbeResult(
                    url="https://probe.example/health",
                    ok=False,
                    status_code=503,
                    error=None,
                )
            ],
            2,
        )
    )

    assert len(sent) == 1
    assert "failed probes: 1/2" in sent[0][0]
    assert sent[0][1]["title"] == "Frontier: XRAY degraded"


def test_xray_degradation_bounds_ntfy_message_by_utf8_bytes(monkeypatch) -> None:
    sent: list[str] = []

    async def _fake_send(text: str, **_kwargs: str) -> bool:
        sent.append(text)
        return True

    monkeypatch.setattr(xray_health, "ntfy_alerts_enabled", lambda: True)
    monkeypatch.setattr(xray_health, "send_ntfy_alert_message", _fake_send)

    asyncio.run(
        xray_health._send_xray_alert(
            3,
            [
                xray_health.ProbeResult(
                    url="https://probe.example/" + "путь🟠" * 1000,
                    ok=False,
                    status_code=503,
                    error=None,
                )
            ],
            1,
        )
    )

    assert len(sent) == 1
    assert len(sent[0].encode("utf-8")) < 4096
    assert sent[0].endswith("\n… message truncated")


@pytest.mark.parametrize("failure", [False, RuntimeError("publication failed")])
def test_gigachat_retries_failed_delivery_before_deduping(monkeypatch, failure) -> None:
    attempts = []

    async def send(text, **kwargs):
        attempts.append(text)
        if len(attempts) == 1:
            if isinstance(failure, Exception):
                raise failure
            return failure
        return True

    monkeypatch.setattr(gigachat_balance, "ntfy_alerts_enabled", lambda: True)
    monkeypatch.setattr(gigachat_balance, "send_ntfy_alert_message", send)
    monkeypatch.setattr(gigachat_balance, "_LAST_LOW_BALANCE_SIGNATURE", "")
    settings = SimpleNamespace(gigachat_balance_alert_threshold=100_000)
    low = [{"usage": "GigaChat-Pro", "value": 42_000}]

    async def scenario():
        if isinstance(failure, Exception):
            with pytest.raises(RuntimeError):
                await gigachat_balance._notify_low_balance_if_needed(settings, low)
        else:
            await gigachat_balance._notify_low_balance_if_needed(settings, low)
        assert gigachat_balance._LAST_LOW_BALANCE_SIGNATURE == ""
        await gigachat_balance._notify_low_balance_if_needed(settings, low)
        await gigachat_balance._notify_low_balance_if_needed(settings, low)
        assert len(attempts) == 2
        await gigachat_balance._notify_low_balance_if_needed(
            settings, [{"usage": "GigaChat-Pro", "value": 200_000}]
        )
        assert gigachat_balance._LAST_LOW_BALANCE_SIGNATURE == ""
        assert len(attempts) == 2
        await gigachat_balance._notify_low_balance_if_needed(settings, low)
        assert len(attempts) == 3

    asyncio.run(scenario())


@pytest.mark.parametrize("outcome", [True, False, RuntimeError("private-response")])
def test_xray_send_reports_delivery_outcome(monkeypatch, caplog, outcome) -> None:
    async def send(*args, **kwargs):
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(xray_health, "ntfy_alerts_enabled", lambda: True)
    monkeypatch.setattr(xray_health, "send_ntfy_alert_message", send)
    result = asyncio.run(xray_health._send_xray_alert(3, [], 1))
    assert result is (outcome is True)
    assert "private-response" not in caplog.text


def test_xray_disabled_delivery_reports_false(monkeypatch) -> None:
    monkeypatch.setattr(xray_health, "ntfy_alerts_enabled", lambda: False)
    assert asyncio.run(xray_health._send_xray_alert(3, [], 1)) is False


def test_xray_cooldown_starts_after_delivery_and_remediation_stays_independent(monkeypatch) -> None:
    from admin.backend.services import xray_runtime

    class Redis:
        def __init__(self):
            self.values = {}
            self.expiry = {}

        async def get(self, key):
            return self.values.get(key)

        async def set(self, key, value, *, ex):
            self.values[key] = value
            self.expiry[key] = ex

        async def lpush(self, *args):
            return None

        async def ltrim(self, *args):
            return None

        async def aclose(self):
            return None

    redis = Redis()
    sends = []
    remediations = []

    async def send(*args, **kwargs):
        sends.append(args)
        return len(sends) > 1

    async def remediate(payload):
        remediations.append(payload)
        return {"triggered": True}

    async def probe(url, *, proxy):
        return xray_health.ProbeResult(url, False, 503, None)

    settings = SimpleNamespace(
        xray_probe_targets=["https://probe.example/health"],
        xray_source_smoke_targets=[], xray_probe_proxy_url="",
        xray_degradation_failure_ratio=0.5, xray_degradation_consecutive_threshold=1,
        xray_alert_cooldown_seconds=300, xray_auto_remediation_enabled=True,
        xray_auto_remediation_cooldown_seconds=600, redis_url="redis://fake",
    )
    monkeypatch.setattr(xray_health, "get_settings", lambda: settings)
    monkeypatch.setattr(xray_health.aioredis, "from_url", lambda *a, **kw: redis)
    monkeypatch.setattr(xray_health, "_probe_once", probe)
    monkeypatch.setattr(xray_health, "ntfy_alerts_enabled", lambda: True)
    monkeypatch.setattr(xray_health, "send_ntfy_alert_message", send)
    monkeypatch.setattr(xray_health, "_trigger_remediation_webhook", remediate)
    monkeypatch.setattr(xray_runtime, "get_runtime_state", lambda: {})

    async def scenario():
        first = await xray_health.run_xray_health_check()
        assert first["alert_sent"] is False
        assert xray_health._XRAY_LAST_ALERT_KEY not in redis.values
        assert first["remediation"] == {"triggered": True}
        assert redis.expiry[xray_health._XRAY_LAST_REMEDIATE_KEY] == 600
        second = await xray_health.run_xray_health_check()
        assert second["alert_sent"] is True
        assert redis.expiry[xray_health._XRAY_LAST_ALERT_KEY] == 300
        third = await xray_health.run_xray_health_check()
        assert third["alert_sent"] is False
        assert len(sends) == 2
        assert len(remediations) == 1

    asyncio.run(scenario())
