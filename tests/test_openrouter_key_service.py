from admin.backend.services.openrouter_key import _normalize_key_payload, fetch_openrouter_key


class _Settings:
    openrouter_api_key = ""
    openrouter_base_url = "https://openrouter.ai/api/v1"
    openrouter_referrer = "https://frontier-intelligence.local"
    redis_url = "redis://redis:6379"


def test_normalize_key_payload_for_paid_account() -> None:
    payload = {
        "data": {
            "label": "prod",
            "limit_remaining": 74.5,
            "limit_reset": "monthly",
            "usage_daily": 12.25,
            "byok_usage_daily": 1.5,
            "is_free_tier": False,
            "expires_at": "2027-12-31T23:59:59Z",
        }
    }

    normalized = _normalize_key_payload(payload)

    assert normalized["available"] is True
    assert normalized["is_free_tier"] is False
    assert normalized["free_model_rpm_limit"] == 20
    assert normalized["free_model_daily_limit"] == 1000


def test_normalize_key_payload_for_free_tier() -> None:
    payload = {"data": {"is_free_tier": True}}

    normalized = _normalize_key_payload(payload)

    assert normalized["free_model_daily_limit"] == 50


async def test_fetch_openrouter_key_without_api_key(monkeypatch) -> None:
    monkeypatch.setattr("admin.backend.services.openrouter_key.get_settings", lambda: _Settings())

    payload = await fetch_openrouter_key()

    assert payload["status"] == "missing_api_key"
    assert payload["available"] is False
    assert payload["free_model_rpm_limit"] == 20
