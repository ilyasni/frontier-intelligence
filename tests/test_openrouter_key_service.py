from admin.backend.services.openrouter_key import (
    _normalize_credits_payload,
    _normalize_key_payload,
    fetch_openrouter_key,
)


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


def test_normalize_credits_payload_uses_account_balance() -> None:
    # Real /credits shape: the account holds a balance even when the key is
    # uncapped (limit_remaining is null on /key). balance = total - usage.
    normalized = _normalize_credits_payload(
        {"data": {"total_credits": 20, "total_usage": 0.018456566}}
    )

    assert normalized["total_credits"] == 20.0
    assert round(normalized["credit_balance"], 4) == 19.9815


def test_normalize_credits_payload_handles_garbage() -> None:
    assert _normalize_credits_payload(None)["credit_balance"] == 0.0
    assert _normalize_credits_payload({"data": None})["credit_balance"] == 0.0


def test_uncapped_key_has_zero_limit_remaining_but_balance_comes_from_credits() -> None:
    # Regression for the false CreditLow alert: an uncapped paid key reports
    # limit_remaining=null → 0.0, which must NOT be read as the balance.
    key = _normalize_key_payload({"data": {"limit": None, "limit_remaining": None, "is_free_tier": False}})
    assert key["limit_remaining"] == 0.0
    credits = _normalize_credits_payload({"data": {"total_credits": 20, "total_usage": 0.02}})
    assert credits["credit_balance"] > 19.0


async def test_fetch_openrouter_key_without_api_key(monkeypatch) -> None:
    monkeypatch.setattr("admin.backend.services.openrouter_key.get_settings", lambda: _Settings())

    payload = await fetch_openrouter_key()

    assert payload["status"] == "missing_api_key"
    assert payload["available"] is False
    assert payload["free_model_rpm_limit"] == 20
