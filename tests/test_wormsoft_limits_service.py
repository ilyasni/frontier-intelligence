from admin.backend.services.wormsoft_limits import _normalize_plans, _normalize_pricing


def test_normalize_plans_sorts_by_price() -> None:
    plans = _normalize_plans(
        {
            "simple": {"amount": 500000, "seconds": 18000, "price": 1500, "periodDays": 30},
            "free": {"amount": 20000, "seconds": 36000, "price": 0, "periodDays": 30},
        }
    )

    assert [item["id"] for item in plans] == ["free", "simple"]
    assert plans[0]["window_hours"] == 10.0
    assert plans[1]["amount"] == 500000


def test_normalize_pricing_keeps_supported_kinds() -> None:
    pricing = _normalize_pricing(
        {
            "wormsoft/agent/medium": {"input": 0.03, "output": 1, "cache": 0.00005},
            "broken": [],
        }
    )

    assert pricing == {
        "wormsoft/agent/medium": {"input": 0.03, "output": 1.0, "cache": 0.00005}
    }
