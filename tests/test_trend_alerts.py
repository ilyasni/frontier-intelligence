from types import SimpleNamespace

from admin.backend.services import trend_alerts


def _settings(**overrides):
    values = {
        "trend_alert_min_doc_count": 5,
        "trend_alert_min_source_count": 3,
        "trend_alert_min_signal_score": 0.8,
        "trend_alert_change_point_min_signal_score": 0.74,
        "trend_alert_min_change_point_strength": 0.7,
        "trend_alert_max_per_7d": 2,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_candidate_reason_accepts_confirmed_high_score() -> None:
    row = {
        "signal_score": 0.82,
        "doc_count": 7,
        "source_count": 5,
        "has_recent_change_point": False,
        "change_point_strength": 0.0,
    }

    assert trend_alerts._candidate_reason(row, _settings()) == "high_score"


def test_candidate_reason_rejects_underconfirmed_signal() -> None:
    row = {
        "signal_score": 0.95,
        "doc_count": 4,
        "source_count": 5,
        "has_recent_change_point": True,
        "change_point_strength": 0.9,
    }

    assert trend_alerts._candidate_reason(row, _settings()) is None


def test_candidate_reason_accepts_strong_change_point() -> None:
    row = {
        "signal_score": 0.75,
        "doc_count": 8,
        "source_count": 4,
        "has_recent_change_point": True,
        "change_point_strength": 0.8,
    }

    assert trend_alerts._candidate_reason(row, _settings()) == "change_point"


def test_weekly_capacity_caps_alerts_to_two() -> None:
    assert trend_alerts._remaining_weekly_capacity(0, _settings()) == 2
    assert trend_alerts._remaining_weekly_capacity(1, _settings()) == 1
    assert trend_alerts._remaining_weekly_capacity(2, _settings()) == 0
    assert trend_alerts._remaining_weekly_capacity(3, _settings()) == 0


def test_alert_message_is_urgent_not_digest() -> None:
    message = trend_alerts._build_alert_message(
        {
            "workspace_id": "disruption",
            "title": "AI browser agents are moving into production design workflows",
            "signal_score": 0.821,
            "doc_count": 7,
            "source_count": 5,
            "change_point_strength": 0.0,
            "detected_at": "2026-04-18T12:00:00Z",
            "insight": "Multiple independent sources describe the same shift.",
            "opportunity": "Watch for new interaction patterns in tooling.",
            "keywords": ["agents", "browser", "design"],
        },
        "high_score",
    )

    assert message.startswith("Frontier urgent trend alert")
    assert "daily digest" not in message.lower()
    assert "score: 0.821 | docs: 7 | sources: 5" in message
    assert "agents, browser, design" in message
