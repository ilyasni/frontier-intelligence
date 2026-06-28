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

    # Headline (the actual news) leads; technical jargon labels are gone.
    assert message.startswith("Срочный тренд · disruption")
    assert "AI browser agents are moving into production design workflows" in message
    assert "daily digest" not in message.lower()
    # Evidence weight is shown in plain Russian, not as a raw score line.
    assert "Объём: 7 материалов · 5 источников" in message
    assert "Теги: agents · browser · design" in message
    # Detected timestamp is rendered as readable Moscow time, not ISO/UTC.
    assert "Время: 18.04.2026 15:00 МСК" in message
    assert "2026-04-18T12:00:00Z" not in message
    # No emoji in the rendered message.
    assert all(ord(ch) < 0x2190 for ch in message)


def test_alert_prefers_russian_title() -> None:
    message = trend_alerts._build_alert_message(
        {
            "workspace_id": "disruption",
            "title": "Apple raises Mac and iPad prices, spares iPhone for now",
            "title_ru": "Apple поднимает цены на Mac и iPad, не трогая iPhone",
            "signal_score": 0.816,
            "doc_count": 7,
            "source_count": 5,
            "change_point_strength": 0.0,
            "detected_at": "2026-06-26T08:20:00.156104+00:00",
            "insight": "Подорожание затрагивает железо, но не флагманский телефон.",
            "opportunity": "",
            "keywords": ["Apple", "MacBook", "iPad"],
        },
        "high_score",
    )

    assert "Apple поднимает цены на Mac и iPad, не трогая iPhone" in message
    # The English headline is replaced, not appended.
    assert "Apple raises Mac and iPad prices" not in message
    assert "Суть: Подорожание затрагивает железо" in message


def test_alert_falls_back_to_english_title_when_no_ru() -> None:
    message = trend_alerts._build_alert_message(
        {
            "workspace_id": "disruption",
            "title": "Anthropic Moves Toward Deal with US",
            "title_ru": None,
            "signal_score": 0.802,
            "doc_count": 5,
            "source_count": 4,
            "change_point_strength": 0.0,
            "detected_at": "2026-06-27T03:35:53.485044+00:00",
            "keywords": ["Anthropic"],
        },
        "high_score",
    )

    assert "Anthropic Moves Toward Deal with US" in message


def test_alert_message_drops_boilerplate_insight() -> None:
    message = trend_alerts._build_alert_message(
        {
            "workspace_id": "disruption",
            "title": "Apple raises Mac and iPad prices, spares iPhone for now",
            "signal_score": 0.816,
            "doc_count": 7,
            "source_count": 5,
            "change_point_strength": 0.0,
            "detected_at": "2026-06-26T08:20:00.156104+00:00",
            "insight": "7 related posts across 5 sources.",
            "opportunity": "",
            "keywords": ["Apple", "App Store", "MacBook", "iPad"],
        },
        "high_score",
    )

    # Count-restating boilerplate must not appear as a "Суть" line.
    assert "Суть" not in message
    assert "7 related posts across 5 sources" not in message
    assert "Время: 26.06.2026 11:20 МСК" in message
