from admin.backend.routers.sources import _build_telegram_diagnostics


def test_build_telegram_diagnostics_marks_drift() -> None:
    diag = _build_telegram_diagnostics(
        {
            "source_type": "telegram",
            "tg_channel": "@oldhandle",
            "cursor_json": {
                "telegram_peer": {
                    "entity_id": 777001,
                    "username": "newhandle",
                    "title": "New Handle",
                }
            },
        }
    )

    assert diag is not None
    assert diag["status"] == "drift"
    assert diag["configured_username"] == "oldhandle"
    assert diag["resolved_username"] == "newhandle"
    assert diag["has_cached_peer"] is True
    assert diag["username_mismatch"] is True


def test_build_telegram_diagnostics_marks_unresolved_without_peer() -> None:
    diag = _build_telegram_diagnostics(
        {
            "source_type": "telegram",
            "tg_channel": "@missinghandle",
            "cursor_json": {},
            "last_error": "telegram_username_unresolved source=tg_ru_missing channel=@missinghandle",
        }
    )

    assert diag is not None
    assert diag["status"] == "unresolved"
    assert diag["configured_username"] == "missinghandle"
    assert diag["resolved_username"] is None
    assert diag["has_cached_peer"] is False
