import json

from admin.backend.routers.sources import (
    _build_telegram_diagnostics,
    _normalize_telegram_channel,
    _redact_source_secrets,
    _update_checkpoint_cursor_for_handle,
)


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


def test_normalize_telegram_channel_accepts_tme_url() -> None:
    assert _normalize_telegram_channel("https://t.me/newhandle/") == "@newhandle"


def test_redact_source_secrets_masks_proxy_password() -> None:
    item = {
        "id": "src1",
        "proxy_config": {
            "host": "proxy.example.com",
            "port": 8080,
            "username": "user",
            "password": "s3cret",
        },
        "extra": {
            "email": {"user": "mail@example.com", "app_password": "hunter2"},
            "api_key": "abcd",
            "note": "keep me",
        },
    }

    redacted = _redact_source_secrets(item)

    # Non-secret fields survive.
    assert redacted["proxy_config"]["host"] == "proxy.example.com"
    assert redacted["proxy_config"]["port"] == 8080
    assert redacted["proxy_config"]["username"] == "user"
    assert redacted["extra"]["email"]["user"] == "mail@example.com"
    assert redacted["extra"]["note"] == "keep me"
    # Secret-looking keys are masked, including nested ones.
    assert redacted["proxy_config"]["password"] == "***"
    assert redacted["extra"]["email"]["app_password"] == "***"
    assert redacted["extra"]["api_key"] == "***"


def test_redact_source_secrets_handles_json_string_column() -> None:
    item = {
        "id": "src2",
        "proxy_config": json.dumps({"host": "h", "password": "p"}),
        "extra": None,
    }

    redacted = _redact_source_secrets(item)

    assert redacted["proxy_config"]["host"] == "h"
    assert redacted["proxy_config"]["password"] == "***"
    assert redacted["extra"] is None


def test_update_checkpoint_cursor_for_handle_keeps_cached_entity_id() -> None:
    cursor = {
        "telegram_peer": {
            "entity_id": 777001,
            "username": "oldhandle",
            "title": "Channel",
        },
        "other": True,
    }

    updated = _update_checkpoint_cursor_for_handle(cursor, "@newhandle")

    assert updated["telegram_peer"]["entity_id"] == 777001
    assert updated["telegram_peer"]["username"] == "newhandle"
    assert updated["telegram_peer"]["title"] == "Channel"
    assert updated["other"] is True
