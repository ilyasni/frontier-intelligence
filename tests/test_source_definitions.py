import pytest

from shared.source_definitions import validate_source_payload


@pytest.mark.unit
def test_validate_source_payload_normalizes_habr_preset():
    source_type, url, tg_channel, extra = validate_source_payload(
        "habr",
        None,
        None,
        {"preset": "habr"},
    )

    assert source_type == "rss"
    assert tg_channel is None
    assert url == "https://habr.com/ru/rss/flows/featured/"
    assert extra["preset"] == "habr"
    assert extra["fetch"]["use_conditional_get"] is True


@pytest.mark.unit
def test_validate_email_requires_host_and_username():
    with pytest.raises(ValueError):
        validate_source_payload("email", None, None, {"fetch": {"host": "imap.example.com"}})
