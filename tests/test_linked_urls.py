"""Unit tests for shared.linked_urls."""
import pytest

from shared.linked_urls import (
    extract_urls_from_plain_text,
    finalize_linked_urls,
)


@pytest.mark.unit
def test_finalize_dedup_and_telegram_filter():
    urls = [
        "https://example.com/a",
        "https://example.com/a",
        "https://t.me/channel/1",
        "https://EXAMPLE.com/b",
    ]
    out = finalize_linked_urls(urls)
    assert out == ["https://example.com/a", "https://EXAMPLE.com/b"]


@pytest.mark.unit
def test_regex_extract():
    text = "See https://git.io/xx and also http://a.org/b?q=1"
    got = extract_urls_from_plain_text(text)
    assert "https://git.io/xx" in got
    assert any("http://a.org/b" in u for u in got)
