import pytest

from worker.services.searxng_client import normalize_searxng_result, sanitize_result_url


@pytest.mark.unit
def test_sanitize_result_url_removes_tracking_params() -> None:
    clean = sanitize_result_url(
        "https://example.com/path/?utm_source=rss&fbclid=abc&id=42#frag"
    )
    assert clean == "https://example.com/path?id=42"


@pytest.mark.unit
def test_sanitize_result_url_blocks_private_hosts() -> None:
    assert sanitize_result_url("http://127.0.0.1:8080/private") is None
    assert sanitize_result_url("http://localhost/test") is None


@pytest.mark.unit
def test_normalize_searxng_result_keeps_http_payload() -> None:
    payload = normalize_searxng_result(
        {
            "url": "https://openai.com/api/?utm_campaign=test",
            "title": "API Platform | OpenAI",
            "content": "Latest models and guides.",
            "engine": "brave",
            "engines": ["duckduckgo", "brave"],
            "score": 4,
        }
    )
    assert payload is not None
    assert payload["url"] == "https://openai.com/api"
    assert payload["engine"] == "brave"
    assert payload["engines"] == ["duckduckgo", "brave"]
