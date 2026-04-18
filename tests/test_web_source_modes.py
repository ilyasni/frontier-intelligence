import pytest
from bs4 import BeautifulSoup

from ingest.sources.web_source import WebSource


@pytest.mark.asyncio
async def test_web_source_skips_hydration_when_full_content_disabled(monkeypatch) -> None:
    html = """
    <article>
      <a href="/post-1">Test title</a>
      <time datetime="2026-03-28T12:00:00Z"></time>
      <p>Listing summary text</p>
    </article>
    """
    item = BeautifulSoup(html, "lxml").select_one("article")

    async def _fail_fetch(*args, **kwargs):
        raise AssertionError("fetch_url_content must not be called")

    monkeypatch.setattr("ingest.sources.web_source.fetch_url_content", _fail_fetch)

    source = WebSource(
        source_id="web_test",
        workspace_id="design",
        config={
            "url": "https://example.com/news",
            "parse": {
                "full_content": False,
                "listing_selector": "article",
                "link_selector": "a[href]",
                "title_selector": "a[href]",
                "date_selector": "time",
            },
        },
        redis=None,
        runtime_store=None,
    )

    normalized = await source.normalize_item(item)

    assert normalized is not None
    assert normalized.url == "https://example.com/post-1"
    assert "Listing summary text" in normalized.summary
    assert normalized.content == normalized.summary


@pytest.mark.asyncio
async def test_web_source_hydrates_when_full_content_enabled(monkeypatch) -> None:
    html = """
    <article>
      <a href="/post-2">Another title</a>
      <p>Listing summary text</p>
    </article>
    """
    item = BeautifulSoup(html, "lxml").select_one("article")
    seen = {"called": False}

    async def _fetch(url, **kwargs):
        seen["called"] = True
        return {"summary": "Hydrated summary", "content": "Hydrated content", "linked_urls": []}

    monkeypatch.setattr("ingest.sources.web_source.fetch_url_content", _fetch)

    source = WebSource(
        source_id="web_test",
        workspace_id="design",
        config={
            "url": "https://example.com/news",
            "parse": {
                "full_content": True,
                "listing_selector": "article",
                "link_selector": "a[href]",
                "title_selector": "a[href]",
            },
        },
        redis=None,
        runtime_store=None,
    )

    normalized = await source.normalize_item(item)

    assert seen["called"] is True
    assert normalized is not None
    assert normalized.summary == "Hydrated summary"
    assert normalized.content == "Hydrated content"
