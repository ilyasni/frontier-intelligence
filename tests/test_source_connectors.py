import sys
from email.message import EmailMessage
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.sources.api_source import APISource
from ingest.sources.base import build_httpx_client
from ingest.sources.email_source import EmailSource
from ingest.sources.rss_source import RSSSource
from ingest.sources.web_source import WebSource


def _redis():
    return MagicMock()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_external_id_uses_canonical_url_without_guid():
    source = RSSSource(
        source_id="rss-1",
        workspace_id="ws",
        config={"source_type": "rss", "url": "https://feed.example/rss"},
        redis=_redis(),
    )
    item = await source.normalize_item(
        {
            "title": "Mobility signal",
            "link": "https://example.com/post?utm_source=tg&utm_medium=social",
            "summary": "AI mobility signal",
        }
    )

    assert item is not None
    assert item.external_id == "https://example.com/post"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_normalizes_html_summary_and_links_to_plain_text():
    source = RSSSource(
        source_id="rss-html-1",
        workspace_id="ws",
        config={"source_type": "rss", "url": "https://feed.example/rss"},
        redis=_redis(),
    )

    item = await source.normalize_item(
        {
            "title": "Medium signal",
            "link": "https://medium.com/p/example",
            "summary": (
                '<div class="medium-feed-item"><p>Hello <strong>world</strong> '
                '<a href="https://example.com/full">Continue</a></p></div>'
            ),
        }
    )

    assert item is not None
    assert item.summary == "Hello world Continue"
    assert item.content == "Hello world Continue"
    assert "https://example.com/full" in item.linked_urls


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_dedupes_repeated_entry():
    source = RSSSource(
        source_id="rss-2",
        workspace_id="ws",
        config={"source_type": "rss", "url": "https://feed.example/rss"},
        redis=_redis(),
    )
    source._checkpoint = {"cursor_json": {"seen_external_ids": ["https://example.com/post"]}}
    source.fetch_index = AsyncMock(
        return_value=[
            {
                "title": "Mobility signal",
                "link": "https://example.com/post?utm_source=tg",
                "summary": "AI mobility signal",
            }
        ]
    )

    events = await source.fetch()

    assert events == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_full_content_hydration_replaces_teaser():
    source = RSSSource(
        source_id="rss-3",
        workspace_id="ws",
        config={
            "source_type": "rss",
            "url": "https://feed.example/rss",
            "parse": {"full_content": True},
        },
        redis=_redis(),
    )
    source.fetch_index = AsyncMock(
        return_value=[
            {
                "title": "Mobility signal",
                "link": "https://example.com/post",
                "summary": "Short teaser",
            }
        ]
    )

    with patch(
        "ingest.sources.base.fetch_url_content",
        new=AsyncMock(
            return_value={
                "title": "Mobility signal",
                "content": "Full article body",
                "summary": "Full summary",
                "linked_urls": ["https://source.example/ref"],
            }
        ),
    ):
        events = await source.fetch()

    assert len(events) == 1
    assert events[0].content == "Full article body"
    assert "https://source.example/ref" in events[0].linked_urls


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_hydration_failure_falls_back_to_teaser():
    source = RSSSource(
        source_id="rss-3b",
        workspace_id="ws",
        config={
            "source_type": "rss",
            "url": "https://feed.example/rss",
            "parse": {"full_content": True},
        },
        redis=_redis(),
    )
    source.fetch_index = AsyncMock(
        return_value=[
            {
                "title": "Mobility signal",
                "link": "https://example.com/post",
                "summary": "Short teaser",
            }
        ]
    )

    with patch(
        "ingest.sources.base.fetch_url_content",
        new=AsyncMock(side_effect=RuntimeError("403 Forbidden")),
    ):
        events = await source.fetch()

    assert len(events) == 1
    assert events[0].content == "Short teaser"
    assert events[0].extra["summary"] == "Short teaser"
    assert events[0].extra["hydration_error"] == "403 Forbidden"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_html_content_fragment_becomes_plain_text():
    source = RSSSource(
        source_id="rss-html-2",
        workspace_id="ws",
        config={"source_type": "rss", "url": "https://feed.example/rss"},
        redis=_redis(),
    )

    item = await source.normalize_item(
        {
            "title": "HTML body",
            "link": "https://example.com/post",
            "content": [
                {
                    "value": (
                        "<div><p>First paragraph.</p><p>Second <em>paragraph</em>.</p>"
                        '<script>alert(1)</script><a href="https://ref.example/a">Ref</a></div>'
                    )
                }
            ],
        }
    )

    assert item is not None
    assert item.content == "First paragraph. Second paragraph. Ref"
    assert "https://ref.example/a" in item.linked_urls


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_filters_by_keyword_and_language():
    source = RSSSource(
        source_id="rss-4",
        workspace_id="ws",
        config={
            "source_type": "rss",
            "url": "https://feed.example/rss",
            "filters": {
                "include_keywords": ["mobility"],
                "lang_allow": ["en"],
            },
        },
        redis=_redis(),
    )
    source.fetch_index = AsyncMock(
        return_value=[
            {
                "title": "Автономный транспорт",
                "link": "https://example.com/post",
                "summary": "Русский текст без совпадения по language allow",
            }
        ]
    )

    events = await source.fetch()

    assert events == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_treats_304_as_not_modified():
    class _Response:
        status_code = 304
        headers = {}
        content = b""

        def raise_for_status(self):
            return None

    source = RSSSource(
        source_id="rss-304",
        workspace_id="ws",
        config={"source_type": "rss", "url": "https://feed.example/rss"},
        redis=_redis(),
    )

    with patch(
        "ingest.sources.rss_source.http_get_with_retries",
        new=AsyncMock(return_value=_Response()),
    ):
        items = await source.fetch_index()

    assert items == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rss_source_discovers_alternate_feed_from_html_page():
    class _HtmlResponse:
        status_code = 200
        headers = {"Content-Type": "text/html; charset=utf-8"}
        text = (
            "<html><head>"
            '<link rel="alternate" type="application/rss+xml" href="/feed.xml" />'
            "</head><body></body></html>"
        )
        content = text.encode()
        url = "https://habr.com/ru/flows/design/articles/"

        def raise_for_status(self):
            return None

    class _FeedResponse:
        status_code = 200
        headers = {"Content-Type": "application/rss+xml", "ETag": "abc"}
        text = "<?xml version='1.0'?><rss><channel></channel></rss>"
        content = text.encode()
        url = "https://habr.com/feed.xml"

        def raise_for_status(self):
            return None

    source = RSSSource(
        source_id="rss-habr-html",
        workspace_id="ws",
        config={"source_type": "rss", "url": "https://habr.com/ru/flows/design/articles/"},
        redis=_redis(),
    )

    with patch(
        "ingest.sources.rss_source.http_get_with_retries",
        new=AsyncMock(side_effect=[_HtmlResponse(), _FeedResponse()]),
    ), patch.dict(
        sys.modules,
        {
            "feedparser": MagicMock(
                parse=MagicMock(
                    return_value=MagicMock(
                        entries=[{"title": "Example", "link": "https://example.com"}]
                    )
                )
            )
        },
    ):
        items = await source.fetch_index()

    assert len(items) == 1
    assert source._checkpoint_updates["cursor_json"]["resolved_feed_url"] == "https://habr.com/feed.xml"
    assert source._checkpoint_updates["etag"] == "abc"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_api_source_uses_saved_cursor_and_advances_page():
    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"id": "1", "title": "API signal", "content": "API body"}]

    source = APISource(
        source_id="api-1",
        workspace_id="ws",
        config={
            "source_type": "api",
            "url": "https://api.example/items",
            "fetch": {"page_param": "page"},
            "parse": {"field_map": {"id": "id", "title": "title", "content": "content"}},
        },
        redis=_redis(),
    )
    source._checkpoint = {"cursor_json": {"page": 3}}

    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=_Response())) as mocked_get:
        items = await source.fetch_index()

    assert len(items) == 1
    assert mocked_get.await_args.kwargs["params"]["page"] == "3"
    assert source._checkpoint_updates["cursor_json"]["page"] == 4


@pytest.mark.unit
@pytest.mark.asyncio
async def test_api_source_expands_item_url_template_for_hn_style_ids():
    class _Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    responses = [
        _Response([101, 102]),
        _Response({"id": 101, "title": "HN one", "url": "https://example.com/1", "text": "alpha"}),
        _Response({"id": 102, "title": "HN two", "url": "https://example.com/2", "text": "beta"}),
    ]

    async def _fake_get(*args, **kwargs):
        return responses.pop(0)

    source = APISource(
        source_id="api-hn",
        workspace_id="ws",
        config={
            "source_type": "api",
            "url": "https://hacker-news.firebaseio.com/v0/topstories.json",
            "fetch": {
                "item_url_template": "https://hacker-news.firebaseio.com/v0/item/{id}.json",
                "max_items_per_run": 2,
            },
            "parse": {
                "field_map": {
                    "id": "id",
                    "title": "title",
                    "url": "url",
                    "content": "text",
                }
            },
        },
        redis=_redis(),
    )

    with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=_fake_get)):
        items = await source.fetch_index()

    assert [item["id"] for item in items] == [101, 102]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_api_source_ignores_saved_next_cursor_when_mapping_is_blank():
    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"id": "1", "title": "API signal", "content": "API body"}]

    source = APISource(
        source_id="api-no-cursor",
        workspace_id="ws",
        config={
            "source_type": "api",
            "url": "https://api.example/items",
            "parse": {"field_map": {"id": "id", "title": "title", "content": "content"}},
        },
        redis=_redis(),
    )
    source._checkpoint = {"cursor_json": {"next_cursor": "should-not-be-used"}}

    with patch("httpx.AsyncClient.get", new=AsyncMock(return_value=_Response())) as mocked_get:
        items = await source.fetch_index()

    assert len(items) == 1
    assert mocked_get.await_args.kwargs["params"] == {}
    assert source._checkpoint_updates["cursor_json"]["next_cursor"] is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_email_source_dedupes_message_id():
    msg = EmailMessage()
    msg["Subject"] = "Google Alert: Mobility"
    msg["Message-ID"] = "<msg-1@example.com>"
    msg["From"] = "alerts@example.com"
    msg.set_content("Interesting link https://example.com/report")

    source = EmailSource(
        source_id="email-1",
        workspace_id="ws",
        config={
            "source_type": "email",
            "fetch": {"host": "imap.example.com", "username": "alerts@example.com"},
        },
        redis=_redis(),
    )
    source._fetch_messages = AsyncMock(return_value=[msg])

    first = await source.fetch()
    source._checkpoint = {"cursor_json": {"seen_external_ids": [first[0].external_id]}}
    second = await source.fetch()

    assert len(first) == 1
    assert second == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_web_source_falls_back_when_article_fetch_fails():
    class _Response:
        text = """
        <html><body>
          <article>
            <a href="/post-1">Read more</a>
            <h2>Waymo expands</h2>
            <time datetime="2026-03-28T10:00:00+00:00">2026-03-28</time>
          </article>
        </body></html>
        """

        def raise_for_status(self):
            return None

    source = WebSource(
        source_id="web-1",
        workspace_id="ws",
        config={
            "source_type": "web",
            "url": "https://waymo.com/blog/",
            "parse": {
                "listing_selector": "article",
                "link_selector": "a[href]",
                "title_selector": "h2",
                "date_selector": "time",
                "article_selector": "article",
            },
        },
        redis=_redis(),
    )

    with patch(
        "ingest.sources.web_source.http_get_with_retries",
        new=AsyncMock(return_value=_Response()),
    ), patch(
        "ingest.sources.web_source.fetch_url_content",
        new=AsyncMock(side_effect=RuntimeError("connect failed")),
    ):
        events = await source.fetch()

    assert len(events) == 1
    assert events[0].url == "https://waymo.com/post-1"
    assert events[0].content == "Waymo expands"
    assert events[0].extra["hydration_error"] == "connect failed"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_web_source_blank_title_falls_back_to_href():
    class _Response:
        text = """
        <html><body>
          <article>
            <a href="/post-blank"><span></span></a>
          </article>
        </body></html>
        """

        def raise_for_status(self):
            return None

    source = WebSource(
        source_id="web-blank-title",
        workspace_id="ws",
        config={
            "source_type": "web",
            "url": "https://example.com/news",
            "parse": {
                "listing_selector": "article",
                "link_selector": "a[href]",
                "title_selector": "span",
                "article_selector": "article",
            },
        },
        redis=_redis(),
    )

    with patch(
        "ingest.sources.web_source.http_get_with_retries",
        new=AsyncMock(return_value=_Response()),
    ), patch(
        "ingest.sources.web_source.fetch_url_content",
        new=AsyncMock(return_value={}),
    ):
        events = await source.fetch()

    assert len(events) == 1
    assert events[0].url == "https://example.com/post-blank"
    assert events[0].content == "/post-blank"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_build_httpx_client_accepts_socks5_proxy():
    fake_client = MagicMock()
    with patch("ingest.sources.base.httpx.AsyncClient", return_value=fake_client) as mocked:
        client = build_httpx_client(
            source_config={"proxy_config": {"type": "socks5", "host": "xray", "port": 10808}},
            timeout=20,
        )

    assert client is fake_client
    assert mocked.call_args.kwargs["proxy"] == "socks5://xray:10808"
