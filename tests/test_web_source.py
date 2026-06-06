from unittest.mock import AsyncMock

import pytest
from bs4 import BeautifulSoup

from ingest.sources.web_source import WebSource


@pytest.mark.unit
@pytest.mark.asyncio
async def test_web_source_normalizes_anchor_listing_item() -> None:
    source = WebSource(
        source_id="web-test",
        workspace_id="disruption",
        config={
            "source_type": "web",
            "url": "https://example.com/newsroom",
            "parse": {
                "full_content": False,
                "listing_selector": "a[href*='/article/']",
                "link_selector": "a[href]",
                "title_selector": "a[href]",
                "date_selector": "",
            },
        },
        redis=AsyncMock(),
        runtime_store=None,
    )

    soup = BeautifulSoup(
        """
        <a href="/article/alpha">
          Alpha launch update
        </a>
        """,
        "lxml",
    )

    item = await source.normalize_item(soup.select_one("a[href]"))

    assert item is not None
    assert item.external_id == "https://example.com/article/alpha"
    assert item.url == "https://example.com/article/alpha"
    assert item.title == "Alpha launch update"
