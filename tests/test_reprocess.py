"""Unit tests for admin/backend/routers/pipeline.py reprocess event building."""
import json
from datetime import datetime, timezone

import pytest

from shared.linked_urls import extract_urls_from_plain_text, finalize_linked_urls


def _make_post_row(
    *,
    grouped_id=None,
    has_media=False,
    url="https://t.me/test/1",
    author="@test",
    extra=None,
):
    """Build a minimal posts-table row dict."""
    if extra is None:
        extra = {"url": url, "author": author}
    return {
        "id": "post-uuid-1",
        "source_id": "src-uuid-1",
        "workspace_id": "disruption",
        "external_id": "42",
        "content": "Test content",
        "published_at": datetime(2026, 3, 25, 10, 0, 0, tzinfo=timezone.utc),
        "grouped_id": grouped_id,
        "has_media": has_media,
        "media_urls": [],
        "extra": extra,
    }


def _build_event(p: dict) -> dict:
    """Replicate the event-building logic from pipeline.py:reprocess_post."""
    extra = p.get("extra") or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}
    media_urls = p.get("media_urls") or []
    if isinstance(media_urls, str):
        try:
            media_urls = json.loads(media_urls)
        except Exception:
            media_urls = []
    if not isinstance(media_urls, list):
        media_urls = []
    content = p.get("content") or ""
    linked = finalize_linked_urls(extract_urls_from_plain_text(content))
    return {
        "source_id": str(p["source_id"]) if p.get("source_id") else "",
        "workspace_id": p["workspace_id"],
        "external_id": p["external_id"] or "",
        "content": content,
        "published_at": p["published_at"].isoformat() if p.get("published_at") else "",
        "grouped_id": str(p["grouped_id"]) if p.get("grouped_id") else "",
        "has_media": str(p.get("has_media", False)),
        "media_urls": json.dumps(media_urls),
        "linked_urls": json.dumps(linked),
        "url": extra.get("url", ""),
        "author": extra.get("author", ""),
    }


@pytest.mark.unit
def test_all_fields_present():
    """Event must contain all PostParsedEvent fields."""
    required_fields = {
        "source_id", "workspace_id", "external_id", "content",
        "published_at", "grouped_id", "has_media", "media_urls", "linked_urls",
        "url", "author",
    }
    row = _make_post_row(grouped_id="999", has_media=True)
    event = _build_event(row)
    assert required_fields == set(event.keys())


@pytest.mark.unit
def test_grouped_id_null_becomes_empty_string():
    row = _make_post_row(grouped_id=None)
    event = _build_event(row)
    assert event["grouped_id"] == ""


@pytest.mark.unit
def test_grouped_id_integer_becomes_string():
    row = _make_post_row(grouped_id=123456789)
    event = _build_event(row)
    assert event["grouped_id"] == "123456789"


@pytest.mark.unit
def test_has_media_false_becomes_string():
    row = _make_post_row(has_media=False)
    event = _build_event(row)
    assert event["has_media"] == "False"


@pytest.mark.unit
def test_has_media_true_becomes_string():
    row = _make_post_row(has_media=True)
    event = _build_event(row)
    assert event["has_media"] == "True"


@pytest.mark.unit
def test_url_and_author_from_extra():
    row = _make_post_row(url="https://habr.com/post/1", author="habr_author")
    event = _build_event(row)
    assert event["url"] == "https://habr.com/post/1"
    assert event["author"] == "habr_author"


@pytest.mark.unit
def test_extra_as_json_string():
    """extra stored as JSON string in DB must be decoded."""
    extra_str = json.dumps({"url": "https://t.me/ch/5", "author": "@ch"})
    row = _make_post_row()
    row["extra"] = extra_str
    event = _build_event(row)
    assert event["url"] == "https://t.me/ch/5"
    assert event["author"] == "@ch"


@pytest.mark.unit
def test_missing_extra_produces_empty_strings():
    row = _make_post_row()
    row["extra"] = None
    event = _build_event(row)
    assert event["url"] == ""
    assert event["author"] == ""


@pytest.mark.unit
def test_linked_urls_extracted_from_content():
    row = _make_post_row()
    row["content"] = "Читай https://example.com/article и комментарии"
    event = _build_event(row)
    assert json.loads(event["linked_urls"]) == ["https://example.com/article"]


@pytest.mark.unit
def test_media_urls_roundtrip():
    row = _make_post_row()
    row["media_urls"] = ["media/ws/k/1.jpg"]
    event = _build_event(row)
    assert json.loads(event["media_urls"]) == ["media/ws/k/1.jpg"]
