"""Формат полей как из Redis Streams (json.dumps → строки)."""

import pytest

from shared.events.posts_parsed_v1 import PostParsedEvent


@pytest.mark.unit
def test_post_parsed_coerces_string_json_lists_and_extra():
    raw = {
        "workspace_id": "ws1",
        "source_id": "src",
        "external_id": "ext-1",
        "content": "hello",
        "media_urls": '["https://a/img.jpg"]',
        "linked_urls": "[]",
        "extra": "{}",
        "has_media": "true",
    }
    ev = PostParsedEvent.model_validate(raw)
    assert ev.media_urls == ["https://a/img.jpg"]
    assert ev.linked_urls == []
    assert ev.extra == {}
    assert ev.has_media is True


@pytest.mark.unit
def test_post_parsed_extra_non_object_string_becomes_empty_dict():
    raw = {
        "workspace_id": "ws1",
        "source_id": "src",
        "external_id": "ext-2",
        "content": "x",
        "extra": "[]",
    }
    ev = PostParsedEvent.model_validate(raw)
    assert ev.extra == {}


@pytest.mark.unit
def test_post_parsed_reprocess_shape_no_published_at_key():
    """Как admin pipeline/reprocess после фикса: без ключа published_at."""
    raw = {
        "workspace_id": "ws1",
        "source_id": "src",
        "external_id": "42",
        "content": "text",
        "grouped_id": "",
        "has_media": "False",
        "media_urls": "[]",
        "linked_urls": "[]",
        "url": "https://t.me/ch/42",
        "author": "@ch",
    }
    ev = PostParsedEvent.model_validate(raw)
    assert ev.published_at is None


@pytest.mark.unit
def test_post_parsed_empty_published_at_becomes_none():
    """Redis stream shape may contain empty strings; event model normalizes them."""
    raw = {
        "workspace_id": "ws1",
        "source_id": "src",
        "external_id": "42",
        "content": "x",
        "published_at": "",
    }
    ev = PostParsedEvent.model_validate(raw)
    assert ev.published_at is None


@pytest.mark.unit
def test_post_parsed_extra_dict_passthrough():
    raw = {
        "workspace_id": "ws1",
        "source_id": "src",
        "external_id": "ext-3",
        "content": "y",
        "extra": {"k": 1},
    }
    ev = PostParsedEvent.model_validate(raw)
    assert ev.extra == {"k": 1}
