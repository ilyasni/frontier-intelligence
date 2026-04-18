"""Vision-события из Redis Stream: нормализация + Pydantic model_validate (Context7 / pydantic.dev)."""
import pytest

from shared.events.posts_vision_v1 import PostVisionEvent, normalize_vision_stream_fields


@pytest.mark.unit
def test_media_s3_keys_from_json_string() -> None:
    raw = {
        "post_id": "p1",
        "workspace_id": "ws",
        "source_id": "src",
        "grouped_id": "",
        "media_s3_keys": '["media/a.jpg", "media/b.png"]',
        "album_item_index": "0",
        "album_total_items": "2",
    }
    n = normalize_vision_stream_fields(raw)
    assert n["media_s3_keys"] == ["media/a.jpg", "media/b.png"]
    assert n["album_item_index"] == 0
    assert n["album_total_items"] == 2


@pytest.mark.unit
def test_media_s3_keys_empty_string() -> None:
    n = normalize_vision_stream_fields({"media_s3_keys": ""})
    assert n["media_s3_keys"] == []


@pytest.mark.unit
def test_media_s3_keys_already_list() -> None:
    n = normalize_vision_stream_fields({"media_s3_keys": ["x", "y"]})
    assert n["media_s3_keys"] == ["x", "y"]


@pytest.mark.unit
def test_album_int_invalid_string_uses_default() -> None:
    n = normalize_vision_stream_fields(
        {"album_item_index": "nope", "album_total_items": "bad"}
    )
    assert n["album_item_index"] == 0
    assert n["album_total_items"] == 1


@pytest.mark.unit
def test_post_vision_event_model_validate_redis_payload() -> None:
    """Как vision_task: model_validate(raw dict из XREADGROUP)."""
    raw = {
        "post_id": "p1",
        "workspace_id": "ws",
        "source_id": "src",
        "grouped_id": "",
        "media_s3_keys": '["media/a.jpg"]',
        "album_item_index": "0",
        "album_total_items": "1",
        "noise_field_from_redis": "ignored",
    }
    event = PostVisionEvent.model_validate(raw)
    assert event.media_s3_keys == ["media/a.jpg"]
    assert event.album_total_items == 1


@pytest.mark.unit
def test_model_config_extra_ignore() -> None:
    minimal = {
        "post_id": "p",
        "workspace_id": "w",
        "source_id": "s",
        "grouped_id": "",
        "media_s3_keys": "[]",
        "unknown_stream_field": "x",
    }
    e = PostVisionEvent.model_validate(minimal)
    assert e.post_id == "p"
    assert not hasattr(e, "unknown_stream_field")


@pytest.mark.unit
def test_grouped_id_null_string_becomes_empty_string() -> None:
    event = PostVisionEvent.model_validate(
        {
            "post_id": "p",
            "workspace_id": "w",
            "source_id": "s",
            "grouped_id": "null",
            "media_s3_keys": "[]",
        }
    )

    assert event.grouped_id == ""
