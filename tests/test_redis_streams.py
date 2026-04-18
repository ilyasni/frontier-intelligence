from shared.redis_streams import DEFAULT_STREAM_GROUPS, _message_age_seconds


def test_message_age_seconds_handles_invalid_ids() -> None:
    assert _message_age_seconds("not-a-stream-id") == 0.0
    assert _message_age_seconds("") == 0.0


def test_default_stream_groups_include_reindex_and_crawl4ai_group() -> None:
    assert ("stream:posts:crawl", "crawl4ai_workers") in DEFAULT_STREAM_GROUPS
    assert ("stream:posts:reindex", "reindex_workers") in DEFAULT_STREAM_GROUPS
