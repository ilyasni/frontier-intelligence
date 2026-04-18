from shared.events.posts_parsed_v1 import PostParsedEvent


def test_posts_parsed_event_coerces_null_published_at_to_none() -> None:
    event = PostParsedEvent(
        workspace_id="disruption",
        source_id="src",
        external_id="1",
        content="hello",
        published_at="null",
    )

    assert event.published_at is None


def test_posts_parsed_event_coerces_empty_published_at_to_none() -> None:
    event = PostParsedEvent(
        workspace_id="disruption",
        source_id="src",
        external_id="1",
        content="hello",
        published_at="",
    )

    assert event.published_at is None


def test_posts_parsed_event_coerces_null_grouped_id_to_none() -> None:
    event = PostParsedEvent(
        workspace_id="disruption",
        source_id="src",
        external_id="1",
        content="hello",
        grouped_id="null",
    )

    assert event.grouped_id is None
