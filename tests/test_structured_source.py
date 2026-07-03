from datetime import UTC, datetime

import pytest

from ingest.sources.base import NormalizedSourceItem, StructuredSource


class _DummyStructuredSource(StructuredSource):
    def __init__(self, redis=None, runtime_store=None) -> None:
        super().__init__(
            source_id="dummy",
            workspace_id="disruption",
            config={"source_type": "web"},
            redis=redis,
            runtime_store=runtime_store,
        )

    async def fetch_index(self) -> list[dict[str, str]]:
        return [
            {"external_id": "same-id", "title": "First"},
            {"external_id": "same-id", "title": "Second"},
        ]

    async def normalize_item(self, raw_item: dict[str, str]) -> NormalizedSourceItem | None:
        return NormalizedSourceItem(
            external_id=raw_item["external_id"],
            url=f"https://example.com/{raw_item['external_id']}",
            title=raw_item["title"],
            content=raw_item["title"],
            published_at=datetime(2026, 5, 31, tzinfo=UTC),
        )


class _MultiItemSource(StructuredSource):
    """Yields two distinct items so a partial publish can be exercised."""

    def __init__(self, redis, runtime_store) -> None:
        super().__init__(
            source_id="multi",
            workspace_id="disruption",
            config={"source_type": "web"},
            redis=redis,
            runtime_store=runtime_store,
        )

    async def fetch_index(self) -> list[dict[str, str]]:
        return [{"external_id": "a"}, {"external_id": "b"}]

    async def normalize_item(self, raw_item: dict[str, str]) -> NormalizedSourceItem | None:
        return NormalizedSourceItem(
            external_id=raw_item["external_id"],
            url=f"https://example.com/{raw_item['external_id']}",
            title=raw_item["external_id"],
            content=raw_item["external_id"],
            published_at=datetime(2026, 5, 31, tzinfo=UTC),
        )


class _FakeRuntimeStore:
    def __init__(self) -> None:
        self.upsert_calls: list[dict] = []

    async def load_checkpoint(self, source_id: str) -> dict:
        return {}

    async def start_run(self, source_id: str) -> str:
        return "run-1"

    async def upsert_checkpoint(self, **kwargs) -> None:
        self.upsert_calls.append(kwargs)

    async def finish_run(self, run_id, **kwargs) -> None:
        self.finish = kwargs


class _FlakyRedis:
    """xadd fails for a chosen external_id, succeeds otherwise."""

    def __init__(self, fail_for: str) -> None:
        self._fail_for = fail_for
        self.pushed: list[str] = []

    async def xadd(self, stream, payload) -> None:
        if payload.get("external_id") == self._fail_for:
            raise RuntimeError("stream unavailable")
        self.pushed.append(payload["external_id"])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_structured_source_skips_duplicate_external_ids_within_one_fetch() -> None:
    source = _DummyStructuredSource()

    events = await source.fetch()

    assert len(events) == 1
    assert events[0].external_id == "same-id"
    seen_ids = source._checkpoint_updates["cursor_json"]["seen_external_ids"]
    assert seen_ids == ["same-id"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cursor_not_persisted_when_publish_partially_fails() -> None:
    store = _FakeRuntimeStore()
    redis = _FlakyRedis(fail_for="b")  # second event fails to publish
    source = _MultiItemSource(redis=redis, runtime_store=store)

    pushed = await source.run()

    # Only one of two events made it to the stream → partial publish.
    assert pushed == 1
    # The run must record a failure, not success, and must NOT persist the advanced
    # cursor (no cursor_json/last_success_at written) so the next run re-reads item "b".
    assert len(store.upsert_calls) == 1
    call = store.upsert_calls[0]
    assert call.get("last_success_at") is None
    assert "cursor_json" not in call or call.get("cursor_json") is None
    assert call.get("last_error")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cursor_persisted_when_all_events_publish() -> None:
    store = _FakeRuntimeStore()
    redis = _FlakyRedis(fail_for="")  # nothing fails
    source = _MultiItemSource(redis=redis, runtime_store=store)

    pushed = await source.run()

    assert pushed == 2
    call = store.upsert_calls[0]
    assert call.get("last_success_at") is not None
    assert call["cursor_json"]["seen_external_ids"] == ["a", "b"]
