from datetime import UTC, datetime

import pytest

from ingest.sources.base import NormalizedSourceItem, StructuredSource


class _DummyStructuredSource(StructuredSource):
    def __init__(self) -> None:
        super().__init__(
            source_id="dummy",
            workspace_id="disruption",
            config={"source_type": "web"},
            redis=None,
            runtime_store=None,
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_structured_source_skips_duplicate_external_ids_within_one_fetch() -> None:
    source = _DummyStructuredSource()

    events = await source.fetch()

    assert len(events) == 1
    assert events[0].external_id == "same-id"
    seen_ids = source._checkpoint_updates["cursor_json"]["seen_external_ids"]
    assert seen_ids == ["same-id"]
