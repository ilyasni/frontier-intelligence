from types import SimpleNamespace

import pytest

from scripts import qdrant_backfill_versioned as backfill


class _FakeMatchValue:
    def __init__(self, *, value: str):
        self.value = value


class _FakeFieldCondition:
    def __init__(self, *, key: str, match=None, range=None):
        self.key = key
        self.match = match
        self.range = range


class _FakeFilter:
    def __init__(self, *, must):
        self.must = must


@pytest.fixture
def patched_qdrant_filter_models(monkeypatch):
    monkeypatch.setattr(backfill, "MatchValue", _FakeMatchValue)
    monkeypatch.setattr(backfill, "FieldCondition", _FakeFieldCondition)
    monkeypatch.setattr(backfill, "Filter", _FakeFilter)


def test_embedding_filter_uses_active_embedding_version(patched_qdrant_filter_models) -> None:
    settings = SimpleNamespace(
        qdrant_filter_embedding_version=True,
        gigachat_embeddings_model="EmbeddingsGigaR",
    )

    qdrant_filter = backfill._embedding_filter(settings)

    assert qdrant_filter is not None
    condition = qdrant_filter.must[0]
    assert condition.key == "embedding_version"
    assert condition.match.value == "EmbeddingsGigaR"


def test_embedding_filter_can_be_disabled() -> None:
    settings = SimpleNamespace(
        qdrant_filter_embedding_version=False,
        gigachat_embeddings_model="EmbeddingsGigaR",
    )

    assert backfill._embedding_filter(settings) is None


def test_selected_kinds_expands_all() -> None:
    assert backfill._selected_kinds("all") == ["documents", "trends"]
    assert backfill._selected_kinds("documents") == ["documents"]
