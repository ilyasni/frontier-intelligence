from dataclasses import dataclass

import pytest

from shared.embedding_models import qdrant_collection_name_for_embedding
from worker.integrations import qdrant_client as qdrant_client_module


@dataclass
class _FakeMatchValue:
    value: str


@dataclass
class _FakeMatchAny:
    any: list[str]


@dataclass
class _FakeFieldCondition:
    key: str
    match: object | None = None
    range: object | None = None


@dataclass
class _FakeFilter:
    must: list[object]


@pytest.fixture
def patched_qdrant_models(monkeypatch):
    monkeypatch.setattr(qdrant_client_module, "MatchValue", _FakeMatchValue)
    monkeypatch.setattr(qdrant_client_module, "MatchAny", _FakeMatchAny)
    monkeypatch.setattr(qdrant_client_module, "FieldCondition", _FakeFieldCondition)
    monkeypatch.setattr(qdrant_client_module, "Filter", _FakeFilter)


def test_payload_filter_includes_embedding_version(patched_qdrant_models) -> None:
    payload_filter = qdrant_client_module._build_payload_filter(
        "disruption",
        embedding_version="EmbeddingsGigaR",
        lang="ru",
    )

    pairs = {
        condition.key: getattr(condition.match, "value", None)
        for condition in payload_filter.must
    }

    assert pairs["workspace_id"] == "disruption"
    assert pairs["embedding_version"] == "EmbeddingsGigaR"
    assert pairs["lang"] == "ru"


def test_trend_filter_includes_embedding_version(patched_qdrant_models) -> None:
    payload_filter = qdrant_client_module._build_trend_filter(
        "disruption",
        embedding_version="EmbeddingsGigaR",
        pipeline="stable",
    )

    pairs = {
        condition.key: getattr(condition.match, "value", None)
        for condition in payload_filter.must
    }

    assert pairs["workspace_id"] == "disruption"
    assert pairs["embedding_version"] == "EmbeddingsGigaR"
    assert pairs["pipeline"] == "stable"


def test_qdrant_collection_name_for_embedding_is_versioned() -> None:
    assert (
        qdrant_collection_name_for_embedding("frontier_docs", "EmbeddingsGigaR")
        == "frontier_docs__embeddingsgigar__dense_2560"
    )
