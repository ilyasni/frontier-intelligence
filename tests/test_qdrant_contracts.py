from dataclasses import dataclass
from types import SimpleNamespace

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


class _FakeAsyncQdrantClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def test_qdrant_client_prefers_versioned_seed_collections_when_aliases_enabled(
    monkeypatch,
) -> None:
    monkeypatch.setattr(qdrant_client_module, "AsyncQdrantClient", _FakeAsyncQdrantClient)
    monkeypatch.setattr(
        qdrant_client_module,
        "get_settings",
        lambda: SimpleNamespace(
            qdrant_url="http://qdrant:6333",
            qdrant_collection="frontier_docs",
            qdrant_collection_alias="frontier_docs_active",
            qdrant_trends_collection="trend_clusters",
            qdrant_trends_collection_alias="trend_clusters_active",
            qdrant_filter_embedding_version=True,
            qdrant_enforce_collection_schema=True,
            gigachat_embeddings_model="EmbeddingsGigaR",
            sparse_vectors_enabled=True,
            embed_dim=2560,
        ),
    )

    client = qdrant_client_module.QdrantFrontierClient()

    assert client.collection == "frontier_docs_active"
    assert client.trends_collection == "trend_clusters_active"
    assert client._document_seed_collection() == "frontier_docs__embeddingsgigar__dense_2560"
    assert client._trend_seed_collection() == "trend_clusters__embeddingsgigar__dense_2560"


def test_qdrant_client_uses_base_collections_without_aliases(monkeypatch) -> None:
    monkeypatch.setattr(qdrant_client_module, "AsyncQdrantClient", _FakeAsyncQdrantClient)
    monkeypatch.setattr(
        qdrant_client_module,
        "get_settings",
        lambda: SimpleNamespace(
            qdrant_url="http://qdrant:6333",
            qdrant_collection="frontier_docs",
            qdrant_collection_alias="",
            qdrant_trends_collection="trend_clusters",
            qdrant_trends_collection_alias="",
            qdrant_filter_embedding_version=True,
            qdrant_enforce_collection_schema=True,
            gigachat_embeddings_model="EmbeddingsGigaR",
            sparse_vectors_enabled=True,
            embed_dim=2560,
        ),
    )

    client = qdrant_client_module.QdrantFrontierClient()

    assert client.collection == "frontier_docs"
    assert client.trends_collection == "trend_clusters"
    assert client._document_seed_collection() == "frontier_docs"
    assert client._trend_seed_collection() == "trend_clusters"
