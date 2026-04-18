import sys
from datetime import UTC, datetime
from importlib.machinery import ModuleSpec
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

_neo4j_stub = ModuleType("neo4j")
_neo4j_stub.__spec__ = ModuleSpec("neo4j", loader=None)
_neo4j_stub.AsyncGraphDatabase = MagicMock()

with patch.dict(sys.modules, {"neo4j": _neo4j_stub}):
    from worker.tasks.reindex_task import ReindexTask


def test_reindex_document_includes_crawl_and_vision_text() -> None:
    task = ReindexTask.__new__(ReindexTask)
    task.settings = SimpleNamespace(gigachat_embeddings_model="EmbeddingsGigaR")
    bundle = {
        "post": {
            "id": "post-1",
            "workspace_id": "disruption",
            "source_id": "src",
            "content": "Original post about EV charging",
            "category": "mobility",
            "relevance_score": 0.9,
            "published_at": datetime(2026, 4, 1, tzinfo=UTC),
            "tags": ["charging"],
            "extra": {"url": "https://example.com/post", "lang": "en"},
            "semantic_cluster_id": "cluster-1",
            "source_extra": {"source_region": "global", "market_scope": "mobility"},
            "source_score": 0.7,
            "source_authority": 0.8,
        },
        "enrichments": {
            "concepts": {"items": [{"name": "EV charging", "category": "mobility", "weight": 4}]},
            "valence": {"valence": "positive", "signal_type": "growth"},
            "crawl": {
                "title": "Charging report",
                "description": "Infrastructure expands",
                "md_excerpt": "Long article body",
                "url": "https://example.com/report",
                "word_count": 1200,
            },
            "vision": {
                "all_labels": ["dashboard", "charging"],
                "ocr_text": "fast charging map",
                "items": [{"scene": "Map UI", "design_signals": ["range anxiety"]}],
            },
        },
    }

    index_text, payload, concepts, crawl_text = task._build_index_document(bundle)

    assert "Charging report" in index_text
    assert "vision ocr" in index_text
    assert "Long article body" in crawl_text
    assert payload["has_crawl"] is True
    assert payload["has_vision"] is True
    assert payload["indexed_content_sources"] == ["post", "crawl", "vision"]
    assert "dashboard" in payload["concepts"]
    assert "EV charging" in payload["concepts"]
    assert any(item["category"] == "vision" for item in concepts)
