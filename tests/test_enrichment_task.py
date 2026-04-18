import sys
from datetime import UTC, datetime
from importlib.machinery import ModuleSpec
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

_neo4j_stub = ModuleType("neo4j")
_neo4j_stub.__spec__ = ModuleSpec("neo4j", loader=None)
_neo4j_stub.AsyncGraphDatabase = MagicMock()

with patch.dict(sys.modules, {"neo4j": _neo4j_stub}):
    from worker.tasks.enrichment_task import EnrichmentTask

sys.modules.pop("worker.integrations.neo4j_client", None)


async def test_dropped_post_clears_graph_status_and_qdrant_id() -> None:
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        default_relevance_threshold=0.6,
        indexing_max_retries=5,
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.relevance = SimpleNamespace(
        run=AsyncMock(return_value={"relevant": False, "score": 0.12, "category": "noise"})
    )
    task.concept = SimpleNamespace(run=AsyncMock())
    task.gigachat = SimpleNamespace(embed=AsyncMock())
    task.qdrant = SimpleNamespace(delete_document=AsyncMock())
    task.neo4j = SimpleNamespace(upsert_concepts=AsyncMock())
    task._get_workspace = AsyncMock(
        return_value={
            "id": "disruption",
            "name": "Disruption",
            "categories": ["technology"],
            "relevance_weights": {"threshold": 0.6},
        }
    )
    task._get_source = AsyncMock(
        return_value={
            "id": "rss-source",
            "is_enabled": True,
            "source_type": "rss",
        }
    )
    task._validate_source_event = lambda event, source: None
    task._save_post = AsyncMock(return_value="post-1")
    task._update_indexing_status = AsyncMock()
    task._upsert_media_group = AsyncMock()
    task._update_post_enrichment = AsyncMock()
    task._get_existing_qdrant_id = AsyncMock(return_value="existing-point")

    await task.process_event(
        "1-0",
        {
            "workspace_id": "disruption",
            "source_id": "rss-source",
            "external_id": "42",
            "content": "irrelevant post",
            "has_media": False,
            "media_urls": [],
            "linked_urls": [],
        },
    )

    task.qdrant.delete_document.assert_awaited_once_with("post-1")
    assert task._update_indexing_status.await_args_list[0].args == ("post-1", "pending")
    assert task._update_indexing_status.await_args_list[-1].args == ("post-1", "dropped")
    assert task._update_indexing_status.await_args_list[-1].kwargs == {
        "qdrant_id": "",
        "graph_status": "skipped",
    }


async def test_relevant_post_writes_lang_valence_and_region_to_qdrant() -> None:
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        default_relevance_threshold=0.6,
        gigachat_embeddings_model="EmbeddingsGigaR",
        indexing_max_retries=5,
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.relevance = SimpleNamespace(
        run=AsyncMock(return_value={"relevant": True, "score": 0.91, "category": "technology"})
    )
    task.concept = SimpleNamespace(
        run=AsyncMock(
            return_value=[{"name": "battery passport", "category": "market", "weight": 4}]
        ),
        last_meta={},
    )
    task.valence = SimpleNamespace(
        run=AsyncMock(
            return_value={
                "valence": "negative",
                "signal_type": "closure",
                "confidence": 0.77,
                "reasoning": "plant pause",
            }
        ),
        last_meta={},
    )
    task.gigachat = SimpleNamespace(embed=AsyncMock(return_value=[0.1, 0.2, 0.3]))
    task.qdrant = SimpleNamespace(upsert_document=AsyncMock(), delete_document=AsyncMock())
    task.neo4j = SimpleNamespace(upsert_concepts=AsyncMock())
    task._get_workspace = AsyncMock(
        return_value={
            "id": "disruption",
            "name": "Disruption",
            "categories": ["technology"],
            "relevance_weights": {"threshold": 0.6},
        }
    )
    task._get_source = AsyncMock(
        return_value={
            "id": "rss-source",
            "is_enabled": True,
            "source_type": "rss",
            "source_score": 0.66,
            "source_authority": 0.74,
            "extra": {"source_region": "ru", "market_scope": "local"},
        }
    )
    task._validate_source_event = lambda event, source: None
    task._save_post = AsyncMock(return_value="post-2")
    task._update_indexing_status = AsyncMock()
    task._upsert_media_group = AsyncMock()
    task._update_post_enrichment = AsyncMock()
    task._update_post_tags = AsyncMock()
    task._save_enrichment = AsyncMock()

    await task.process_event(
        "2-0",
        {
            "workspace_id": "disruption",
            "source_id": "rss-source",
            "external_id": "43",
            "content": "Factory shutdown delays EV launch and battery passport rollout",
            "published_at": datetime.now(UTC).isoformat(),
            "has_media": False,
            "media_urls": [],
            "linked_urls": [],
            "extra": {"lang": "ru"},
        },
    )

    upsert_args = task.qdrant.upsert_document.await_args.args
    assert upsert_args[0] == "post-2"
    payload = upsert_args[2]
    assert payload["lang"] == "ru"
    assert payload["valence"] == "negative"
    assert payload["signal_type"] == "closure"
    assert payload["source_region"] == "ru"
    assert payload["market_scope"] == "local"
    task._save_enrichment.assert_any_await(
        "post-2",
        "valence",
        {
            "valence": "negative",
            "signal_type": "closure",
            "confidence": 0.77,
            "reasoning": "plant pause",
        },
    )


async def test_startup_reclaim_continues_after_deleted_pel_hole() -> None:
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(indexing_claim_idle_ms=600_000)
    task.redis = SimpleNamespace(
        xautoclaim=AsyncMock(
            side_effect=[
                ("1700000000000-0", []),
                ("0-0", [("3-0", {"source_id": "rss-source", "external_id": "99"})]),
            ]
        )
    )
    task._gather_process_bounded = AsyncMock()

    await task._startup_reclaim()

    assert task.redis.xautoclaim.await_count == 2
    task._gather_process_bounded.assert_awaited_once_with(
        [
            ("3-0", {"source_id": "rss-source", "external_id": "99"}),
        ]
    )


async def test_media_post_is_marked_skipped_when_vision_disabled() -> None:
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        default_relevance_threshold=0.6,
        gigachat_embeddings_model="EmbeddingsGigaR",
        indexing_max_retries=5,
        vision_enabled=False,
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.relevance = SimpleNamespace(
        run=AsyncMock(return_value={"relevant": False, "score": 0.11, "category": "noise"})
    )
    task.concept = SimpleNamespace(run=AsyncMock())
    task.gigachat = SimpleNamespace(embed=AsyncMock())
    task.qdrant = SimpleNamespace(delete_document=AsyncMock())
    task.neo4j = SimpleNamespace(upsert_concepts=AsyncMock())
    task._get_workspace = AsyncMock(
        return_value={
            "id": "disruption",
            "name": "Disruption",
            "categories": ["technology"],
            "relevance_weights": {"threshold": 0.6},
        }
    )
    task._get_source = AsyncMock(
        return_value={
            "id": "rss-source",
            "is_enabled": True,
            "source_type": "rss",
        }
    )
    task._validate_source_event = lambda event, source: None
    task._save_post = AsyncMock(return_value="post-vision-off")
    task._update_indexing_status = AsyncMock()
    task._upsert_media_group = AsyncMock()
    task._update_post_enrichment = AsyncMock()
    task._get_existing_qdrant_id = AsyncMock(return_value="")
    task._save_enrichment = AsyncMock()
    task._update_vision_status = AsyncMock()

    await task.process_event(
        "4-0",
        {
            "workspace_id": "disruption",
            "source_id": "rss-source",
            "external_id": "44",
            "content": "media post",
            "has_media": True,
            "media_urls": ["s3://bucket/image.jpg"],
            "linked_urls": [],
        },
    )

    task.redis.xadd.assert_not_awaited()
    task._save_enrichment.assert_any_await(
        "post-vision-off",
        "vision",
        {
            "vision_mode": "skip",
            "vision_skip_reason": "vision_disabled",
            "items": [],
            "all_labels": [],
            "ocr_text": "",
        },
    )
    task._update_vision_status.assert_awaited_once_with("post-vision-off", "skipped")
