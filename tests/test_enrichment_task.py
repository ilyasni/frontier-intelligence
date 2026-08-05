import sys
import logging
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
    # `workspace_id` добавлен 2026-08-05 вместе со счётчиком стадий
    # (frontier_pipeline_stage_total): без него у 39% дропа не было разреза по
    # воркспейсам, а сам дроп не существовал как метрика. Пробрасывается на всех
    # путях, где событие в области видимости; там, где нет, метка становится
    # `unknown` — см. note_pipeline_stage.
    assert task._update_indexing_status.await_args_list[-1].kwargs == {
        "qdrant_id": "",
        "graph_status": "skipped",
        "workspace_id": "disruption",
    }
    assert (
        task._update_indexing_status.await_args_list[0].kwargs["workspace_id"] == "disruption"
    )


async def test_disabled_source_event_is_acked_even_if_status_write_fails() -> None:
    """Regression: a disabled-source reject runs before _save_post, so the posts
    row is absent and the indexing_status write hits an FK violation. That must
    not skip the XACK — otherwise the message loops in the PEL forever."""
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        default_relevance_threshold=0.6,
        indexing_max_retries=5,
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task._get_workspace = AsyncMock(return_value={"id": "disruption", "name": "Disruption"})
    task._get_source = AsyncMock(
        return_value={"id": "rss-source", "is_enabled": False, "source_type": "rss"}
    )
    # Simulate the FK violation on the pre-save reject path.
    task._update_indexing_status = AsyncMock(side_effect=RuntimeError("fk violation"))
    task._save_post = AsyncMock()
    # Real _validate_source_event and _record_reject_status run (not stubbed).

    await task.process_event(
        "1-0",
        {
            "workspace_id": "disruption",
            "source_id": "rss-source",
            "external_id": "42",
            "content": "post from a disabled source",
            "has_media": False,
            "media_urls": [],
            "linked_urls": [],
        },
    )

    # Despite the status write raising, the message is acknowledged and never saved.
    task.redis.xack.assert_awaited_once_with("stream:posts:parsed", "enrichment_workers", "1-0")
    task._save_post.assert_not_awaited()


async def test_reclaim_drops_poison_message_past_delivery_cap() -> None:
    """Backstop: a message redelivered past indexing_max_deliveries is force-
    dropped to the DLQ (using the real PEL counter), not reprocessed forever."""
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        indexing_max_deliveries=20,
        indexing_dlq_stream="stream:posts:parsed:dlq",
    )
    task.redis = SimpleNamespace(
        xpending_range=AsyncMock(
            return_value=[
                {"message_id": "10-0", "times_delivered": 42},
                {"message_id": "20-0", "times_delivered": 2},
            ]
        ),
        xadd=AsyncMock(),
        xack=AsyncMock(),
    )
    task._record_reject_status = AsyncMock()

    messages = [
        ("10-0", {"source_id": "s", "external_id": "poison"}),
        ("20-0", {"source_id": "s", "external_id": "ok"}),
    ]
    fresh = await task._drop_poison_pending(messages)

    # Poison dropped to DLQ + acked; the healthy one is kept for processing.
    assert [mid for mid, _ in fresh] == ["20-0"]
    task.redis.xack.assert_awaited_once_with("stream:posts:parsed", "enrichment_workers", "10-0")
    assert task.redis.xadd.await_args.args[0] == "stream:posts:parsed:dlq"
    task._record_reject_status.assert_awaited_once()


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
            "_llm": {
                "provider": "",
                "requested_model": "",
                "actual_model": "",
            },
        },
    )


async def test_llm_failed_relevance_retries_instead_of_dropping() -> None:
    """status=='failed' (LLM недоступен) → re-XADD с retry_count, а не drop+ACK навсегда."""
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        default_relevance_threshold=0.6,
        indexing_max_retries=5,
        vision_enabled=False,
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.relevance = SimpleNamespace(
        run=AsyncMock(
            return_value={
                "relevant": False,
                "score": 0.0,
                "category": "other",
                "reasoning": "boom",
                "_provider": "wormsoft",
                "_requested_model": "wormsoft/agent/medium",
                "_actual_model": "",
                "_usage": None,
                "_llm_status": "failed",
                "_llm_skip_reason": "",
                "_llm_error": "boom",
            }
        )
    )
    task.concept = SimpleNamespace(run=AsyncMock(), last_meta={})
    task.valence = SimpleNamespace(run=AsyncMock(), last_meta={})
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
        return_value={"id": "rss-source", "is_enabled": True, "source_type": "rss"}
    )
    task._validate_source_event = lambda event, source: None
    task._save_post = AsyncMock(return_value="post-fail")
    task._update_indexing_status = AsyncMock()
    task._upsert_media_group = AsyncMock()
    task._update_post_enrichment = AsyncMock()
    task._get_existing_qdrant_id = AsyncMock(return_value="")
    task._use_joint_relevance_concepts = lambda event: False

    data = {
        "workspace_id": "disruption",
        "source_id": "rss-source",
        "external_id": "46",
        "content": "some post that failed to be judged",
        "has_media": False,
        "media_urls": [],
        "linked_urls": [],
    }
    await task.process_event("6-0", data)

    # Пост НЕ помечен dropped; вместо этого — re-XADD с retry_count=1
    dropped_calls = [
        c for c in task._update_indexing_status.await_args_list if c.args[1:2] == ("dropped",)
    ]
    assert dropped_calls == []
    task.qdrant.delete_document.assert_not_awaited()
    assert any(
        len(c.args) > 1 and isinstance(c.args[1], dict) and c.args[1].get("retry_count") == "1"
        for c in task.redis.xadd.await_args_list
    ), task.redis.xadd.await_args_list


async def test_not_called_relevance_still_drops() -> None:
    """status=='not_called' (пустой контент) — честный дроп, НЕ ретрай."""
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        default_relevance_threshold=0.6,
        indexing_max_retries=5,
        vision_enabled=False,
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.relevance = SimpleNamespace(
        run=AsyncMock(
            return_value={
                "relevant": False,
                "score": 0.0,
                "category": "other",
                "reasoning": "empty content",
                "_provider": "wormsoft",
                "_requested_model": "wormsoft/agent/medium",
                "_actual_model": "",
                "_usage": None,
                "_llm_status": "not_called",
                "_llm_skip_reason": "empty_content",
                "_llm_error": "",
            }
        )
    )
    task.concept = SimpleNamespace(run=AsyncMock(), last_meta={})
    task.valence = SimpleNamespace(run=AsyncMock(), last_meta={})
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
        return_value={"id": "rss-source", "is_enabled": True, "source_type": "rss"}
    )
    task._validate_source_event = lambda event, source: None
    task._save_post = AsyncMock(return_value="post-empty2")
    task._update_indexing_status = AsyncMock()
    task._upsert_media_group = AsyncMock()
    task._update_post_enrichment = AsyncMock()
    task._get_existing_qdrant_id = AsyncMock(return_value="")
    task._use_joint_relevance_concepts = lambda event: False

    await task.process_event(
        "7-0",
        {
            "workspace_id": "disruption",
            "source_id": "rss-source",
            "external_id": "47",
            "content": "",
            "has_media": False,
            "media_urls": [],
            "linked_urls": [],
        },
    )

    assert task._update_indexing_status.await_args_list[-1].args == ("post-empty2", "dropped")
    # Никаких re-XADD (ретраев) для честного дропа
    assert not any(
        (len(c.args) > 1 and isinstance(c.args[1], dict) and "retry_count" in c.args[1])
        for c in task.redis.xadd.await_args_list
    )


async def test_startup_reclaim_continues_after_deleted_pel_hole() -> None:
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        indexing_claim_idle_ms=600_000,
        indexing_max_deliveries=20,
        indexing_dlq_stream="stream:posts:parsed:dlq",
    )
    task.redis = SimpleNamespace(
        xautoclaim=AsyncMock(
            side_effect=[
                ("1700000000000-0", []),
                ("0-0", [("3-0", {"source_id": "rss-source", "external_id": "99"})]),
            ]
        ),
        xpending_range=AsyncMock(return_value=[]),
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


async def test_empty_content_relevance_logs_not_called_status(caplog) -> None:
    task = EnrichmentTask.__new__(EnrichmentTask)
    task.settings = SimpleNamespace(
        default_relevance_threshold=0.6,
        indexing_max_retries=5,
        vision_enabled=False,
    )
    task.redis = SimpleNamespace(xack=AsyncMock(), xadd=AsyncMock())
    task.relevance = SimpleNamespace(
        run=AsyncMock(
            return_value={
                "relevant": False,
                "score": 0.0,
                "category": "other",
                "reasoning": "empty content",
                "_provider": "wormsoft",
                "_requested_model": "wormsoft/agent/medium",
                "_actual_model": "",
                "_usage": None,
                "_llm_status": "not_called",
                "_llm_skip_reason": "empty_content",
                "_llm_error": "",
            }
        )
    )
    task.concept = SimpleNamespace(run=AsyncMock(), last_meta={})
    task.valence = SimpleNamespace(run=AsyncMock(), last_meta={})
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
    task._save_post = AsyncMock(return_value="post-empty")
    task._update_indexing_status = AsyncMock()
    task._upsert_media_group = AsyncMock()
    task._update_post_enrichment = AsyncMock()
    task._get_existing_qdrant_id = AsyncMock(return_value="")

    with caplog.at_level(logging.INFO, logger="worker.tasks.enrichment_task"):
        await task.process_event(
            "5-0",
            {
                "workspace_id": "disruption",
                "source_id": "rss-source",
                "external_id": "45",
                "content": "",
                "has_media": False,
                "media_urls": [],
                "linked_urls": [],
            },
        )

    assert "task=relevance" in caplog.text
    assert "status=not_called" in caplog.text
    assert "skip_reason=empty_content" in caplog.text
