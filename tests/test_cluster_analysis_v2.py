from datetime import UTC, datetime, timedelta

from worker.services.semantic_clustering import (
    ClusterPost,
    _april_fools_penalty,
    _components,
    _detect_change_points,
    _digest,
    _golden_metrics,
    _merge_cluster_settings,
    _merge_semantic_candidates,
    _merge_signal_candidates,
    _representative,
    _semantic_identity,
    _signal_results,
    _temporal_metrics,
    _trend_cluster_index_points,
)


def _post(
    post_id: str,
    source_id: str,
    hours_ago: int,
    relevance: float,
    source_score: float,
    vector: list[float],
    tags=None,
    title="Cluster title",
    published_at: datetime | None = None,
) -> ClusterPost:
    return ClusterPost(
        post_id=post_id,
        workspace_id="disruption",
        source_id=source_id,
        content=title,
        published_at=published_at or datetime.now(UTC) - timedelta(hours=hours_ago),
        relevance_score=relevance,
        source_score=source_score,
        tags=tags or ["ai"],
        title=title,
        url="https://example.com/" + post_id,
        vector=vector,
    )


def test_representative_penalizes_duplicate_source_bias() -> None:
    centroid = [1.0, 0.0]
    posts = [
        _post("a", "same-source", 1, 0.82, 0.7, [1.0, 0.0], title="Repeated source"),
        _post("b", "same-source", 2, 0.81, 0.7, [1.0, 0.0], title="Repeated source alt"),
        _post("c", "independent", 3, 0.8, 0.72, [1.0, 0.0], title="Independent source"),
    ]

    representative = _representative(posts, centroid)

    assert representative.post_id == "c"


def test_semantic_identity_reuses_existing_cluster_on_doc_overlap() -> None:
    group = [
        _post("post-1", "s1", 1, 0.9, 0.8, [1.0, 0.0]),
        _post("post-2", "s2", 2, 0.88, 0.7, [0.99, 0.01]),
    ]
    cluster_key, existing_id, related = _semantic_identity(
        group,
        group[0],
        ["ai", "agents"],
        [
            {
                "id": "semantic:existing",
                "cluster_key": "disruption:known",
                "doc_ids": ["post-2", "post-9"],
                "title": "Cluster title",
                "top_concepts": ["ai", "agents"],
            }
        ],
    )

    assert cluster_key == "disruption:known"
    assert existing_id == "semantic:existing"
    assert related == ["semantic:existing"]


def test_components_do_not_merge_distant_vectors() -> None:
    groups = _components(
        [
            _post("p1", "s1", 1, 0.8, 0.6, [1.0, 0.0]),
            _post("p2", "s2", 1, 0.8, 0.6, [0.99, 0.01]),
            _post("p3", "s3", 1, 0.8, 0.6, [0.0, 1.0]),
        ],
        threshold=0.92,
        max_gap_h=24,
    )

    assert sorted(len(group) for group in groups) == [1, 2]


def test_signal_results_produce_stable_and_emerging_layers() -> None:
    semantic = [
        {
            "cluster_id": "semantic-1",
            "cluster_key": _digest("semantic-1", "ws"),
            "workspace_id": "disruption",
            "title": "AI browser agent",
            "representative_post_id": "p1",
            "post_count": 2,
            "source_count": 2,
            "doc_ids": ["p1", "p2"],
            "source_ids": ["s1", "s2"],
            "top_concepts": ["ai", "browser"],
            "evidence": [
                {"post_id": "p1", "source_id": "s1"},
                {"post_id": "p2", "source_id": "s2"},
            ],
            "posts": [
                _post("p1", "s1", 2, 0.9, 0.8, [1.0, 0.0], title="AI browser agent"),
                _post("p2", "s2", 4, 0.84, 0.75, [0.98, 0.02], title="AI browser agent"),
            ],
            "avg_relevance": 0.87,
            "avg_source_score": 0.775,
            "first_seen_at": datetime.now(UTC) - timedelta(hours=4),
            "last_seen_at": datetime.now(UTC) - timedelta(hours=2),
            "centroid": [0.99, 0.01],
            "explainability": {"top_terms": ["ai", "browser"]},
        },
        {
            "cluster_id": "semantic-2",
            "cluster_key": _digest("semantic-2", "ws"),
            "workspace_id": "disruption",
            "title": "AI browser workflow",
            "representative_post_id": "p3",
            "post_count": 2,
            "source_count": 2,
            "doc_ids": ["p3", "p4"],
            "source_ids": ["s3", "s4"],
            "top_concepts": ["ai", "workflow"],
            "evidence": [
                {"post_id": "p3", "source_id": "s3"},
                {"post_id": "p4", "source_id": "s4"},
            ],
            "posts": [
                _post("p3", "s3", 6, 0.82, 0.76, [0.97, 0.03], title="AI browser workflow"),
                _post("p4", "s4", 8, 0.8, 0.73, [0.96, 0.04], title="AI browser workflow"),
            ],
            "avg_relevance": 0.81,
            "avg_source_score": 0.745,
            "first_seen_at": datetime.now(UTC) - timedelta(hours=8),
            "last_seen_at": datetime.now(UTC) - timedelta(hours=6),
            "centroid": [0.965, 0.035],
            "explainability": {"top_terms": ["ai", "workflow"]},
        },
        {
            "cluster_id": "semantic-3",
            "cluster_key": _digest("semantic-3", "ws"),
            "workspace_id": "disruption",
            "title": "Quantum interface experiment",
            "representative_post_id": "p5",
            "post_count": 2,
            "source_count": 1,
            "doc_ids": ["p5", "p6"],
            "source_ids": ["s5"],
            "top_concepts": ["quantum", "interface"],
            "evidence": [
                {"post_id": "p5", "source_id": "s5"},
                {"post_id": "p6", "source_id": "s5"},
            ],
            "posts": [
                _post("p5", "s5", 2, 0.75, 0.6, [0.0, 1.0], title="Quantum interface experiment"),
                _post("p6", "s5", 3, 0.73, 0.6, [0.01, 0.99], title="Quantum interface experiment"),
            ],
            "avg_relevance": 0.74,
            "avg_source_score": 0.6,
            "first_seen_at": datetime.now(UTC) - timedelta(hours=3),
            "last_seen_at": datetime.now(UTC) - timedelta(hours=2),
            "centroid": [0.005, 0.995],
            "explainability": {"top_terms": ["quantum", "interface"]},
        },
    ]

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    signal_series_by_id = {
        "semantic-1": [
            {
                "window_start": now - timedelta(hours=72),
                "window_end": now - timedelta(hours=48),
                "doc_count": 1,
                "avg_relevance": 0.8,
                "avg_source_score": 0.7,
                "freshness_score": 0.5,
            },
            {
                "window_start": now - timedelta(hours=48),
                "window_end": now - timedelta(hours=24),
                "doc_count": 1,
                "avg_relevance": 0.82,
                "avg_source_score": 0.72,
                "freshness_score": 0.7,
            },
            {
                "window_start": now - timedelta(hours=24),
                "window_end": now,
                "doc_count": 3,
                "avg_relevance": 0.88,
                "avg_source_score": 0.78,
                "freshness_score": 1.0,
            },
        ],
        "semantic-2": [
            {
                "window_start": now - timedelta(hours=72),
                "window_end": now - timedelta(hours=48),
                "doc_count": 1,
                "avg_relevance": 0.78,
                "avg_source_score": 0.7,
                "freshness_score": 0.5,
            },
            {
                "window_start": now - timedelta(hours=48),
                "window_end": now - timedelta(hours=24),
                "doc_count": 1,
                "avg_relevance": 0.8,
                "avg_source_score": 0.72,
                "freshness_score": 0.7,
            },
            {
                "window_start": now - timedelta(hours=24),
                "window_end": now,
                "doc_count": 4,
                "avg_relevance": 0.84,
                "avg_source_score": 0.76,
                "freshness_score": 1.0,
            },
        ],
        "semantic-3": [
            {
                "window_start": now - timedelta(hours=24),
                "window_end": now,
                "doc_count": 2,
                "avg_relevance": 0.74,
                "avg_source_score": 0.6,
                "freshness_score": 1.0,
            },
        ],
    }

    stable, emerging = _signal_results(
        semantic,
        [],
        [],
        {
            "trend_cluster_similarity_threshold": 0.87,
            "trend_cluster_max_gap_hours": 24 * 30,
            "trend_cluster_min_semantic_clusters": 2,
            "trend_cluster_min_docs": 4,
            "trend_cluster_stable_threshold": 0.58,
            "trend_cluster_emerging_threshold": 0.7,
            "trend_cluster_min_source_diversity": 0.2,
            "cluster_min_evidence_count": 2,
            "signal_short_window_hours": 24,
            "signal_baseline_window_days": 14,
            "signal_velocity_weight": 0.14,
            "signal_acceleration_weight": 0.1,
            "change_point_method": "window",
            "change_point_penalty": "auto",
            "change_point_min_size": 2,
            "change_point_jump": 1,
            "change_point_recent_hours": 48,
            "signal_merge_similarity_threshold": 0.72,
            "signal_merge_doc_overlap_threshold": 0.25,
            "signal_min_source_count": 1,
        },
        signal_series_by_id=signal_series_by_id,
    )

    assert any(item["signal_stage"] == "stable" for item in stable)
    assert any(item["signal_stage"] in {"weak", "emerging"} for item in emerging)


def test_trend_cluster_index_points_are_qdrant_payload_ready() -> None:
    now = datetime.now(UTC)
    points = _trend_cluster_index_points(
        "run-1",
        [
            {
                "signal_id": "trend-1",
                "signal_key": "signal-key-1",
                "workspace_id": "disruption",
                "title": "AI browser agent",
                "signal_stage": "stable",
                "signal_score": 0.82,
                "burst_score": 0.71,
                "coherence_score": 0.9,
                "novelty_score": 0.4,
                "source_diversity_score": 0.5,
                "freshness_score": 0.6,
                "evidence_strength_score": 0.7,
                "velocity_score": 0.1,
                "acceleration_score": 0.05,
                "baseline_rate": 0.02,
                "current_rate": 0.08,
                "change_point_count": 1,
                "change_point_strength": 0.2,
                "has_recent_change_point": True,
                "semantic_cluster_ids": ["semantic-1"],
                "doc_ids": ["post-1", "post-2"],
                "source_ids": ["source-1", "source-2"],
                "source_count": 2,
                "keywords": ["ai", "browser"],
                "evidence": [{"title": "AI browser agent ships", "published_at": now}],
                "first_seen_at": now,
                "last_seen_at": now,
                "centroid": [0.1, 0.2, 0.3],
            }
        ],
    )

    assert points[0]["cluster_id"] == "trend-1"
    assert points[0]["dense_vector"] == [0.1, 0.2, 0.3]
    payload = points[0]["payload"]
    assert payload["workspace_id"] == "disruption"
    assert payload["doc_count"] == 2
    assert payload["first_seen_at"] == now.isoformat()
    assert "AI browser agent" in payload["index_text"]


def test_golden_metrics_reads_fixture() -> None:
    metrics = _golden_metrics(
        {"post-1": "cluster-a", "post-2": "cluster-a", "post-3": "cluster-b"},
        {"semantic-1": "trend-a", "semantic-2": "trend-a"},
    )

    assert metrics["same_story_accuracy"] == 1.0
    assert metrics["different_story_accuracy"] == 1.0
    assert metrics["same_trend_accuracy"] == 1.0


def test_merge_cluster_settings_applies_workspace_overrides() -> None:
    merged = _merge_cluster_settings(
        {
            "semantic_dedupe_similarity_threshold": 0.92,
            "trend_cluster_min_docs": 4,
        },
        {
            "semantic_dedupe_similarity_threshold": 0.89,
            "trend_cluster_min_docs": 3,
            "unknown_key": 999,
        },
    )

    assert merged == {
        "semantic_dedupe_similarity_threshold": 0.89,
        "trend_cluster_min_docs": 3,
    }


def test_temporal_metrics_capture_recent_rise() -> None:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    series = [
        {
            "window_start": now - timedelta(hours=72),
            "window_end": now - timedelta(hours=48),
            "doc_count": 1,
        },
        {
            "window_start": now - timedelta(hours=48),
            "window_end": now - timedelta(hours=24),
            "doc_count": 1,
        },
        {"window_start": now - timedelta(hours=24), "window_end": now, "doc_count": 6},
    ]

    metrics = _temporal_metrics(
        series,
        {
            "signal_short_window_hours": 24,
            "signal_baseline_window_days": 14,
            "change_point_method": "window",
            "change_point_penalty": "auto",
            "change_point_min_size": 2,
            "change_point_jump": 1,
            "change_point_recent_hours": 48,
        },
    )

    assert metrics["current_rate"] > metrics["baseline_rate"]
    assert metrics["velocity_score"] > 0
    assert metrics["acceleration_score"] > 0


def test_change_point_detector_does_not_false_positive_flat_series() -> None:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    series = [
        {
            "window_start": now - timedelta(hours=96),
            "window_end": now - timedelta(hours=72),
            "doc_count": 2,
        },
        {
            "window_start": now - timedelta(hours=72),
            "window_end": now - timedelta(hours=48),
            "doc_count": 2,
        },
        {
            "window_start": now - timedelta(hours=48),
            "window_end": now - timedelta(hours=24),
            "doc_count": 2,
        },
        {"window_start": now - timedelta(hours=24), "window_end": now, "doc_count": 2},
    ]

    metrics = _detect_change_points(
        series,
        {
            "change_point_method": "window",
            "change_point_penalty": "auto",
            "change_point_min_size": 2,
            "change_point_jump": 1,
            "change_point_recent_hours": 48,
        },
    )

    assert metrics["change_point_strength"] == 0.0
    assert metrics["breakpoints"] == []


def test_merge_signal_candidates_merges_neighboring_duplicates() -> None:
    first = {
        "existing_id": "trend-1",
        "workspace_id": "ai_trends",
        "title": "AI browser agent workflow",
        "doc_ids": ["p1", "p2", "p3"],
        "semantic_cluster_ids": ["s1", "s2"],
        "keywords": ["ai", "browser", "agent"],
        "signal_score": 0.8,
        "source_ids": ["src1", "src2"],
        "source_count": 2,
        "evidence": [{"post_id": "p1"}],
        "explainability": {},
    }
    second = {
        "existing_id": "trend-2",
        "workspace_id": "ai_trends",
        "title": "AI browser agents",
        "doc_ids": ["p2", "p3", "p4"],
        "semantic_cluster_ids": ["s2", "s3"],
        "keywords": ["ai", "browser", "workflow"],
        "signal_score": 0.7,
        "source_ids": ["src2", "src3"],
        "source_count": 2,
        "evidence": [{"post_id": "p4"}],
        "explainability": {},
    }

    merged, merged_count = _merge_signal_candidates(
        [first, second],
        {
            "signal_merge_similarity_threshold": 0.72,
            "signal_merge_doc_overlap_threshold": 0.25,
        },
    )

    assert merged_count == 1
    assert len(merged) == 1
    assert set(merged[0]["doc_ids"]) == {"p1", "p2", "p3", "p4"}


def test_merge_semantic_candidates_merges_close_ai_clusters() -> None:
    first_posts = [
        _post("a1", "s1", 4, 0.84, 0.7, [1.0, 0.0], title="AI browser agent workflow"),
        _post("a2", "s2", 6, 0.82, 0.68, [0.99, 0.01], title="AI browser agent workflow"),
    ]
    second_posts = [
        _post("b1", "s3", 8, 0.83, 0.69, [0.98, 0.02], title="AI browser agents for workflows"),
        _post("b2", "s4", 10, 0.8, 0.67, [0.97, 0.03], title="AI browser agents for workflows"),
    ]
    semantic = [
        {
            "cluster_id": "semantic-a",
            "cluster_key": "semantic-a",
            "workspace_id": "ai_trends",
            "title": "AI browser agent workflow",
            "representative_post_id": "a1",
            "post_count": 2,
            "source_count": 2,
            "doc_ids": ["a1", "a2"],
            "source_ids": ["s1", "s2"],
            "top_concepts": ["ai", "browser", "agent"],
            "evidence": [{"post_id": "a1", "source_id": "s1"}],
            "representative_evidence": {"post_id": "a1", "source_id": "s1"},
            "related_cluster_ids": [],
            "avg_relevance": 0.83,
            "avg_source_score": 0.69,
            "freshness_score": 0.8,
            "coherence_score": 0.98,
            "first_seen_at": min(p.published_at for p in first_posts),
            "last_seen_at": max(p.published_at for p in first_posts),
            "centroid": [0.995, 0.005],
            "posts": first_posts,
            "explainability": {
                "top_terms": ["ai", "browser", "agent"],
                "top_concepts": ["ai", "browser", "agent"],
            },
        },
        {
            "cluster_id": "semantic-b",
            "cluster_key": "semantic-b",
            "workspace_id": "ai_trends",
            "title": "AI browser agents for workflows",
            "representative_post_id": "b1",
            "post_count": 2,
            "source_count": 2,
            "doc_ids": ["b1", "b2"],
            "source_ids": ["s3", "s4"],
            "top_concepts": ["ai", "browser", "workflow"],
            "evidence": [{"post_id": "b1", "source_id": "s3"}],
            "representative_evidence": {"post_id": "b1", "source_id": "s3"},
            "related_cluster_ids": [],
            "avg_relevance": 0.815,
            "avg_source_score": 0.68,
            "freshness_score": 0.78,
            "coherence_score": 0.97,
            "first_seen_at": min(p.published_at for p in second_posts),
            "last_seen_at": max(p.published_at for p in second_posts),
            "centroid": [0.975, 0.025],
            "posts": second_posts,
            "explainability": {
                "top_terms": ["ai", "browser", "workflow"],
                "top_concepts": ["ai", "browser", "workflow"],
            },
        },
    ]

    merged, merged_count = _merge_semantic_candidates(
        semantic,
        {
            "semantic_merge_enabled": True,
            "semantic_merge_similarity_threshold": 0.68,
            "semantic_merge_title_overlap_threshold": 0.34,
            "semantic_merge_concept_overlap_threshold": 0.45,
            "semantic_merge_max_gap_hours": 240,
        },
    )

    assert merged_count == 1
    assert len(merged) == 1
    assert set(merged[0]["doc_ids"]) == {"a1", "a2", "b1", "b2"}


def test_signal_results_can_drop_weak_noise_when_disabled() -> None:
    semantic = [
        {
            "cluster_id": "semantic-weak",
            "cluster_key": _digest("semantic-weak", "ws"),
            "workspace_id": "ai_trends",
            "title": "Generic AI mention",
            "representative_post_id": "w1",
            "post_count": 1,
            "source_count": 1,
            "doc_ids": ["w1"],
            "source_ids": ["s1"],
            "top_concepts": ["ai"],
            "evidence": [{"post_id": "w1", "source_id": "s1"}],
            "posts": [_post("w1", "s1", 12, 0.63, 0.42, [1.0, 0.0], title="Generic AI mention")],
            "avg_relevance": 0.63,
            "avg_source_score": 0.42,
            "first_seen_at": datetime.now(UTC) - timedelta(hours=12),
            "last_seen_at": datetime.now(UTC) - timedelta(hours=12),
            "centroid": [1.0, 0.0],
            "explainability": {"top_terms": ["generic", "ai"]},
        }
    ]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    signal_series_by_id = {
        "semantic-weak": [
            {
                "window_start": now - timedelta(hours=24),
                "window_end": now,
                "doc_count": 1,
                "avg_relevance": 0.63,
                "avg_source_score": 0.42,
                "freshness_score": 0.7,
            },
        ]
    }

    stable, emerging = _signal_results(
        semantic,
        [],
        [],
        {
            "trend_cluster_similarity_threshold": 0.87,
            "trend_cluster_max_gap_hours": 24 * 30,
            "trend_cluster_min_semantic_clusters": 2,
            "trend_cluster_min_docs": 4,
            "trend_cluster_stable_threshold": 0.58,
            "trend_cluster_emerging_threshold": 0.7,
            "trend_cluster_min_source_diversity": 0.2,
            "cluster_min_evidence_count": 2,
            "signal_short_window_hours": 24,
            "signal_baseline_window_days": 14,
            "signal_velocity_weight": 0.14,
            "signal_acceleration_weight": 0.1,
            "change_point_method": "window",
            "change_point_penalty": "auto",
            "change_point_min_size": 2,
            "change_point_jump": 1,
            "change_point_recent_hours": 48,
            "signal_merge_similarity_threshold": 0.72,
            "signal_merge_doc_overlap_threshold": 0.25,
            "signal_min_source_count": 1,
            "persist_weak_signals": False,
            "weak_signal_min_score": 0.54,
            "weak_signal_min_confidence": 0.6,
            "weak_signal_min_source_diversity": 0.32,
            "weak_signal_min_source_count": 2,
        },
        signal_series_by_id=signal_series_by_id,
    )

    assert stable == []
    assert emerging == []


def test_april_fools_penalty_flags_explicit_prank_posts() -> None:
    april_fools_at = datetime(2026, 4, 1, 12, tzinfo=UTC)
    posts = [
        _post(
            "p1",
            "s1",
            2,
            0.8,
            0.7,
            [1.0, 0.0],
            title="April Fools prank: new AGI toaster",
            published_at=april_fools_at,
        ),
        _post(
            "p2",
            "s2",
            3,
            0.81,
            0.72,
            [0.98, 0.02],
            title="Шутка 1 апреля про AGI toaster",
            published_at=april_fools_at + timedelta(hours=1),
        ),
    ]

    penalty = _april_fools_penalty(
        posts,
        {
            "april_fools_guard_enabled": True,
            "april_fools_guard_penalty": 0.45,
            "april_fools_guard_stage_block_ratio": 0.34,
        },
    )

    assert penalty["flagged_post_ids"] == ["p1", "p2"]
    assert penalty["flagged_ratio"] == 1.0
    assert penalty["stage_blocked"] is True
    assert penalty["score_multiplier"] < 1.0


def test_signal_results_demote_april_fools_cluster_to_weak() -> None:
    april_fools_at = datetime(2026, 4, 1, 12, tzinfo=UTC)
    semantic = [
        {
            "cluster_id": "semantic-prank-a",
            "cluster_key": _digest("semantic-prank-a", "ws"),
            "workspace_id": "disruption",
            "title": "April Fools AI agent prank",
            "representative_post_id": "p1",
            "post_count": 2,
            "source_count": 2,
            "doc_ids": ["p1", "p2"],
            "source_ids": ["s1", "s2"],
            "top_concepts": ["ai", "agent", "prank"],
            "evidence": [
                {"post_id": "p1", "source_id": "s1"},
                {"post_id": "p2", "source_id": "s2"},
            ],
            "posts": [
                _post(
                    "p1",
                    "s1",
                    2,
                    0.9,
                    0.8,
                    [1.0, 0.0],
                    title="April Fools AI agent prank",
                    published_at=april_fools_at,
                ),
                _post(
                    "p2",
                    "s2",
                    4,
                    0.87,
                    0.79,
                    [0.99, 0.01],
                    title="1 April joke about AI agent launch",
                    published_at=april_fools_at + timedelta(hours=1),
                ),
            ],
            "avg_relevance": 0.885,
            "avg_source_score": 0.795,
            "first_seen_at": datetime.now(UTC) - timedelta(hours=4),
            "last_seen_at": datetime.now(UTC) - timedelta(hours=2),
            "centroid": [0.995, 0.005],
            "explainability": {"top_terms": ["april", "fools", "ai"]},
        },
        {
            "cluster_id": "semantic-prank-b",
            "cluster_key": _digest("semantic-prank-b", "ws"),
            "workspace_id": "disruption",
            "title": "Satire: autonomous AI office launch",
            "representative_post_id": "p3",
            "post_count": 2,
            "source_count": 2,
            "doc_ids": ["p3", "p4"],
            "source_ids": ["s3", "s4"],
            "top_concepts": ["ai", "office", "satire"],
            "evidence": [
                {"post_id": "p3", "source_id": "s3"},
                {"post_id": "p4", "source_id": "s4"},
            ],
            "posts": [
                _post(
                    "p3",
                    "s3",
                    6,
                    0.86,
                    0.77,
                    [0.98, 0.02],
                    title="Satire: autonomous AI office launch",
                    published_at=april_fools_at + timedelta(hours=2),
                ),
                _post(
                    "p4",
                    "s4",
                    8,
                    0.84,
                    0.76,
                    [0.97, 0.03],
                    title="April fools autonomous office rollout",
                    published_at=april_fools_at + timedelta(hours=3),
                ),
            ],
            "avg_relevance": 0.85,
            "avg_source_score": 0.765,
            "first_seen_at": datetime.now(UTC) - timedelta(hours=8),
            "last_seen_at": datetime.now(UTC) - timedelta(hours=6),
            "centroid": [0.975, 0.025],
            "explainability": {"top_terms": ["satire", "office", "ai"]},
        },
    ]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    signal_series_by_id = {
        "semantic-prank-a": [
            {
                "window_start": now - timedelta(hours=72),
                "window_end": now - timedelta(hours=48),
                "doc_count": 1,
                "avg_relevance": 0.82,
                "avg_source_score": 0.74,
                "freshness_score": 0.5,
            },
            {
                "window_start": now - timedelta(hours=48),
                "window_end": now - timedelta(hours=24),
                "doc_count": 1,
                "avg_relevance": 0.84,
                "avg_source_score": 0.76,
                "freshness_score": 0.7,
            },
            {
                "window_start": now - timedelta(hours=24),
                "window_end": now,
                "doc_count": 3,
                "avg_relevance": 0.9,
                "avg_source_score": 0.8,
                "freshness_score": 1.0,
            },
        ],
        "semantic-prank-b": [
            {
                "window_start": now - timedelta(hours=72),
                "window_end": now - timedelta(hours=48),
                "doc_count": 1,
                "avg_relevance": 0.8,
                "avg_source_score": 0.73,
                "freshness_score": 0.5,
            },
            {
                "window_start": now - timedelta(hours=48),
                "window_end": now - timedelta(hours=24),
                "doc_count": 1,
                "avg_relevance": 0.82,
                "avg_source_score": 0.75,
                "freshness_score": 0.7,
            },
            {
                "window_start": now - timedelta(hours=24),
                "window_end": now,
                "doc_count": 4,
                "avg_relevance": 0.88,
                "avg_source_score": 0.79,
                "freshness_score": 1.0,
            },
        ],
    }

    stable, emerging = _signal_results(
        semantic,
        [],
        [],
        {
            "trend_cluster_similarity_threshold": 0.87,
            "trend_cluster_max_gap_hours": 24 * 30,
            "trend_cluster_min_semantic_clusters": 2,
            "trend_cluster_min_docs": 4,
            "trend_cluster_stable_threshold": 0.58,
            "trend_cluster_emerging_threshold": 0.5,
            "trend_cluster_min_source_diversity": 0.2,
            "cluster_min_evidence_count": 2,
            "signal_short_window_hours": 24,
            "signal_baseline_window_days": 14,
            "signal_velocity_weight": 0.14,
            "signal_acceleration_weight": 0.1,
            "change_point_method": "window",
            "change_point_penalty": "auto",
            "change_point_min_size": 2,
            "change_point_jump": 1,
            "change_point_recent_hours": 48,
            "signal_merge_similarity_threshold": 0.72,
            "signal_merge_doc_overlap_threshold": 0.25,
            "signal_min_source_count": 1,
            "april_fools_guard_enabled": True,
            "april_fools_guard_penalty": 0.45,
            "april_fools_guard_stage_block_ratio": 0.34,
        },
        signal_series_by_id=signal_series_by_id,
    )

    assert stable == []
    assert all(item["signal_stage"] == "weak" for item in emerging)
    assert all(
        item["explainability"]["april_fools_guard"]["stage_blocked"] is True for item in emerging
    )


def test_signal_results_handle_missing_top_terms_in_semantic_payload() -> None:
    now = datetime.now(UTC)
    semantic = [
        {
            "cluster_id": "semantic-legacy",
            "cluster_key": _digest("semantic-legacy", "ws"),
            "workspace_id": "disruption",
            "title": "Legacy signal payload",
            "representative_post_id": "legacy-1",
            "post_count": 1,
            "source_count": 1,
            "doc_ids": ["legacy-1"],
            "source_ids": ["s1"],
            "top_concepts": ["launch", "product"],
            "evidence": [{"post_id": "legacy-1", "source_id": "s1"}],
            "posts": [
                _post("legacy-1", "s1", 1, 0.82, 0.74, [1.0, 0.0], title="Legacy signal payload"),
            ],
            "avg_relevance": 0.82,
            "avg_source_score": 0.74,
            "first_seen_at": now - timedelta(hours=3),
            "last_seen_at": now - timedelta(hours=1),
            "centroid": [1.0, 0.0],
            "explainability": {},
        }
    ]
    signal_series_by_id = {
        "semantic-legacy": [
            {
                "window_start": now - timedelta(hours=72),
                "window_end": now - timedelta(hours=48),
                "doc_count": 1,
                "avg_relevance": 0.7,
                "avg_source_score": 0.68,
                "freshness_score": 0.5,
            },
            {
                "window_start": now - timedelta(hours=48),
                "window_end": now - timedelta(hours=24),
                "doc_count": 1,
                "avg_relevance": 0.76,
                "avg_source_score": 0.7,
                "freshness_score": 0.7,
            },
            {
                "window_start": now - timedelta(hours=24),
                "window_end": now,
                "doc_count": 2,
                "avg_relevance": 0.82,
                "avg_source_score": 0.74,
                "freshness_score": 1.0,
            },
        ]
    }

    stable, emerging = _signal_results(
        semantic,
        [],
        [],
        {
            "trend_cluster_similarity_threshold": 0.87,
            "trend_cluster_max_gap_hours": 24 * 30,
            "trend_cluster_min_semantic_clusters": 2,
            "trend_cluster_min_docs": 4,
            "trend_cluster_stable_threshold": 0.58,
            "trend_cluster_emerging_threshold": 0.5,
            "trend_cluster_min_source_diversity": 0.2,
            "cluster_min_evidence_count": 1,
            "signal_short_window_hours": 24,
            "signal_baseline_window_days": 14,
            "signal_velocity_weight": 0.14,
            "signal_acceleration_weight": 0.1,
            "change_point_method": "window",
            "change_point_penalty": "auto",
            "change_point_min_size": 2,
            "change_point_jump": 1,
            "change_point_recent_hours": 48,
            "signal_merge_similarity_threshold": 0.72,
            "signal_merge_doc_overlap_threshold": 0.25,
            "signal_min_source_count": 1,
            "april_fools_guard_enabled": True,
            "april_fools_guard_penalty": 0.45,
            "april_fools_guard_stage_block_ratio": 0.34,
        },
        signal_series_by_id=signal_series_by_id,
    )

    assert stable == []
    assert len(emerging) == 1
    assert emerging[0]["explainability"]["top_terms"] == []
