from worker.integrations.qdrant_client import _final_rank_score, _freshness_boost, _trend_rank_score


def test_freshness_boost_prefers_recent_documents() -> None:
    recent = _freshness_boost("2099-01-01T00:00:00+00:00")
    stale = _freshness_boost("2020-01-01T00:00:00+00:00")

    assert recent > stale


def test_final_rank_score_uses_source_score_and_freshness() -> None:
    boosted, breakdown = _final_rank_score(
        0.8,
        {"source_score": 0.9, "published_at": "2099-01-01T00:00:00+00:00"},
    )
    plain, _ = _final_rank_score(
        0.8,
        {"source_score": 0.0, "published_at": "2020-01-01T00:00:00+00:00"},
    )

    assert boosted > plain
    assert breakdown["source_score"] == 0.9


def test_trend_rank_score_boosts_strong_clusters() -> None:
    boosted, breakdown = _trend_rank_score(
        0.7,
        {"signal_score": 0.9, "burst_score": 0.8, "source_count": 6},
    )
    plain, _ = _trend_rank_score(
        0.7,
        {"signal_score": 0.0, "burst_score": 0.0, "source_count": 1},
    )

    assert boosted > plain
    assert breakdown["signal_score"] == 0.9
