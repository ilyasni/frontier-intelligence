from shared.source_quality import (
    compute_source_quality,
    normalize_optional_bool,
    recommend_content_mode,
    source_quality_payload,
)


def test_compute_source_quality_rewards_healthy_high_signal_sources() -> None:
    quality = compute_source_quality(
        authority=0.9,
        success_count=12,
        error_count=1,
        fetched_count=100,
        emitted_count=70,
        relevant_ratio=0.65,
        avg_tag_count=3.4,
        linked_ratio=0.55,
        freshness_hours=10,
    )

    assert quality.composite > 0.7
    assert quality.runtime_health > 0.8
    assert quality.signal_yield > 0.5


def test_recommend_content_mode_prefers_summary_for_flaky_rss() -> None:
    mode = recommend_content_mode(
        source_type="rss",
        last_run_status="error",
        last_run_error_text="403 Forbidden during hydration",
        fetched_count=20,
        emitted_count=0,
        error_rate=0.5,
    )

    assert mode == "summary-only"


def test_recommend_content_mode_respects_listing_only_web_config() -> None:
    mode = recommend_content_mode(
        source_type="web",
        last_run_status="success",
        last_run_error_text=None,
        fetched_count=20,
        emitted_count=10,
        error_rate=0.0,
        parse_full_content=False,
    )

    assert mode == "listing-only"


def test_recommend_content_mode_keeps_full_content_for_healthy_web_runs() -> None:
    mode = recommend_content_mode(
        source_type="web",
        last_run_status="success",
        last_run_error_text=None,
        fetched_count=20,
        emitted_count=0,
        error_rate=0.0,
        parse_full_content=True,
    )

    assert mode == "full-content"


def test_source_quality_payload_builds_breakdown() -> None:
    payload = source_quality_payload(
        {
            "source_type": "web",
            "source_authority": 0.7,
            "recent_success_count": 4,
            "recent_error_count": 1,
            "last_run_fetched_count": 20,
            "last_run_emitted_count": 10,
            "relevant_ratio": 0.5,
            "avg_tag_count": 2.0,
            "linked_ratio": 0.6,
            "freshness_hours": 5,
            "quality_tier": "trusted",
            "extra": {"parse": {"full_content": True}},
        }
    )

    assert 0.0 <= payload["source_score"] <= 1.0
    assert payload["quality_tier"] == "trusted"
    assert payload["recommended_content_mode"] in {"full-content", "listing-only", "summary-only", "native"}
    assert set(payload["score_breakdown"]) == {"authority", "runtime_health", "signal_yield", "freshness"}


def test_source_quality_payload_uses_web_parse_mode_from_extra() -> None:
    payload = source_quality_payload(
        {
            "source_type": "web",
            "source_authority": 0.62,
            "recent_success_count": 6,
            "recent_error_count": 3,
            "last_run_fetched_count": 20,
            "last_run_emitted_count": 0,
            "quality_tier": "trusted",
            "extra": {"parse": {"full_content": False}},
        }
    )

    assert payload["recommended_content_mode"] == "listing-only"


def test_normalize_optional_bool_handles_string_flags() -> None:
    assert normalize_optional_bool("false") is False
    assert normalize_optional_bool("true") is True
    assert normalize_optional_bool(None) is None
