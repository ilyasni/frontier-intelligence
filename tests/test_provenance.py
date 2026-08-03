"""Unit tests for shared.provenance (cross-source de-dup / independence proxy)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from shared.provenance import (
    EchoEdge,
    ProvenancePost,
    canonical_url,
    compute_independence_score,
    echo_edges,
    independence_metrics,
    same_artifact_groups,
)


def _p(
    post_id: str,
    source_id: str,
    *,
    url: str | None = None,
    title: str = "",
    day: int = 1,
    hour: int = 9,
    vector: list[float] | None = None,
    actors: tuple[str, ...] = (),
) -> ProvenancePost:
    return ProvenancePost(
        post_id=post_id,
        source_id=source_id,
        published_at=datetime(2026, 7, day, hour, 0, tzinfo=UTC),
        url=url,
        title=title,
        vector=vector,
        actors=actors,
    )


# --- canonical_url -----------------------------------------------------------


@pytest.mark.unit
def test_canonical_url_strips_tracking_and_normalizes_host():
    assert canonical_url("HTTPS://Example.com/Article?utm_source=x&id=5#frag") == (
        "https://example.com/Article?id=5"
    )


@pytest.mark.unit
def test_canonical_url_folds_amp_mobile_and_trailing_slash():
    a = canonical_url("https://m.example.com/post/")
    b = canonical_url("https://example.com/post/amp")
    c = canonical_url("https://example.com/post")
    assert a == b == c == "https://example.com/post"


@pytest.mark.unit
def test_canonical_url_none_and_relative():
    assert canonical_url(None) is None
    assert canonical_url("not a url") == "not a url"


# --- same_artifact_groups ----------------------------------------------------


@pytest.mark.unit
def test_same_url_two_feeds_collapse_to_one_voice():
    # The live "UX Debt" case: one Medium article via two tag-feeds.
    url = "https://medium.com/@a/ux-debt-509083c72c01"
    posts = [
        _p("p1", "rss_medium_design", url=url, title="UX Debt", hour=2),
        _p("p2", "rss_medium_ux", url=url + "?utm_source=feed", title="UX Debt", hour=2),
    ]
    groups = same_artifact_groups(posts)
    assert len(groups) == 1
    assert groups[0][0].post_id == "p1"  # earliest = origin


@pytest.mark.unit
def test_distinct_articles_stay_separate():
    posts = [
        _p("p1", "s1", url="https://a.com/1", title="Alpha"),
        _p("p2", "s2", url="https://b.com/2", title="Beta"),
    ]
    assert len(same_artifact_groups(posts)) == 2


@pytest.mark.unit
def test_near_identical_text_collapses_within_gap():
    v = [1.0, 0.0, 0.0]
    posts = [
        _p("p1", "s1", url="https://a.com/1", title="x", vector=v, day=1),
        _p("p2", "s2", url="https://b.com/2", title="y", vector=[0.999, 0.01, 0.0], day=1, hour=15),
    ]
    assert len(same_artifact_groups(posts)) == 1


@pytest.mark.unit
def test_near_identical_text_not_collapsed_beyond_gap():
    v = [1.0, 0.0, 0.0]
    posts = [
        _p("p1", "s1", url="https://a.com/1", vector=v, day=1),
        _p("p2", "s2", url="https://b.com/2", vector=v, day=10),  # 9 days apart > 72h
    ]
    assert len(same_artifact_groups(posts, max_gap_hours=72)) == 2


@pytest.mark.unit
def test_same_title_collapses():
    posts = [
        _p("p1", "s1", url="https://a.com/1", title="Sber launches fuel map!", hour=9),
        _p("p2", "s2", url="https://b.com/2", title="SBER LAUNCHES FUEL MAP", hour=11),
    ]
    assert len(same_artifact_groups(posts)) == 1


# --- independence_metrics ----------------------------------------------------


@pytest.mark.unit
def test_ux_debt_metrics_two_to_one():
    url = "https://medium.com/@a/ux-debt"
    posts = [
        _p("p1", "rss_medium_design", url=url, title="UX Debt"),
        _p("p2", "rss_medium_ux", url=url, title="UX Debt"),
    ]
    m = independence_metrics(posts)
    assert m.raw_source_count == 2
    assert m.deduped_source_count == 1
    assert m.distinct_voices == 1
    assert m.echo_ratio == 0.5


@pytest.mark.unit
def test_syndication_spike_collapses_and_flags_single_day():
    # One press release echoed across 6 same-day feeds (same title) + 1 dup URL pair.
    press = [
        _p(f"pr{i}", f"feed{i}", url=f"https://feed{i}.ru/x", title="Сбер запустил карту топлива", hour=8 + i)
        for i in range(6)
    ]
    dup_url = "https://3dnews.ru/1144859"
    dups = [
        _p("d1", "rss_3dnews_ai", url=dup_url, title="3DNews piece", hour=7),
        _p("d2", "rss_3dnews_ev", url=dup_url, title="3DNews piece", hour=7),
    ]
    m = independence_metrics(press + dups)
    assert m.raw_source_count == 8  # 6 feeds + 2 3dnews feeds
    assert m.distinct_voices == 2  # press-release voice + 3dnews voice
    assert m.deduped_source_count == 2
    assert m.echo_ratio == round((8 - 2) / 8, 4)
    assert m.single_day_spike is False  # only 2 voices, not >=3


@pytest.mark.unit
def test_independent_multiday_high_dispersion():
    posts = [
        _p("p1", "s1", url="https://a.com/1", title="A", day=1, actors=("sber",)),
        _p("p2", "s2", url="https://b.com/2", title="B", day=3, actors=("alfa",)),
        _p("p3", "s3", url="https://c.com/3", title="C", day=5, actors=("yandex",)),
    ]
    m = independence_metrics(posts)
    assert m.distinct_voices == 3
    assert m.echo_ratio == 0.0
    assert m.distinct_originators == 3
    assert m.arrival_dispersion == 1.0  # 3 distinct days / 3 voices
    assert m.single_day_spike is False
    assert m.independence_score > 0.7


@pytest.mark.unit
def test_single_day_spike_penalizes_score():
    # 3 distinct-URL posts, same day → looks like many voices but same-day spike.
    posts = [
        _p(f"p{i}", f"s{i}", url=f"https://s{i}.com/x", title=f"T{i}", day=1, hour=8 + i)
        for i in range(3)
    ]
    m = independence_metrics(posts)
    assert m.distinct_voices == 3
    assert m.arrival_dispersion == round(1 / 3, 4)
    assert m.single_day_spike is True


@pytest.mark.unit
def test_empty_posts():
    m = independence_metrics([])
    assert m.distinct_voices == 0
    assert m.independence_score == 0.0
    assert m.first_seen_at is None


# --- echo_edges --------------------------------------------------------------


@pytest.mark.unit
def test_echo_edges_origin_is_earliest():
    url = "https://a.com/x"
    posts = [
        _p("late", "s2", url=url, title="X", day=2, hour=10),
        _p("early", "s1", url=url, title="X", day=1, hour=10),
    ]
    groups = same_artifact_groups(posts)
    edges = echo_edges(groups)
    assert len(edges) == 1
    e = edges[0]
    assert isinstance(e, EchoEdge)
    assert e.origin_post_id == "early"
    assert e.echo_post_id == "late"
    assert e.method == "canonical_url"
    assert e.lag_hours == 24.0


@pytest.mark.unit
def test_no_echo_edges_for_singletons():
    posts = [_p("p1", "s1", url="https://a.com/1"), _p("p2", "s2", url="https://b.com/2")]
    assert echo_edges(same_artifact_groups(posts)) == []


# --- compute_independence_score (used by the Neo4j originator backfill) -------


@pytest.mark.unit
def test_score_originators_raise_it():
    base = dict(
        deduped_source_count=2, echo_ratio=0.0, arrival_dispersion=1.0, single_day_spike=False
    )
    assert compute_independence_score(distinct_originators=5, **base) > (
        compute_independence_score(distinct_originators=1, **base)
    )


@pytest.mark.unit
def test_score_single_day_spike_halves():
    kw = dict(deduped_source_count=5, distinct_originators=5, echo_ratio=0.0, arrival_dispersion=1.0)
    assert compute_independence_score(single_day_spike=True, **kw) == round(
        compute_independence_score(single_day_spike=False, **kw) * 0.5, 4
    )


@pytest.mark.unit
def test_score_falls_back_to_deduped_when_no_originators():
    kw = dict(echo_ratio=0.0, arrival_dispersion=1.0, single_day_spike=False)
    assert compute_independence_score(
        deduped_source_count=5, distinct_originators=None, **kw
    ) == compute_independence_score(deduped_source_count=5, distinct_originators=5, **kw)
