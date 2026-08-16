"""A quality metric earns its place only if some input makes it alarm.

Three metrics failed that test against live clusters and were dropped on 2026-08-16:
`semantic_cluster_purity` (a one-post cluster's coherence is exactly 1.0 by
construction, and multi-post clusters cannot fall below the dedupe threshold),
`over_merge_rate` (its 0.84 line sits below the 0.92 at which clusters are joined at
all — 0 of 9520 crossed it), and `empty_low_evidence_cluster_rate`, which failed the
mirror-image test: evidence is capped by construction while most groups are two
singleton clusters, so it read 0.83-0.99 in every workspace at both levels and could
never read calm. `over_split_rate` was renamed to `singleton_cluster_share` because it
never measured over-splitting: the semantic stage is near-duplicate dedup, so a post
with no near-twin *must* end up alone.

Real over-splitting is measured instead against provenance — same canonical URL or
identical headline means the same artifact regardless of what the clustering decided —
by `same_artifact_cluster_split_rate` and `same_artifact_trend_split_rate`.

These tests encode the rule rather than the verdict — each surviving metric must be
shown both calm and alarmed.
"""
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from worker.services.semantic_clustering import ClusterPost, _metrics

_DROPPED = (
    "over_split_rate",
    "over_merge_rate",
    "semantic_cluster_purity",
    "empty_low_evidence_cluster_rate",
)

_NOW = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)


def _post(post_id: str, *, url: str = "", title: str = "", hours_ago: int = 0) -> ClusterPost:
    return ClusterPost(
        post_id=post_id,
        workspace_id="disruption",
        source_id=f"src-{post_id}",
        content="body",
        published_at=_NOW - timedelta(hours=hours_ago),
        relevance_score=0.5,
        source_score=0.5,
        tags=[],
        title=title,
        url=url,
        vector=[1.0, 0.0],
    )


def _cluster(
    idx: int,
    *,
    post_count: int = 1,
    source_count: int = 1,
    evidence: int = 1,
    posts: list[ClusterPost] | None = None,
):
    doc_ids = [f"p{idx}-{k}" for k in range(post_count)]
    return {
        "cluster_id": f"c{idx}",
        "doc_ids": doc_ids,
        "post_count": post_count,
        "source_count": source_count,
        "evidence": [{"post_id": doc_id} for doc_id in doc_ids[:evidence]],
        "title": f"cluster {idx}",
        "posts": posts if posts is not None else [_post(doc_id) for doc_id in doc_ids],
    }


def _signal(idx: int, *, title: str = "alpha beta", semantic_ids: list[str] | None = None):
    return {
        "signal_id": f"s{idx}",
        "semantic_cluster_ids": semantic_ids if semantic_ids is not None else [f"c{idx}"],
        "evidence": [{"post_id": f"e{idx}-{k}"} for k in range(3)],
        "title": title,
    }


def _cfg(min_evidence: int = 2) -> dict[str, Any]:
    return {"cluster_min_evidence_count": min_evidence}


# One artifact (same canonical URL) whose two posts sit in ONE cluster.
_JOINED = [
    _cluster(
        0,
        post_count=2,
        posts=[
            _post("p0-0", url="https://example.com/a"),
            _post("p0-1", url="https://example.com/a?utm_source=rss"),
        ],
    )
]
# The same artifact, torn across two clusters.
_TORN = [
    _cluster(0, posts=[_post("p0-0", url="https://example.com/a")]),
    _cluster(1, posts=[_post("p1-0", url="https://example.com/a?utm_source=rss")]),
]


# metric -> (input that must read 0.0, input that must read above 0.0)
_ALARM_CASES: dict[str, tuple[tuple, tuple]] = {
    "singleton_cluster_share": (
        ([_cluster(0, post_count=3), _cluster(1, post_count=2)], [], []),
        ([_cluster(0), _cluster(1)], [], []),
    ),
    "source_monoculture_rate": (
        ([_cluster(0, post_count=3, source_count=3)], [], []),
        ([_cluster(0, post_count=3, source_count=1)], [], []),
    ),
    "trend_duplication_rate": (
        ([_cluster(0)], [_signal(0, title="alpha beta")], []),
        ([_cluster(0)], [_signal(0, title="alpha beta"), _signal(1, title="alpha beta")], []),
    ),
    "multi_post_clusters": (
        ([_cluster(0)], [], []),
        ([_cluster(0, post_count=2)], [], []),
    ),
    "same_artifact_groups": (
        ([_cluster(0), _cluster(1)], [], []),
        (_TORN, [], []),
    ),
    "same_artifact_cluster_split_rate": (
        (_JOINED, [], []),
        (_TORN, [], []),
    ),
    "same_artifact_trend_groups": (
        (_TORN, [], []),
        (_TORN, [_signal(0, semantic_ids=["c0", "c1"])], []),
    ),
    "same_artifact_trend_split_rate": (
        (_TORN, [_signal(0, semantic_ids=["c0", "c1"])], []),
        (_TORN, [_signal(0, semantic_ids=["c0"]), _signal(1, semantic_ids=["c1"])], []),
    ),
}


@pytest.mark.unit
@pytest.mark.parametrize("metric", sorted(_ALARM_CASES))
def test_every_metric_has_an_input_that_makes_it_alarm(metric: str) -> None:
    calm, alarming = _ALARM_CASES[metric]
    assert _metrics(*calm, _cfg())[metric] == 0.0, f"{metric} never reads calm"
    assert _metrics(*alarming, _cfg())[metric] > 0.0, f"{metric} cannot be made to alarm"


@pytest.mark.unit
@pytest.mark.parametrize("dropped", _DROPPED)
def test_metrics_that_could_not_alarm_stay_dropped(dropped: str) -> None:
    quality = _metrics([_cluster(0)], [_signal(0)], [], _cfg())
    assert dropped not in quality


@pytest.mark.unit
def test_no_metric_ships_without_a_proof_that_it_can_alarm() -> None:
    """Adding a metric without an alarm case must fail here, or the rule is toothless.

    Without this, the tests above only cover the metrics somebody remembered to list —
    exactly how `over_merge_rate` sat at a constant 0.0 for months without anyone
    noticing. The golden-set keys are excluded because they come from a labelled
    fixture rather than run data.
    """
    golden = {
        "same_story_accuracy",
        "different_story_accuracy",
        "same_trend_accuracy",
        "same_story_pairs",
        "different_story_pairs",
        "same_trend_pairs",
    }
    produced = set(_metrics([_cluster(0)], [_signal(0)], [], _cfg()))
    unproven = produced - set(_ALARM_CASES) - golden
    assert not unproven, (
        f"metrics with no alarm case: {sorted(unproven)} — either add an input to "
        "_ALARM_CASES that makes it non-zero, or drop the metric"
    )


@pytest.mark.unit
def test_singleton_share_is_the_plain_fraction_of_one_post_clusters() -> None:
    semantic = [_cluster(0), _cluster(1), _cluster(2, post_count=4), _cluster(3, post_count=2)]
    assert _metrics(semantic, [], [], _cfg())["singleton_cluster_share"] == 0.5


@pytest.mark.unit
def test_source_monoculture_ignores_singletons_so_it_is_not_singleton_share_again() -> None:
    """Its old denominator was every cluster, where a singleton is single-source by
    definition; live data had the two agreeing to within 104 clusters of 9520."""
    semantic = [
        _cluster(0),  # singleton, single-source — must not count either way
        _cluster(1),
        _cluster(2, post_count=4, source_count=1),  # the real monoculture
        _cluster(3, post_count=2, source_count=2),
    ]
    quality = _metrics(semantic, [], [], _cfg())

    assert quality["singleton_cluster_share"] == 0.5
    assert quality["multi_post_clusters"] == 2
    assert quality["source_monoculture_rate"] == 0.5


@pytest.mark.unit
def test_split_rate_follows_provenance_not_the_clustering_threshold() -> None:
    """Identical headline within 72h is the same artifact even at different URLs."""
    torn_by_title = [
        _cluster(0, posts=[_post("p0-0", title="Rivian opens its charging network")]),
        _cluster(
            1,
            posts=[_post("p1-0", title="Rivian opens its charging network!", hours_ago=12)],
        ),
    ]
    assert _metrics(torn_by_title, [], [], _cfg())["same_artifact_cluster_split_rate"] == 1.0

    # Same headline, but 100h apart — beyond the 72h provenance gap, so not one
    # artifact and not a split.
    far_apart = [
        _cluster(0, posts=[_post("p0-0", title="Rivian opens its charging network")]),
        _cluster(
            1,
            posts=[_post("p1-0", title="Rivian opens its charging network", hours_ago=100)],
        ),
    ]
    quality = _metrics(far_apart, [], [], _cfg())
    assert quality["same_artifact_groups"] == 0
    assert quality["same_artifact_cluster_split_rate"] == 0.0


@pytest.mark.unit
def test_zero_split_rate_is_distinguishable_from_nothing_to_measure() -> None:
    """A rate of 0.0 over an empty denominator means "no data", not "no problem" —
    the counts ship so the two can be told apart in cluster_runs.metrics."""
    nothing = _metrics([_cluster(0), _cluster(1)], [], [], _cfg())
    assert nothing["same_artifact_groups"] == 0
    assert nothing["same_artifact_cluster_split_rate"] == 0.0

    healthy = _metrics(_JOINED, [], [], _cfg())
    assert healthy["same_artifact_groups"] == 1
    assert healthy["same_artifact_cluster_split_rate"] == 0.0
