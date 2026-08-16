"""A quality metric earns its place only if some input makes it alarm.

Two metrics failed that test against 9520 live clusters and were dropped on
2026-08-16: `semantic_cluster_purity` (a one-post cluster's coherence is exactly 1.0
by construction, and multi-post clusters cannot fall below the dedupe threshold) and
`over_merge_rate` (its 0.84 line sits below the 0.92 at which clusters are joined at
all — 0 of 9520 crossed it). `over_split_rate` was renamed to `singleton_cluster_share`
because it never measured over-splitting: the semantic stage is near-duplicate dedup,
so a post with no near-twin *must* end up alone.

These tests encode the rule rather than the verdict — each surviving metric must be
shown both calm and alarmed.
"""
from typing import Any

import pytest

from worker.services.semantic_clustering import _metrics

_DROPPED = ("over_split_rate", "over_merge_rate", "semantic_cluster_purity")


def _cluster(idx: int, *, post_count: int = 1, source_count: int = 1, evidence: int = 1):
    return {
        "cluster_id": f"c{idx}",
        "doc_ids": [f"p{idx}-{k}" for k in range(post_count)],
        "post_count": post_count,
        "source_count": source_count,
        "evidence": [{"post_id": f"p{idx}-{k}"} for k in range(evidence)],
        "title": f"cluster {idx}",
    }


def _signal(idx: int, *, title: str = "alpha beta", evidence: int = 3):
    return {
        "signal_id": f"s{idx}",
        "semantic_cluster_ids": [f"c{idx}"],
        "evidence": [{"post_id": f"e{idx}-{k}"} for k in range(evidence)],
        "title": title,
    }


def _cfg(min_evidence: int = 2) -> dict[str, Any]:
    return {"cluster_min_evidence_count": min_evidence}


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
    "empty_low_evidence_cluster_rate": (
        ([_cluster(0, post_count=3, source_count=2, evidence=3)], [], []),
        ([_cluster(0, post_count=3, source_count=2, evidence=1)], [], []),
    ),
    "trend_duplication_rate": (
        ([_cluster(0)], [_signal(0, title="alpha beta")], []),
        ([_cluster(0)], [_signal(0, title="alpha beta"), _signal(1, title="alpha beta")], []),
    ),
}


@pytest.mark.parametrize("metric", sorted(_ALARM_CASES))
def test_every_metric_has_an_input_that_makes_it_alarm(metric: str) -> None:
    calm, alarming = _ALARM_CASES[metric]
    assert _metrics(*calm, _cfg())[metric] == 0.0, f"{metric} never reads calm"
    assert _metrics(*alarming, _cfg())[metric] > 0.0, f"{metric} cannot be made to alarm"


@pytest.mark.parametrize("dropped", _DROPPED)
def test_metrics_that_could_not_alarm_stay_dropped(dropped: str) -> None:
    quality = _metrics([_cluster(0)], [_signal(0)], [], _cfg())
    assert dropped not in quality


def test_no_metric_ships_without_a_proof_that_it_can_alarm() -> None:
    """Adding a metric without an alarm case must fail here, or the rule is toothless.

    Without this, the tests above only cover the metrics somebody remembered to list —
    exactly how `over_merge_rate` sat at a constant 0.0 for months without anyone
    noticing. The golden-set accuracies are excluded because they come from a labelled
    fixture rather than run data; note that they do not appear in production at all,
    since prod images ship no tests/ directory for the fixture to live in.
    """
    golden = {"same_story_accuracy", "different_story_accuracy", "same_trend_accuracy"}
    produced = set(_metrics([_cluster(0)], [_signal(0)], [], _cfg()))
    unproven = produced - set(_ALARM_CASES) - golden
    assert not unproven, (
        f"metrics with no alarm case: {sorted(unproven)} — either add an input to "
        "_ALARM_CASES that makes it non-zero, or drop the metric"
    )


def test_singleton_share_is_the_plain_fraction_of_one_post_clusters() -> None:
    semantic = [_cluster(0), _cluster(1), _cluster(2, post_count=4), _cluster(3, post_count=2)]
    assert _metrics(semantic, [], [], _cfg())["singleton_cluster_share"] == 0.5
