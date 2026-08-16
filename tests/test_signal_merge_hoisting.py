"""`_merge_signal_candidates` must merge exactly as it did before the hoisting.

The inner loop is O(k²) — 5091 emerging signals on workspace disruption, ~13M pairs —
and it used to rebuild four sets per pair, re-tokenizing the same titles with a regex
13M times. The fix computes those per item instead. That is a pure speed change, so the
guard has to be equivalence.

The reference below reproduces the pre-fix *decision* path verbatim — every overlap
term, the merge condition, the absorbed-id bookkeeping and the doc/semantic
accumulation. It deliberately omits the payload-rewriting half of the merge branch
(keywords, evidence, source_ids, series_posts, provenance), which the hoisting does not
touch; the assertions compare only what the reference actually models.
"""
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from worker.services.clustering_math import jaccard as _jaccard, terms as _terms
from worker.services.semantic_clustering import _merge_signal_candidates

_NOW = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)

_CFG = {
    "signal_merge_similarity_threshold": 0.72,
    "signal_merge_doc_overlap_threshold": 0.25,
    "trend_cluster_max_gap_hours": 24 * 30,
}


def _reference_merge(
    items: list[dict[str, Any]], cluster_cfg: dict[str, Any]
) -> tuple[list[dict[str, Any]], int]:
    """The pre-fix implementation, rebuilding every set inside the pair loop."""
    threshold = float(cluster_cfg["signal_merge_similarity_threshold"])
    doc_overlap_threshold = float(cluster_cfg["signal_merge_doc_overlap_threshold"])
    max_gap_hours = int(cluster_cfg.get("trend_cluster_max_gap_hours", 24 * 30))
    merged_count = 0
    items = sorted(items, key=lambda item: item["signal_score"], reverse=True)
    kept: list[dict[str, Any]] = []
    absorbed_ids: set[str] = set()
    for idx, current in enumerate(items):
        if current.get("existing_id") in absorbed_ids or current.get("signal_id") in absorbed_ids:
            continue
        current_docs = set(current.get("doc_ids") or [])
        current_semantic = set(current.get("semantic_cluster_ids") or [])
        current_terms = set(_terms(current.get("title") or ""))
        current_concepts = set(current.get("keywords") or [])
        merged_into_current: list[str] = []
        for other in items[idx + 1 :]:
            other_id = other.get("existing_id") or other.get("signal_id")
            if other_id in absorbed_ids or other.get("workspace_id") != current.get("workspace_id"):
                continue
            other_docs = set(other.get("doc_ids") or [])
            doc_overlap = len(current_docs & other_docs) / max(len(current_docs | other_docs), 1)
            semantic_overlap = _jaccard(
                current_semantic, set(other.get("semantic_cluster_ids") or [])
            )
            concept_overlap = _jaccard(current_concepts, set(other.get("keywords") or []))
            title_overlap = _jaccard(current_terms, set(_terms(other.get("title") or "")))
            current_first = current.get("first_seen_at")
            current_last = current.get("last_seen_at")
            other_first = other.get("first_seen_at")
            other_last = other.get("last_seen_at")
            temporal_overlap = 0.0
            if current_first and current_last and other_first and other_last:
                gap_hours = (
                    abs(
                        (
                            max(current_first, other_first) - min(current_last, other_last)
                        ).total_seconds()
                    )
                    / 3600.0
                )
                temporal_overlap = (
                    1.0
                    if gap_hours <= max_gap_hours
                    else max(0.0, 1.0 - (gap_hours / max(max_gap_hours, 1)))
                )
            similarity = (
                doc_overlap * 0.28
                + semantic_overlap * 0.22
                + concept_overlap * 0.24
                + title_overlap * 0.16
                + temporal_overlap * 0.10
            )
            semantic_title_merge = (
                concept_overlap >= 0.6 and title_overlap >= 0.45 and temporal_overlap >= 0.4
            )
            if (
                doc_overlap >= doc_overlap_threshold
                or similarity >= threshold
                or semantic_title_merge
            ):
                absorbed_ids.add(other_id)
                merged_into_current.append(other_id)
                current_docs.update(other_docs)
                current_semantic.update(other.get("semantic_cluster_ids") or [])
                current["doc_ids"] = sorted(current_docs)
                current["semantic_cluster_ids"] = sorted(current_semantic)
                merged_count += 1
        kept.append(current)
    return kept, merged_count


_WORDS = ["ai", "robotaxi", "lidar", "cockpit", "sdv", "battery", "adas", "hmi", "chip"]


def _candidate(rng: random.Random, idx: int, workspaces: list[str]) -> dict[str, Any]:
    first = _NOW - timedelta(hours=rng.uniform(0.0, 700.0))
    return {
        "signal_id": f"sig-{idx}",
        "existing_id": None,
        "workspace_id": rng.choice(workspaces),
        "signal_score": round(rng.uniform(0.0, 1.0), 4),
        # Small pools make real overlaps happen instead of an all-misses run.
        "doc_ids": sorted({f"p{rng.randrange(24)}" for _ in range(rng.randrange(1, 5))}),
        "semantic_cluster_ids": sorted({f"c{rng.randrange(12)}" for _ in range(rng.randrange(1, 4))}),
        "keywords": rng.sample(_WORDS, rng.randrange(1, 5)),
        "title": " ".join(rng.sample(_WORDS, rng.randrange(1, 5))),
        "source_ids": sorted({f"s{rng.randrange(6)}" for _ in range(rng.randrange(1, 3))}),
        "source_count": 1,
        "evidence": [{"post_id": f"e{idx}"}],
        "series_posts": [],
        "explainability": {},
        "first_seen_at": first,
        "last_seen_at": first + timedelta(hours=rng.uniform(0.0, 40.0)),
    }


def _shape(kept: list[dict[str, Any]]) -> list[tuple]:
    return [
        (
            item["signal_id"],
            tuple(item.get("doc_ids") or []),
            tuple(item.get("semantic_cluster_ids") or []),
        )
        for item in kept
    ]


@pytest.mark.unit
@pytest.mark.parametrize("seed", range(30))
def test_hoisting_does_not_change_which_signals_merge(seed: int) -> None:
    rng = random.Random(seed)
    workspaces = ["disruption"] if seed % 3 else ["disruption", "design"]
    count = rng.randrange(2, 26)
    payload = [_candidate(rng, idx, workspaces) for idx in range(count)]

    fast, fast_merged = _merge_signal_candidates([dict(x) for x in payload], _CFG)
    slow, slow_merged = _reference_merge([dict(x) for x in payload], _CFG)

    assert fast_merged == slow_merged, f"merge count differs at seed={seed}"
    assert _shape(fast) == _shape(slow), f"kept set differs at seed={seed}"


@pytest.mark.unit
def test_merge_still_absorbs_an_obvious_duplicate() -> None:
    # A guard against the equivalence test passing because nothing ever merges.
    base = _candidate(random.Random(1), 0, ["disruption"])
    twin = dict(base, signal_id="sig-twin", signal_score=base["signal_score"] - 0.1)
    kept, merged = _merge_signal_candidates([base, twin], _CFG)
    assert merged == 1
    assert [item["signal_id"] for item in kept] == ["sig-0"]


@pytest.mark.unit
def test_merge_never_crosses_workspaces() -> None:
    left = _candidate(random.Random(2), 0, ["disruption"])
    right = dict(left, signal_id="sig-other", workspace_id="design", signal_score=0.1)
    kept, merged = _merge_signal_candidates([left, right], _CFG)
    assert merged == 0
    assert sorted(item["signal_id"] for item in kept) == ["sig-0", "sig-other"]
