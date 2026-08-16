"""Equivalence tests for the vectorized similarity-graph builder.

`_components_np` (numpy matmul) must produce a byte-identical partitioning to the
pure-Python reference `_components_py` for every input, including threshold-boundary
ties. These tests are the safety net for the vectorization of the clustering hotspot.
"""
import random
from datetime import UTC, datetime, timedelta

import pytest

from worker.services import semantic_clustering as sc
from worker.services.clustering_math import cosine
from worker.services.semantic_clustering import (
    ClusterPost,
    _components,
    _components_np,
    _components_py,
)

requires_numpy = pytest.mark.skipif(sc.np is None, reason="numpy not available")

_BASE = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


def _post(post_id: str, vector: list[float], hour_offset: float = 0.0) -> ClusterPost:
    return ClusterPost(
        post_id=post_id,
        workspace_id="ws",
        source_id="src",
        content="",
        published_at=_BASE + timedelta(hours=hour_offset),
        relevance_score=0.5,
        source_score=0.5,
        tags=[],
        title="",
        url="",
        vector=vector,
    )


def _ids(groups: list[list[ClusterPost]]) -> list[list[str]]:
    return [[p.post_id for p in g] for g in groups]


def _random_posts(rng: random.Random, n: int, dim: int, span_hours: float) -> list[ClusterPost]:
    posts = []
    for i in range(n):
        vector = [rng.gauss(0.0, 1.0) for _ in range(dim)]
        posts.append(_post(f"p{i}", vector, hour_offset=rng.uniform(0.0, span_hours)))
    return posts


@requires_numpy
@pytest.mark.parametrize("seed", range(12))
@pytest.mark.parametrize("threshold", [0.0, 0.2, 0.5, 0.8, 0.95])
def test_components_np_matches_py_on_random_data(seed: int, threshold: float) -> None:
    rng = random.Random(seed)
    posts = _random_posts(rng, n=40, dim=24, span_hours=200.0)
    max_gap_h = 72
    assert _ids(_components_np(posts, threshold, max_gap_h)) == _ids(
        _components_py(posts, threshold, max_gap_h)
    )


@requires_numpy
@pytest.mark.parametrize("seed", range(6))
def test_components_np_matches_py_with_clustered_structure(seed: int) -> None:
    # Build a few tight clusters (shared base vector + small jitter) so real edges form.
    rng = random.Random(seed)
    dim = 16
    posts: list[ClusterPost] = []
    idx = 0
    for c in range(4):
        base = [rng.gauss(0.0, 1.0) for _ in range(dim)]
        for _ in range(6):
            jitter = [v + rng.gauss(0.0, 0.05) for v in base]
            posts.append(_post(f"p{idx}", jitter, hour_offset=rng.uniform(0.0, 40.0)))
            idx += 1
    for threshold in (0.3, 0.6, 0.85):
        assert _ids(_components_np(posts, threshold, 48)) == _ids(
            _components_py(posts, threshold, 48)
        ), f"mismatch at threshold={threshold}, seed={seed}"


@requires_numpy
@pytest.mark.parametrize("seed", range(6))
@pytest.mark.parametrize("threshold", [0.0, 0.5, 0.8])
def test_components_np_matches_py_across_block_sizes(
    monkeypatch: pytest.MonkeyPatch, seed: int, threshold: float
) -> None:
    """The partitioning must not depend on how rows are split into blocks.

    `_components_np` processes rows in blocks sized to `_COMPONENT_BLOCK_BYTES`, and at
    the production budget any test-sized input lands in a single block — so without
    shrinking the budget here the multi-block path (the entire point of the blocking)
    would never be exercised. n is deliberately not a multiple of the block sizes so the
    ragged final block is covered too.
    """
    rng = random.Random(seed)
    posts = _random_posts(rng, n=37, dim=24, span_hours=200.0)
    max_gap_h = 72
    expected = _ids(_components_py(posts, threshold, max_gap_h))
    for rows_per_block in (1, 3, 8, len(posts)):
        # rows_per_block = budget // (8 * n)  -> budget = 8 * n * rows_per_block
        monkeypatch.setattr(sc, "_COMPONENT_BLOCK_BYTES", 8 * len(posts) * rows_per_block)
        assert _ids(_components_np(posts, threshold, max_gap_h)) == expected, (
            f"mismatch at rows_per_block={rows_per_block}, "
            f"threshold={threshold}, seed={seed}"
        )


@requires_numpy
def test_components_np_blocked_matches_py_at_threshold_tie(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The near-threshold re-decision reads `boundary` with block-local row indices;
    # one row per block is where an off-by-one in that indexing would surface.
    v1 = [1.0, 2.0, 3.0, 4.0]
    v2 = [4.0, 3.0, 2.0, 1.0]
    posts = [_post("a", v1), _post("b", v2), _post("c", list(v1)), _post("d", list(v2))]
    t = cosine(v1, v2)
    monkeypatch.setattr(sc, "_COMPONENT_BLOCK_BYTES", 8 * len(posts))  # 1 row per block
    for threshold in (t, t + 1e-12, t - 1e-12):
        assert _ids(_components_np(posts, threshold, 24)) == _ids(
            _components_py(posts, threshold, 24)
        ), f"mismatch at threshold offset {threshold - t:+.0e}"


@requires_numpy
def test_components_np_matches_py_at_exact_threshold_tie() -> None:
    # threshold set exactly to a pair's cosine: the inclusive `>=` must agree.
    v1 = [1.0, 2.0, 3.0, 4.0]
    v2 = [4.0, 3.0, 2.0, 1.0]
    t = cosine(v1, v2)
    posts = [_post("a", v1), _post("b", v2)]
    # exactly at threshold -> edge present in both
    assert _ids(_components_np(posts, t, 24)) == _ids(_components_py(posts, t, 24))
    # a hair above the tie -> edge absent in both
    assert _ids(_components_np(posts, t + 1e-12, 24)) == _ids(
        _components_py(posts, t + 1e-12, 24)
    )


@requires_numpy
def test_components_np_matches_py_identical_vectors() -> None:
    # Identical vectors: cosine ~1.0 but matmul/sqrt rounding may not be exactly 1.0.
    v = [0.11, 0.22, 0.33, 0.44, 0.55]
    posts = [_post("a", list(v)), _post("b", list(v)), _post("c", list(v))]
    for threshold in (0.5, 0.999999, 1.0):
        assert _ids(_components_np(posts, threshold, 24)) == _ids(
            _components_py(posts, threshold, 24)
        ), f"mismatch at threshold={threshold}"


def test_components_identical_vectors_form_one_cluster() -> None:
    v = [1.0, 0.0, 1.0]
    posts = [_post("a", list(v)), _post("b", list(v)), _post("c", list(v))]
    groups = _components(posts, threshold=0.5, max_gap_h=24)
    assert len(groups) == 1
    assert sorted(p.post_id for p in groups[0]) == ["a", "b", "c"]


def test_components_gap_filter_splits_similar_posts() -> None:
    v = [1.0, 1.0, 1.0]
    # identical vectors but published_at far apart -> no edge despite high similarity
    posts = [_post("a", list(v), hour_offset=0.0), _post("b", list(v), hour_offset=100.0)]
    groups = _components(posts, threshold=0.5, max_gap_h=24)
    assert _ids(groups) == [["a"], ["b"]]


def test_components_zero_vector_is_isolated() -> None:
    posts = [
        _post("a", [1.0, 1.0, 1.0]),
        _post("b", [1.0, 1.0, 1.0]),
        _post("z", [0.0, 0.0, 0.0]),
    ]
    groups = _components(posts, threshold=0.5, max_gap_h=24)
    shape = sorted(sorted(p.post_id for p in g) for g in groups)
    assert shape == [["a", "b"], ["z"]]


def test_components_singleton_and_empty() -> None:
    assert _components([], threshold=0.5, max_gap_h=24) == []
    one = _components([_post("solo", [1.0, 2.0])], threshold=0.5, max_gap_h=24)
    assert _ids(one) == [["solo"]]


def test_components_dispatcher_matches_reference() -> None:
    # The public _components (numpy when available) must match the pure-Python reference.
    rng = random.Random(99)
    posts = _random_posts(rng, n=30, dim=20, span_hours=150.0)
    assert _ids(_components(posts, 0.4, 60)) == _ids(_components_py(posts, 0.4, 60))
