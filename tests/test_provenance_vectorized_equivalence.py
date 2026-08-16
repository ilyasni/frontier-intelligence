"""Equivalence tests for the vectorized same-artifact grouping.

`_same_artifact_groups_np` (blocked numpy matmul) must return exactly what the
pure-Python reference `_same_artifact_groups_py` returns — same groups, same member
order, same group order — for every input, including threshold and gap boundary ties.
These tests are the safety net for the vectorization of the provenance hotspot, where
a 2560-dim cosine per pair was 38% of signal analysis.

The last two tests are about the tests: one asserts the random fixtures actually
produce merges (an all-singletons fixture would make every equivalence assertion pass
vacuously), the other flips a single edge and asserts the comparison notices.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from shared import provenance as prov
from shared.provenance import (
    NEAR_DUP_THRESHOLD,
    ProvenancePost,
    _cosine,
    _hours_between,
    _near_dup_pairs_np,
    _rows_per_block,
    _same_artifact_groups_np,
    _same_artifact_groups_py,
    same_artifact_groups,
)

requires_numpy = pytest.mark.skipif(prov.np is None, reason="numpy not available")

_BASE = datetime(2026, 7, 1, 9, 0, tzinfo=UTC)


def _p(
    post_id: str,
    vector: list[float] | None,
    *,
    hour_offset: float = 0.0,
    url: str | None = None,
    title: str = "",
) -> ProvenancePost:
    return ProvenancePost(
        post_id=post_id,
        source_id=f"src-{post_id}",
        published_at=_BASE + timedelta(hours=hour_offset),
        url=url,
        title=title,
        vector=vector,
    )


def _ids(groups: list[list[ProvenancePost]]) -> list[list[str]]:
    return [[p.post_id for p in g] for g in groups]


def _merges(groups: list[list[ProvenancePost]]) -> int:
    """Posts absorbed into a group with someone else — i.e. edges that mattered."""
    return sum(len(g) - 1 for g in groups)


def _random_posts(
    rng: random.Random,
    n: int,
    dim: int,
    *,
    span_hours: float = 200.0,
    dup_share: float = 0.35,
    urls: bool = True,
    titles: bool = True,
) -> list[ProvenancePost]:
    """Random posts with a deliberate share of near-duplicates.

    Independent gaussian vectors at 2560 dims are near-orthogonal, so a purely random
    fixture would never cross a 0.97 threshold and the equivalence check would compare
    two lists of singletons. `dup_share` of the posts are jittered copies of an earlier
    one, which is what makes the near-text leg fire at all.
    """
    posts: list[ProvenancePost] = []
    for i in range(n):
        if posts and rng.random() < dup_share:
            source = rng.choice(posts)
            jitter = 10 ** rng.uniform(-4, -1)
            vector = [v + rng.gauss(0.0, jitter) for v in (source.vector or [])]
            title = source.title
            url = source.url
        else:
            vector = [rng.gauss(0.0, 1.0) for _ in range(dim)]
            title = f"headline {rng.randrange(8)}" if titles else ""
            url = f"https://e{rng.randrange(6)}.com/a{rng.randrange(10)}" if urls else None
        posts.append(
            _p(f"p{i}", vector, hour_offset=rng.uniform(0.0, span_hours), url=url, title=title)
        )
    return posts


# --- equivalence -------------------------------------------------------------


@requires_numpy
@pytest.mark.unit
@pytest.mark.parametrize("seed", range(12))
@pytest.mark.parametrize("threshold", [0.5, 0.9, NEAR_DUP_THRESHOLD, 0.999, 1.0])
def test_np_matches_py_on_random_data(seed: int, threshold: float) -> None:
    rng = random.Random(seed)
    posts = _random_posts(rng, n=40, dim=24)
    assert _ids(_same_artifact_groups_np(posts, near_dup_threshold=threshold)) == _ids(
        _same_artifact_groups_py(posts, near_dup_threshold=threshold)
    )


@requires_numpy
@pytest.mark.unit
@pytest.mark.parametrize("seed", range(6))
def test_np_matches_py_without_urls_or_titles(seed: int) -> None:
    """Isolate the near-text leg: with the two exact legs silenced it is the only edge
    source, so any disagreement here is the matmul's and nothing else's."""
    rng = random.Random(100 + seed)
    posts = _random_posts(rng, n=30, dim=32, urls=False, titles=False)
    for threshold in (0.8, NEAR_DUP_THRESHOLD, 0.995):
        assert _ids(_same_artifact_groups_np(posts, near_dup_threshold=threshold)) == _ids(
            _same_artifact_groups_py(posts, near_dup_threshold=threshold)
        ), f"mismatch at threshold={threshold}, seed={seed}"


@requires_numpy
@pytest.mark.unit
@pytest.mark.parametrize("seed", range(6))
@pytest.mark.parametrize("threshold", [0.7, NEAR_DUP_THRESHOLD])
def test_np_matches_py_across_block_sizes(
    monkeypatch: pytest.MonkeyPatch, seed: int, threshold: float
) -> None:
    """The partition must not depend on how rows are split into blocks.

    At the production budget any test-sized input lands in a single block, so without
    shrinking the budget the multi-block path — the entire point of the blocking —
    would never run. n is deliberately not a multiple of the block sizes so the ragged
    final block is covered too.
    """
    rng = random.Random(200 + seed)
    posts = _random_posts(rng, n=37, dim=24)
    expected = _ids(_same_artifact_groups_py(posts, near_dup_threshold=threshold))
    for rows in (1, 3, 8, len(posts)):
        # rows_per_block = budget // (8 * m)  ->  budget = 8 * m * rows
        monkeypatch.setattr(prov, "_NEAR_DUP_BLOCK_BYTES", 8 * len(posts) * rows)
        assert _ids(_same_artifact_groups_np(posts, near_dup_threshold=threshold)) == expected, (
            f"mismatch at rows_per_block={rows}, threshold={threshold}, seed={seed}"
        )


@requires_numpy
@pytest.mark.unit
def test_np_matches_py_at_exact_cosine_tie(monkeypatch: pytest.MonkeyPatch) -> None:
    """Threshold set exactly to a pair's cosine: the inclusive `>=` must agree.

    Also run one row per block — the near-threshold re-decision indexes `boundary`
    with block-local rows, and that is where an off-by-one would surface.
    """
    v1 = [1.0, 2.0, 3.0, 4.0]
    v2 = [4.0, 3.0, 2.0, 1.0]
    t = _cosine(v1, v2)
    posts = [
        _p("a", list(v1)),
        _p("b", list(v2), hour_offset=1),
        _p("c", list(v1), hour_offset=2),
        _p("d", list(v2), hour_offset=3),
        _p("e", list(v1), hour_offset=4),
        _p("f", list(v2), hour_offset=5),
    ]
    for budget in (8 * len(posts), prov._NEAR_DUP_BLOCK_BYTES):
        monkeypatch.setattr(prov, "_NEAR_DUP_BLOCK_BYTES", budget)
        for threshold in (t, t + 1e-12, t - 1e-12):
            assert _ids(_same_artifact_groups_np(posts, near_dup_threshold=threshold)) == _ids(
                _same_artifact_groups_py(posts, near_dup_threshold=threshold)
            ), f"mismatch at threshold offset {threshold - t:+.0e}, budget={budget}"


@requires_numpy
@pytest.mark.unit
def test_np_matches_py_at_exact_gap_boundary() -> None:
    """Gap exactly at max_gap_hours, and a microsecond either side of it.

    The vectorized path rebuilds the gap from integer microsecond offsets rather than
    calling `_hours_between`; this pins that the two agree on the inclusive boundary
    instead of trusting that they round alike.
    """
    v = [0.5, 0.25, 0.125, 1.0]
    for delta in (timedelta(0), timedelta(microseconds=-1), timedelta(microseconds=1)):
        posts = [_p(f"q{i}", list(v), hour_offset=0.0) for i in range(5)]
        far = ProvenancePost(
            post_id="far",
            source_id="src-far",
            published_at=_BASE + timedelta(hours=72) + delta,
            vector=list(v),
        )
        posts.append(far)
        assert _ids(_same_artifact_groups_np(posts, max_gap_hours=72.0)) == _ids(
            _same_artifact_groups_py(posts, max_gap_hours=72.0)
        ), f"mismatch at boundary offset {delta}"


@requires_numpy
@pytest.mark.unit
@pytest.mark.parametrize("seed", range(6))
def test_np_matches_py_with_missing_and_zero_vectors(seed: int) -> None:
    """None, empty and all-zero vectors: `_cosine` returns 0.0 or is never called, and
    the matmul path must reach the same verdict through its `denom > 0` guard."""
    rng = random.Random(300 + seed)
    posts = _random_posts(rng, n=24, dim=16)
    posts[2] = _p("p2", None, hour_offset=3, title=posts[2].title, url=posts[2].url)
    posts[5] = _p("p5", [], hour_offset=5)
    posts[9] = _p("p9", [0.0] * 16, hour_offset=9)
    posts[11] = _p("p11", [0.0] * 16, hour_offset=11)
    for threshold in (0.0, 0.5, NEAR_DUP_THRESHOLD):
        assert _ids(_same_artifact_groups_np(posts, near_dup_threshold=threshold)) == _ids(
            _same_artifact_groups_py(posts, near_dup_threshold=threshold)
        ), f"mismatch at threshold={threshold}, seed={seed}"


@requires_numpy
@pytest.mark.unit
@pytest.mark.parametrize("seed", range(8))
def test_public_dispatcher_matches_reference(seed: int) -> None:
    """What production calls — including the short-input and fallback branches."""
    rng = random.Random(400 + seed)
    # straddles _NEAR_DUP_MIN_VECTORIZED so both sides of the dispatch are covered
    for n in (0, 1, 2, 3, 4, 20):
        posts = _random_posts(rng, n=n, dim=12)
        assert _ids(same_artifact_groups(posts)) == _ids(_same_artifact_groups_py(posts))


@requires_numpy
@pytest.mark.unit
def test_mixed_vector_widths_fall_back_to_reference() -> None:
    """Different widths short-circuit `_cosine` to 0.0, which one matrix cannot hold."""
    posts = [_p(f"w{i}", [1.0, 2.0, 3.0], hour_offset=i) for i in range(5)]
    posts.append(_p("odd", [1.0, 2.0], hour_offset=5))
    for threshold in (0.0, NEAR_DUP_THRESHOLD):
        assert _ids(same_artifact_groups(posts, near_dup_threshold=threshold)) == _ids(
            _same_artifact_groups_py(posts, near_dup_threshold=threshold)
        ), f"mismatch at threshold={threshold}"


@requires_numpy
@pytest.mark.unit
def test_pairs_are_strictly_upper_triangular() -> None:
    """The documented contract. Relaxing the triangle guard to `>=` adds self-pairs,
    which `dsu.union(i, i)` silently absorbs — invisible in every partition test."""
    posts = _random_posts(random.Random(55), n=30, dim=16)
    pairs = _near_dup_pairs_np(posts, 0.5, 72.0)
    assert pairs, "fixture produced no pairs to check"
    assert all(i < j for i, j in pairs)
    assert len(set(pairs)) == len(pairs), "duplicate pairs"


@requires_numpy
@pytest.mark.unit
def test_gap_rounds_exactly_like_hours_between() -> None:
    """`_hours_between` divides twice — to seconds, then to hours — and the vectorized
    gap must round the same way.

    Folding the two into one `/ 3.6e9` shifts about one microsecond value in seven by a
    single ulp. That is invisible at any ordinary threshold and decisive at one sitting
    on the seam, which is exactly the kind of difference a tolerance-based test would
    wave through. This fixture puts the threshold on the seam.
    """
    micros = 259_199_999_016
    two_divisions = micros / 1e6 / 3600.0
    one_division = micros / 3.6e9
    assert two_divisions != one_division, "fixture no longer straddles the rounding forms"

    v = [0.5, 0.25, 0.125, 1.0]
    seam = ProvenancePost(
        post_id="seam",
        source_id="src-seam",
        published_at=_BASE + timedelta(microseconds=micros),
        vector=list(v),
    )
    posts = [_p("origin", list(v)), seam]
    assert _hours_between(seam.published_at, posts[0].published_at) == two_divisions

    # threshold exactly at the two-division value: the pair is inside the gap
    assert _ids(_same_artifact_groups_np(posts, max_gap_hours=two_divisions)) == _ids(
        _same_artifact_groups_py(posts, max_gap_hours=two_divisions)
    )
    assert len(_same_artifact_groups_np(posts, max_gap_hours=two_divisions)) == 1


# --- the constant-memory invariant -------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("m", [1, 2, 10, 512, 5_000, 50_000, 500_000])
def test_block_size_keeps_peak_constant_in_n(m: int) -> None:
    """One block must stay inside the byte budget however large the input gets.

    This is the invariant, not a detail: the full n×n form is what memcg-SIGKILLed
    signal analysis 28 runs in a row, and a SIGKILL raises no MemoryError for the
    fallback to catch. A single row is the floor — O(m), never O(m²).
    """
    rows = _rows_per_block(m)
    assert rows >= 1
    assert rows <= m
    assert rows * m * 8 <= prov._NEAR_DUP_BLOCK_BYTES or rows == 1


@requires_numpy
@pytest.mark.unit
def test_pass_blocks_the_matmul_instead_of_allocating_the_whole_matrix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_rows_per_block` returning a bounded number is worth nothing if the pass
    ignores it — and ignoring it changes no partition, so no equivalence test can see
    it. One `np.outer` per block makes the block structure observable.
    """
    posts = _random_posts(random.Random(77), n=64, dim=16)
    heights: list[int] = []
    real_outer = prov.np.outer

    def spy(a, b):
        heights.append(len(a))
        return real_outer(a, b)

    monkeypatch.setattr(prov.np, "outer", spy)
    monkeypatch.setattr(prov, "_NEAR_DUP_BLOCK_BYTES", 8 * len(posts) * 4)  # 4 rows/block
    _near_dup_pairs_np(posts, NEAR_DUP_THRESHOLD, 72.0)

    assert len(heights) > 1, "budget ignored — the whole matrix went through in one block"
    assert sum(heights) == len(posts), "blocks must tile the rows exactly once"
    assert max(heights) * len(posts) * 8 <= prov._NEAR_DUP_BLOCK_BYTES


@requires_numpy
@pytest.mark.unit
def test_block_count_grows_while_block_stays_bounded() -> None:
    """Doubling n must not double the block width — that is what keeps the peak flat."""
    assert _rows_per_block(2_000) > _rows_per_block(4_000)
    assert _rows_per_block(2_000) * 2_000 == pytest.approx(
        _rows_per_block(4_000) * 4_000, rel=0.01
    )


# --- tests about the tests ---------------------------------------------------


@requires_numpy
@pytest.mark.unit
def test_random_fixture_actually_produces_merges() -> None:
    """Guards every equivalence assertion above from passing vacuously.

    If the fixtures degenerated to all-singletons — near-orthogonal vectors never
    crossing the threshold — the comparisons would still be green while proving
    nothing about the near-text leg.
    """
    merged_at_production_threshold = 0
    for seed in range(12):
        posts = _random_posts(random.Random(seed), n=40, dim=24)
        groups = _same_artifact_groups_np(posts, near_dup_threshold=NEAR_DUP_THRESHOLD)
        merged_at_production_threshold += _merges(groups)
        assert len(groups) < len(posts), f"seed {seed} produced no group at all"
    assert merged_at_production_threshold >= 12

    # and the near-text leg specifically, with the exact legs silenced
    text_only = _random_posts(random.Random(7), n=40, dim=24, urls=False, titles=False)
    assert _near_dup_pairs_np(text_only, NEAR_DUP_THRESHOLD, 72.0), "near-text leg never fired"


@requires_numpy
@pytest.mark.unit
def test_equivalence_check_detects_a_single_flipped_edge() -> None:
    """Mutation check: one edge's worth of difference must fail the comparison.

    Two posts sit just above the threshold. Nudging only the vectorized path's
    threshold past their cosine removes exactly that one edge; if `_ids(...) == _ids(...)`
    cannot see that, it cannot see a real vectorization bug either.
    """
    rng = random.Random(11)
    base = [rng.gauss(0.0, 1.0) for _ in range(64)]
    near = [v + rng.gauss(0.0, 0.01) for v in base]
    posts = [_p("a", base), _p("b", near, hour_offset=1.0)]
    posts += [
        _p(f"f{i}", [rng.gauss(0.0, 1.0) for _ in range(64)], hour_offset=i) for i in range(4)
    ]

    t = _cosine(base, near)
    assert NEAR_DUP_THRESHOLD < t < 1.0, "fixture must straddle the production threshold"

    honest = _ids(_same_artifact_groups_py(posts, near_dup_threshold=t))
    assert _ids(_same_artifact_groups_np(posts, near_dup_threshold=t)) == honest
    assert ["a", "b"] in honest, "the edge under test must actually be present"

    # the mutant: the same pair, just out of reach for the vectorized path only. The
    # offset is well outside `_NEAR_DUP_COSINE_EPS`, so the edge is dropped outright
    # rather than by the near-threshold re-decision.
    mutated = _ids(_same_artifact_groups_np(posts, near_dup_threshold=t + 1e-6))
    assert mutated != honest, "comparison is blind to a single missing edge"
    assert ["a", "b"] not in mutated
