"""Cross-source provenance / independence proxy.

Collapses re-syndicated duplicates inside a cluster so that "independence"
reflects distinct originators, not distinct feeds. See
``docs/provenance-independence-layer.md`` for the design and its honest limits.

IMPORTANT: everything here is a PROXY — it measures outlet/artifact multiplicity
and arrival shape, NOT proven copy-lineage. A shared upstream trigger (one release
picked up by many independent outlets) still looks like several voices. Keep the
outputs inside the Confidence Model (``Гипотеза`` / ``Вероятно``), never
``Подтверждено``. Matches the ``signal-nature-lens`` reference in the skills.

Grouping signals, from strongest to weakest:
1. same canonical URL  -> exact re-ingestion / syndication of one artifact
2. near-identical text  -> same material under different URLs (cosine >= threshold)
3. identical normalized title within a time gap -> same headline re-posted

The near-dup cosine threshold defaults to 0.97 (near-identical), deliberately much
stricter than a topical-similarity threshold: at 0.60 (a text-reuse recipe tuned for
dedicated reuse embeddings) our topic embeddings would merge every post in a topic.
Paraphrased echoes with different text/URL are NOT caught here — that is what the
distinct-originator (named-actor) count is for.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

try:
    import numpy as np
except Exception:  # pragma: no cover - transitive (fastembed/onnxruntime) in worker/admin
    np = None

logger = logging.getLogger(__name__)

# Query-param prefixes dropped when canonicalizing (superset of ingest.sources.base
# _TRACKED_QUERY_PREFIXES; kept in sync intentionally).
_TRACKED_QUERY_PREFIXES = ("utm_", "rss", "ref", "source", "fbclid", "gclid", "yclid")
_AMP_SUFFIXES = ("/amp", "/amp/", ".amp")
_MOBILE_HOST_PREFIXES = ("m.", "amp.", "mobile.")

_TITLE_STRIP = re.compile(r"[^\w\s]", re.UNICODE)
_WS = re.compile(r"\s+")

NEAR_DUP_THRESHOLD = 0.97
NEAR_DUP_MAX_GAP_HOURS = 72.0

# Width of the band around near_dup_threshold within which the vectorized (matmul)
# cosine is re-decided with the exact pure-Python `_cosine`. A matmul sums in a
# different order than the sequential Python loop, and normalizes as
# sqrt(na)*sqrt(nb) rather than sqrt(na*nb), so the two can differ by ~1e-13 at
# 2560 dims. This band is orders of magnitude wider than that error and orders of
# magnitude narrower than any real gap between cosines, so re-deciding inside it
# makes the vectorized edge set identical to the pure-Python one. Mirrors
# `_COMPONENT_COSINE_EPS` in worker.services.semantic_clustering.
_NEAR_DUP_COSINE_EPS = 1e-9

# Byte budget for one B×N intermediate in the blocked cosine pass. The pass keeps a
# few such arrays alive at once, so the real peak is a small multiple of this — but
# constant in n, never quadratic. That is the whole point: a full n×n similarity
# matrix is what OOM-killed signal analysis on the largest workspace, and a memcg
# SIGKILL raises no MemoryError, so no `except` below could catch it. Smaller than
# the clustering budget because this runs per cluster, inside a pass that is already
# holding every member post's vector.
_NEAR_DUP_BLOCK_BYTES = 8 * 1024 * 1024

# Below this member count the pure-Python path wins: laying a float64 matrix out of
# Python lists costs more than the single cosine it saves. Measured at the production
# embedding width (2560), best-of-N ms per call — the crossover is sharp and early:
#   n=2   py 0.235   np 0.259   -> py
#   n=3   py 0.736   np 0.329   -> np, 2.2x
#   n=8   py 7.44    np 0.94    -> np, 8x
#   n=150 py 2543    np 17.9    -> np, 142x
_NEAR_DUP_MIN_VECTORIZED = 3

_ONE_MICROSECOND = timedelta(microseconds=1)


def _rows_per_block(m: int) -> int:
    """Rows per matmul block: the widest B keeping one B×m float64 inside the budget.

    Clamped to at least one row, so a single row wider than the budget still runs
    (one row is O(m), never O(m²) — the shape that OOM-killed the job).
    """
    return max(1, min(m, int(_NEAR_DUP_BLOCK_BYTES // (8 * max(m, 1)))))


@dataclass(frozen=True)
class ProvenancePost:
    """Minimal post view needed to reason about re-syndication inside a cluster."""

    post_id: str
    source_id: str
    published_at: datetime
    url: str | None = None
    title: str = ""
    vector: list[float] | None = None
    # Named originators (organizations / products) extracted upstream, lowercased.
    actors: tuple[str, ...] = ()


@dataclass(frozen=True)
class EchoEdge:
    """One echo -> origin relation inside a same-artifact group."""

    echo_post_id: str
    origin_post_id: str
    method: str  # "canonical_url" | "near_text" | "same_title"
    score: float  # 1.0 for exact url/title, cosine for near_text
    lag_hours: float  # published_at(echo) - published_at(origin), hours, >= 0


def canonical_url(url: str | None) -> str | None:
    """Cross-source canonical form of a URL.

    Stricter than ``ingest.sources.base.canonicalize_url``: also strips a trailing
    slash, AMP suffixes, and ``m.``/``amp.``/``mobile.`` host prefixes so the same
    article syndicated through mobile/AMP variants collapses to one key.
    """
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return url.strip() or None
    host = parsed.netloc.lower()
    for prefix in _MOBILE_HOST_PREFIXES:
        if host.startswith(prefix):
            host = host[len(prefix) :]
            break
    query = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if not any(k.lower().startswith(prefix) for prefix in _TRACKED_QUERY_PREFIXES)
    ]
    path = parsed.path
    for suffix in _AMP_SUFFIXES:
        if path.endswith(suffix):
            path = path[: -len(suffix)] or "/"
            break
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    clean = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=host,
        path=path,
        query=urlencode(query, doseq=True),
        fragment="",
    )
    return urlunparse(clean)


def _norm_title(title: str) -> str:
    """Lowercase, drop punctuation, collapse whitespace — for exact-headline match."""
    if not title:
        return ""
    return _WS.sub(" ", _TITLE_STRIP.sub(" ", title.lower())).strip()


def _cosine(a: list[float] | None, b: list[float] | None) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / math.sqrt(na * nb)


def _hours_between(a: datetime, b: datetime) -> float:
    return abs((a - b).total_seconds()) / 3600.0


class _DSU:
    """Tiny union-find over integer indices."""

    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def _groups_from_dsu(posts: list[ProvenancePost], dsu: _DSU) -> list[list[ProvenancePost]]:
    """Materialize the partition. Depends on the components alone, not on edge order.

    ``_DSU.union`` always attaches the larger root to the smaller, so a component's
    root is its minimum index no matter which edges were fed in or when. Bucket
    insertion order therefore follows ascending index, and both sorts below are
    stable — so any two edge sets with the same transitive closure produce a
    byte-identical result here. That is what lets the vectorized path skip the
    pure-Python path's incremental `dsu.find` short-circuit.
    """
    buckets: dict[int, list[ProvenancePost]] = {}
    for i, post in enumerate(posts):
        buckets.setdefault(dsu.find(i), []).append(post)
    groups = [sorted(g, key=lambda p: p.published_at) for g in buckets.values()]
    groups.sort(key=lambda g: g[0].published_at)
    return groups


def _same_artifact_groups_py(
    posts: list[ProvenancePost],
    *,
    near_dup_threshold: float = NEAR_DUP_THRESHOLD,
    max_gap_hours: float = NEAR_DUP_MAX_GAP_HOURS,
) -> list[list[ProvenancePost]]:
    """Pure-Python O(n²) grouping. Reference implementation for the vectorized path."""
    n = len(posts)
    if n <= 1:
        return [list(posts)] if posts else []

    dsu = _DSU(n)

    # 1. exact canonical-URL match (transitive via a bucket)
    by_url: dict[str, int] = {}
    for i, post in enumerate(posts):
        cu = canonical_url(post.url)
        if not cu:
            continue
        if cu in by_url:
            dsu.union(by_url[cu], i)
        else:
            by_url[cu] = i

    # 2/3. pairwise near-text and same-title, gated by time gap.
    norm_titles = [_norm_title(p.title) for p in posts]
    for i in range(n):
        for j in range(i + 1, n):
            if dsu.find(i) == dsu.find(j):
                continue
            gap = _hours_between(posts[i].published_at, posts[j].published_at)
            if gap > max_gap_hours:
                continue
            if norm_titles[i] and norm_titles[i] == norm_titles[j]:
                dsu.union(i, j)
                continue
            if posts[i].vector and posts[j].vector:
                if _cosine(posts[i].vector, posts[j].vector) >= near_dup_threshold:
                    dsu.union(i, j)

    return _groups_from_dsu(posts, dsu)


def _near_dup_pairs_np(
    posts: list[ProvenancePost], threshold: float, max_gap_hours: float
) -> list[tuple[int, int]]:
    """Index pairs (i < j) whose vectors are near-identical within the time gap.

    Blocked matmul instead of a 2560-dim cosine per pair. Only *edges* are ever
    needed, never the whole similarity matrix, so rows are processed in blocks sized
    to ``_NEAR_DUP_BLOCK_BYTES``: peak memory is constant in n.

    Two things make the result exactly equal to the pure-Python predicate:

    * the time gap is computed from integer microsecond offsets, so
      ``|u_i - u_j| / 1e6 / 3600`` is the same float64 ``_hours_between`` produces —
      identical operands, identical operations, no tolerance needed;
    * cosines landing within ``_NEAR_DUP_COSINE_EPS`` of the threshold are re-decided
      with the exact ``_cosine``, so matmul's differing summation order can never flip
      an edge. float64 is load-bearing here: a float32 matmul errs by ~1e-6, which
      dwarfs the band and would silently break that guarantee.

    Callers guarantee a uniform vector width (see ``same_artifact_groups``).
    """
    idx = [i for i, post in enumerate(posts) if post.vector]
    m = len(idx)
    if m < 2:
        return []

    ref = posts[idx[0]].published_at
    matrix = np.asarray([posts[i].vector for i in idx], dtype=np.float64)
    norms = np.sqrt((matrix * matrix).sum(axis=1))
    micros = np.array(
        [(posts[i].published_at - ref) // _ONE_MICROSECOND for i in idx], dtype=np.int64
    )
    positions = np.arange(m)
    rows_per_block = _rows_per_block(m)

    pairs: list[tuple[int, int]] = []
    for start in range(0, m, rows_per_block):
        stop = min(start + rows_per_block, m)
        dot = matrix[start:stop] @ matrix.T
        denom = np.outer(norms[start:stop], norms)
        with np.errstate(divide="ignore", invalid="ignore"):
            cos = np.where(denom > 0.0, dot / denom, 0.0)
        # Freed before the gap arrays are allocated: keeps the live set at ~3 blocks.
        del dot, denom
        # Two divisions, not one by 3.6e9: `_hours_between` rounds twice and this must
        # round the same way to stay bit-for-bit identical to it.
        gap_h = np.abs(micros[start:stop, None] - micros[None, :]) / 1e6 / 3600.0
        gap_ok = (gap_h <= max_gap_hours) & (positions[None, :] > positions[start:stop, None])
        del gap_h
        # `edge` only has to be right away from the band: an exact tie has
        # |cos - threshold| = 0 <= eps, so it lands in `boundary` and is settled by
        # `_cosine` below regardless of whether this reads >= or >. Both arrays feed one
        # candidate set; neither decides alone.
        edge = (cos >= threshold) & gap_ok
        boundary = gap_ok & (np.abs(cos - threshold) <= _NEAR_DUP_COSINE_EPS)
        for block_i, block_j in zip(*np.nonzero(edge | boundary)):
            i, j = idx[start + int(block_i)], idx[int(block_j)]
            # Near-threshold pairs defer to the exact cosine; the gap already holds.
            if bool(boundary[block_i, block_j]) and (
                _cosine(posts[i].vector, posts[j].vector) < threshold
            ):
                continue
            pairs.append((i, j))
    return pairs


def _same_artifact_groups_np(
    posts: list[ProvenancePost],
    *,
    near_dup_threshold: float = NEAR_DUP_THRESHOLD,
    max_gap_hours: float = NEAR_DUP_MAX_GAP_HOURS,
) -> list[list[ProvenancePost]]:
    """Vectorized equivalent of ``_same_artifact_groups_py``.

    Same three legs, same partition — only the near-text leg is computed differently.
    The pure-Python path interleaves the legs and short-circuits on pairs already
    united, so it evaluates strictly fewer cosines; this path evaluates them all at
    once in a matmul. The extra pairs it decides are, by construction, either already
    united (the short-circuit) or united by the title leg anyway, so they add edges
    only *inside* existing components and leave the transitive closure — and hence
    ``_groups_from_dsu`` — untouched.
    """
    n = len(posts)
    if n <= 1:
        return [list(posts)] if posts else []

    dsu = _DSU(n)

    # 1. exact canonical-URL match (transitive via a bucket)
    by_url: dict[str, int] = {}
    for i, post in enumerate(posts):
        cu = canonical_url(post.url)
        if not cu:
            continue
        if cu in by_url:
            dsu.union(by_url[cu], i)
        else:
            by_url[cu] = i

    # 2. identical normalized title within the gap. Bucketing by title turns the
    # pairwise scan into one pass per bucket; sorting lets it stop at the first pair
    # beyond the gap instead of scanning the bucket's tail.
    by_title: dict[str, list[int]] = {}
    for i, post in enumerate(posts):
        norm = _norm_title(post.title)
        if norm:
            by_title.setdefault(norm, []).append(i)
    for bucket in by_title.values():
        if len(bucket) < 2:
            continue
        bucket.sort(key=lambda i: posts[i].published_at)
        for offset, earlier in enumerate(bucket):
            for later in bucket[offset + 1 :]:
                if (
                    _hours_between(posts[later].published_at, posts[earlier].published_at)
                    > max_gap_hours
                ):
                    break
                dsu.union(earlier, later)

    # 3. near-identical text within the gap
    for i, j in _near_dup_pairs_np(posts, near_dup_threshold, max_gap_hours):
        dsu.union(i, j)

    return _groups_from_dsu(posts, dsu)


def same_artifact_groups(
    posts: list[ProvenancePost],
    *,
    near_dup_threshold: float = NEAR_DUP_THRESHOLD,
    max_gap_hours: float = NEAR_DUP_MAX_GAP_HOURS,
) -> list[list[ProvenancePost]]:
    """Group posts that are the same underlying material (one "voice" each).

    Union of: same canonical URL (any time gap), near-identical text within the gap,
    identical normalized non-empty title within the gap. Groups are returned sorted
    by earliest ``published_at``; members within a group are sorted the same way, so
    ``group[0]`` is the candidate origin.

    Dispatches to the vectorized path when it can pay off. Mixed vector widths go to
    the reference: ``_cosine`` short-circuits those pairs to 0.0, which one matrix
    cannot express, and no production workspace mixes embedding models anyway.
    """
    if np is None or len(posts) < _NEAR_DUP_MIN_VECTORIZED:
        return _same_artifact_groups_py(
            posts, near_dup_threshold=near_dup_threshold, max_gap_hours=max_gap_hours
        )
    if len({len(post.vector) for post in posts if post.vector}) > 1:
        return _same_artifact_groups_py(
            posts, near_dup_threshold=near_dup_threshold, max_gap_hours=max_gap_hours
        )
    try:
        return _same_artifact_groups_np(
            posts, near_dup_threshold=near_dup_threshold, max_gap_hours=max_gap_hours
        )
    except Exception:
        logger.exception("vectorized same_artifact_groups failed; falling back to pure-Python")
        return _same_artifact_groups_py(
            posts, near_dup_threshold=near_dup_threshold, max_gap_hours=max_gap_hours
        )


def exact_artifact_groups(
    posts: list[ProvenancePost],
    *,
    max_gap_hours: float = NEAR_DUP_MAX_GAP_HOURS,
) -> list[list[ProvenancePost]]:
    """Same-artifact groups over a whole run, using only the two EXACT legs.

    ``same_artifact_groups`` still compares every pair — vectorized now, but O(n^2)
    all the same, and at a run's whole post population (n ~ 2-6k) that is tens of
    billions of multiply-adds. Fine for one cluster's members, not for this. So this
    variant keeps the identical canonicalization and drops the near-text leg, leaving
    O(n) hashing plus a bounded pass inside each equal-title bucket.

    Dropping that leg costs recall, never precision: every group here is also a group
    under ``same_artifact_groups``. That direction is the safe one for a quality
    metric — a missed echo understates fragmentation, it cannot invent it.

    Groups of one are omitted: a lone artifact cannot be split across clusters, so it
    carries no information for the split-rate metrics and would only pad denominators.
    """
    if len(posts) < 2:
        return []

    index = {id(post): i for i, post in enumerate(posts)}
    dsu = _DSU(len(posts))

    # 1. exact canonical-URL match — any time gap, transitive via one bucket per URL.
    by_url: dict[str, int] = {}
    for i, post in enumerate(posts):
        cu = canonical_url(post.url)
        if not cu:
            continue
        if cu in by_url:
            dsu.union(by_url[cu], i)
        else:
            by_url[cu] = i

    # 2. identical normalized title within the gap. Bucketing by title first turns the
    # pairwise scan into one pass per bucket; sorting lets it stop at the first pair
    # beyond the gap instead of scanning the bucket's tail.
    by_title: dict[str, list[ProvenancePost]] = {}
    for post in posts:
        norm = _norm_title(post.title)
        if norm:
            by_title.setdefault(norm, []).append(post)
    for bucket in by_title.values():
        if len(bucket) < 2:
            continue
        bucket.sort(key=lambda p: p.published_at)
        for offset, earlier in enumerate(bucket):
            for later in bucket[offset + 1 :]:
                if _hours_between(later.published_at, earlier.published_at) > max_gap_hours:
                    break
                dsu.union(index[id(earlier)], index[id(later)])

    buckets: dict[int, list[ProvenancePost]] = {}
    for i, post in enumerate(posts):
        buckets.setdefault(dsu.find(i), []).append(post)
    groups = [sorted(g, key=lambda p: p.published_at) for g in buckets.values() if len(g) >= 2]
    groups.sort(key=lambda g: g[0].published_at)
    return groups


def echo_edges(groups: list[list[ProvenancePost]]) -> list[EchoEdge]:
    """Origin = earliest post per group; every other member is an echo of it."""
    edges: list[EchoEdge] = []
    for group in groups:
        if len(group) < 2:
            continue
        origin = group[0]
        origin_cu = canonical_url(origin.url)
        origin_title = _norm_title(origin.title)
        for echo in group[1:]:
            if origin_cu and canonical_url(echo.url) == origin_cu:
                method, score = "canonical_url", 1.0
            elif origin_title and _norm_title(echo.title) == origin_title:
                method, score = "same_title", 1.0
            else:
                method = "near_text"
                score = round(_cosine(origin.vector, echo.vector), 4)
            edges.append(
                EchoEdge(
                    echo_post_id=echo.post_id,
                    origin_post_id=origin.post_id,
                    method=method,
                    score=score,
                    lag_hours=round(_hours_between(echo.published_at, origin.published_at), 3),
                )
            )
    return edges


@dataclass
class IndependenceMetrics:
    raw_source_count: int
    deduped_source_count: int
    distinct_voices: int
    echo_ratio: float
    distinct_originators: int | None
    arrival_dispersion: float
    single_day_spike: bool
    independence_score: float
    first_seen_at: datetime | None
    groups: list[list[ProvenancePost]] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "raw_source_count": self.raw_source_count,
            "deduped_source_count": self.deduped_source_count,
            "distinct_voices": self.distinct_voices,
            "echo_ratio": self.echo_ratio,
            "distinct_originators": self.distinct_originators,
            "arrival_dispersion": self.arrival_dispersion,
            "single_day_spike": self.single_day_spike,
            "independence_score": self.independence_score,
            "first_seen_at": self.first_seen_at.isoformat() if self.first_seen_at else None,
        }


def _arrival_dispersion(groups: list[list[ProvenancePost]]) -> tuple[float, bool]:
    """Spread of per-voice first-appearance across calendar days.

    Returns (dispersion in [0,1], single_day_spike). Low dispersion + several voices
    on one day is the re-syndication fingerprint. dispersion = distinct_days / voices.
    """
    voices = len(groups)
    if voices <= 1:
        return (0.0, False)
    days = {g[0].published_at.date() for g in groups}
    dispersion = round(len(days) / voices, 4)
    single_day_spike = len(days) == 1 and voices >= 3
    return (dispersion, single_day_spike)


def compute_independence_score(
    *,
    deduped_source_count: int,
    distinct_originators: int | None,
    echo_ratio: float,
    arrival_dispersion: float,
    single_day_spike: bool,
) -> float:
    """Uncalibrated heuristic in [0,1] (P2). Rewards distinct originators/voices and
    staggered arrival; penalizes echoes and single-day multi-source spikes. Falls back
    to deduped_source_count when originators are unknown. Treat as Гипотеза."""
    originator_signal = (
        distinct_originators if distinct_originators is not None else deduped_source_count
    )
    originator_norm = min(originator_signal, 5) / 5.0
    score = 0.5 * originator_norm + 0.3 * arrival_dispersion + 0.2 * (1.0 - echo_ratio)
    if single_day_spike:
        score *= 0.5
    return round(max(0.0, min(1.0, score)), 4)


def independence_metrics(
    posts: list[ProvenancePost],
    *,
    near_dup_threshold: float = NEAR_DUP_THRESHOLD,
    max_gap_hours: float = NEAR_DUP_MAX_GAP_HOURS,
) -> IndependenceMetrics:
    """Compute proxy independence metrics for one cluster's member posts."""
    if not posts:
        return IndependenceMetrics(0, 0, 0, 0.0, None, 0.0, False, 0.0, None, [])

    groups = same_artifact_groups(
        posts, near_dup_threshold=near_dup_threshold, max_gap_hours=max_gap_hours
    )
    raw_source_count = len({p.source_id for p in posts})
    # one voice = one same-artifact group; its "source" is the earliest member's feed
    deduped_source_count = len({g[0].source_id for g in groups})
    distinct_voices = len(groups)
    echo_ratio = round((len(posts) - distinct_voices) / len(posts), 4)

    actors = {a for p in posts for a in p.actors if a}
    distinct_originators = len(actors) if actors else None

    dispersion, single_day_spike = _arrival_dispersion(groups)
    first_seen_at = min(p.published_at for p in posts)

    independence_score = compute_independence_score(
        deduped_source_count=deduped_source_count,
        distinct_originators=distinct_originators,
        echo_ratio=echo_ratio,
        arrival_dispersion=dispersion,
        single_day_spike=single_day_spike,
    )

    return IndependenceMetrics(
        raw_source_count=raw_source_count,
        deduped_source_count=deduped_source_count,
        distinct_voices=distinct_voices,
        echo_ratio=echo_ratio,
        distinct_originators=distinct_originators,
        arrival_dispersion=dispersion,
        single_day_spike=single_day_spike,
        independence_score=independence_score,
        first_seen_at=first_seen_at,
        groups=groups,
    )
