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

import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# Query-param prefixes dropped when canonicalizing (superset of ingest.sources.base
# _TRACKED_QUERY_PREFIXES; kept in sync intentionally).
_TRACKED_QUERY_PREFIXES = ("utm_", "rss", "ref", "source", "fbclid", "gclid", "yclid")
_AMP_SUFFIXES = ("/amp", "/amp/", ".amp")
_MOBILE_HOST_PREFIXES = ("m.", "amp.", "mobile.")

_TITLE_STRIP = re.compile(r"[^\w\s]", re.UNICODE)
_WS = re.compile(r"\s+")

NEAR_DUP_THRESHOLD = 0.97
NEAR_DUP_MAX_GAP_HOURS = 72.0


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

    # 2/3. pairwise near-text and same-title, gated by time gap. Clusters are small
    # (member posts), so O(n^2) is fine and avoids a corpus-scale index here.
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

    buckets: dict[int, list[ProvenancePost]] = {}
    for i, post in enumerate(posts):
        buckets.setdefault(dsu.find(i), []).append(post)
    groups = [sorted(g, key=lambda p: p.published_at) for g in buckets.values()]
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
