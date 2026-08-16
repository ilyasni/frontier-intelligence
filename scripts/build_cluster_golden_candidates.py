"""Generate labelling candidates for config/cluster_golden_set.json.

Why a generator instead of hand-picked pairs: a labelled set is only worth its cost
where the algorithm is actually undecided. Pairs far above the trend threshold are
grouped no matter what, pairs far below are separated no matter what, and labelling
either teaches nothing — the accuracy would read ~1.0 forever and never move when the
clustering got worse. The pairs that matter sit in a band around the threshold, so
that is what this samples.

Two categories are produced automatically and need no human:

* ``same_story`` — pairs that provenance already proves are one artifact (same
  canonical URL, or an identical headline within 72h). Identity, not similarity.
* ``different_story`` — pairs drawn from clusters far apart in embedding space and
  sharing no source, where "different" is not in doubt.

``same_trend`` cannot be harvested: whether two distinct stories belong to one trend
is an editorial judgement about this deployment's subject matter, not a fact about the
data. Those pairs are emitted UNLABELLED, sorted by how close they sit to the decision
boundary, for a human to mark.

Usage (inside the admin image, which has the deps and the DB URL):

    python -m scripts.build_cluster_golden_candidates --workspace disruption \\
        --out /tmp/golden_candidates.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
from pathlib import Path
from typing import Any

from shared.provenance import ProvenancePost, exact_artifact_groups
from worker.integrations.qdrant_client import QdrantFrontierClient
from worker.services.semantic_clustering import (
    _cluster_settings,
    _cos,
    _load_semantic_state,
)

logger = logging.getLogger(__name__)

# Half-width of the band around the trend threshold that counts as "undecided".
# 0.06 against thresholds of 0.84-0.87 spans roughly 0.78-0.93 — wide enough to hold
# both mistaken merges and mistaken splits, narrow enough that every pair in it is a
# judgement call rather than an obvious answer.
_BAND = 0.06


def _nearest_neighbour_pairs(
    semantic: list[dict[str, Any]],
    threshold: float,
    band: float,
    limit: int,
    rng: random.Random,
) -> list[dict[str, Any]]:
    """Each sampled cluster paired with its single most similar neighbour.

    Sampling two clusters at random does not work: in a corpus of ~9500 clusters
    almost every random pair is unrelated, so a labeller marks 50 pairs "no" and the
    positive half of the set never fills. The pairs where "is this one trend?" is a
    real question are a cluster and its nearest neighbour, which is what this returns.

    The band is kept wide and deliberately straddles the threshold: pairs above it are
    ones the system already groups (a healthy run scores them right), pairs below are
    ones it splits. A set drawn only from below could never do anything but rise, and
    the point of a fixed labelled set is to catch a regression.
    """
    import numpy as np

    matrix = np.asarray([item["centroid"] for item in semantic], dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    matrix = matrix / np.where(norms > 0, norms, 1.0)

    order = list(range(len(semantic)))
    rng.shuffle(order)
    picked = order[: min(len(order), limit * 8)]

    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    block = 256
    for start in range(0, len(picked), block):
        rows = picked[start : start + block]
        sims = matrix[rows] @ matrix.T
        for offset, row in enumerate(rows):
            sims[offset, row] = -1.0  # never pair a cluster with itself
        best = np.argmax(sims, axis=1)
        for offset, row in enumerate(rows):
            other = int(best[offset])
            similarity = float(sims[offset, other])
            if abs(similarity - threshold) > band:
                continue
            a, b = semantic[row], semantic[other]
            key = tuple(sorted((a["cluster_id"], b["cluster_id"])))
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "pair": [a["cluster_id"], b["cluster_id"]],
                    "similarity": round(similarity, 4),
                    "label": None,
                    "titles": [a.get("title", ""), b.get("title", "")],
                }
            )
            if len(out) >= limit:
                return out
    return out


async def _collect(workspace_id: str, limit: int, seed: int, band: float) -> dict[str, Any]:
    from shared.db import get_session_factory

    rng = random.Random(seed)
    qdrant = QdrantFrontierClient()
    try:
        async with get_session_factory()() as session:
            cluster_cfg = await _cluster_settings(session, workspace_id)
            semantic = await _load_semantic_state(session, workspace_id, cluster_cfg, qdrant)
    finally:
        await qdrant.close()

    threshold = float(cluster_cfg["trend_cluster_similarity_threshold"])
    logger.info(
        "loaded %d semantic clusters for workspace=%s (trend threshold %.2f)",
        len(semantic),
        workspace_id,
        threshold,
    )

    posts = [
        ProvenancePost(
            post_id=post.post_id,
            source_id=post.source_id,
            published_at=post.published_at,
            url=post.url,
            title=post.title,
        )
        for item in semantic
        for post in (item.get("posts") or [])
    ]
    same_story = [
        [group[0].post_id, echo.post_id]
        for group in exact_artifact_groups(posts)
        for echo in group[1:]
    ]
    rng.shuffle(same_story)

    # different_story: far apart in embedding space AND no shared source, so neither
    # topic drift nor a syndication chain can make the pair arguable.
    by_source = {item["cluster_id"]: set(item.get("source_ids") or []) for item in semantic}
    different_story: list[list[str]] = []
    for _ in range(limit * 200):
        if len(different_story) >= limit:
            break
        if len(semantic) < 2:
            break
        a, b = rng.sample(semantic, 2)
        if _cos(a.get("centroid") or [], b.get("centroid") or []) >= threshold - 0.25:
            continue
        if by_source.get(a["cluster_id"], set()) & by_source.get(b["cluster_id"], set()):
            continue
        different_story.append([a["doc_ids"][0], b["doc_ids"][0]])

    same_trend_candidates = _nearest_neighbour_pairs(semantic, threshold, band, limit, rng)
    same_trend_candidates.sort(key=lambda c: -c["similarity"])
    return {
        "workspace_id": workspace_id,
        "trend_threshold": threshold,
        "same_story": same_story[:limit],
        "different_story": different_story,
        "same_trend_unlabelled": same_trend_candidates,
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--limit", type=int, default=60, help="pairs per category")
    parser.add_argument("--seed", type=int, default=20260816)
    # Ширина полосы вокруг порога. Дефолт _BAND держит выборку в зоне, где решение
    # спорно; шире — чтобы в наборе оказались и пары ВЫШЕ порога, которые система
    # уже склеивает. Без них same_trend_accuracy стартует с нуля и умеет только
    # расти, то есть не ловит регрессию — а ради регрессии набор и заводится.
    parser.add_argument("--band", type=float, default=_BAND)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    payload = await _collect(args.workspace, args.limit, args.seed, args.band)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "wrote %s: same_story=%d different_story=%d same_trend_unlabelled=%d",
        args.out,
        len(payload["same_story"]),
        len(payload["different_story"]),
        len(payload["same_trend_unlabelled"]),
    )


if __name__ == "__main__":
    asyncio.run(_main())
