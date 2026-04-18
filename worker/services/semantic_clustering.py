from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import get_settings
from worker.integrations.qdrant_client import QdrantFrontierClient
from worker.services.missing_signals import run_missing_signals_analysis

try:
    import ruptures as rpt
except Exception:  # pragma: no cover - optional local dependency, enabled in worker image
    rpt = None

logger = logging.getLogger(__name__)


@dataclass
class ClusterPost:
    post_id: str
    workspace_id: str
    source_id: str
    content: str
    published_at: datetime
    relevance_score: float
    source_score: float
    tags: list[str]
    title: str
    url: str
    vector: list[float]


_APRIL_FOOLS_CUES = (
    "april fool",
    "april fools",
    "1 april",
    "1st april",
    "first of april",
    "prank",
    "joke",
    "gotcha",
    "satire",
    "satirical",
    "hoax",
    "шутка",
    "розыгрыш",
    "первоапрель",
    "1 апреля",
    "первое апреля",
    "сатира",
)


def _digest(value: str, prefix: str) -> str:
    return f"{prefix}:{hashlib.sha1(value.encode('utf-8')).hexdigest()[:16]}"


def _cos(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return 0.0 if na == 0 or nb == 0 else dot / (na * nb)


def _freshness(dt: datetime) -> float:
    age = max((datetime.now(UTC) - dt).total_seconds() / 3600.0, 0.0)
    if age <= 24:
        return 1.0
    if age <= 72:
        return 0.75
    if age <= 168:
        return 0.45
    return 0.2


def _cfg(value, default=None):
    if hasattr(value, "default"):
        return value.default
    return value if value is not None else default


def _merge_cluster_settings(
    base: dict[str, Any], overrides: dict[str, Any] | None
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in (overrides or {}).items():
        if key in merged and value is not None:
            merged[key] = value
    return merged


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / max(len(a | b), 1)


def _bucket_start(dt: datetime, bucket_hours: int) -> datetime:
    base = dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    if bucket_hours >= 24:
        return base.replace(hour=0)
    hour = base.hour - (base.hour % max(bucket_hours, 1))
    return base.replace(hour=hour)


def _is_april_fools_post(post: ClusterPost) -> bool:
    published = post.published_at.astimezone(UTC)
    if (published.month, published.day) not in {(4, 1), (4, 2)}:
        return False
    haystack = f"{post.title} {post.content[:800]}".lower()
    return any(cue in haystack for cue in _APRIL_FOOLS_CUES)


def _april_fools_penalty(posts: list[ClusterPost], cluster_cfg: dict[str, Any]) -> dict[str, Any]:
    if not posts or not bool(cluster_cfg.get("april_fools_guard_enabled", True)):
        return {
            "flagged_post_ids": [],
            "flagged_ratio": 0.0,
            "score_multiplier": 1.0,
            "stage_blocked": False,
        }

    flagged = [post.post_id for post in posts if _is_april_fools_post(post)]
    ratio = round(len(flagged) / max(len(posts), 1), 4)
    penalty = max(0.0, min(1.0, float(cluster_cfg.get("april_fools_guard_penalty", 0.45))))
    multiplier = round(max(0.0, 1.0 - ratio * penalty), 4)
    stage_blocked = ratio >= float(cluster_cfg.get("april_fools_guard_stage_block_ratio", 0.34))
    return {
        "flagged_post_ids": flagged,
        "flagged_ratio": ratio,
        "score_multiplier": multiplier,
        "stage_blocked": stage_blocked,
    }


def _terms(text: str) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-_]{2,}", (text or "").lower())


def _top_terms(posts: list[ClusterPost], limit: int = 8) -> list[str]:
    counter: Counter[str] = Counter()
    for post in posts:
        counter.update(_terms(post.title or post.content[:400]))
    return [name for name, _ in counter.most_common(limit)]


def _centroid(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        return []
    dims = len(vectors[0])
    acc = [0.0] * dims
    for vector in vectors:
        for idx, value in enumerate(vector):
            acc[idx] += value
    count = float(len(vectors))
    return [value / count for value in acc]


def _components(
    posts: list[ClusterPost], threshold: float, max_gap_h: int
) -> list[list[ClusterPost]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for idx, a in enumerate(posts):
        for b in posts[idx + 1 :]:
            if abs((a.published_at - b.published_at).total_seconds()) / 3600.0 > max_gap_h:
                continue
            if _cos(a.vector, b.vector) >= threshold:
                graph[a.post_id].add(b.post_id)
                graph[b.post_id].add(a.post_id)
    by_id = {p.post_id: p for p in posts}
    seen: set[str] = set()
    groups: list[list[ClusterPost]] = []
    for post in posts:
        if post.post_id in seen:
            continue
        stack = [post.post_id]
        seen.add(post.post_id)
        ids: list[str] = []
        while stack:
            cur = stack.pop()
            ids.append(cur)
            for nxt in graph.get(cur, set()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        groups.append([by_id[i] for i in ids])
    return groups


def _representative(posts: list[ClusterPost], centroid: list[float]) -> ClusterPost:
    counts = Counter(post.source_id for post in posts)
    best, best_score = posts[0], -1.0
    for post in posts:
        score = (
            post.relevance_score * 0.46
            + post.source_score * 0.22
            + _freshness(post.published_at) * 0.14
            + _cos(post.vector, centroid) * 0.12
            + min(len(post.tags), 5) / 5.0 * 0.06
            - max(0.0, (counts[post.source_id] - 1) * 0.03)
        )
        if score > best_score:
            best, best_score = post, score
    return best


def _coherence(posts: list[ClusterPost], centroid: list[float]) -> float:
    return (
        0.0
        if not posts or not centroid
        else sum(_cos(post.vector, centroid) for post in posts) / len(posts)
    )


def _json_ready(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            default=lambda item: item.isoformat() if isinstance(item, datetime) else str(item),
        )
    )


def _trend_cluster_index_text(item: dict[str, Any]) -> str:
    doc_ids = item.get("doc_ids") or []
    source_count = int(item.get("source_count") or 0)
    keywords = [str(keyword) for keyword in (item.get("keywords") or []) if str(keyword).strip()]
    evidence_titles = [
        str(evidence.get("title") or "").strip()
        for evidence in (item.get("evidence") or [])
        if str(evidence.get("title") or "").strip()
    ]
    parts = [
        str(item.get("title") or "").strip(),
        f"{len(doc_ids)} related posts across {source_count} sources.",
        "Use as cluster-aware evidence in synthesis.",
        f"Signal stage: {item.get('signal_stage') or 'stable'}",
        f"Keywords: {', '.join(keywords)}" if keywords else "",
        f"Evidence: {' | '.join(evidence_titles[:5])}" if evidence_titles else "",
    ]
    return "\n".join(part for part in parts if part)[:4000]


def _trend_cluster_index_payload(
    signal_id: str, run_id: str, item: dict[str, Any]
) -> dict[str, Any]:
    doc_ids = list(item.get("doc_ids") or [])
    source_count = int(item.get("source_count") or 0)
    return _json_ready(
        {
            "workspace_id": item["workspace_id"],
            "cluster_key": item.get("signal_key"),
            "pipeline": "stable",
            "title": item.get("title"),
            "insight": f"{len(doc_ids)} related posts across {source_count} sources.",
            "opportunity": "Use as cluster-aware evidence in synthesis.",
            "time_horizon": "near-term",
            "signal_stage": item.get("signal_stage"),
            "signal_score": float(item.get("signal_score") or 0.0),
            "burst_score": float(item.get("burst_score") or 0.0),
            "coherence": float(item.get("coherence_score") or 0.0),
            "novelty": float(item.get("novelty_score") or 0.0),
            "source_diversity_score": float(item.get("source_diversity_score") or 0.0),
            "freshness_score": float(item.get("freshness_score") or 0.0),
            "evidence_strength_score": float(item.get("evidence_strength_score") or 0.0),
            "velocity_score": float(item.get("velocity_score") or 0.0),
            "acceleration_score": float(item.get("acceleration_score") or 0.0),
            "baseline_rate": float(item.get("baseline_rate") or 0.0),
            "current_rate": float(item.get("current_rate") or 0.0),
            "change_point_count": int(item.get("change_point_count") or 0),
            "change_point_strength": float(item.get("change_point_strength") or 0.0),
            "has_recent_change_point": bool(item.get("has_recent_change_point")),
            "doc_count": len(doc_ids),
            "source_count": source_count,
            "doc_ids": doc_ids,
            "semantic_cluster_ids": list(item.get("semantic_cluster_ids") or []),
            "keywords": list(item.get("keywords") or []),
            "evidence": list(item.get("evidence") or []),
            "first_seen_at": item.get("first_seen_at"),
            "last_seen_at": item.get("last_seen_at"),
            "detected_at": item.get("detected_at") or datetime.now(UTC),
            "run_id": run_id,
            "index_text": _trend_cluster_index_text(item),
            "source": "postgres.trend_clusters",
            "id": signal_id,
        }
    )


def _trend_cluster_index_points(run_id: str, stable: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for item in stable:
        signal_id = str(item.get("signal_id") or "").strip()
        centroid = item.get("centroid") or []
        if not signal_id or not centroid:
            continue
        points.append(
            {
                "cluster_id": signal_id,
                "dense_vector": centroid,
                "payload": _trend_cluster_index_payload(signal_id, run_id, item),
            }
        )
    return points


async def _index_trend_clusters_in_qdrant(
    qdrant: QdrantFrontierClient,
    *,
    run_id: str,
    stable: list[dict[str, Any]],
) -> dict[str, int]:
    points = _trend_cluster_index_points(run_id, stable)
    if not points:
        return {"indexed": 0, "failed": 0, "skipped": len(stable)}
    try:
        indexed = await qdrant.upsert_trend_clusters(points)
    except Exception:
        logger.exception("Trend cluster Qdrant indexing failed for run_id=%s", run_id)
        return {"indexed": 0, "failed": len(points), "skipped": len(stable) - len(points)}
    return {
        "indexed": indexed,
        "failed": len(points) - indexed,
        "skipped": len(stable) - len(points),
    }


async def _rows(session: AsyncSession, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    result = await session.execute(text(sql), params)
    return [dict(row) for row in result.mappings().all()]


async def _fetch_posts(
    session: AsyncSession, workspace_id: str | None, window_days: int, limit: int
) -> list[dict[str, Any]]:
    where = [
        "i.embedding_status = 'done'",
        "COALESCE(p.relevance_score, 0) >= 0.6",
        "p.published_at IS NOT NULL",
    ]
    params: dict[str, Any] = {
        "window_start": datetime.now(UTC) - timedelta(days=window_days),
        "limit": limit,
        "workspace_id": workspace_id,
    }
    if workspace_id:
        where.append("p.workspace_id = :workspace_id")
    return await _rows(
        session,
        f"""
        SELECT p.id, p.workspace_id, p.source_id, p.content, p.published_at,
               COALESCE(p.relevance_score, 0) AS relevance_score,
               COALESCE(s.source_score, s.source_authority, 0) AS source_score,
               COALESCE(p.tags, '[]'::jsonb) AS tags,
               COALESCE(p.extra, '{{}}'::jsonb) AS extra
        FROM posts p
        JOIN indexing_status i ON i.post_id = p.id
        JOIN sources s ON s.id = p.source_id
        WHERE {' AND '.join(where)} AND p.published_at >= :window_start
        ORDER BY p.published_at DESC
        LIMIT :limit
        """,
        params,
    )


def _posts_from_docs(
    rows: list[dict[str, Any]], documents: list[dict[str, Any]]
) -> list[ClusterPost]:
    by_id = {row["id"]: row for row in rows}
    posts: list[ClusterPost] = []
    for doc in documents:
        payload = doc.get("payload") or {}
        row = by_id.get(payload.get("post_id"))
        if not row or not doc.get("vector"):
            continue
        extra = row.get("extra") or {}
        title = str(extra.get("title") or "")[:180] or (row["content"] or "")[:120]
        posts.append(
            ClusterPost(
                post_id=row["id"],
                workspace_id=row["workspace_id"],
                source_id=row["source_id"],
                content=row["content"] or "",
                published_at=row["published_at"],
                relevance_score=float(row.get("relevance_score") or 0.0),
                source_score=float(row.get("source_score") or 0.0),
                tags=list(row.get("tags") or []),
                title=title,
                url=str(extra.get("url") or payload.get("url") or ""),
                vector=list(doc["vector"]),
            )
        )
    return posts


async def _create_run(
    session: AsyncSession,
    workspace_id: str | None,
    thresholds: dict[str, Any],
    *,
    stage: str = "full",
) -> str:
    run_id = _digest(f"{workspace_id or 'all'}|{datetime.now(UTC).isoformat()}", "cluster-run")
    await session.execute(
        text(
            """
            INSERT INTO cluster_runs (id, workspace_id, stage, status, thresholds, summary, metrics, started_at, created_at, updated_at)
            VALUES (:id, :workspace_id, :stage, 'running', CAST(:thresholds AS jsonb), '{}'::jsonb, '{}'::jsonb, NOW(), NOW(), NOW())
            """
        ),
        {
            "id": run_id,
            "workspace_id": workspace_id,
            "thresholds": json.dumps(thresholds),
            "stage": stage,
        },
    )
    return run_id


async def _finish_run(
    session: AsyncSession,
    run_id: str,
    status: str,
    summary: dict[str, Any],
    metrics: dict[str, Any],
) -> None:
    await session.execute(
        text(
            """
            UPDATE cluster_runs
            SET status = :status, summary = CAST(:summary AS jsonb), metrics = CAST(:metrics AS jsonb),
                finished_at = NOW(), updated_at = NOW()
            WHERE id = :id
            """
        ),
        {
            "id": run_id,
            "status": status,
            "summary": json.dumps(summary),
            "metrics": json.dumps(metrics),
        },
    )


async def _existing(
    session: AsyncSession, table: str, workspace_id: str | None, age_hours: int
) -> list[dict[str, Any]]:
    return await _rows(
        session,
        f"""
        SELECT * FROM {table}
        WHERE detected_at >= NOW() - make_interval(hours => :age_hours)
          AND (CAST(:workspace_id AS text) IS NULL OR workspace_id = CAST(:workspace_id AS text))
        ORDER BY detected_at DESC
        """,
        {"workspace_id": workspace_id, "age_hours": age_hours},
    )


async def _cluster_settings(session: AsyncSession, workspace_id: str | None) -> dict[str, Any]:
    settings = get_settings()
    base = {
        "semantic_cluster_max_posts": int(_cfg(settings.semantic_cluster_max_posts, 400)),
        "semantic_cluster_window_days": int(_cfg(settings.semantic_cluster_window_days, 7)),
        "semantic_dedupe_similarity_threshold": float(
            _cfg(settings.semantic_dedupe_similarity_threshold, 0.92)
        ),
        "semantic_dedupe_max_gap_hours": int(_cfg(settings.semantic_dedupe_max_gap_hours, 96)),
        "semantic_merge_enabled": bool(_cfg(settings.semantic_merge_enabled, True)),
        "semantic_merge_similarity_threshold": float(
            _cfg(settings.semantic_merge_similarity_threshold, 0.78)
        ),
        "semantic_merge_title_overlap_threshold": float(
            _cfg(settings.semantic_merge_title_overlap_threshold, 0.4)
        ),
        "semantic_merge_concept_overlap_threshold": float(
            _cfg(settings.semantic_merge_concept_overlap_threshold, 0.5)
        ),
        "semantic_merge_max_gap_hours": int(_cfg(settings.semantic_merge_max_gap_hours, 168)),
        "semantic_cluster_cooling_hours": int(_cfg(settings.semantic_cluster_cooling_hours, 48)),
        "semantic_cluster_archive_hours": int(
            _cfg(settings.semantic_cluster_archive_hours, 24 * 14)
        ),
        "trend_cluster_similarity_threshold": float(
            _cfg(settings.trend_cluster_similarity_threshold, 0.87)
        ),
        "trend_cluster_max_gap_hours": int(_cfg(settings.trend_cluster_max_gap_hours, 24 * 30)),
        "trend_cluster_window_days": int(_cfg(settings.trend_cluster_window_days, 30)),
        "trend_cluster_min_semantic_clusters": int(
            _cfg(settings.trend_cluster_min_semantic_clusters, 2)
        ),
        "trend_cluster_min_docs": int(_cfg(settings.trend_cluster_min_docs, 4)),
        "trend_cluster_stable_threshold": float(
            _cfg(settings.trend_cluster_stable_threshold, 0.58)
        ),
        "trend_cluster_emerging_threshold": float(
            _cfg(settings.trend_cluster_emerging_threshold, 0.42)
        ),
        "trend_cluster_min_source_diversity": float(
            _cfg(settings.trend_cluster_min_source_diversity, 0.2)
        ),
        "cluster_min_evidence_count": int(_cfg(settings.cluster_min_evidence_count, 2)),
        "signal_short_window_hours": int(_cfg(settings.signal_short_window_hours, 24)),
        "signal_analysis_window_days": int(_cfg(settings.signal_analysis_window_days, 3)),
        "signal_baseline_window_days": int(_cfg(settings.signal_baseline_window_days, 14)),
        "signal_velocity_weight": float(_cfg(settings.signal_velocity_weight, 0.14)),
        "signal_acceleration_weight": float(_cfg(settings.signal_acceleration_weight, 0.1)),
        "change_point_method": str(_cfg(settings.change_point_method, "window")),
        "change_point_penalty": _cfg(settings.change_point_penalty, "auto"),
        "change_point_min_size": int(_cfg(settings.change_point_min_size, 2)),
        "change_point_jump": int(_cfg(settings.change_point_jump, 1)),
        "change_point_recent_hours": int(_cfg(settings.change_point_recent_hours, 48)),
        "signal_merge_similarity_threshold": float(
            _cfg(settings.signal_merge_similarity_threshold, 0.72)
        ),
        "signal_merge_doc_overlap_threshold": float(
            _cfg(settings.signal_merge_doc_overlap_threshold, 0.25)
        ),
        "persist_weak_signals": bool(_cfg(settings.persist_weak_signals, True)),
        "weak_signal_min_score": float(_cfg(settings.weak_signal_min_score, 0.42)),
        "weak_signal_min_confidence": float(_cfg(settings.weak_signal_min_confidence, 0.52)),
        "weak_signal_min_source_diversity": float(
            _cfg(settings.weak_signal_min_source_diversity, 0.2)
        ),
        "weak_signal_min_source_count": int(_cfg(settings.weak_signal_min_source_count, 1)),
        "signal_min_source_count": int(_cfg(settings.signal_min_source_count, 1)),
        "april_fools_guard_enabled": bool(_cfg(settings.april_fools_guard_enabled, True)),
        "april_fools_guard_penalty": float(_cfg(settings.april_fools_guard_penalty, 0.45)),
        "april_fools_guard_stage_block_ratio": float(
            _cfg(settings.april_fools_guard_stage_block_ratio, 0.34)
        ),
    }
    if not workspace_id:
        return base
    row = (
        (
            await session.execute(
                text("SELECT COALESCE(extra, '{}'::jsonb) AS extra FROM workspaces WHERE id = :id"),
                {"id": workspace_id},
            )
        )
        .mappings()
        .first()
    )
    extra = dict(row or {}).get("extra") or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}
    return _merge_cluster_settings(base, (extra or {}).get("cluster_analysis") or {})


def _semantic_identity(
    group: list[ClusterPost],
    representative: ClusterPost,
    top_concepts: list[str],
    existing_clusters: list[dict[str, Any]],
) -> tuple[str, str | None, list[str]]:
    doc_ids = {post.post_id for post in group}
    rep_terms = set(_terms(representative.title))
    best_key, best_id, best_score = "", None, 0.0
    related: set[str] = set()
    for existing in existing_clusters:
        existing_doc_ids = set(existing.get("doc_ids") or [])
        title_terms = set(_terms(existing.get("title") or ""))
        overlap = len(doc_ids & existing_doc_ids)
        title_overlap = len(rep_terms & title_terms) / max(len(rep_terms | title_terms), 1)
        concept_overlap = len(set(top_concepts) & set(existing.get("top_concepts") or [])) / max(
            len(set(top_concepts) | set(existing.get("top_concepts") or [])), 1
        )
        score = overlap * 0.65 + title_overlap * 0.2 + concept_overlap * 0.15
        if overlap > 0 or score >= 0.35:
            related.add(existing["id"])
        if score > best_score:
            best_score, best_key, best_id = score, existing.get("cluster_key") or "", existing["id"]
    if best_id and best_score >= 0.35:
        return best_key, best_id, sorted(related)
    signature = "|".join(
        [
            representative.workspace_id,
            representative.source_id,
            " ".join(_terms(representative.title))[:120],
            ",".join(sorted(top_concepts[:4])),
        ]
    )
    return _digest(signature, representative.workspace_id), None, sorted(related)


async def _upsert_semantic(
    session: AsyncSession, run_id: str, item: dict[str, Any], existing_id: str | None
) -> str:
    cluster_id = existing_id or _digest(item["cluster_key"], "semantic")
    await session.execute(
        text(
            """
            INSERT INTO semantic_clusters (
                id, workspace_id, cluster_key, title, representative_post_id, post_count, source_count,
                doc_ids, source_ids, top_concepts, evidence, representative_evidence, related_cluster_ids,
                lifecycle_state, avg_relevance, avg_source_score, freshness_score, coherence_score, explainability,
                time_window, embedding_version, first_seen_at, last_seen_at, detected_at, created_at, updated_at
            ) VALUES (
                :id, :workspace_id, :cluster_key, :title, :representative_post_id, :post_count, :source_count,
                CAST(:doc_ids AS jsonb), CAST(:source_ids AS jsonb), CAST(:top_concepts AS jsonb), CAST(:evidence AS jsonb),
                CAST(:representative_evidence AS jsonb), CAST(:related_cluster_ids AS jsonb), :lifecycle_state,
                :avg_relevance, :avg_source_score, :freshness_score, :coherence_score, CAST(:explainability AS jsonb),
                :time_window, :embedding_version, :first_seen_at, :last_seen_at, NOW(), NOW(), NOW()
            ) ON CONFLICT (id) DO UPDATE SET
                cluster_key = EXCLUDED.cluster_key, title = EXCLUDED.title, representative_post_id = EXCLUDED.representative_post_id,
                post_count = EXCLUDED.post_count, source_count = EXCLUDED.source_count, doc_ids = EXCLUDED.doc_ids,
                source_ids = EXCLUDED.source_ids, top_concepts = EXCLUDED.top_concepts, evidence = EXCLUDED.evidence,
                representative_evidence = EXCLUDED.representative_evidence, related_cluster_ids = EXCLUDED.related_cluster_ids,
                lifecycle_state = EXCLUDED.lifecycle_state, avg_relevance = EXCLUDED.avg_relevance,
                avg_source_score = EXCLUDED.avg_source_score, freshness_score = EXCLUDED.freshness_score,
                coherence_score = EXCLUDED.coherence_score, explainability = EXCLUDED.explainability,
                embedding_version = EXCLUDED.embedding_version,
                first_seen_at = LEAST(semantic_clusters.first_seen_at, EXCLUDED.first_seen_at),
                last_seen_at = GREATEST(semantic_clusters.last_seen_at, EXCLUDED.last_seen_at),
                detected_at = NOW(), updated_at = NOW()
            """
        ),
        {
            "id": cluster_id,
            "workspace_id": item["workspace_id"],
            "cluster_key": item["cluster_key"],
            "title": item["title"],
            "representative_post_id": item["representative_post_id"],
            "post_count": item["post_count"],
            "source_count": item["source_count"],
            "doc_ids": json.dumps(item["doc_ids"]),
            "source_ids": json.dumps(item["source_ids"]),
            "top_concepts": json.dumps(item["top_concepts"]),
            "evidence": json.dumps(item["evidence"]),
            "representative_evidence": json.dumps(item["representative_evidence"]),
            "related_cluster_ids": json.dumps(item["related_cluster_ids"]),
            "lifecycle_state": item["lifecycle_state"],
            "avg_relevance": item["avg_relevance"],
            "avg_source_score": item["avg_source_score"],
            "freshness_score": item["freshness_score"],
            "coherence_score": item["coherence_score"],
            "explainability": json.dumps({**item["explainability"], "run_id": run_id}),
            "time_window": "7d",
            "embedding_version": get_settings().gigachat_embeddings_model,
            "first_seen_at": item["first_seen_at"],
            "last_seen_at": item["last_seen_at"],
        },
    )
    await session.execute(
        text(
            "UPDATE posts SET semantic_cluster_id = :cluster_id, updated_at = NOW() WHERE id = ANY(:doc_ids)"
        ),
        {"cluster_id": cluster_id, "doc_ids": item["doc_ids"]},
    )
    return cluster_id


async def _upsert_signal(
    session: AsyncSession, table: str, run_id: str, item: dict[str, Any]
) -> str:
    if table == "trend_clusters":
        signal_id = item.get("existing_id") or _digest(item["signal_key"], "trend")
        await session.execute(
            text(
                """
                INSERT INTO trend_clusters (
                    id, workspace_id, cluster_key, pipeline, title, insight, opportunity, time_horizon,
                    burst_score, coherence, novelty, source_diversity_score, freshness_score, evidence_strength_score,
                    velocity_score, acceleration_score, baseline_rate, current_rate, change_point_count,
                    change_point_strength, has_recent_change_point, signal_score, signal_stage, doc_count,
                    source_count, doc_ids, semantic_cluster_ids, keywords, explainability, category,
                    detected_at, created_at, updated_at
                ) VALUES (
                    :id, :workspace_id, :cluster_key, 'stable', :title, :insight, :opportunity, :time_horizon,
                    :burst_score, :coherence, :novelty, :source_diversity_score, :freshness_score, :evidence_strength_score,
                    :velocity_score, :acceleration_score, :baseline_rate, :current_rate, :change_point_count,
                    :change_point_strength, :has_recent_change_point, :signal_score, :signal_stage, :doc_count, :source_count, CAST(:doc_ids AS jsonb),
                    CAST(:semantic_cluster_ids AS jsonb), CAST(:keywords AS jsonb), CAST(:explainability AS jsonb),
                    NULL, NOW(), NOW(), NOW()
                ) ON CONFLICT (id) DO UPDATE SET
                    cluster_key = EXCLUDED.cluster_key, title = EXCLUDED.title, insight = EXCLUDED.insight,
                    opportunity = EXCLUDED.opportunity, time_horizon = EXCLUDED.time_horizon, burst_score = EXCLUDED.burst_score,
                    coherence = EXCLUDED.coherence, novelty = EXCLUDED.novelty, source_diversity_score = EXCLUDED.source_diversity_score,
                    freshness_score = EXCLUDED.freshness_score, evidence_strength_score = EXCLUDED.evidence_strength_score,
                    velocity_score = EXCLUDED.velocity_score, acceleration_score = EXCLUDED.acceleration_score,
                    baseline_rate = EXCLUDED.baseline_rate, current_rate = EXCLUDED.current_rate,
                    change_point_count = EXCLUDED.change_point_count, change_point_strength = EXCLUDED.change_point_strength,
                    has_recent_change_point = EXCLUDED.has_recent_change_point,
                    signal_score = EXCLUDED.signal_score, signal_stage = EXCLUDED.signal_stage, doc_count = EXCLUDED.doc_count,
                    source_count = EXCLUDED.source_count, doc_ids = EXCLUDED.doc_ids, semantic_cluster_ids = EXCLUDED.semantic_cluster_ids,
                    keywords = EXCLUDED.keywords, explainability = EXCLUDED.explainability, detected_at = NOW(), updated_at = NOW()
                """
            ),
            {
                "id": signal_id,
                "workspace_id": item["workspace_id"],
                "cluster_key": item["signal_key"],
                "title": item["title"],
                "insight": f"{len(item['doc_ids'])} related posts across {item['source_count']} sources.",
                "opportunity": "Use as cluster-aware evidence in synthesis.",
                "time_horizon": "near-term",
                "burst_score": item["burst_score"],
                "coherence": item["coherence_score"],
                "novelty": item["novelty_score"],
                "source_diversity_score": item["source_diversity_score"],
                "freshness_score": item["freshness_score"],
                "evidence_strength_score": item["evidence_strength_score"],
                "velocity_score": item["velocity_score"],
                "acceleration_score": item["acceleration_score"],
                "baseline_rate": item["baseline_rate"],
                "current_rate": item["current_rate"],
                "change_point_count": item["change_point_count"],
                "change_point_strength": item["change_point_strength"],
                "has_recent_change_point": item["has_recent_change_point"],
                "signal_score": item["signal_score"],
                "signal_stage": item["signal_stage"],
                "doc_count": len(item["doc_ids"]),
                "source_count": item["source_count"],
                "doc_ids": json.dumps(item["doc_ids"]),
                "semantic_cluster_ids": json.dumps(item["semantic_cluster_ids"]),
                "keywords": json.dumps(item["keywords"]),
                "explainability": json.dumps({**item["explainability"], "run_id": run_id}),
            },
        )
        return signal_id
    signal_id = item.get("existing_id") or _digest(item["signal_key"], "emerging")
    await session.execute(
        text(
            """
            INSERT INTO emerging_signals (
                id, workspace_id, signal_key, title, signal_stage, signal_score, confidence,
                velocity_score, acceleration_score, baseline_rate, current_rate, change_point_count,
                change_point_strength, has_recent_change_point, supporting_semantic_cluster_ids, doc_ids, source_ids, source_count, keywords, evidence,
                explainability, recommended_watch_action, detected_at, first_seen_at, last_seen_at, created_at, updated_at
            ) VALUES (
                :id, :workspace_id, :signal_key, :title, :signal_stage, :signal_score, :confidence,
                :velocity_score, :acceleration_score, :baseline_rate, :current_rate, :change_point_count,
                :change_point_strength, :has_recent_change_point, CAST(:semantic_cluster_ids AS jsonb), CAST(:doc_ids AS jsonb), CAST(:source_ids AS jsonb), :source_count,
                CAST(:keywords AS jsonb), CAST(:evidence AS jsonb), CAST(:explainability AS jsonb), :recommended_watch_action,
                NOW(), :first_seen_at, :last_seen_at, NOW(), NOW()
            ) ON CONFLICT (id) DO UPDATE SET
                signal_key = EXCLUDED.signal_key, title = EXCLUDED.title, signal_stage = EXCLUDED.signal_stage,
                signal_score = EXCLUDED.signal_score, confidence = EXCLUDED.confidence,
                velocity_score = EXCLUDED.velocity_score, acceleration_score = EXCLUDED.acceleration_score,
                baseline_rate = EXCLUDED.baseline_rate, current_rate = EXCLUDED.current_rate,
                change_point_count = EXCLUDED.change_point_count, change_point_strength = EXCLUDED.change_point_strength,
                has_recent_change_point = EXCLUDED.has_recent_change_point,
                supporting_semantic_cluster_ids = EXCLUDED.supporting_semantic_cluster_ids, doc_ids = EXCLUDED.doc_ids,
                source_ids = EXCLUDED.source_ids, source_count = EXCLUDED.source_count, keywords = EXCLUDED.keywords,
                evidence = EXCLUDED.evidence, explainability = EXCLUDED.explainability,
                recommended_watch_action = EXCLUDED.recommended_watch_action, detected_at = NOW(),
                first_seen_at = LEAST(emerging_signals.first_seen_at, EXCLUDED.first_seen_at),
                last_seen_at = GREATEST(emerging_signals.last_seen_at, EXCLUDED.last_seen_at), updated_at = NOW()
            """
        ),
        {
            "id": signal_id,
            "workspace_id": item["workspace_id"],
            "signal_key": item["signal_key"],
            "title": item["title"],
            "signal_stage": item["signal_stage"],
            "signal_score": item["signal_score"],
            "confidence": item["confidence"],
            "velocity_score": item["velocity_score"],
            "acceleration_score": item["acceleration_score"],
            "baseline_rate": item["baseline_rate"],
            "current_rate": item["current_rate"],
            "change_point_count": item["change_point_count"],
            "change_point_strength": item["change_point_strength"],
            "has_recent_change_point": item["has_recent_change_point"],
            "semantic_cluster_ids": json.dumps(item["semantic_cluster_ids"]),
            "doc_ids": json.dumps(item["doc_ids"]),
            "source_ids": json.dumps(item["source_ids"]),
            "source_count": item["source_count"],
            "keywords": json.dumps(item["keywords"]),
            "evidence": json.dumps(item["evidence"]),
            "explainability": json.dumps({**item["explainability"], "run_id": run_id}),
            "recommended_watch_action": item["recommended_watch_action"],
            "first_seen_at": item["first_seen_at"],
            "last_seen_at": item["last_seen_at"],
        },
    )
    return signal_id


def _series_rows_for_posts(
    *,
    workspace_id: str,
    entity_kind: str,
    entity_id: str,
    posts: list[ClusterPost],
    bucket_hours: int,
) -> list[dict[str, Any]]:
    buckets: dict[datetime, list[ClusterPost]] = defaultdict(list)
    for post in posts:
        buckets[_bucket_start(post.published_at, bucket_hours)].append(post)
    rows: list[dict[str, Any]] = []
    for window_start, bucket_posts in sorted(buckets.items()):
        window_end = window_start + timedelta(hours=bucket_hours)
        rows.append(
            {
                "id": _digest(
                    f"{workspace_id}|{entity_kind}|{entity_id}|{window_start.isoformat()}", "series"
                ),
                "workspace_id": workspace_id,
                "entity_kind": entity_kind,
                "entity_id": entity_id,
                "window_start": window_start,
                "window_end": window_end,
                "doc_count": len(bucket_posts),
                "source_count": len({post.source_id for post in bucket_posts}),
                "avg_relevance": round(
                    sum(post.relevance_score for post in bucket_posts) / max(len(bucket_posts), 1),
                    4,
                ),
                "avg_source_score": round(
                    sum(post.source_score for post in bucket_posts) / max(len(bucket_posts), 1), 4
                ),
                "freshness_score": round(
                    max(_freshness(post.published_at) for post in bucket_posts), 4
                ),
                "window_rate": round(len(bucket_posts) / max(bucket_hours, 1), 6),
                "metadata_json": {
                    "post_ids": [post.post_id for post in bucket_posts],
                    "source_ids": sorted({post.source_id for post in bucket_posts}),
                },
            }
        )
    return rows


async def _replace_signal_series(
    session: AsyncSession,
    *,
    workspace_id: str,
    entity_kind: str,
    rows: list[dict[str, Any]],
) -> None:
    entity_ids = sorted({str(row["entity_id"]) for row in rows})
    if entity_ids:
        await session.execute(
            text(
                """
                DELETE FROM signal_time_series
                WHERE workspace_id = :workspace_id
                  AND entity_kind = :entity_kind
                  AND entity_id = ANY(:entity_ids)
                """
            ),
            {"workspace_id": workspace_id, "entity_kind": entity_kind, "entity_ids": entity_ids},
        )
    for row in rows:
        await session.execute(
            text(
                """
                INSERT INTO signal_time_series (
                    id, workspace_id, entity_kind, entity_id, window_start, window_end,
                    doc_count, source_count, avg_relevance, avg_source_score, freshness_score,
                    window_rate, metadata_json, created_at, updated_at
                ) VALUES (
                    :id, :workspace_id, :entity_kind, :entity_id, :window_start, :window_end,
                    :doc_count, :source_count, :avg_relevance, :avg_source_score, :freshness_score,
                    :window_rate, CAST(:metadata_json AS jsonb), NOW(), NOW()
                )
                ON CONFLICT (workspace_id, entity_kind, entity_id, window_start, window_end) DO UPDATE SET
                    doc_count = EXCLUDED.doc_count,
                    source_count = EXCLUDED.source_count,
                    avg_relevance = EXCLUDED.avg_relevance,
                    avg_source_score = EXCLUDED.avg_source_score,
                    freshness_score = EXCLUDED.freshness_score,
                    window_rate = EXCLUDED.window_rate,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = NOW()
                """
            ),
            {**row, "metadata_json": json.dumps(row["metadata_json"])},
        )


async def _load_series(
    session: AsyncSession,
    *,
    workspace_id: str | None,
    entity_kind: str,
    entity_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    if not entity_ids:
        return {}
    rows = await _rows(
        session,
        """
        SELECT
            entity_id,
            window_start,
            window_end,
            doc_count,
            source_count,
            avg_relevance,
            avg_source_score,
            freshness_score,
            window_rate,
            COALESCE(metadata_json, '{}'::jsonb) AS metadata_json
        FROM signal_time_series
        WHERE entity_kind = :entity_kind
          AND entity_id = ANY(:entity_ids)
          AND (CAST(:workspace_id AS text) IS NULL OR workspace_id = CAST(:workspace_id AS text))
        ORDER BY entity_id ASC, window_start ASC
        """,
        {"workspace_id": workspace_id, "entity_kind": entity_kind, "entity_ids": entity_ids},
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["entity_id"])].append(row)
    return grouped


def _detect_change_points(
    series: list[dict[str, Any]], cluster_cfg: dict[str, Any]
) -> dict[str, Any]:
    if len(series) < max(int(cluster_cfg["change_point_min_size"]) * 2, 4):
        return {
            "breakpoints": [],
            "last_breakpoint_at": None,
            "change_point_strength": 0.0,
            "has_recent_change_point": False,
        }
    signal = [float(item["doc_count"]) for item in series]
    breakpoints: list[int] = []
    if rpt is not None:
        try:
            method = str(cluster_cfg["change_point_method"]).lower()
            min_size = int(cluster_cfg["change_point_min_size"])
            jump = int(cluster_cfg["change_point_jump"])
            if method == "pelt":
                algo = rpt.Pelt(model="l2", min_size=min_size, jump=jump).fit(signal)
                pen_value = (
                    max(len(signal) ** 0.5, 2.0)
                    if str(cluster_cfg["change_point_penalty"]).lower() == "auto"
                    else float(cluster_cfg["change_point_penalty"])
                )
                breakpoints = [idx for idx in algo.predict(pen=pen_value) if idx < len(signal)]
            else:
                algo = rpt.Window(width=max(2, min(len(signal) - 1, min_size * 2)), model="l2").fit(
                    signal
                )
                pen_value = (
                    max(len(signal) ** 0.5, 2.0)
                    if str(cluster_cfg["change_point_penalty"]).lower() == "auto"
                    else float(cluster_cfg["change_point_penalty"])
                )
                breakpoints = [idx for idx in algo.predict(pen=pen_value) if idx < len(signal)]
        except Exception:
            breakpoints = []
    if not breakpoints:
        diffs = [signal[idx] - signal[idx - 1] for idx in range(1, len(signal))]
        if diffs:
            avg = sum(abs(diff) for diff in diffs) / len(diffs)
            threshold = max(avg * 1.5, 1.0)
            breakpoints = [idx for idx, diff in enumerate(diffs, start=1) if abs(diff) >= threshold]
    last_breakpoint_at = series[breakpoints[-1]]["window_end"] if breakpoints else None
    prev = signal[breakpoints[-1] - 1] if breakpoints and breakpoints[-1] - 1 >= 0 else signal[0]
    current = signal[breakpoints[-1]] if breakpoints else signal[-1]
    strength = round(abs(current - prev) / max(max(signal), 1.0), 4)
    recent_cutoff = datetime.now(UTC) - timedelta(
        hours=int(cluster_cfg["change_point_recent_hours"])
    )
    return {
        "breakpoints": breakpoints,
        "last_breakpoint_at": last_breakpoint_at.isoformat() if last_breakpoint_at else None,
        "change_point_strength": strength,
        "has_recent_change_point": bool(last_breakpoint_at and last_breakpoint_at >= recent_cutoff),
    }


def _temporal_metrics(series: list[dict[str, Any]], cluster_cfg: dict[str, Any]) -> dict[str, Any]:
    if not series:
        return {
            "current_rate": 0.0,
            "baseline_rate": 0.0,
            "velocity_score": 0.0,
            "acceleration_score": 0.0,
            "change_point_count": 0,
            "change_point_strength": 0.0,
            "has_recent_change_point": False,
            "breakpoints": [],
            "last_breakpoint_at": None,
        }
    short_hours = max(int(cluster_cfg["signal_short_window_hours"]), 1)
    now = datetime.now(UTC)
    short_cutoff = now - timedelta(hours=short_hours)
    baseline_cutoff = now - timedelta(days=int(cluster_cfg["signal_baseline_window_days"]))
    current_docs = sum(
        int(item["doc_count"]) for item in series if item["window_end"] >= short_cutoff
    )
    current_rate = round(current_docs / short_hours, 6)
    baseline_rows = [
        item for item in series if baseline_cutoff <= item["window_end"] < short_cutoff
    ]
    baseline_docs = sum(int(item["doc_count"]) for item in baseline_rows)
    baseline_hours = max(len(baseline_rows) * short_hours, short_hours)
    baseline_rate = round(baseline_docs / baseline_hours, 6)
    prev_cutoff = short_cutoff - timedelta(hours=short_hours)
    prev_docs = sum(
        int(item["doc_count"])
        for item in series
        if prev_cutoff <= item["window_end"] < short_cutoff
    )
    prev_rate = round(prev_docs / short_hours, 6)
    change_points = _detect_change_points(series, cluster_cfg)
    return {
        "current_rate": current_rate,
        "baseline_rate": baseline_rate,
        "velocity_score": round(current_rate - baseline_rate, 6),
        "acceleration_score": round(current_rate - prev_rate, 6),
        "change_point_count": len(change_points["breakpoints"]),
        "change_point_strength": change_points["change_point_strength"],
        "has_recent_change_point": change_points["has_recent_change_point"],
        "breakpoints": change_points["breakpoints"],
        "last_breakpoint_at": change_points["last_breakpoint_at"],
    }


def _merge_signal_candidates(
    items: list[dict[str, Any]], cluster_cfg: dict[str, Any]
) -> tuple[list[dict[str, Any]], int]:
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
                current["source_ids"] = sorted(
                    set(current.get("source_ids") or []) | set(other.get("source_ids") or [])
                )
                current["source_count"] = len(current["source_ids"])
                current["doc_ids"] = sorted(current_docs)
                current["semantic_cluster_ids"] = sorted(current_semantic)
                current["keywords"] = [
                    name
                    for name, _ in Counter(
                        (current.get("keywords") or []) + (other.get("keywords") or [])
                    ).most_common(8)
                ]
                current["evidence"] = (current.get("evidence") or []) + [
                    item
                    for item in (other.get("evidence") or [])
                    if item not in (current.get("evidence") or [])
                ]
                merged_count += 1
        if merged_into_current:
            explainability = dict(current.get("explainability") or {})
            explainability["merged_signal_ids"] = merged_into_current
            current["explainability"] = explainability
        kept.append(current)
    return kept, merged_count


async def _load_semantic_state(
    session: AsyncSession,
    workspace_id: str | None,
    cluster_cfg: dict[str, Any],
    qdrant: QdrantFrontierClient,
) -> list[dict[str, Any]]:
    rows = await _existing(
        session,
        "semantic_clusters",
        workspace_id,
        int(cluster_cfg["semantic_cluster_archive_hours"]),
    )
    doc_ids = sorted({doc_id for row in rows for doc_id in (row.get("doc_ids") or [])})
    if not doc_ids:
        return []
    post_rows = await _rows(
        session,
        """
        SELECT p.id, p.workspace_id, p.source_id, p.content, p.published_at,
               COALESCE(p.relevance_score, 0) AS relevance_score,
               COALESCE(s.source_score, s.source_authority, 0) AS source_score,
               COALESCE(p.tags, '[]'::jsonb) AS tags,
               COALESCE(p.extra, '{}'::jsonb) AS extra
        FROM posts p
        JOIN sources s ON s.id = p.source_id
        WHERE p.id = ANY(:doc_ids)
        """,
        {"doc_ids": doc_ids},
    )
    posts = _posts_from_docs(post_rows, await qdrant.fetch_documents(doc_ids))
    posts_by_id = {post.post_id: post for post in posts}
    loaded: list[dict[str, Any]] = []
    for row in rows:
        group_posts = [
            posts_by_id[doc_id] for doc_id in row.get("doc_ids") or [] if doc_id in posts_by_id
        ]
        if not group_posts:
            continue
        row["posts"] = group_posts
        row["cluster_id"] = row["id"]
        row["centroid"] = _centroid([post.vector for post in group_posts])
        row["avg_relevance"] = float(row.get("avg_relevance") or 0.0)
        row["avg_source_score"] = float(row.get("avg_source_score") or 0.0)
        loaded.append(row)
    return loaded


def _semantic_results(
    posts: list[ClusterPost],
    groups: list[list[ClusterPost]],
    existing_clusters: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for group in groups:
        centroid = _centroid([post.vector for post in group])
        representative = _representative(group, centroid)
        concepts = [
            name
            for name, _ in Counter(tag for post in group for tag in post.tags if tag).most_common(8)
        ]
        top_terms = _top_terms(group)
        cluster_key, existing_id, related = _semantic_identity(
            group, representative, concepts, existing_clusters
        )
        evidence = [
            {
                "post_id": p.post_id,
                "source_id": p.source_id,
                "published_at": p.published_at.isoformat(),
                "title": p.title,
                "url": p.url,
            }
            for p in sorted(group, key=lambda x: x.published_at, reverse=True)[:5]
        ]
        source_dist = Counter(post.source_id for post in group)
        results.append(
            {
                "cluster_key": cluster_key,
                "existing_id": existing_id,
                "workspace_id": representative.workspace_id,
                "title": representative.title,
                "representative_post_id": representative.post_id,
                "post_count": len(group),
                "source_count": len(source_dist),
                "doc_ids": [p.post_id for p in group],
                "source_ids": sorted(source_dist),
                "top_concepts": concepts,
                "evidence": evidence,
                "representative_evidence": next(
                    (item for item in evidence if item["post_id"] == representative.post_id),
                    evidence[0],
                ),
                "related_cluster_ids": related,
                "lifecycle_state": "active" if existing_id else "new",
                "avg_relevance": round(sum(p.relevance_score for p in group) / len(group), 4),
                "avg_source_score": round(sum(p.source_score for p in group) / len(group), 4),
                "freshness_score": round(max(_freshness(p.published_at) for p in group), 4),
                "coherence_score": round(_coherence(group, centroid), 4),
                "first_seen_at": min(p.published_at for p in group),
                "last_seen_at": max(p.published_at for p in group),
                "centroid": centroid,
                "posts": group,
                "explainability": {
                    "why_clustered": "Embedding similarity within time window.",
                    "top_terms": top_terms,
                    "top_concepts": concepts,
                    "source_distribution": dict(source_dist),
                    "time_span": {
                        "first_seen_at": min(p.published_at for p in group).isoformat(),
                        "last_seen_at": max(p.published_at for p in group).isoformat(),
                    },
                },
            }
        )
    return results


def _merge_semantic_candidates(
    items: list[dict[str, Any]], cluster_cfg: dict[str, Any]
) -> tuple[list[dict[str, Any]], int]:
    if not bool(cluster_cfg.get("semantic_merge_enabled", True)):
        return items, 0
    threshold = float(cluster_cfg.get("semantic_merge_similarity_threshold", 0.78))
    title_threshold = float(cluster_cfg.get("semantic_merge_title_overlap_threshold", 0.4))
    concept_threshold = float(cluster_cfg.get("semantic_merge_concept_overlap_threshold", 0.5))
    max_gap_hours = int(cluster_cfg.get("semantic_merge_max_gap_hours", 168))
    merged_count = 0
    kept: list[dict[str, Any]] = []
    absorbed_ids: set[str] = set()
    items = sorted(
        items, key=lambda item: (item["last_seen_at"], item["coherence_score"]), reverse=True
    )

    for idx, current in enumerate(items):
        current_id = (
            current.get("existing_id") or current.get("cluster_id") or current.get("cluster_key")
        )
        if current_id in absorbed_ids:
            continue
        current_terms = set(
            current.get("explainability", {}).get("top_terms") or _terms(current.get("title") or "")
        )
        current_concepts = set(current.get("top_concepts") or [])
        current_doc_ids = set(current.get("doc_ids") or [])
        merged_ids: list[str] = []

        for other in items[idx + 1 :]:
            other_id = (
                other.get("existing_id") or other.get("cluster_id") or other.get("cluster_key")
            )
            if other_id in absorbed_ids or other.get("workspace_id") != current.get("workspace_id"):
                continue
            gap_hours = (
                abs((current["last_seen_at"] - other["last_seen_at"]).total_seconds()) / 3600.0
            )
            if gap_hours > max_gap_hours:
                continue
            centroid_similarity = _cos(current.get("centroid") or [], other.get("centroid") or [])
            title_overlap = _jaccard(
                current_terms,
                set(
                    other.get("explainability", {}).get("top_terms")
                    or _terms(other.get("title") or "")
                ),
            )
            concept_overlap = _jaccard(current_concepts, set(other.get("top_concepts") or []))
            doc_overlap = len(current_doc_ids & set(other.get("doc_ids") or [])) / max(
                len(current_doc_ids | set(other.get("doc_ids") or [])), 1
            )
            temporal_proximity = max(0.0, 1.0 - (gap_hours / max(max_gap_hours, 1)))
            similarity = (
                centroid_similarity * 0.45
                + concept_overlap * 0.25
                + title_overlap * 0.15
                + temporal_proximity * 0.10
                + doc_overlap * 0.05
            )
            if similarity < threshold and not (
                title_overlap >= title_threshold and concept_overlap >= concept_threshold
            ):
                continue

            absorbed_ids.add(other_id)
            merged_ids.append(other_id)
            current["doc_ids"] = sorted(
                set(current.get("doc_ids") or []) | set(other.get("doc_ids") or [])
            )
            current["source_ids"] = sorted(
                set(current.get("source_ids") or []) | set(other.get("source_ids") or [])
            )
            current["source_count"] = len(current["source_ids"])
            current["post_count"] = len(current["doc_ids"])
            current["posts"] = sorted(
                (current.get("posts") or []) + (other.get("posts") or []),
                key=lambda post: post.published_at,
                reverse=True,
            )
            current["top_concepts"] = [
                name
                for name, _ in Counter(
                    (current.get("top_concepts") or []) + (other.get("top_concepts") or [])
                ).most_common(8)
            ]
            current["evidence"] = (current.get("evidence") or []) + [
                item
                for item in (other.get("evidence") or [])
                if item not in (current.get("evidence") or [])
            ]
            current["related_cluster_ids"] = sorted(
                set(current.get("related_cluster_ids") or [])
                | set(other.get("related_cluster_ids") or [])
            )
            current["first_seen_at"] = min(current["first_seen_at"], other["first_seen_at"])
            current["last_seen_at"] = max(current["last_seen_at"], other["last_seen_at"])
            current["avg_relevance"] = round(
                sum(post.relevance_score for post in current["posts"])
                / max(len(current["posts"]), 1),
                4,
            )
            current["avg_source_score"] = round(
                sum(post.source_score for post in current["posts"]) / max(len(current["posts"]), 1),
                4,
            )
            current["freshness_score"] = round(
                max(_freshness(post.published_at) for post in current["posts"]), 4
            )
            current["centroid"] = _centroid([post.vector for post in current["posts"]])
            current["coherence_score"] = round(_coherence(current["posts"], current["centroid"]), 4)
            current["representative_post_id"] = _representative(
                current["posts"], current["centroid"]
            ).post_id
            current["representative_evidence"] = next(
                (
                    item
                    for item in current["evidence"]
                    if item["post_id"] == current["representative_post_id"]
                ),
                current["evidence"][0],
            )
            current["title"] = next(
                (
                    post.title
                    for post in current["posts"]
                    if post.post_id == current["representative_post_id"]
                ),
                current["title"],
            )
            merged_count += 1
            current_terms = set(_top_terms(current["posts"]))
            current_concepts = set(current["top_concepts"])
            current_doc_ids = set(current["doc_ids"])

        if merged_ids:
            explainability = dict(current.get("explainability") or {})
            explainability["merged_semantic_ids"] = merged_ids
            explainability["top_terms"] = _top_terms(current["posts"])
            explainability["top_concepts"] = current["top_concepts"]
            explainability["source_distribution"] = dict(
                Counter(post.source_id for post in current["posts"])
            )
            explainability["time_span"] = {
                "first_seen_at": current["first_seen_at"].isoformat(),
                "last_seen_at": current["last_seen_at"].isoformat(),
            }
            current["explainability"] = explainability
        kept.append(current)
    return kept, merged_count


def _signal_results(
    semantic: list[dict[str, Any]],
    existing_trends: list[dict[str, Any]],
    existing_emerging: list[dict[str, Any]],
    cluster_cfg: dict[str, Any],
    *,
    signal_series_by_id: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    trend_posts: list[ClusterPost] = []
    semantic_by_id: dict[str, dict[str, Any]] = {}
    for item in semantic:
        semantic_id = item.get("cluster_id") or _digest(item["cluster_key"], "semantic")
        item["cluster_id"] = semantic_id
        semantic_by_id[semantic_id] = item
        rep_post = next(
            (post for post in item["posts"] if post.post_id == item["representative_post_id"]),
            item["posts"][0],
        )
        trend_posts.append(
            ClusterPost(
                semantic_id,
                rep_post.workspace_id,
                rep_post.source_id,
                rep_post.content,
                item["last_seen_at"],
                item["avg_relevance"],
                item["avg_source_score"],
                item["top_concepts"],
                item["title"],
                rep_post.url,
                item["centroid"],
            )
        )
    groups = _components(
        trend_posts,
        float(cluster_cfg["trend_cluster_similarity_threshold"]),
        int(cluster_cfg["trend_cluster_max_gap_hours"]),
    )
    stable, emerging = [], []
    signal_series_by_id = signal_series_by_id or {}
    for group in groups:
        semantic_group = [semantic_by_id[item.post_id] for item in group]
        doc_ids = sorted({doc_id for item in semantic_group for doc_id in item["doc_ids"]})
        source_ids = sorted(
            {source_id for item in semantic_group for source_id in item["source_ids"]}
        )
        evidence = [ev for item in semantic_group for ev in item["evidence"][:2]][:6]
        keywords = [
            name
            for name, _ in Counter(
                term for item in semantic_group for term in item["top_concepts"]
            ).most_common(8)
        ]
        vectors = [post.vector for item in semantic_group for post in item["posts"]]
        centroid = _centroid(vectors)
        coherence_score = round(sum(_cos(v, centroid) for v in vectors) / max(len(vectors), 1), 4)
        last_seen = max(item["last_seen_at"] for item in semantic_group)
        first_seen = min(item["first_seen_at"] for item in semantic_group)
        source_diversity = round(
            (
                0.0
                if len(source_ids) <= 1
                else min(1.0, len(source_ids) / max(len(semantic_group), 2))
            ),
            4,
        )
        freshness_score = round(_freshness(last_seen), 4)
        evidence_strength = round(
            min(
                1.0,
                (len(evidence) / max(int(cluster_cfg["cluster_min_evidence_count"]), 1)) * 0.5
                + sum(item["avg_relevance"] for item in semantic_group)
                / max(len(semantic_group), 1)
                * 0.5,
            ),
            4,
        )
        recent_docs = sum(
            1
            for item in semantic_group
            for post in item["posts"]
            if post.published_at >= datetime.now(UTC) - timedelta(days=7)
        )
        burst_score = round(
            min(1.0, recent_docs / max(len(doc_ids), 1) + min(len(semantic_group), 4) * 0.08), 4
        )
        novelty_score = round(
            min(1.0, (len(set(keywords[:5])) / 5.0) * 0.5 + source_diversity * 0.5), 4
        )
        rep = max(
            group,
            key=lambda item: item.relevance_score
            + item.source_score
            + _freshness(item.published_at),
        )
        existing = next(
            (
                row
                for row in [*existing_trends, *existing_emerging]
                if row.get("workspace_id") == rep.workspace_id
                and set(row.get("doc_ids") or []) & set(doc_ids)
            ),
            None,
        )
        signal_key = (
            existing.get("cluster_key")
            if existing and existing.get("cluster_key")
            else (
                existing.get("signal_key")
                if existing
                else _digest(
                    "|".join(
                        [
                            rep.workspace_id,
                            " ".join(_terms(rep.title))[:120],
                            ",".join(sorted(keywords[:5])),
                        ]
                    ),
                    "signal",
                )
            )
        )
        combined_series: dict[datetime, dict[str, Any]] = {}
        for semantic_item in semantic_group:
            for point in signal_series_by_id.get(semantic_item["cluster_id"], []):
                bucket = combined_series.setdefault(
                    point["window_start"],
                    {
                        "window_start": point["window_start"],
                        "window_end": point["window_end"],
                        "doc_count": 0,
                        "source_ids": set(),
                        "avg_relevance_sum": 0.0,
                        "avg_source_score_sum": 0.0,
                        "rows": 0,
                        "freshness_score": 0.0,
                    },
                )
                bucket["doc_count"] += int(point["doc_count"])
                bucket["source_ids"].update(point.get("metadata_json", {}).get("source_ids", []))
                bucket["avg_relevance_sum"] += float(point["avg_relevance"])
                bucket["avg_source_score_sum"] += float(point["avg_source_score"])
                bucket["rows"] += 1
                bucket["freshness_score"] = max(
                    bucket["freshness_score"], float(point["freshness_score"])
                )
        signal_series = []
        for bucket in sorted(combined_series.values(), key=lambda item: item["window_start"]):
            signal_series.append(
                {
                    "window_start": bucket["window_start"],
                    "window_end": bucket["window_end"],
                    "doc_count": bucket["doc_count"],
                    "source_count": len(bucket["source_ids"]),
                    "avg_relevance": round(bucket["avg_relevance_sum"] / max(bucket["rows"], 1), 4),
                    "avg_source_score": round(
                        bucket["avg_source_score_sum"] / max(bucket["rows"], 1), 4
                    ),
                    "freshness_score": bucket["freshness_score"],
                    "window_rate": round(
                        bucket["doc_count"] / max(int(cluster_cfg["signal_short_window_hours"]), 1),
                        6,
                    ),
                }
            )
        temporal = _temporal_metrics(signal_series, cluster_cfg)
        raw_signal_score = round(
            max(
                0.0,
                min(
                    1.0,
                    burst_score * 0.18
                    + temporal["velocity_score"] * float(cluster_cfg["signal_velocity_weight"])
                    + temporal["acceleration_score"]
                    * float(cluster_cfg["signal_acceleration_weight"])
                    + coherence_score * 0.2
                    + novelty_score * 0.12
                    + source_diversity * 0.12
                    + freshness_score * 0.08
                    + evidence_strength * 0.06
                    + temporal["change_point_strength"] * 0.14,
                ),
            ),
            4,
        )
        april_fools = _april_fools_penalty(
            [post for item in semantic_group for post in item["posts"]],
            cluster_cfg,
        )
        signal_score = round(raw_signal_score * float(april_fools["score_multiplier"]), 4)
        stage = "weak"
        if (
            len(semantic_group) >= int(cluster_cfg["trend_cluster_min_semantic_clusters"])
            and len(doc_ids) >= int(cluster_cfg["trend_cluster_min_docs"])
            and len(source_ids) >= int(cluster_cfg["signal_min_source_count"])
            and signal_score >= float(cluster_cfg["trend_cluster_stable_threshold"])
            and source_diversity >= float(cluster_cfg["trend_cluster_min_source_diversity"])
            and (temporal["has_recent_change_point"] or temporal["velocity_score"] > 0)
        ):
            stage = "stable"
        elif (
            signal_score >= float(cluster_cfg["trend_cluster_emerging_threshold"])
            and len(evidence) >= int(cluster_cfg["cluster_min_evidence_count"])
            and len(source_ids) >= int(cluster_cfg["signal_min_source_count"])
            and source_diversity >= float(cluster_cfg["trend_cluster_min_source_diversity"])
            and (
                temporal["has_recent_change_point"]
                or temporal["velocity_score"] > 0
                or temporal["acceleration_score"] > 0
            )
        ):
            stage = "emerging"
        elif last_seen < datetime.now(UTC) - timedelta(days=10) or (
            temporal["velocity_score"] <= 0 and not temporal["has_recent_change_point"]
        ):
            stage = "fading"
        if stage in {"stable", "emerging"} and bool(april_fools["stage_blocked"]):
            stage = "weak"
        confidence = round(
            min(1.0, coherence_score * 0.45 + evidence_strength * 0.30 + source_diversity * 0.25), 4
        )
        if (
            stage == "weak"
            and not bool(cluster_cfg.get("persist_weak_signals", True))
            and (
                signal_score < float(cluster_cfg.get("weak_signal_min_score", 0.42))
                or confidence < float(cluster_cfg.get("weak_signal_min_confidence", 0.52))
                or source_diversity
                < float(cluster_cfg.get("weak_signal_min_source_diversity", 0.2))
                or len(source_ids) < int(cluster_cfg.get("weak_signal_min_source_count", 1))
            )
        ):
            continue
        merged_top_terms = [
            name
            for name, _ in Counter(
                term
                for item in semantic_group
                for term in (item.get("explainability", {}) or {}).get("top_terms", [])
            ).most_common(8)
        ]
        payload = {
            "existing_id": existing.get("id") if existing else None,
            "signal_key": signal_key,
            "workspace_id": rep.workspace_id,
            "title": rep.title,
            "signal_stage": stage,
            "signal_score": signal_score,
            "confidence": confidence,
            "semantic_cluster_ids": sorted(item["cluster_id"] for item in semantic_group),
            "doc_ids": doc_ids,
            "source_ids": source_ids,
            "source_count": len(source_ids),
            "keywords": keywords,
            "evidence": evidence,
            "recommended_watch_action": (
                "Track for 48h and compare source diversity before promoting to stable trend."
                if stage in {"weak", "emerging"}
                else "Promote to trend evidence and watch for fading."
            ),
            "burst_score": burst_score,
            "coherence_score": coherence_score,
            "novelty_score": novelty_score,
            "source_diversity_score": source_diversity,
            "freshness_score": freshness_score,
            "evidence_strength_score": evidence_strength,
            "velocity_score": temporal["velocity_score"],
            "acceleration_score": temporal["acceleration_score"],
            "baseline_rate": temporal["baseline_rate"],
            "current_rate": temporal["current_rate"],
            "change_point_count": temporal["change_point_count"],
            "change_point_strength": temporal["change_point_strength"],
            "has_recent_change_point": temporal["has_recent_change_point"],
            "first_seen_at": first_seen,
            "last_seen_at": last_seen,
            "centroid": centroid,
            "series_posts": [post for item in semantic_group for post in item["posts"]],
            "explainability": {
                "top_terms": merged_top_terms,
                "top_concepts": keywords,
                "source_distribution": Counter(ev["source_id"] for ev in evidence),
                "time_span": {
                    "first_seen_at": first_seen.isoformat(),
                    "last_seen_at": last_seen.isoformat(),
                },
                "change_points": {
                    "breakpoints": temporal["breakpoints"],
                    "last_breakpoint_at": temporal["last_breakpoint_at"],
                },
                "april_fools_guard": april_fools,
                "scores": {
                    "burst_score": burst_score,
                    "velocity_score": temporal["velocity_score"],
                    "acceleration_score": temporal["acceleration_score"],
                    "coherence_score": coherence_score,
                    "novelty_score": novelty_score,
                    "source_diversity_score": source_diversity,
                    "freshness_score": freshness_score,
                    "evidence_strength_score": evidence_strength,
                    "change_point_strength": temporal["change_point_strength"],
                    "raw_signal_score": raw_signal_score,
                    "signal_score": signal_score,
                },
            },
        }
        (stable if stage == "stable" else emerging).append(payload)
    stable, merged_stable = _merge_signal_candidates(stable, cluster_cfg)
    emerging, merged_emerging = _merge_signal_candidates(emerging, cluster_cfg)
    for item in stable:
        item["merged_signal_count"] = merged_stable
    for item in emerging:
        item["merged_signal_count"] = merged_emerging
    return stable, emerging


async def _lifecycle_updates(
    session: AsyncSession,
    workspace_id: str | None,
    touched_semantic: set[str],
    touched_trends: set[str],
    touched_emerging: set[str],
    cluster_cfg: dict[str, Any],
) -> None:
    await session.execute(
        text(
            """
            UPDATE semantic_clusters SET lifecycle_state = 'archived', updated_at = NOW()
            WHERE (CAST(:workspace_id AS text) IS NULL OR workspace_id = CAST(:workspace_id AS text))
              AND id <> ALL(:touched) AND detected_at < :archive_cutoff AND lifecycle_state <> 'archived'
            """
        ),
        {
            "workspace_id": workspace_id,
            "touched": list(touched_semantic),
            "archive_cutoff": datetime.now(UTC)
            - timedelta(hours=int(cluster_cfg["semantic_cluster_archive_hours"])),
        },
    )
    await session.execute(
        text(
            """
            UPDATE semantic_clusters SET lifecycle_state = 'cooling', updated_at = NOW()
            WHERE (CAST(:workspace_id AS text) IS NULL OR workspace_id = CAST(:workspace_id AS text))
              AND id <> ALL(:touched) AND detected_at < :cooling_cutoff AND detected_at >= :archive_cutoff
              AND lifecycle_state NOT IN ('cooling','archived')
            """
        ),
        {
            "workspace_id": workspace_id,
            "touched": list(touched_semantic),
            "cooling_cutoff": datetime.now(UTC)
            - timedelta(hours=int(cluster_cfg["semantic_cluster_cooling_hours"])),
            "archive_cutoff": datetime.now(UTC)
            - timedelta(hours=int(cluster_cfg["semantic_cluster_archive_hours"])),
        },
    )
    await session.execute(
        text(
            "UPDATE trend_clusters SET signal_stage = 'fading', updated_at = NOW() WHERE (CAST(:workspace_id AS text) IS NULL OR workspace_id = CAST(:workspace_id AS text)) AND id <> ALL(:touched) AND detected_at < NOW() - INTERVAL '7 days' AND signal_stage <> 'fading'"
        ),
        {"workspace_id": workspace_id, "touched": list(touched_trends)},
    )
    await session.execute(
        text(
            "UPDATE emerging_signals SET signal_stage = 'fading', updated_at = NOW() WHERE (CAST(:workspace_id AS text) IS NULL OR workspace_id = CAST(:workspace_id AS text)) AND id <> ALL(:touched) AND detected_at < NOW() - INTERVAL '7 days' AND signal_stage <> 'fading'"
        ),
        {"workspace_id": workspace_id, "touched": list(touched_emerging)},
    )


def _golden_metrics(semantic_map: dict[str, str], trend_map: dict[str, str]) -> dict[str, Any]:
    path = Path(
        _cfg(
            get_settings().cluster_evaluation_fixture_path,
            "tests/fixtures/cluster_analysis_golden_set.json",
        )
    )
    if not path.exists():
        return {}
    try:
        fixture = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    same_story = fixture.get("same_story", [])
    different_story = fixture.get("different_story", [])
    same_trend = fixture.get("same_trend", [])
    return {
        "same_story_accuracy": round(
            sum(1 for a, b in same_story if semantic_map.get(a) == semantic_map.get(b))
            / max(len(same_story), 1),
            4,
        ),
        "different_story_accuracy": round(
            sum(1 for a, b in different_story if semantic_map.get(a) != semantic_map.get(b))
            / max(len(different_story), 1),
            4,
        ),
        "same_trend_accuracy": round(
            sum(1 for a, b in same_trend if trend_map.get(a) == trend_map.get(b))
            / max(len(same_trend), 1),
            4,
        ),
    }


def _metrics(
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
    cluster_cfg: dict[str, Any],
) -> dict[str, Any]:
    semantic_total = max(len(semantic), 1)
    quality = {
        "semantic_cluster_purity": round(
            sum(item["coherence_score"] for item in semantic) / semantic_total, 4
        ),
        "over_merge_rate": round(
            sum(
                1
                for item in semantic
                if item["coherence_score"]
                < float(cluster_cfg["semantic_dedupe_similarity_threshold"]) - 0.08
            )
            / semantic_total,
            4,
        ),
        "over_split_rate": round(
            sum(1 for item in semantic if item["post_count"] == 1) / semantic_total, 4
        ),
        "trend_duplication_rate": round(
            (
                len([" ".join(_terms(item["title"])) for item in stable])
                - len(set(" ".join(_terms(item["title"])) for item in stable))
            )
            / max(len(stable), 1),
            4,
        ),
        "empty_low_evidence_cluster_rate": round(
            sum(
                1
                for item in [*semantic, *stable, *emerging]
                if len(item["evidence"]) < int(cluster_cfg["cluster_min_evidence_count"])
            )
            / max(len(semantic) + len(stable) + len(emerging), 1),
            4,
        ),
        "source_monoculture_rate": round(
            sum(1 for item in semantic if item["source_count"] <= 1) / semantic_total, 4
        ),
    }
    semantic_map = {doc_id: item["cluster_id"] for item in semantic for doc_id in item["doc_ids"]}
    trend_map = {
        semantic_id: item.get("signal_id", "")
        for item in [*stable, *emerging]
        for semantic_id in item["semantic_cluster_ids"]
    }
    quality.update(_golden_metrics(semantic_map, trend_map))
    return quality


def _thresholds_from_cfg(cluster_cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "semantic_similarity": cluster_cfg["semantic_dedupe_similarity_threshold"],
        "semantic_gap_hours": cluster_cfg["semantic_dedupe_max_gap_hours"],
        "semantic_merge_enabled": cluster_cfg["semantic_merge_enabled"],
        "semantic_merge_similarity_threshold": cluster_cfg["semantic_merge_similarity_threshold"],
        "semantic_merge_title_overlap_threshold": cluster_cfg[
            "semantic_merge_title_overlap_threshold"
        ],
        "semantic_merge_concept_overlap_threshold": cluster_cfg[
            "semantic_merge_concept_overlap_threshold"
        ],
        "semantic_merge_max_gap_hours": cluster_cfg["semantic_merge_max_gap_hours"],
        "trend_similarity": cluster_cfg["trend_cluster_similarity_threshold"],
        "trend_gap_hours": cluster_cfg["trend_cluster_max_gap_hours"],
        "stable_threshold": cluster_cfg["trend_cluster_stable_threshold"],
        "emerging_threshold": cluster_cfg["trend_cluster_emerging_threshold"],
        "min_source_diversity": cluster_cfg["trend_cluster_min_source_diversity"],
        "min_evidence_count": cluster_cfg["cluster_min_evidence_count"],
        "signal_short_window_hours": cluster_cfg["signal_short_window_hours"],
        "signal_baseline_window_days": cluster_cfg["signal_baseline_window_days"],
        "signal_velocity_weight": cluster_cfg["signal_velocity_weight"],
        "signal_acceleration_weight": cluster_cfg["signal_acceleration_weight"],
        "change_point_method": cluster_cfg["change_point_method"],
        "change_point_penalty": cluster_cfg["change_point_penalty"],
        "change_point_recent_hours": cluster_cfg["change_point_recent_hours"],
        "signal_merge_similarity_threshold": cluster_cfg["signal_merge_similarity_threshold"],
        "signal_merge_doc_overlap_threshold": cluster_cfg["signal_merge_doc_overlap_threshold"],
        "persist_weak_signals": cluster_cfg["persist_weak_signals"],
        "weak_signal_min_score": cluster_cfg["weak_signal_min_score"],
        "weak_signal_min_confidence": cluster_cfg["weak_signal_min_confidence"],
        "weak_signal_min_source_diversity": cluster_cfg["weak_signal_min_source_diversity"],
        "weak_signal_min_source_count": cluster_cfg["weak_signal_min_source_count"],
        "signal_min_source_count": cluster_cfg["signal_min_source_count"],
    }


async def _persist_signal_outputs(
    session: AsyncSession,
    *,
    run_id: str,
    workspace_id: str | None,
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
    cluster_cfg: dict[str, Any],
) -> tuple[set[str], set[str]]:
    touched_trends: set[str] = set()
    touched_emerging: set[str] = set()

    trend_series_rows: list[dict[str, Any]] = []
    emerging_series_rows: list[dict[str, Any]] = []
    bucket_hours = int(cluster_cfg["signal_short_window_hours"])

    for item in stable:
        signal_id = await _upsert_signal(session, "trend_clusters", run_id, item)
        item["signal_id"] = signal_id
        touched_trends.add(signal_id)
        trend_series_rows.extend(
            _series_rows_for_posts(
                workspace_id=item["workspace_id"],
                entity_kind="trend",
                entity_id=signal_id,
                posts=item.get("series_posts") or [],
                bucket_hours=bucket_hours,
            )
        )
    for item in emerging:
        signal_id = await _upsert_signal(session, "emerging_signals", run_id, item)
        item["signal_id"] = signal_id
        touched_emerging.add(signal_id)
        emerging_series_rows.extend(
            _series_rows_for_posts(
                workspace_id=item["workspace_id"],
                entity_kind="emerging",
                entity_id=signal_id,
                posts=item.get("series_posts") or [],
                bucket_hours=bucket_hours,
            )
        )

    await _replace_signal_series(
        session, workspace_id=workspace_id or "", entity_kind="trend", rows=trend_series_rows
    )
    await _replace_signal_series(
        session, workspace_id=workspace_id or "", entity_kind="emerging", rows=emerging_series_rows
    )
    return touched_trends, touched_emerging


async def _run_signal_analysis_core(
    session: AsyncSession,
    *,
    workspace_id: str | None,
    run_id: str,
    cluster_cfg: dict[str, Any],
    semantic: list[dict[str, Any]],
) -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any], set[str], set[str]
]:
    series_by_semantic = await _load_series(
        session,
        workspace_id=workspace_id,
        entity_kind="semantic",
        entity_ids=[item["cluster_id"] for item in semantic],
    )
    existing_trends = await _existing(
        session, "trend_clusters", workspace_id, int(cluster_cfg["trend_cluster_max_gap_hours"])
    )
    existing_emerging = await _existing(
        session, "emerging_signals", workspace_id, int(cluster_cfg["trend_cluster_max_gap_hours"])
    )
    stable, emerging = _signal_results(
        semantic,
        existing_trends,
        existing_emerging,
        cluster_cfg,
        signal_series_by_id=series_by_semantic,
    )
    touched_trends, touched_emerging = await _persist_signal_outputs(
        session,
        run_id=run_id,
        workspace_id=workspace_id,
        stable=stable,
        emerging=emerging,
        cluster_cfg=cluster_cfg,
    )
    quality = _metrics(semantic, stable, emerging, cluster_cfg)
    summary = {
        "stable_trends_created_or_updated": len(stable),
        "emerging_signals_created_or_updated": len(emerging),
        "change_points_detected": sum(
            int(item.get("change_point_count") or 0) for item in [*stable, *emerging]
        ),
        "signals_promoted_to_emerging": sum(
            1 for item in emerging if item.get("signal_stage") == "emerging"
        ),
        "signals_promoted_to_stable": sum(
            1 for item in stable if item.get("signal_stage") == "stable"
        ),
        "signals_merged": sum(
            int(item.get("merged_signal_count") or 0) for item in [*stable, *emerging]
        ),
    }
    quality["change_points_detected"] = summary["change_points_detected"]
    quality["signals_merged"] = summary["signals_merged"]
    return stable, emerging, summary, quality, touched_trends, touched_emerging


async def run_semantic_clustering(workspace_id: str | None = None) -> dict[str, Any]:
    qdrant = QdrantFrontierClient()
    from shared.db import get_session_factory

    async with get_session_factory()() as session:
        cluster_cfg = await _cluster_settings(session, workspace_id)
        run_id = await _create_run(
            session, workspace_id, _thresholds_from_cfg(cluster_cfg), stage="full"
        )
        await session.commit()
        qdrant_index = {"indexed": 0, "failed": 0, "skipped": 0}
        try:
            rows = await _fetch_posts(
                session,
                workspace_id,
                max(
                    int(cluster_cfg["semantic_cluster_window_days"]),
                    int(cluster_cfg["trend_cluster_window_days"]),
                ),
                max(int(cluster_cfg["semantic_cluster_max_posts"]), 50),
            )
            posts = _posts_from_docs(
                rows, await qdrant.fetch_documents([row["id"] for row in rows])
            )
            semantic = _semantic_results(
                posts,
                _components(
                    posts,
                    float(cluster_cfg["semantic_dedupe_similarity_threshold"]),
                    int(cluster_cfg["semantic_dedupe_max_gap_hours"]),
                ),
                await _existing(
                    session,
                    "semantic_clusters",
                    workspace_id,
                    int(cluster_cfg["semantic_cluster_archive_hours"]),
                ),
            )
            semantic, merged_semantic = _merge_semantic_candidates(semantic, cluster_cfg)
            touched_semantic: set[str] = set()
            semantic_series_rows: list[dict[str, Any]] = []
            for item in semantic:
                cluster_id = await _upsert_semantic(session, run_id, item, item.get("existing_id"))
                item["cluster_id"] = cluster_id
                touched_semantic.add(cluster_id)
                semantic_series_rows.extend(
                    _series_rows_for_posts(
                        workspace_id=item["workspace_id"],
                        entity_kind="semantic",
                        entity_id=cluster_id,
                        posts=item["posts"],
                        bucket_hours=int(cluster_cfg["signal_short_window_hours"]),
                    )
                )
            await _replace_signal_series(
                session,
                workspace_id=workspace_id or "",
                entity_kind="semantic",
                rows=semantic_series_rows,
            )
            await session.commit()

            stable, emerging, signal_summary, quality, touched_trends, touched_emerging = (
                await _run_signal_analysis_core(
                    session,
                    workspace_id=workspace_id,
                    run_id=run_id,
                    cluster_cfg=cluster_cfg,
                    semantic=semantic,
                )
            )
            await _lifecycle_updates(
                session,
                workspace_id,
                touched_semantic,
                touched_trends,
                touched_emerging,
                cluster_cfg,
            )
            summary = {
                "post_candidates": len(posts),
                "semantic_clusters_created_or_updated": len(semantic),
                "semantic_clusters_merged": merged_semantic,
                **signal_summary,
            }
            await _finish_run(session, run_id, "success", summary, quality)
            await session.commit()
            qdrant_index = await _index_trend_clusters_in_qdrant(
                qdrant, run_id=run_id, stable=stable
            )
        except Exception:
            await _finish_run(session, run_id, "error", {"workspace_id": workspace_id}, {})
            await session.commit()
            raise
        finally:
            await qdrant.close()
    return {
        "workspace_id": workspace_id,
        "run_id": run_id,
        "semantic_clusters": len(semantic),
        "trend_clusters": len(stable),
        "emerging_signals": len(emerging),
        "quality_metrics": quality,
        "qdrant_trend_clusters": qdrant_index,
        "semantic_results": [
            {
                "cluster_id": item["cluster_id"],
                "workspace_id": item["workspace_id"],
                "post_count": item["post_count"],
                "source_count": item["source_count"],
                "lifecycle_state": item["lifecycle_state"],
                "coherence_score": item["coherence_score"],
                "freshness_score": item["freshness_score"],
                "top_concepts": item["top_concepts"],
            }
            for item in semantic
        ],
        "trend_results": [
            {
                "trend_id": item["signal_id"],
                "workspace_id": item["workspace_id"],
                "doc_count": len(item["doc_ids"]),
                "signal_stage": item["signal_stage"],
                "signal_score": item["signal_score"],
                "burst_score": item["burst_score"],
                "coherence": item["coherence_score"],
                "novelty": item["novelty_score"],
                "velocity_score": item["velocity_score"],
                "acceleration_score": item["acceleration_score"],
                "change_point_strength": item["change_point_strength"],
            }
            for item in stable
        ],
        "emerging_results": [
            {
                "signal_id": item["signal_id"],
                "workspace_id": item["workspace_id"],
                "doc_count": len(item["doc_ids"]),
                "signal_stage": item["signal_stage"],
                "signal_score": item["signal_score"],
                "confidence": item["confidence"],
                "velocity_score": item["velocity_score"],
                "acceleration_score": item["acceleration_score"],
                "change_point_strength": item["change_point_strength"],
            }
            for item in emerging
        ],
    }


async def run_signal_analysis(workspace_id: str | None = None) -> dict[str, Any]:
    qdrant = QdrantFrontierClient()
    from shared.db import get_session_factory

    async with get_session_factory()() as session:
        cluster_cfg = await _cluster_settings(session, workspace_id)
        run_id = await _create_run(
            session, workspace_id, _thresholds_from_cfg(cluster_cfg), stage="signal-analysis"
        )
        await session.commit()
        missing_signals: list[dict[str, Any]] = []
        qdrant_index = {"indexed": 0, "failed": 0, "skipped": 0}
        try:
            semantic = await _load_semantic_state(session, workspace_id, cluster_cfg, qdrant)
            touched_semantic = {item["cluster_id"] for item in semantic}
            stable, emerging, summary, quality, touched_trends, touched_emerging = (
                await _run_signal_analysis_core(
                    session,
                    workspace_id=workspace_id,
                    run_id=run_id,
                    cluster_cfg=cluster_cfg,
                    semantic=semantic,
                )
            )
            try:
                missing_signals = await run_missing_signals_analysis(
                    session,
                    workspace_id=workspace_id,
                    semantic=semantic,
                    stable=stable,
                    emerging=emerging,
                )
            except Exception:
                logger.exception("Missing signals analysis failed for workspace=%s", workspace_id)
                missing_signals = []
            await _lifecycle_updates(
                session,
                workspace_id,
                touched_semantic,
                touched_trends,
                touched_emerging,
                cluster_cfg,
            )
            await _finish_run(
                session,
                run_id,
                "success",
                {
                    "semantic_clusters_loaded": len(semantic),
                    "missing_signals_created_or_updated": len(missing_signals),
                    **summary,
                },
                quality,
            )
            await session.commit()
            qdrant_index = await _index_trend_clusters_in_qdrant(
                qdrant, run_id=run_id, stable=stable
            )
        except Exception:
            await _finish_run(session, run_id, "error", {"workspace_id": workspace_id}, {})
            await session.commit()
            raise
        finally:
            await qdrant.close()
    return {
        "workspace_id": workspace_id,
        "run_id": run_id,
        "semantic_clusters": len(semantic),
        "trend_clusters": len(stable),
        "emerging_signals": len(emerging),
        "missing_signals": len(missing_signals),
        "qdrant_trend_clusters": qdrant_index,
        "quality_metrics": quality,
        "missing_signal_results": [
            {
                "id": item["id"],
                "topic": item["topic"],
                "gap_score": item["gap_score"],
                "frontier_frequency": item["frontier_frequency"],
                "searxng_frequency": item["searxng_frequency"],
            }
            for item in missing_signals
        ],
    }
