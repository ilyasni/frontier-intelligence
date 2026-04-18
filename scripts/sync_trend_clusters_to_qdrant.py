from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return list(value) if isinstance(value, tuple) else []


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


async def _trend_rows(workspace_id: str | None, limit: int) -> list[dict[str, Any]]:
    from sqlalchemy import text

    from shared.db import get_session_factory

    async with get_session_factory()() as session:
        result = await session.execute(
            text(
                """
                SELECT
                    id, workspace_id, cluster_key, pipeline, title, insight, opportunity,
                    time_horizon, burst_score, coherence, novelty, source_diversity_score,
                    freshness_score, evidence_strength_score, velocity_score, acceleration_score,
                    baseline_rate, current_rate, change_point_count, change_point_strength,
                    has_recent_change_point, signal_score, signal_stage, doc_count, source_count,
                    doc_ids, semantic_cluster_ids, keywords, explainability, detected_at
                FROM trend_clusters
                WHERE (CAST(:workspace_id AS text) IS NULL OR workspace_id = CAST(:workspace_id AS text))
                ORDER BY detected_at DESC
                LIMIT :limit
                """
            ),
            {"workspace_id": workspace_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]


async def sync_trend_clusters_to_qdrant(
    *,
    workspace_id: str | None = None,
    limit: int = 1000,
    dry_run: bool = False,
) -> dict[str, Any]:
    from worker.integrations.qdrant_client import QdrantFrontierClient
    from worker.services.semantic_clustering import _centroid, _trend_cluster_index_payload

    rows = await _trend_rows(workspace_id, limit)
    qdrant = QdrantFrontierClient()
    try:
        doc_ids = sorted({str(doc_id) for row in rows for doc_id in _as_list(row.get("doc_ids"))})
        vectors_by_doc: dict[str, list[float]] = {}
        for chunk in _chunks(doc_ids, 256):
            for document in await qdrant.fetch_documents(chunk):
                payload = document.get("payload") or {}
                post_id = str(payload.get("post_id") or payload.get("doc_id") or "")
                vector = document.get("vector") or []
                if post_id and vector:
                    vectors_by_doc[post_id] = vector

        points: list[dict[str, Any]] = []
        skipped: list[str] = []
        for row in rows:
            row_doc_ids = [str(doc_id) for doc_id in _as_list(row.get("doc_ids"))]
            vectors = [vectors_by_doc[doc_id] for doc_id in row_doc_ids if doc_id in vectors_by_doc]
            if not vectors:
                skipped.append(str(row["id"]))
                continue
            explainability = _as_dict(row.get("explainability"))
            time_span = _as_dict(explainability.get("time_span"))
            item = {
                "signal_id": row["id"],
                "signal_key": row["cluster_key"],
                "workspace_id": row["workspace_id"],
                "title": row["title"],
                "signal_stage": row["signal_stage"],
                "signal_score": row["signal_score"],
                "burst_score": row["burst_score"],
                "coherence_score": row["coherence"],
                "novelty_score": row["novelty"],
                "source_diversity_score": row["source_diversity_score"],
                "freshness_score": row["freshness_score"],
                "evidence_strength_score": row["evidence_strength_score"],
                "velocity_score": row["velocity_score"],
                "acceleration_score": row["acceleration_score"],
                "baseline_rate": row["baseline_rate"],
                "current_rate": row["current_rate"],
                "change_point_count": row["change_point_count"],
                "change_point_strength": row["change_point_strength"],
                "has_recent_change_point": row["has_recent_change_point"],
                "doc_ids": row_doc_ids,
                "semantic_cluster_ids": _as_list(row.get("semantic_cluster_ids")),
                "keywords": _as_list(row.get("keywords")),
                "source_count": row["source_count"],
                "evidence": [],
                "first_seen_at": _parse_dt(time_span.get("first_seen_at")),
                "last_seen_at": _parse_dt(time_span.get("last_seen_at")),
                "detected_at": row["detected_at"],
            }
            points.append(
                {
                    "cluster_id": row["id"],
                    "dense_vector": _centroid(vectors),
                    "payload": _trend_cluster_index_payload(
                        str(row["id"]),
                        "manual-qdrant-backfill",
                        item,
                    ),
                }
            )

        indexed = 0 if dry_run else await qdrant.upsert_trend_clusters(points)
        return {
            "workspace_id": workspace_id,
            "rows": len(rows),
            "points_prepared": len(points),
            "indexed": indexed,
            "dry_run": dry_run,
            "skipped_without_vectors": skipped,
        }
    finally:
        await qdrant.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill trend_clusters from PostgreSQL to Qdrant."
    )
    parser.add_argument("--workspace", dest="workspace_id", default=None)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(
        sync_trend_clusters_to_qdrant(
            workspace_id=args.workspace_id,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    )
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
