"""Qdrant integration — hybrid search and upsert."""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    FieldCondition,
    Filter,
    Fusion,
    FusionQuery,
    MatchAny,
    MatchValue,
    PointIdsList,
    PointStruct,
    Prefetch,
    Range,
)

try:
    from qdrant_client.models import DatetimeRange
except ImportError:  # pragma: no cover - compatibility with older qdrant-client builds
    DatetimeRange = Range

from shared.config import get_settings
from shared.qdrant_sparse import sparse_encode as _sparse_encode

logger = logging.getLogger(__name__)


def _freshness_boost(published_at: str | None) -> float:
    if not published_at:
        return 0.0
    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        age_hours = max((datetime.now(UTC) - dt).total_seconds() / 3600.0, 0.0)
    except Exception:
        return 0.0
    if age_hours <= 24:
        return 1.0
    if age_hours <= 72:
        return 0.75
    if age_hours <= 168:
        return 0.45
    return 0.15


def _final_rank_score(raw_score: float, payload: dict[str, Any]) -> tuple[float, dict[str, float]]:
    source_score = max(0.0, min(1.0, float(payload.get("source_score") or 0.0)))
    freshness = _freshness_boost(payload.get("published_at"))
    final = raw_score * (1.0 + source_score * 0.20 + freshness * 0.08)
    return final, {
        "semantic": round(raw_score, 4),
        "source_score": round(source_score, 4),
        "freshness": round(freshness, 4),
    }


def _match_condition(key: str, value: str | list[str] | None) -> FieldCondition | None:
    if value is None:
        return None
    if isinstance(value, list):
        values = [str(item).strip().lower() for item in value if str(item).strip()]
        if not values:
            return None
        if len(values) == 1:
            return FieldCondition(key=key, match=MatchValue(value=values[0]))
        return FieldCondition(key=key, match=MatchAny(any=values))
    text = str(value).strip().lower()
    if not text:
        return None
    return FieldCondition(key=key, match=MatchValue(value=text))


def _build_payload_filter(
    workspace_id: str,
    *,
    lang: str | None = None,
    valence: str | list[str] | None = None,
    signal_type: str | list[str] | None = None,
    source_region: str | None = None,
    days_back: int | None = None,
) -> Filter:
    must = [FieldCondition(key="workspace_id", match=MatchValue(value=workspace_id))]
    for condition in (
        _match_condition("lang", lang),
        _match_condition("valence", valence),
        _match_condition("signal_type", signal_type),
        _match_condition("source_region", source_region),
    ):
        if condition is not None:
            must.append(condition)
    if days_back:
        must.append(
            FieldCondition(
                key="published_at",
                range=DatetimeRange(
                    gte=(datetime.now(UTC) - timedelta(days=days_back)).isoformat()
                ),
            )
        )
    return Filter(must=must)


def _build_trend_filter(
    workspace_id: str,
    *,
    pipeline: str | None = None,
    signal_stage: str | list[str] | None = None,
    days_back: int | None = None,
) -> Filter:
    must = [FieldCondition(key="workspace_id", match=MatchValue(value=workspace_id))]
    for condition in (
        _match_condition("pipeline", pipeline),
        _match_condition("signal_stage", signal_stage),
    ):
        if condition is not None:
            must.append(condition)
    if days_back:
        must.append(
            FieldCondition(
                key="detected_at",
                range=DatetimeRange(
                    gte=(datetime.now(UTC) - timedelta(days=days_back)).isoformat()
                ),
            )
        )
    return Filter(must=must)


def _trend_rank_score(raw_score: float, payload: dict[str, Any]) -> tuple[float, dict[str, float]]:
    signal_score = max(0.0, min(1.0, float(payload.get("signal_score") or 0.0)))
    burst_score = max(0.0, min(1.0, float(payload.get("burst_score") or 0.0)))
    source_count = max(0.0, min(1.0, float(payload.get("source_count") or 0.0) / 6.0))
    final = raw_score * (1.0 + signal_score * 0.15 + burst_score * 0.10 + source_count * 0.05)
    return final, {
        "semantic": round(raw_score, 4),
        "signal_score": round(signal_score, 4),
        "burst_score": round(burst_score, 4),
        "source_diversity_proxy": round(source_count, 4),
    }


class QdrantFrontierClient:
    def __init__(self):
        settings = get_settings()
        self.client = AsyncQdrantClient(url=settings.qdrant_url, timeout=30)
        self.collection = settings.qdrant_collection
        self.trends_collection = settings.qdrant_trends_collection

    async def upsert_document(
        self,
        doc_id: str,
        dense_vector: list[float],
        payload: dict[str, Any],
        text: str = "",
    ) -> None:
        sparse = _sparse_encode(text) if text else None

        vectors: dict = {"dense": dense_vector}
        if sparse:
            vectors["sparse"] = sparse

        point = PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)),
            vector=vectors,
            payload={**payload, "doc_id": doc_id},
        )
        await self.client.upsert(
            collection_name=self.collection,
            points=[point],
            wait=True,
        )

    async def delete_document(self, doc_id: str) -> None:
        """Delete a point from Qdrant by doc_id (same hashing as upsert)."""
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id))
        await self.client.delete(
            collection_name=self.collection,
            points_selector=PointIdsList(points=[point_id]),
            wait=True,
        )

    async def upsert_trend_clusters(self, clusters: list[dict[str, Any]]) -> int:
        """Upsert trend-cluster vectors into the secondary Qdrant index."""
        points: list[PointStruct] = []
        for cluster in clusters:
            cluster_id = str(cluster.get("cluster_id") or "").strip()
            dense_vector = cluster.get("dense_vector") or []
            if not cluster_id or not dense_vector:
                logger.warning("Skipping trend cluster Qdrant upsert without id/vector")
                continue
            payload = dict(cluster.get("payload") or {})
            points.append(
                PointStruct(
                    id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"trend_cluster:{cluster_id}")),
                    vector={"dense": dense_vector},
                    payload={
                        **payload,
                        "cluster_id": cluster_id,
                        "point_kind": "trend_cluster",
                    },
                )
            )

        if not points:
            return 0

        await self.client.upsert(
            collection_name=self.trends_collection,
            points=points,
            wait=True,
        )
        return len(points)

    async def fetch_documents(self, doc_ids: list[str]) -> list[dict[str, Any]]:
        """Return vectors and payloads for a batch of doc_ids."""
        if not doc_ids:
            return []
        point_ids = [str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)) for doc_id in doc_ids]
        records = await self.client.retrieve(
            collection_name=self.collection,
            ids=point_ids,
            with_payload=True,
            with_vectors=True,
        )
        documents = []
        for record in records:
            dense = None
            vectors = getattr(record, "vector", None)
            if isinstance(vectors, dict):
                dense = vectors.get("dense")
            documents.append(
                {
                    "id": str(record.id),
                    "payload": getattr(record, "payload", {}) or {},
                    "vector": dense,
                }
            )
        return documents

    async def close(self) -> None:
        await self.client.close()

    async def hybrid_search(
        self,
        query_vector: list[float],
        workspace_id: str,
        limit: int = 10,
        query_text: str = "",
        *,
        lang: str | None = None,
        days_back: int | None = None,
        valence: str | list[str] | None = None,
        signal_type: str | list[str] | None = None,
        source_region: str | None = None,
    ) -> list[dict]:
        payload_filter = _build_payload_filter(
            workspace_id,
            lang=lang,
            days_back=days_back,
            valence=valence,
            signal_type=signal_type,
            source_region=source_region,
        )

        sparse = _sparse_encode(query_text) if query_text else None

        if sparse:
            results = await self.client.query_points(
                collection_name=self.collection,
                prefetch=[
                    Prefetch(
                        query=query_vector,
                        using="dense",
                        limit=limit * 2,
                        filter=payload_filter,
                    ),
                    Prefetch(
                        query=sparse,
                        using="sparse",
                        limit=limit * 2,
                        filter=payload_filter,
                    ),
                ],
                query=FusionQuery(fusion=Fusion.RRF),
                limit=limit,
                with_payload=True,
            )
            points = results.points
        else:
            results = await self.client.query_points(
                collection_name=self.collection,
                query=query_vector,
                using="dense",
                query_filter=payload_filter,
                limit=limit,
                with_payload=True,
            )
            points = results.points

        reranked = []
        for r in points:
            payload = r.payload or {}
            final_score, breakdown = _final_rank_score(float(r.score or 0.0), payload)
            reranked.append(
                {
                    "id": str(r.id),
                    "score": final_score,
                    "raw_score": r.score,
                    "payload": payload,
                    "score_breakdown": breakdown,
                }
            )
        reranked.sort(key=lambda item: item["score"], reverse=True)
        return reranked

    async def search_trend_clusters(
        self,
        query_vector: list[float],
        workspace_id: str,
        limit: int = 10,
        *,
        pipeline: str | None = "stable",
        signal_stage: str | list[str] | None = None,
        days_back: int | None = None,
    ) -> list[dict]:
        payload_filter = _build_trend_filter(
            workspace_id,
            pipeline=pipeline,
            signal_stage=signal_stage,
            days_back=days_back,
        )
        results = await self.client.query_points(
            collection_name=self.trends_collection,
            query=query_vector,
            using="dense",
            query_filter=payload_filter,
            limit=limit,
            with_payload=True,
        )
        reranked = []
        for point in results.points:
            payload = point.payload or {}
            final_score, breakdown = _trend_rank_score(float(point.score or 0.0), payload)
            reranked.append(
                {
                    "id": str(point.id),
                    "score": final_score,
                    "raw_score": point.score,
                    "payload": payload,
                    "score_breakdown": breakdown,
                }
            )
        reranked.sort(key=lambda item: item["score"], reverse=True)
        return reranked
