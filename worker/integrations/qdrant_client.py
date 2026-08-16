"""Qdrant integration — hybrid search and upsert."""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    CreateAlias,
    CreateAliasOperation,
    Distance,
    FieldCondition,
    Filter,
    Fusion,
    FusionQuery,
    HnswConfigDiff,
    MatchAny,
    MatchValue,
    PayloadSchemaType,
    PointIdsList,
    PointStruct,
    Prefetch,
    QueryRequest,
    Range,
    SparseIndexParams,
    SparseVectorParams,
    VectorParams,
)

try:
    from qdrant_client.models import DatetimeRange
except ImportError:  # pragma: no cover - compatibility with older qdrant-client builds
    DatetimeRange = Range

from shared.embedding_models import embedding_profile, qdrant_collection_name_for_embedding
from shared.config import get_settings
from shared.qdrant_sparse import sparse_encode as _sparse_encode

logger = logging.getLogger(__name__)

_FRONTIER_PAYLOAD_INDEXES = [
    ("workspace_id", PayloadSchemaType.KEYWORD),
    ("source_id", PayloadSchemaType.KEYWORD),
    ("category", PayloadSchemaType.KEYWORD),
    ("lang", PayloadSchemaType.KEYWORD),
    ("valence", PayloadSchemaType.KEYWORD),
    ("signal_type", PayloadSchemaType.KEYWORD),
    ("source_region", PayloadSchemaType.KEYWORD),
    ("market_scope", PayloadSchemaType.KEYWORD),
    ("published_at", PayloadSchemaType.DATETIME),
    ("relevance_score", PayloadSchemaType.FLOAT),
    ("embedding_version", PayloadSchemaType.KEYWORD),
]

_TREND_PAYLOAD_INDEXES = [
    ("workspace_id", PayloadSchemaType.KEYWORD),
    ("pipeline", PayloadSchemaType.KEYWORD),
    ("signal_stage", PayloadSchemaType.KEYWORD),
    ("keywords", PayloadSchemaType.KEYWORD),
    ("burst_score", PayloadSchemaType.FLOAT),
    ("signal_score", PayloadSchemaType.FLOAT),
    ("detected_at", PayloadSchemaType.DATETIME),
    ("doc_count", PayloadSchemaType.INTEGER),
    ("source_count", PayloadSchemaType.INTEGER),
    ("embedding_version", PayloadSchemaType.KEYWORD),
]

# Корпус автора (ТЗ B). Ни workspace_id, ни embedding_version здесь нет намеренно:
# коллекция живёт вне общего поиска, _build_payload_filter к ней не применяется,
# и версия эмбеддинга зашита в само имя коллекции.
_OWN_CORPUS_PAYLOAD_INDEXES = [
    ("doc_kind", PayloadSchemaType.KEYWORD),
    ("path", PayloadSchemaType.KEYWORD),
    ("indexed_at", PayloadSchemaType.DATETIME),
]

# Страховка от пустого QDRANT_OWN_CORPUS_COLLECTION: с пустой базой
# qdrant_collection_name_for_embedding подставляет "frontier_docs", и личный корпус
# уехал бы в общий индекс.
_DEFAULT_OWN_CORPUS_COLLECTION = "own_corpus"

# Max doc_ids per `retrieve` in fetch_documents. Dense vectors come back inline, so
# one call covering a whole 14-day archive window returned a single ~185 MB body
# (measured on workspace disruption: 5.6k docs × 2560 dims), and its parsed form
# briefly coexists with the copies the caller makes. Chunking bounds that spike;
# callers already treat the result as unordered and tolerate missing points.
_FETCH_DOCUMENTS_CHUNK = 512


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
    embedding_version: str | None = None,
    lang: str | None = None,
    valence: str | list[str] | None = None,
    signal_type: str | list[str] | None = None,
    source_region: str | None = None,
    days_back: int | None = None,
) -> Filter:
    must = [FieldCondition(key="workspace_id", match=MatchValue(value=workspace_id))]
    embedding_version_text = str(embedding_version or "").strip()
    if embedding_version_text:
        must.append(
            FieldCondition(
                key="embedding_version",
                match=MatchValue(value=embedding_version_text),
            )
        )
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
    embedding_version: str | None = None,
    pipeline: str | None = None,
    signal_stage: str | list[str] | None = None,
    days_back: int | None = None,
) -> Filter:
    must = [FieldCondition(key="workspace_id", match=MatchValue(value=workspace_id))]
    embedding_version_text = str(embedding_version or "").strip()
    if embedding_version_text:
        must.append(
            FieldCondition(
                key="embedding_version",
                match=MatchValue(value=embedding_version_text),
            )
        )
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
        self.settings = settings
        self.client = AsyncQdrantClient(url=settings.qdrant_url, timeout=30)
        self.collection_base = settings.qdrant_collection
        self.collection_alias = str(settings.qdrant_collection_alias or "").strip()
        self.collection = self.collection_alias or self.collection_base
        self.versioned_collection = qdrant_collection_name_for_embedding(
            self.collection_base,
            settings.gigachat_embeddings_model,
        )
        self.trends_collection_base = settings.qdrant_trends_collection
        self.trends_collection_alias = str(settings.qdrant_trends_collection_alias or "").strip()
        self.trends_collection = self.trends_collection_alias or self.trends_collection_base
        self.versioned_trends_collection = qdrant_collection_name_for_embedding(
            self.trends_collection_base,
            settings.gigachat_embeddings_model,
        )
        # Корпус автора (ТЗ B). Алиаса нет намеренно: базовое имя не создаётся вообще,
        # всегда используется версионированное — смена модели эмбеддингов не должна
        # молча оставить own_stake считаться по коллекции с другой размерностью.
        self.own_corpus_collection_base = (
            str(getattr(settings, "qdrant_own_corpus_collection", "") or "").strip()
            or _DEFAULT_OWN_CORPUS_COLLECTION
        )
        self.own_corpus_collection = qdrant_collection_name_for_embedding(
            self.own_corpus_collection_base,
            settings.gigachat_embeddings_model,
        )
        self.embedding_version = str(settings.gigachat_embeddings_model or "").strip()
        self._documents_runtime_ready = False
        self._trends_runtime_ready = False
        self._own_corpus_runtime_ready = False
        self._documents_schema_checked = False
        self._trends_schema_checked = False
        self._own_corpus_schema_checked = False

    def _effective_embedding_version(self, embedding_version: str | None) -> str | None:
        if not self.settings.qdrant_filter_embedding_version:
            return None
        if embedding_version is not None:
            text = str(embedding_version).strip()
            return text or None
        return self.embedding_version or None

    @staticmethod
    def _distance_enum(distance_name: str) -> Distance:
        normalized = str(distance_name or "cosine").strip().lower()
        return {
            "cosine": Distance.COSINE,
            "dot": Distance.DOT,
            "euclid": Distance.EUCLID,
            "euclidean": Distance.EUCLID,
            "manhattan": Distance.MANHATTAN,
        }.get(normalized, Distance.COSINE)

    def _document_seed_collection(self) -> str:
        return self.versioned_collection if self.collection_alias else self.collection

    def _trend_seed_collection(self) -> str:
        return self.versioned_trends_collection if self.trends_collection_alias else self.trends_collection

    def _document_vectors_config(self) -> dict[str, VectorParams]:
        size, distance = self._expected_dense_schema(self.embedding_version)
        return {
            "dense": VectorParams(
                size=int(size or self.settings.embed_dim),
                distance=self._distance_enum(distance),
                hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
            )
        }

    def _trend_vectors_config(self) -> dict[str, VectorParams]:
        size, distance = self._expected_dense_schema(self.embedding_version)
        return {
            "dense": VectorParams(
                size=int(size or self.settings.embed_dim),
                distance=self._distance_enum(distance),
                hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
            )
        }

    def _own_corpus_vectors_config(self) -> dict[str, VectorParams]:
        size, distance = self._expected_dense_schema(self.embedding_version)
        return {
            "dense": VectorParams(
                size=int(size or self.settings.embed_dim),
                distance=self._distance_enum(distance),
                hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
            )
        }

    def _document_sparse_vectors_config(self) -> dict[str, SparseVectorParams] | None:
        if not self.settings.sparse_vectors_enabled:
            return None
        return {
            "sparse": SparseVectorParams(index=SparseIndexParams(on_disk=False))
        }

    async def _ensure_payload_indexes(
        self,
        collection_name: str,
        indexes: list[tuple[str, PayloadSchemaType]],
    ) -> None:
        for field_name, schema_type in indexes:
            try:
                await self.client.create_payload_index(
                    collection_name=collection_name,
                    field_name=field_name,
                    field_schema=schema_type,
                )
            except Exception as exc:
                message = str(exc).lower()
                if "already exists" not in message and "already has" not in message:
                    logger.warning(
                        "qdrant_payload_index_failed",
                        extra={
                            "collection_name": collection_name,
                            "field_name": field_name,
                            "error": str(exc),
                        },
                    )

    async def _ensure_collection_exists(
        self,
        *,
        collection_name: str,
        vectors_config: dict[str, VectorParams],
        sparse_vectors_config: dict[str, SparseVectorParams] | None = None,
        payload_indexes: list[tuple[str, PayloadSchemaType]] | None = None,
    ) -> None:
        if await self.client.collection_exists(collection_name):
            return
        await self.client.create_collection(
            collection_name=collection_name,
            vectors_config=vectors_config,
            sparse_vectors_config=sparse_vectors_config,
            on_disk_payload=False,
        )
        if payload_indexes:
            await self._ensure_payload_indexes(collection_name, payload_indexes)

    async def _alias_target(self, alias_name: str) -> str | None:
        if not alias_name:
            return None
        aliases = await self.client.get_aliases()
        for alias in getattr(aliases, "aliases", []) or []:
            if str(getattr(alias, "alias_name", "")).strip() == alias_name:
                collection_name = str(getattr(alias, "collection_name", "")).strip()
                return collection_name or None
        return None

    async def _ensure_alias_binding(self, *, alias_name: str, target_collection: str) -> None:
        if not alias_name:
            return
        current_target = await self._alias_target(alias_name)
        if current_target == target_collection:
            return
        if current_target:
            logger.info(
                "qdrant_alias_preserved",
                extra={
                    "alias_name": alias_name,
                    "current_target": current_target,
                    "prepared_target": target_collection,
                },
            )
            return
        await self.client.update_collection_aliases(
            change_aliases_operations=[
                CreateAliasOperation(
                    create_alias=CreateAlias(
                        collection_name=target_collection,
                        alias_name=alias_name,
                    )
                )
            ]
        )

    async def _ensure_documents_runtime_ready(self) -> None:
        if self._documents_runtime_ready:
            return
        seed_collection = self._document_seed_collection()
        await self._ensure_collection_exists(
            collection_name=seed_collection,
            vectors_config=self._document_vectors_config(),
            sparse_vectors_config=self._document_sparse_vectors_config(),
            payload_indexes=_FRONTIER_PAYLOAD_INDEXES,
        )
        if self.collection_alias:
            await self._ensure_alias_binding(
                alias_name=self.collection_alias,
                target_collection=seed_collection,
            )
        self._documents_runtime_ready = True

    async def _ensure_trends_runtime_ready(self) -> None:
        if self._trends_runtime_ready:
            return
        seed_collection = self._trend_seed_collection()
        await self._ensure_collection_exists(
            collection_name=seed_collection,
            vectors_config=self._trend_vectors_config(),
            payload_indexes=_TREND_PAYLOAD_INDEXES,
        )
        if self.trends_collection_alias:
            await self._ensure_alias_binding(
                alias_name=self.trends_collection_alias,
                target_collection=seed_collection,
            )
        self._trends_runtime_ready = True

    async def _ensure_own_corpus_ready(self) -> None:
        """Создать коллекцию корпуса автора.

        Только dense: корпус запрашивается вектором карточки, а не текстом, BM25-ветка
        ему не нужна. Алиас не создаётся — цель всегда версионированное имя.
        Вызывается только с пути записи (индексация корпуса), не с пути поиска.
        """
        if self._own_corpus_runtime_ready:
            return
        await self._ensure_collection_exists(
            collection_name=self.own_corpus_collection,
            vectors_config=self._own_corpus_vectors_config(),
            payload_indexes=_OWN_CORPUS_PAYLOAD_INDEXES,
        )
        self._own_corpus_runtime_ready = True

    async def _own_corpus_exists(self) -> bool:
        """Проверка существования коллекции без её создания.

        Читающий путь не должен создавать коллекцию: отсутствие корпуса — это 0.0
        на карточке, а не запись в Qdrant с горячего пути поиска. Положительный ответ
        кэшируется, отрицательный — нет: корпус появляется внешним скриптом, и
        долгоживущий процесс MCP обязан это увидеть без рестарта.
        """
        if self._own_corpus_runtime_ready:
            return True
        try:
            exists = bool(await self.client.collection_exists(self.own_corpus_collection))
        except Exception as exc:
            logger.warning(
                "qdrant_own_corpus_exists_check_failed",
                extra={
                    "collection_name": self.own_corpus_collection,
                    "error": str(exc),
                },
            )
            return False
        if exists:
            self._own_corpus_runtime_ready = True
        return exists

    @staticmethod
    def _expected_dense_schema(model: str) -> tuple[int | None, str]:
        profile = embedding_profile(model)
        size = profile.get("dimension")
        distance = str(profile.get("distance_metric") or "cosine").strip().lower()
        return (int(size) if size else None, distance)

    @staticmethod
    def _distance_name(value: Any) -> str:
        return str(getattr(value, "value", value) or "").strip().lower()

    @classmethod
    def _schema_mismatch_reason(
        cls,
        actual_size: int | None,
        actual_distance: str,
        expected_size: int | None,
        expected_distance: str,
    ) -> str:
        if expected_size is not None and actual_size != expected_size:
            return f"vector_size_mismatch actual={actual_size} expected={expected_size}"
        if expected_distance and actual_distance and actual_distance != expected_distance:
            return (
                f"distance_metric_mismatch actual={actual_distance} "
                f"expected={expected_distance}"
            )
        return ""

    @classmethod
    def _extract_dense_schema(cls, collection_info: Any) -> tuple[int | None, str]:
        vectors = getattr(
            getattr(getattr(collection_info, "config", None), "params", None),
            "vectors",
            None,
        )
        if isinstance(vectors, dict):
            dense = vectors.get("dense")
            return (
                int(getattr(dense, "size", 0) or 0) or None,
                cls._distance_name(getattr(dense, "distance", "")),
            )
        if vectors is not None:
            return (
                int(getattr(vectors, "size", 0) or 0) or None,
                cls._distance_name(getattr(vectors, "distance", "")),
            )
        return None, ""

    async def _ensure_collection_schema(
        self,
        *,
        collection_name: str,
        expected_model: str,
        checked_attr: str,
    ) -> None:
        if not self.settings.qdrant_enforce_collection_schema:
            return
        if getattr(self, checked_attr, False):
            return
        info = await self.client.get_collection(collection_name=collection_name)
        actual_size, actual_distance = self._extract_dense_schema(info)
        expected_size, expected_distance = self._expected_dense_schema(expected_model)
        mismatch = self._schema_mismatch_reason(
            actual_size,
            actual_distance,
            expected_size,
            expected_distance,
        )
        if mismatch:
            raise RuntimeError(
                f"qdrant_collection_schema_mismatch collection={collection_name} "
                f"embedding_version={expected_model} {mismatch}"
            )
        setattr(self, checked_attr, True)

    async def upsert_document(
        self,
        doc_id: str,
        dense_vector: list[float],
        payload: dict[str, Any],
        text: str = "",
    ) -> None:
        await self._ensure_documents_runtime_ready()
        await self._ensure_collection_schema(
            collection_name=self.collection,
            expected_model=self.embedding_version,
            checked_attr="_documents_schema_checked",
        )
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
        await self._ensure_documents_runtime_ready()
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id))
        await self.client.delete(
            collection_name=self.collection,
            points_selector=PointIdsList(points=[point_id]),
            wait=True,
        )

    async def upsert_trend_clusters(self, clusters: list[dict[str, Any]]) -> int:
        """Upsert trend-cluster vectors into the secondary Qdrant index."""
        await self._ensure_trends_runtime_ready()
        await self._ensure_collection_schema(
            collection_name=self.trends_collection,
            expected_model=self.embedding_version,
            checked_attr="_trends_schema_checked",
        )
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
        """Return vectors and payloads for a batch of doc_ids (order not preserved).

        Retrieval is chunked at _FETCH_DOCUMENTS_CHUNK so a large doc_id list cannot
        turn into one enormous response body. Points Qdrant does not know are skipped
        silently, as before.
        """
        if not doc_ids:
            return []
        await self._ensure_documents_runtime_ready()
        documents = []
        for start in range(0, len(doc_ids), _FETCH_DOCUMENTS_CHUNK):
            chunk = doc_ids[start : start + _FETCH_DOCUMENTS_CHUNK]
            records = await self.client.retrieve(
                collection_name=self.collection,
                ids=[str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)) for doc_id in chunk],
                with_payload=True,
                with_vectors=True,
            )
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

    async def upsert_own_corpus_chunk(
        self,
        *,
        path: str,
        chunk_idx: int,
        dense_vector: list[float],
        doc_kind: str,
        indexed_at: str | None = None,
        extra_payload: dict[str, Any] | None = None,
    ) -> str:
        """Записать один чанк корпуса автора, вернуть id точки.

        Идемпотентность: id = uuid5(NAMESPACE_URL, "own:{path}:{chunk_idx}") — повторный
        прогон индексации перезаписывает точку, а не плодит дубли.

        Текст чанка в payload не кладётся: коллекция личная, а для отладки хватает
        path + chunk_idx. Если вызывающему нужен ещё один служебный ключ — extra_payload,
        канонические ключи его перекрывают.
        """
        normalized_path = str(path or "").strip()
        if not normalized_path:
            raise ValueError("own_corpus_chunk_requires_path")
        if not dense_vector:
            raise ValueError("own_corpus_chunk_requires_dense_vector")
        await self._ensure_own_corpus_ready()
        await self._ensure_collection_schema(
            collection_name=self.own_corpus_collection,
            expected_model=self.embedding_version,
            checked_attr="_own_corpus_schema_checked",
        )
        point_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"own:{normalized_path}:{int(chunk_idx)}")
        )
        payload: dict[str, Any] = {
            **(extra_payload or {}),
            "doc_kind": str(doc_kind or "").strip().lower(),
            "path": normalized_path,
            "chunk_idx": int(chunk_idx),
            "indexed_at": indexed_at or datetime.now(UTC).isoformat(),
        }
        await self.client.upsert(
            collection_name=self.own_corpus_collection,
            points=[
                PointStruct(
                    id=point_id,
                    vector={"dense": list(dense_vector)},
                    payload=payload,
                )
            ],
            wait=True,
        )
        return point_id

    async def own_corpus_size(self) -> int:
        """Число точек в корпусе автора; 0, если коллекции ещё нет.

        Пока корпус в десятки чанков, own_stake читается как «есть / нет замера»,
        а не как величина — размер корпуса рядом с числом это единственная честная
        подпись (риск 1 ТЗ B).
        """
        if not await self._own_corpus_exists():
            return 0
        try:
            result = await self.client.count(
                collection_name=self.own_corpus_collection,
                exact=False,
            )
        except Exception as exc:
            logger.warning(
                "qdrant_own_corpus_count_failed",
                extra={
                    "collection_name": self.own_corpus_collection,
                    "error": str(exc),
                },
            )
            return 0
        return int(getattr(result, "count", 0) or 0)

    async def own_stake_scores(
        self,
        vectors: dict[str, list[float]],
        *,
        top_k: int,
    ) -> dict[str, float] | None:
        """doc_id → максимальный косинус к корпусу автора; None — замер не состоялся.

        Максимум, а не среднее: вопрос в том, есть ли у автора хотя бы один свой замер
        по теме, а не размазана ли тема по всему корпусу.

        Число возвращается ОТДЕЛЬНЫМ словарём и никогда не подмешивается в score —
        порядок выдачи с включённым own_stake обязан совпадать с выключенным.

        Контракт отказа. 0.0 значит «корпус проверен, своего замера по теме нет», и
        писать его можно только там, где корпус действительно проверен: пустая
        коллекция и отсутствующая коллекция (обе видны вызывающему как
        own_corpus_size() == 0). Недоступный Qdrant и невыровненный batch-ответ дают
        None — измерения не было, и ноль здесь был бы выдуманным числом, которое
        поиск отдаст как факт, а разметка обратной связи запишет в card_feedback
        навсегда. Исключение не бросается: вторая ось не имеет права ронять поиск.
        Doc_id без пригодного вектора в ответе просто отсутствует — тоже «не измеряли».
        """
        scores: dict[str, float] = {}
        if not vectors:
            return scores
        # float() обязателен: QueryRequest валидирует вектор в strict-режиме и
        # отвергает int'ы, которые может дать сериализация чужого клиента.
        usable: dict[str, list[float]] = {
            str(doc_id): [float(value) for value in vector]
            for doc_id, vector in vectors.items()
            if vector
        }
        if not usable:
            return scores
        if not await self._own_corpus_exists():
            # Коллекции нет — это проверенный ноль, а не неизвестность:
            # own_corpus_size() вернёт 0 и снимет двусмысленность на карточке.
            return {doc_id: 0.0 for doc_id in usable}

        doc_ids = list(usable.keys())
        limit = max(1, int(top_k))
        requests = [
            QueryRequest(
                query=usable[doc_id],
                using="dense",
                limit=limit,
                with_payload=False,
            )
            for doc_id in doc_ids
        ]
        try:
            responses = await self.client.query_batch_points(
                collection_name=self.own_corpus_collection,
                requests=requests,
            )
        except Exception as exc:
            logger.warning(
                "qdrant_own_stake_batch_failed",
                extra={
                    "collection_name": self.own_corpus_collection,
                    "batch_size": len(requests),
                    "error": str(exc),
                },
            )
            return None

        responses = list(responses or [])
        if len(responses) != len(doc_ids):
            # Ответ не выровнен с запросом — доверять позиционному соответствию
            # нечему, и «недостающие» doc_id остались бы нулями, которых никто
            # не измерял. Деградируем целиком.
            logger.warning(
                "qdrant_own_stake_batch_size_mismatch",
                extra={
                    "collection_name": self.own_corpus_collection,
                    "requested": len(doc_ids),
                    "received": len(responses),
                },
            )
            return None
        for doc_id, response in zip(doc_ids, responses):
            best = 0.0
            for point in getattr(response, "points", None) or []:
                best = max(best, float(getattr(point, "score", 0.0) or 0.0))
            scores[doc_id] = round(max(0.0, min(1.0, best)), 4)
        return scores

    async def close(self) -> None:
        await self.client.close()

    async def hybrid_search(
        self,
        query_vector: list[float],
        workspace_id: str,
        limit: int = 10,
        query_text: str = "",
        *,
        embedding_version: str | None = None,
        lang: str | None = None,
        days_back: int | None = None,
        valence: str | list[str] | None = None,
        signal_type: str | list[str] | None = None,
        source_region: str | None = None,
    ) -> list[dict]:
        await self._ensure_documents_runtime_ready()
        payload_filter = _build_payload_filter(
            workspace_id,
            embedding_version=self._effective_embedding_version(embedding_version),
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
        embedding_version: str | None = None,
        pipeline: str | None = "stable",
        signal_stage: str | list[str] | None = None,
        days_back: int | None = None,
    ) -> list[dict]:
        await self._ensure_trends_runtime_ready()
        payload_filter = _build_trend_filter(
            workspace_id,
            embedding_version=self._effective_embedding_version(embedding_version),
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
