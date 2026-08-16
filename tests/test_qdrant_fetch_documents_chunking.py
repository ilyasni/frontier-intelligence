"""`fetch_documents` must split its retrieve calls into bounded chunks.

Dense vectors come back inline, so one retrieve covering a whole archive window
returned a single ~185 MB body (measured on workspace disruption), and its parsed form
briefly coexists with the copies the caller makes. These tests pin the chunking and the
contract the callers already rely on: every id is asked for exactly once, unknown
points are skipped rather than invented, and the order is not promised.
"""
import uuid
from types import SimpleNamespace
from typing import Any

from worker.integrations import qdrant_client as qdrant_module
from worker.integrations.qdrant_client import QdrantFrontierClient


class _RecordingQdrant:
    """Stands in for AsyncQdrantClient and records how retrieve was batched."""

    def __init__(self, known_point_ids: set[str] | None = None) -> None:
        self.known_point_ids = known_point_ids
        self.batches: list[list[str]] = []

    async def retrieve(
        self,
        *,
        collection_name: str,
        ids: list[str],
        with_payload: bool,
        with_vectors: bool,
    ) -> list[Any]:
        self.batches.append(list(ids))
        return [
            SimpleNamespace(id=point_id, payload={"post_id": point_id}, vector={"dense": [0.5]})
            for point_id in ids
            if self.known_point_ids is None or point_id in self.known_point_ids
        ]


def _client(fake: _RecordingQdrant) -> QdrantFrontierClient:
    """A client with only the attributes fetch_documents touches — no connection."""
    client = object.__new__(QdrantFrontierClient)
    client.client = fake
    client.collection = "frontier_docs_active"
    client._documents_runtime_ready = True  # short-circuits _ensure_documents_runtime_ready
    return client


def _point_id(doc_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id))


async def test_fetch_documents_splits_into_bounded_chunks() -> None:
    fake = _RecordingQdrant()
    doc_ids = [f"doc-{index}" for index in range(1300)]

    documents = await _client(fake).fetch_documents(doc_ids)

    chunk = qdrant_module._FETCH_DOCUMENTS_CHUNK
    assert [len(batch) for batch in fake.batches] == [chunk, chunk, 1300 - 2 * chunk]
    assert max(len(batch) for batch in fake.batches) <= chunk
    assert len(documents) == 1300


async def test_fetch_documents_asks_for_every_id_exactly_once() -> None:
    fake = _RecordingQdrant()
    doc_ids = [f"doc-{index}" for index in range(1300)]

    await _client(fake).fetch_documents(doc_ids)

    asked = [point_id for batch in fake.batches for point_id in batch]
    assert asked == [_point_id(doc_id) for doc_id in doc_ids]


async def test_fetch_documents_skips_points_qdrant_does_not_know() -> None:
    doc_ids = [f"doc-{index}" for index in range(600)]
    fake = _RecordingQdrant(known_point_ids={_point_id(doc_id) for doc_id in doc_ids[:5]})

    documents = await _client(fake).fetch_documents(doc_ids)

    assert len(fake.batches) == 2  # chunking still happens for the misses
    assert len(documents) == 5


async def test_fetch_documents_returns_early_without_calling_qdrant() -> None:
    fake = _RecordingQdrant()

    assert await _client(fake).fetch_documents([]) == []
    assert fake.batches == []
