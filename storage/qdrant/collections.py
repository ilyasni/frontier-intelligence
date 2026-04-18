"""
Initialize Qdrant collections for Frontier Intelligence.
Run: python storage/qdrant/collections.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    VectorParams,
    Distance,
    SparseVectorParams,
    SparseIndexParams,
    HnswConfigDiff,
    PayloadSchemaType,
)


async def init_collections(qdrant_url: str = "http://localhost:6333"):
    client = AsyncQdrantClient(url=qdrant_url, timeout=30)

    # Main documents collection: hybrid dense + sparse
    print("Creating frontier_docs collection...")
    await client.recreate_collection(
        collection_name="frontier_docs",
        vectors_config={
            "dense": VectorParams(
                size=2560,
                distance=Distance.COSINE,
                hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
            )
        },
        sparse_vectors_config={
            "sparse": SparseVectorParams(
                index=SparseIndexParams(on_disk=False)
            )
        },
        on_disk_payload=False,
    )

    # Create payload indexes for efficient filtering
    for field, schema_type in [
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
    ]:
        await client.create_payload_index(
            collection_name="frontier_docs",
            field_name=field,
            field_schema=schema_type,
        )
    print("frontier_docs: OK")

    # Trend clusters collection
    print("Creating trend_clusters collection...")
    await client.recreate_collection(
        collection_name="trend_clusters",
        vectors_config={
            "dense": VectorParams(
                size=2560,
                distance=Distance.COSINE,
            )
        },
        on_disk_payload=False,
    )
    for field, schema_type in [
        ("workspace_id", PayloadSchemaType.KEYWORD),
        ("pipeline", PayloadSchemaType.KEYWORD),
        ("category", PayloadSchemaType.KEYWORD),
        ("burst_score", PayloadSchemaType.FLOAT),
    ]:
        await client.create_payload_index(
            collection_name="trend_clusters",
            field_name=field,
            field_schema=schema_type,
        )
    print("trend_clusters: OK")

    await client.close()
    print("Qdrant collections initialized successfully.")


if __name__ == "__main__":
    qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
    asyncio.run(init_collections(qdrant_url))
