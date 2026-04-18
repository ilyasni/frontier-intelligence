"""
Initialize all storage systems for Frontier Intelligence.
Idempotent — safe to re-run.

Usage:
    cd /opt/frontier-intelligence
    python scripts/init_storage.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from sqlalchemy import text

from shared.config import get_settings
from shared.embedding_models import expected_embedding_dim
from shared.sqlalchemy_pool import ASYNC_ENGINE_POOL_KWARGS


async def init_postgres(settings):
    from sqlalchemy.ext.asyncio import create_async_engine

    print("[1/3] Initializing PostgreSQL...")
    engine = create_async_engine(
        settings.database_url,
        echo=False,
        **ASYNC_ENGINE_POOL_KWARGS,
    )

    # Schema is loaded via docker-entrypoint-initdb.d/init.sql on first start.
    # Here we just verify connectivity and seed workspaces.
    async with engine.begin() as conn:
        await conn.execute(text("SELECT 1"))
    print("     PostgreSQL: connected")

    # Seed workspaces
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "config", "workspaces.yml"
    )
    with open(config_path) as f:
        config = yaml.safe_load(f)

    async with engine.begin() as conn:
        for ws in config.get("workspaces", []):
            import json

            await conn.execute(
                text(
                    """
                INSERT INTO workspaces (id, name, description, categories, relevance_weights,
                    design_lenses, cross_workspace_bridges, is_active, created_at, updated_at)
                VALUES (:id, :name, :description, CAST(:categories AS jsonb), CAST(:weights AS jsonb),
                    CAST(:lenses AS jsonb), CAST(:bridges AS jsonb), :is_active, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    categories = EXCLUDED.categories,
                    relevance_weights = EXCLUDED.relevance_weights,
                    design_lenses = EXCLUDED.design_lenses,
                    cross_workspace_bridges = EXCLUDED.cross_workspace_bridges,
                    is_active = EXCLUDED.is_active,
                    updated_at = NOW()
            """
                ),
                {
                    "id": ws["id"],
                    "name": ws["name"],
                    "description": ws.get("description", ""),
                    "categories": json.dumps(ws.get("categories", [])),
                    "weights": json.dumps(ws.get("relevance_weights", {})),
                    "lenses": json.dumps(ws.get("design_lenses", [])),
                    "bridges": json.dumps(ws.get("cross_workspace_bridges", [])),
                    "is_active": ws.get("is_active", True),
                },
            )
    print("     Workspaces seeded")
    await engine.dispose()


async def init_qdrant(settings):
    print("[2/3] Initializing Qdrant...")
    from qdrant_client import AsyncQdrantClient
    from qdrant_client.models import (
        Distance,
        HnswConfigDiff,
        PayloadSchemaType,
        SparseIndexParams,
        SparseVectorParams,
        VectorParams,
    )

    expected_dim = expected_embedding_dim(settings.gigachat_embeddings_model)
    if expected_dim is not None and settings.embed_dim != expected_dim:
        raise ValueError(
            "Embedding config mismatch: "
            f"model={settings.gigachat_embeddings_model} expects dim={expected_dim}, "
            f"but EMBED_DIM={settings.embed_dim}"
        )

    client = AsyncQdrantClient(url=settings.qdrant_url, timeout=30)

    collections = await client.get_collections()
    existing = {c.name for c in collections.collections}

    async def ensure_payload_indexes(collection_name, indexes):
        for field, schema_type in indexes:
            try:
                await client.create_payload_index(collection_name, field, field_schema=schema_type)
            except Exception as e:
                message = str(e).lower()
                if "already exists" not in message and "already has" not in message:
                    print(f"     Warning: {collection_name}.{field} payload index: {e}")

    frontier_payload_indexes = [
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
    ]
    trend_payload_indexes = [
        ("workspace_id", PayloadSchemaType.KEYWORD),
        ("pipeline", PayloadSchemaType.KEYWORD),
        ("signal_stage", PayloadSchemaType.KEYWORD),
        ("keywords", PayloadSchemaType.KEYWORD),
        ("burst_score", PayloadSchemaType.FLOAT),
        ("signal_score", PayloadSchemaType.FLOAT),
        ("detected_at", PayloadSchemaType.DATETIME),
        ("doc_count", PayloadSchemaType.INTEGER),
        ("source_count", PayloadSchemaType.INTEGER),
    ]

    if "frontier_docs" not in existing:
        await client.create_collection(
            collection_name="frontier_docs",
            vectors_config={
                "dense": VectorParams(
                    size=settings.embed_dim,
                    distance=Distance.COSINE,
                    hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
                )
            },
            sparse_vectors_config={
                "sparse": SparseVectorParams(index=SparseIndexParams(on_disk=False))
            },
        )
        print("     frontier_docs: created")
    else:
        print("     frontier_docs: already exists")
    await ensure_payload_indexes("frontier_docs", frontier_payload_indexes)

    if "trend_clusters" not in existing:
        await client.create_collection(
            collection_name="trend_clusters",
            vectors_config={
                "dense": VectorParams(
                    size=settings.embed_dim,
                    distance=Distance.COSINE,
                    hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
                )
            },
        )
        print("     trend_clusters: created")
    else:
        print("     trend_clusters: already exists")
    await ensure_payload_indexes("trend_clusters", trend_payload_indexes)

    await client.close()


async def init_neo4j(settings):
    print("[3/3] Initializing Neo4j...")
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    constraints = [
        "CREATE CONSTRAINT workspace_id IF NOT EXISTS FOR (w:Workspace) REQUIRE w.id IS UNIQUE",
        "CREATE CONSTRAINT concept_name_workspace IF NOT EXISTS FOR (c:Concept) REQUIRE (c.name, c.workspace_id) IS UNIQUE",
        "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT source_id IF NOT EXISTS FOR (s:Source) REQUIRE s.id IS UNIQUE",
        "CREATE CONSTRAINT trend_id IF NOT EXISTS FOR (t:TrendCluster) REQUIRE t.id IS UNIQUE",
        "CREATE INDEX concept_mentions IF NOT EXISTS FOR (c:Concept) ON (c.mentions)",
        "CREATE INDEX concept_workspace IF NOT EXISTS FOR (c:Concept) ON (c.workspace_id)",
        "CREATE INDEX document_workspace IF NOT EXISTS FOR (d:Document) ON (d.workspace_id)",
    ]

    async with driver.session() as session:
        for cypher in constraints:
            try:
                await session.run(cypher)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"     Warning: {e}")

    print("     Neo4j: constraints created")
    await driver.close()


async def main():
    settings = get_settings()
    print("=== Frontier Intelligence — Storage Init ===\n")

    try:
        await init_postgres(settings)
    except Exception as e:
        print(f"     PostgreSQL ERROR: {e}")

    try:
        await init_qdrant(settings)
    except Exception as e:
        print(f"     Qdrant ERROR: {e}")

    try:
        await init_neo4j(settings)
    except Exception as e:
        print(f"     Neo4j ERROR: {e}")

    print("\n=== Done ===")


if __name__ == "__main__":
    asyncio.run(main())
