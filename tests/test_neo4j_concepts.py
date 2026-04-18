"""Integration tests for Neo4j concept graph upsert.

Requires a running Neo4j instance. Set NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD
environment variables, or they will default to the docker-compose values.
Tests are skipped if Neo4j is unreachable.

Запуск только интеграционных тестов: ``pytest -m integration tests/test_neo4j_concepts.py``
"""
import importlib.util
import os

import pytest

# conftest не должен подменять neo4j на MagicMock — нужен реальный драйвер.
# Если пакета нет: тесты собираются и помечаются skip (pytest skipif).
pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("neo4j") is None,
    reason="neo4j driver missing: pip install neo4j",
)

NEO4J_URL = os.getenv("NEO4J_URL", "bolt://192.168.31.222:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

TEST_WORKSPACE = "test-integration-ws"
TEST_DOC = "test-doc-integration-1"


@pytest.fixture(scope="module")
async def neo4j_client():
    """Create client and skip module if Neo4j unreachable."""
    from unittest.mock import patch

    with patch.dict(os.environ, {
        "NEO4J_URL": NEO4J_URL,
        "NEO4J_USER": NEO4J_USER,
        "NEO4J_PASSWORD": NEO4J_PASSWORD,
        # Stub other required settings
        "DATABASE_URL": "postgresql+asyncpg://x:x@localhost/x",
        "REDIS_URL": "redis://localhost:6379",
    }):
        from shared.config import get_settings
        from worker.integrations.neo4j_client import Neo4jFrontierClient

        client = None
        try:
            get_settings.cache_clear()
            client = Neo4jFrontierClient()
            await client.driver.verify_connectivity()
            yield client
        except Exception as exc:
            pytest.skip(f"Neo4j unreachable: {exc}")
        finally:
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass
            get_settings.cache_clear()


@pytest.fixture(autouse=True)
async def cleanup(neo4j_client):
    """Delete test nodes before and after each test."""
    async def _delete():
        async with neo4j_client.driver.session() as session:
            await (await session.run(
                "MATCH (n) WHERE n.workspace_id = $ws OR n.id = $doc "
                "DETACH DELETE n",
                ws=TEST_WORKSPACE, doc=TEST_DOC,
            )).consume()

    await _delete()
    yield
    await _delete()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upsert_creates_workspace_and_concepts(neo4j_client):
    """upsert_concepts must create Workspace node and Concept nodes."""
    concepts = [
        {"name": "OpenAI", "category": "company", "weight": 1},
        {"name": "GPT-5", "category": "product", "weight": 1},
    ]
    await neo4j_client.upsert_concepts(TEST_WORKSPACE, TEST_DOC, concepts)

    async with neo4j_client.driver.session() as session:
        result = await session.run(
            "MATCH (w:Workspace {id: $ws}) RETURN w.id",
            ws=TEST_WORKSPACE,
        )
        rows = await result.values()
    assert rows, "Workspace node not created"

    async with neo4j_client.driver.session() as session:
        result = await session.run(
            "MATCH (c:Concept {workspace_id: $ws}) RETURN c.name ORDER BY c.name",
            ws=TEST_WORKSPACE,
        )
        names = [r[0] for r in await result.values()]
    assert sorted(names) == ["GPT-5", "OpenAI"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upsert_creates_contains_relations(neo4j_client):
    """Workspace must have CONTAINS relations to each Concept."""
    concepts = [{"name": "HDBSCAN", "category": "algorithm", "weight": 1}]
    await neo4j_client.upsert_concepts(TEST_WORKSPACE, TEST_DOC, concepts)

    async with neo4j_client.driver.session() as session:
        result = await session.run(
            "MATCH (w:Workspace {id: $ws})-[:CONTAINS]->(c:Concept) RETURN c.name",
            ws=TEST_WORKSPACE,
        )
        names = [r[0] for r in await result.values()]
    assert "HDBSCAN" in names


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upsert_is_idempotent(neo4j_client):
    """Calling upsert_concepts twice must not create duplicate nodes."""
    concepts = [{"name": "Qdrant", "category": "tool", "weight": 1}]
    await neo4j_client.upsert_concepts(TEST_WORKSPACE, TEST_DOC, concepts)
    await neo4j_client.upsert_concepts(TEST_WORKSPACE, TEST_DOC, concepts)

    async with neo4j_client.driver.session() as session:
        result = await session.run(
            "MATCH (c:Concept {name: 'Qdrant', workspace_id: $ws}) RETURN count(c) as cnt",
            ws=TEST_WORKSPACE,
        )
        row = await result.single()
    assert row["cnt"] == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cooccurrence_relation_created(neo4j_client):
    """Two concepts in same document must get RELATED_TO co-occurrence edge."""
    concepts = [
        {"name": "LangChain", "category": "framework", "weight": 1},
        {"name": "GigaChat", "category": "model", "weight": 1},
    ]
    await neo4j_client.upsert_concepts(TEST_WORKSPACE, TEST_DOC, concepts)

    async with neo4j_client.driver.session() as session:
        result = await session.run(
            """
            MATCH (a:Concept {name: 'LangChain', workspace_id: $ws})
                  -[r:RELATED_TO]-
                  (b:Concept {name: 'GigaChat', workspace_id: $ws})
            RETURN r.count
            """,
            ws=TEST_WORKSPACE,
        )
        row = await result.single()
    assert row is not None, "RELATED_TO relation not created"
    assert row["r.count"] >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cooccurrence_is_idempotent_for_same_document(neo4j_client):
    """Reprocessing the same document must not inflate RELATED_TO count."""
    concepts = [
        {"name": "LangChain", "category": "framework", "weight": 1},
        {"name": "GigaChat", "category": "model", "weight": 1},
    ]
    await neo4j_client.upsert_concepts(TEST_WORKSPACE, TEST_DOC, concepts)
    await neo4j_client.upsert_concepts(TEST_WORKSPACE, TEST_DOC, concepts)

    async with neo4j_client.driver.session() as session:
        result = await session.run(
            """
            MATCH (a:Concept {name: 'LangChain', workspace_id: $ws})
                  -[r:RELATED_TO]-
                  (b:Concept {name: 'GigaChat', workspace_id: $ws})
            RETURN r.count AS count, r.doc_ids AS doc_ids
            """,
            ws=TEST_WORKSPACE,
        )
        row = await result.single()

    assert row is not None, "RELATED_TO relation not created"
    assert row["count"] == 1
    assert row["doc_ids"] == [TEST_DOC]
