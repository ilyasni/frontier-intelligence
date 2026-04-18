"""Neo4j integration — concept graph upsert and traversal."""
import logging
from typing import Any, Optional

from neo4j import AsyncGraphDatabase

from shared.config import get_settings

logger = logging.getLogger(__name__)


class Neo4jFrontierClient:
    def __init__(self):
        settings = get_settings()
        self.driver = AsyncGraphDatabase.driver(
            settings.neo4j_url,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    async def close(self):
        await self.driver.close()

    async def ensure_workspace_node(self, workspace_id: str) -> None:
        """Create (:Workspace) root node if it doesn't exist."""
        async with self.driver.session() as session:
            await session.execute_write(self._ensure_workspace_tx, workspace_id)

    @staticmethod
    async def _ensure_workspace_tx(tx, workspace_id: str) -> None:
        await (await tx.run(
            "MERGE (:Workspace {id: $workspace_id})",
            workspace_id=workspace_id,
        )).consume()

    @staticmethod
    def _normalize_concepts(concepts: list[dict]) -> list[dict]:
        deduped: dict[str, dict[str, Any]] = {}
        for concept in concepts:
            name = (concept.get("name") or "").strip()
            if not name:
                continue
            current = deduped.get(name)
            weight = int(concept.get("weight", 1) or 1)
            category = concept.get("category", "other")
            if current is None or weight > current["weight"]:
                deduped[name] = {
                    "name": name,
                    "category": category,
                    "weight": weight,
                }
        return list(deduped.values())

    @staticmethod
    def _build_related_pairs(concepts: list[dict[str, Any]]) -> list[dict[str, str]]:
        names = sorted({concept["name"] for concept in concepts})
        return [
            {"a": name_a, "b": name_b}
            for idx, name_a in enumerate(names)
            for name_b in names[idx + 1 :]
        ]

    @staticmethod
    async def _upsert_concepts_tx(
        tx,
        workspace_id: str,
        doc_id: str,
        concepts: list[dict[str, Any]],
        related_pairs: list[dict[str, str]],
    ) -> None:
        await (await tx.run(
            """
            MERGE (ws:Workspace {id: $workspace_id})
            MERGE (d:Document {id: $doc_id})
            SET d.workspace_id = $workspace_id
            WITH ws, d
            UNWIND $concepts AS concept
            MERGE (c:Concept {name: concept.name, workspace_id: $workspace_id})
            SET c.category = concept.category
            MERGE (ws)-[:CONTAINS]->(c)
            MERGE (d)-[r:MENTIONS]->(c)
            SET r.weight = concept.weight
            """,
            workspace_id=workspace_id,
            doc_id=doc_id,
            concepts=concepts,
        )).consume()

        if not related_pairs:
            return

        await (await tx.run(
            """
            UNWIND $related_pairs AS pair
            MATCH (a:Concept {name: pair.a, workspace_id: $workspace_id})
            MATCH (b:Concept {name: pair.b, workspace_id: $workspace_id})
            MERGE (a)-[r:RELATED_TO]-(b)
            WITH r, COALESCE(r.doc_ids, []) AS doc_ids
            SET r.doc_ids = CASE
                    WHEN $doc_id IN doc_ids THEN doc_ids
                    ELSE doc_ids + $doc_id
                END,
                r.count = CASE
                    WHEN $doc_id IN doc_ids THEN size(doc_ids)
                    ELSE size(doc_ids) + 1
                END
            """,
            workspace_id=workspace_id,
            doc_id=doc_id,
            related_pairs=related_pairs,
        )).consume()

    async def upsert_concepts(
        self,
        workspace_id: str,
        doc_id: str,
        concepts: list[dict],
    ) -> None:
        """MERGE concept nodes and co-occurrence relationships."""
        if not concepts:
            return

        normalized_concepts = self._normalize_concepts(concepts)
        if not normalized_concepts:
            return

        async with self.driver.session() as session:
            await session.execute_write(
                self._upsert_concepts_tx,
                workspace_id,
                doc_id,
                normalized_concepts,
                self._build_related_pairs(normalized_concepts),
            )

    async def get_concept_graph(
        self,
        workspace_id: str,
        concept: Optional[str] = None,
        depth: int = 2,
        limit: int = 50,
    ) -> dict:
        """Return subgraph as nodes + edges dict."""
        async with self.driver.session() as session:
            if concept:
                result = await session.run(
                    """
                    MATCH path = (c:Concept {workspace_id: $ws, name: $name})-[*1..$depth]-(neighbor:Concept)
                    WITH c, neighbor, relationships(path)[0] as rel
                    RETURN c.name as source, neighbor.name as target,
                           type(rel) as rel_type, rel.count as weight
                    LIMIT $limit
                    """,
                    ws=workspace_id, name=concept, depth=depth, limit=limit,
                )
            else:
                result = await session.run(
                    """
                    MATCH (a:Concept {workspace_id: $ws})-[r:RELATED_TO]-(b:Concept {workspace_id: $ws})
                    RETURN a.name as source, b.name as target, r.count as weight
                    ORDER BY r.count DESC
                    LIMIT $limit
                    """,
                    ws=workspace_id, limit=limit,
                )

            edges = []
            nodes = set()
            async for row in result:
                edges.append({"source": row["source"], "target": row["target"], "weight": row["weight"]})
                nodes.add(row["source"])
                nodes.add(row["target"])

            return {"nodes": list(nodes), "edges": edges}
