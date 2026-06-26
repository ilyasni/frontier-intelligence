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
        # Контур D (RSI): ключ концепта — НОРМАЛИЗОВАННОЕ имя (apoc.text.clean), а не сырое.
        # Так варианты («OpenAI» / «open ai» / «Open-AI») сразу ложатся в ОДИН узел, и дубли
        # больше не копятся (см. одноразовый merge накопленного в graph_maintenance).
        # Пустую нормализацию (имя из одних символов) откатываем на toLower(trim(name)).
        await (await tx.run(
            """
            MERGE (ws:Workspace {id: $workspace_id})
            MERGE (d:Document {id: $doc_id})
            SET d.workspace_id = $workspace_id
            WITH ws, d
            UNWIND $concepts AS concept
            WITH ws, d, concept,
                 CASE WHEN apoc.text.clean(concept.name) = ''
                      THEN toLower(trim(concept.name))
                      ELSE apoc.text.clean(concept.name) END AS norm
            MERGE (c:Concept {norm: norm, workspace_id: $workspace_id})
            ON CREATE SET c.name = concept.name, c.category = concept.category
            ON MATCH SET c.category = COALESCE(concept.category, c.category),
                c.aliases = CASE
                    WHEN c.name = concept.name OR concept.name IN COALESCE(c.aliases, [])
                        THEN c.aliases
                        ELSE COALESCE(c.aliases, []) + concept.name
                    END
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
            WITH pair,
                 CASE WHEN apoc.text.clean(pair.a) = '' THEN toLower(trim(pair.a)) ELSE apoc.text.clean(pair.a) END AS na,
                 CASE WHEN apoc.text.clean(pair.b) = '' THEN toLower(trim(pair.b)) ELSE apoc.text.clean(pair.b) END AS nb
            MATCH (a:Concept {norm: na, workspace_id: $workspace_id})
            MATCH (b:Concept {norm: nb, workspace_id: $workspace_id})
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

    async def graph_health(self, workspace_id: str) -> dict[str, Any]:
        """Контур D (RSI): метрики здоровья концепт-графа для Grafana.

        concept_count, related_edges, orphan_count (узлы без RELATED_TO),
        duplicate_clusters (группы имён, совпадающих после нормализации), edge_density.
        """
        async with self.driver.session() as session:
            row = await (
                await session.run(
                    "MATCH (c:Concept {workspace_id: $ws}) RETURN count(c) AS concept_count",
                    ws=workspace_id,
                )
            ).single()
            concept_count = int((row or {}).get("concept_count") or 0)

            orphan_row = await (
                await session.run(
                    """
                    MATCH (c:Concept {workspace_id: $ws})
                    WHERE NOT (c)-[:RELATED_TO]-()
                    RETURN count(c) AS orphan_count
                    """,
                    ws=workspace_id,
                )
            ).single()
            orphan_count = int((orphan_row or {}).get("orphan_count") or 0)

            edge_row = await (
                await session.run(
                    """
                    MATCH (:Concept {workspace_id: $ws})-[r:RELATED_TO]-(:Concept {workspace_id: $ws})
                    RETURN count(DISTINCT r) AS related_edges
                    """,
                    ws=workspace_id,
                )
            ).single()
            related_edges = int((edge_row or {}).get("related_edges") or 0)

            dup_row = await (
                await session.run(
                    """
                    MATCH (c:Concept {workspace_id: $ws})
                    WITH apoc.text.clean(c.name) AS norm, count(*) AS cnt
                    WHERE cnt > 1
                    RETURN count(*) AS dup_clusters, coalesce(sum(cnt - 1), 0) AS dup_redundant
                    """,
                    ws=workspace_id,
                )
            ).single()
            dup_clusters = int((dup_row or {}).get("dup_clusters") or 0)
            dup_redundant = int((dup_row or {}).get("dup_redundant") or 0)

        density = round(related_edges / concept_count, 4) if concept_count else 0.0
        return {
            "concept_count": concept_count,
            "related_edges": related_edges,
            "orphan_count": orphan_count,
            "duplicate_clusters": dup_clusters,
            "duplicate_redundant_nodes": dup_redundant,
            "edge_density": density,
        }

    async def find_duplicate_entities(self, workspace_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Контур D: группы концептов, совпадающих после нормализации (dry-run, без слияния)."""
        groups: list[dict[str, Any]] = []
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (c:Concept {workspace_id: $ws})
                WITH apoc.text.clean(c.name) AS norm, collect(c.name) AS names, count(*) AS cnt
                WHERE cnt > 1
                RETURN norm, names, cnt
                ORDER BY cnt DESC
                LIMIT $limit
                """,
                ws=workspace_id, limit=limit,
            )
            async for row in result:
                groups.append({"normalized": row["norm"], "variants": row["names"], "count": row["cnt"]})
        return groups

    async def load_concept_names(self, workspace_id: str) -> list[dict[str, Any]]:
        """Контур D+: все имена концептов workspace (для генерации кандидатов по акронимам)."""
        out: list[dict[str, Any]] = []
        async with self.driver.session() as session:
            result = await session.run(
                "MATCH (c:Concept {workspace_id: $ws}) RETURN c.name AS name, c.norm AS norm",
                ws=workspace_id,
            )
            async for row in result:
                if row["norm"]:
                    out.append({"name": row["name"], "norm": row["norm"]})
        return out

    async def cooccurrence_for_pairs(
        self, workspace_id: str, pairs: list[dict[str, str]]
    ) -> dict[tuple[str, str], int]:
        """Контур D+: RELATED_TO-count для пар норм (со-встречаемость = фильтр точности)."""
        if not pairs:
            return {}
        out: dict[tuple[str, str], int] = {}
        async with self.driver.session() as session:
            result = await session.run(
                """
                UNWIND $pairs AS p
                MATCH (x:Concept {norm: p.a, workspace_id: $ws})-[r:RELATED_TO]-(y:Concept {norm: p.b, workspace_id: $ws})
                RETURN p.a AS a, p.b AS b, r.count AS cnt
                """,
                ws=workspace_id, pairs=pairs,
            )
            async for row in result:
                out[(row["a"], row["b"])] = int(row["cnt"] or 0)
        return out

    async def merge_concept_pair(
        self, workspace_id: str, keep_norm: str, drop_norm: str
    ) -> dict[str, Any]:
        """Контур D+: слить два конкретных концепта (keep остаётся каноническим, drop → alias)."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (keep:Concept {norm: $keep, workspace_id: $ws})
                MATCH (drop:Concept {norm: $drop, workspace_id: $ws})
                WITH keep, drop, ([drop.name] + coalesce(drop.aliases, [])) AS drop_aliases
                CALL apoc.refactor.mergeNodes([keep, drop], {properties: 'discard', mergeRels: true})
                YIELD node
                SET node.aliases = apoc.coll.toSet(coalesce(node.aliases, []) + drop_aliases)
                RETURN node.name AS canonical, node.norm AS norm
                """,
                ws=workspace_id, keep=keep_norm, drop=drop_norm,
            )
            row = await result.single()
        return {"merged": bool(row), "canonical": (row or {}).get("canonical")}

    async def merge_duplicate_entities(self, workspace_id: str, limit: int = 50) -> dict[str, Any]:
        """Контур D: слить нормализационные дубли в канонический узел (по mentions) с aliases.

        APPLY-операция (мутирует граф). Канонический = вариант с наибольшим числом упоминаний;
        остальные имена пишутся в node.aliases. Использует apoc.refactor.mergeNodes (mergeRels).
        """
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (c:Concept {workspace_id: $ws})
                WITH apoc.text.clean(c.name) AS norm, collect(c) AS nodes
                WHERE size(nodes) > 1
                WITH norm, nodes LIMIT $limit
                UNWIND nodes AS n
                OPTIONAL MATCH (n)<-[:MENTIONS]-(d:Document)
                WITH norm, n, count(d) AS mentions
                ORDER BY mentions DESC, size(n.name) ASC
                WITH norm, collect(n) AS ordered, collect(n.name) AS names
                CALL apoc.refactor.mergeNodes(ordered, {properties: 'discard', mergeRels: true})
                YIELD node
                SET node.aliases = [x IN names WHERE x <> node.name]
                RETURN count(node) AS merged_groups
                """,
                ws=workspace_id, limit=limit,
            )
            row = await result.single()
        return {"merged_groups": int((row or {}).get("merged_groups") or 0)}

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
                # Neo4j не принимает параметр в верхней границе var-length пути
                # (`[*1..$depth]` → Cypher error / 500). Подставляем валидированный
                # int в текст запроса, ограничив диапазон, чтобы не было инъекции/взрыва.
                safe_depth = max(1, min(int(depth), 5))
                # Матч по НОРМАЛИЗОВАННОМУ имени (регистр/пробелы/пунктуация не важны),
                # затем по cleaned-aliases (так «ИИ» найдёт канонический «искусственный
                # интеллект»), затем точное имя. norm индексирован — поиск быстрый.
                result = await session.run(
                    f"""
                    WITH apoc.text.clean($name) AS nrm
                    MATCH (c:Concept {{workspace_id: $ws}})
                    WHERE c.norm = nrm
                       OR c.name = $name
                       OR any(a IN coalesce(c.aliases, []) WHERE apoc.text.clean(a) = nrm)
                    WITH c LIMIT 1
                    MATCH path = (c)-[*1..{safe_depth}]-(neighbor:Concept)
                    WITH c, neighbor, relationships(path)[0] AS rel
                    RETURN c.name AS source, neighbor.name AS target,
                           type(rel) AS rel_type, rel.count AS weight
                    LIMIT $limit
                    """,
                    ws=workspace_id, name=concept, limit=limit,
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
