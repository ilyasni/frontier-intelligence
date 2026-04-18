"""Graph router for admin UI."""

from collections import defaultdict
from itertools import combinations

from fastapi import APIRouter, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from admin.backend.db import get_engine

router = APIRouter()


@router.get("")
async def get_graph(
    workspace_id: str | None = None,
    limit: int = Query(120, ge=10, le=500),
    min_mentions: int = Query(2, ge=1, le=50),
    max_nodes: int = Query(50, ge=10, le=150),
):
    engine = get_engine()
    clauses = ["pe.kind = 'concepts'"]
    params: dict[str, object] = {"limit": limit}
    if workspace_id:
        clauses.append("p.workspace_id = :workspace_id")
        params["workspace_id"] = workspace_id
    where = " AND ".join(clauses)

    async with AsyncSession(engine) as session:
        result = await session.execute(
            text(
                f"""
                SELECT
                    p.id AS post_id,
                    p.workspace_id,
                    p.source_id,
                    p.category,
                    p.published_at,
                    pe.data
                FROM post_enrichments pe
                JOIN posts p ON p.id = pe.post_id
                WHERE {where}
                ORDER BY COALESCE(p.published_at, p.created_at) DESC, pe.updated_at DESC
                LIMIT :limit
                """
            ),
            params,
        )
        rows = result.mappings().all()

    node_stats: dict[str, dict[str, object]] = {}
    edge_stats: dict[tuple[str, str], dict[str, object]] = {}
    posts_seen = 0

    for row in rows:
        raw_items = (row.get("data") or {}).get("items") or []
        concepts_by_key: dict[str, dict[str, object]] = {}
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            raw_name = str(item.get("name") or "").strip()
            if not raw_name:
                continue
            key = raw_name.casefold()
            weight = float(item.get("weight") or 1)
            category = str(item.get("category") or row.get("category") or "uncategorized")
            current = concepts_by_key.get(key)
            if not current or weight > float(current["weight"]):
                concepts_by_key[key] = {
                    "id": key,
                    "label": raw_name,
                    "weight": weight,
                    "category": category,
                    "source_id": row.get("source_id"),
                }
        if len(concepts_by_key) < 1:
            continue

        posts_seen += 1
        for concept in concepts_by_key.values():
            stats = node_stats.setdefault(
                concept["id"],
                {
                    "id": concept["id"],
                    "label": concept["label"],
                    "category": concept["category"],
                    "mentions": 0,
                    "total_weight": 0.0,
                    "max_weight": 0.0,
                    "posts": [],
                },
            )
            stats["mentions"] = int(stats["mentions"]) + 1
            stats["total_weight"] = float(stats["total_weight"]) + float(concept["weight"])
            stats["max_weight"] = max(float(stats["max_weight"]), float(concept["weight"]))
            posts = stats["posts"]
            if len(posts) < 5:
                posts.append(
                    {
                        "post_id": row["post_id"],
                        "source_id": row.get("source_id"),
                        "category": row.get("category"),
                        "weight": concept["weight"],
                    }
                )

        for left, right in combinations(sorted(concepts_by_key), 2):
            pair = (left, right)
            edge = edge_stats.setdefault(
                pair,
                {
                    "id": f"{left}__{right}",
                    "source": left,
                    "target": right,
                    "mentions": 0,
                    "weight": 0.0,
                    "categories": defaultdict(int),
                },
            )
            edge["mentions"] = int(edge["mentions"]) + 1
            edge["weight"] = float(edge["weight"]) + min(
                float(concepts_by_key[left]["weight"]),
                float(concepts_by_key[right]["weight"]),
            )
            edge["categories"][str(row.get("category") or "uncategorized")] += 1

    retained_nodes = [
        node
        for node in node_stats.values()
        if int(node["mentions"]) >= min_mentions
    ]
    retained_nodes.sort(
        key=lambda item: (
            -int(item["mentions"]),
            -float(item["total_weight"]),
            str(item["label"]).casefold(),
        )
    )
    retained_nodes = retained_nodes[:max_nodes]
    retained_ids = {node["id"] for node in retained_nodes}

    nodes = [
        {
            **node,
            "avg_weight": round(float(node["total_weight"]) / max(int(node["mentions"]), 1), 2),
            "posts_count": len(node["posts"]),
        }
        for node in retained_nodes
    ]

    edges = []
    for edge in edge_stats.values():
        if edge["source"] not in retained_ids or edge["target"] not in retained_ids:
            continue
        dominant_category = max(
            edge["categories"].items(),
            key=lambda item: item[1],
        )[0] if edge["categories"] else "uncategorized"
        edges.append(
            {
                "id": edge["id"],
                "source": edge["source"],
                "target": edge["target"],
                "mentions": int(edge["mentions"]),
                "weight": round(float(edge["weight"]), 2),
                "category": dominant_category,
            }
        )
    edges.sort(key=lambda item: (-item["mentions"], -item["weight"], item["id"]))

    return {
        "nodes": nodes,
        "edges": edges,
        "meta": {
            "workspace_id": workspace_id,
            "source_rows": len(rows),
            "posts_seen": posts_seen,
            "nodes": len(nodes),
            "edges": len(edges),
            "limit": limit,
            "min_mentions": min_mentions,
            "max_nodes": max_nodes,
        },
    }
