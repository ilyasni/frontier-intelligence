"""MCP Gateway — Streamable HTTP транспорт поверх REST API (порт 8100).

Запускается отдельным процессом на порту 8102.
Claude Code подключается: http://<host>:8102/mcp
"""
import sys

sys.path.insert(0, "/app")

import os

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

REST_BASE = os.environ.get("MCP_REST_BASE", "http://localhost:8100")

mcp = FastMCP(
    "Frontier Intelligence",
    host=os.environ.get("MCP_GATEWAY_HOST", "0.0.0.0"),
    port=int(os.environ.get("MCP_GATEWAY_PORT", "8102")),
    transport_security=TransportSecuritySettings(
        allowed_hosts=["*"],
        allowed_origins=["*"],
        enable_dns_rebinding_protection=False,
    ),
)


@mcp.tool(
    description=(
        "Search frontier intelligence documents using hybrid vector search. "
        "Returns relevant posts and optionally synthesizes insights via GigaChat."
    )
)
async def search_frontier(
    query: str,
    workspace: str = "disruption",
    limit: int = 10,
    synthesize: bool = False,
    lang: str | None = None,
    days_back: int | None = None,
    valence: str | None = None,
    signal_type: str | None = None,
    source_region: str | None = None,
    entities: list[str] | None = None,
) -> dict:
    """Search frontier intelligence documents."""
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/search_frontier",
            json={
                "query": query,
                "workspace": workspace,
                "limit": limit,
                "synthesize": synthesize,
                "lang": lang,
                "days_back": days_back,
                "valence": valence,
                "signal_type": signal_type,
                "source_region": source_region,
                "entities": entities,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description=(
        "Balanced analytical search with growth signals, counter-signals, RU verification, "
        "competitor evidence, and known blind spots."
    )
)
async def search_balanced(
    query: str,
    workspace: str = "disruption",
    limit: int = 10,
    synthesize: bool = True,
    lang: str | None = None,
    source_region: str | None = None,
    entities: list[str] | None = None,
    days_back: int | None = 7,
) -> dict:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/search_balanced",
            json={
                "query": query,
                "workspace": workspace,
                "limit": limit,
                "synthesize": synthesize,
                "lang": lang,
                "source_region": source_region,
                "entities": entities,
                "days_back": days_back,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Semantic search over stable trend clusters mirrored to Qdrant."
)
async def search_trend_clusters(
    query: str,
    workspace: str = "disruption",
    limit: int = 10,
    pipeline: str | None = "stable",
    stages: list[str] | None = None,
    days_back: int | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/search_trend_clusters",
            json={
                "query": query,
                "workspace": workspace,
                "limit": limit,
                "pipeline": pipeline,
                "stages": stages,
                "days_back": days_back,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Search stored GigaChat Vision labels, scenes, and OCR enrichments."
)
async def search_by_vision(
    query: str = "",
    workspace: str | None = None,
    limit: int = 20,
    has_ocr: bool | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/search_by_vision",
            json={
                "query": query,
                "workspace": workspace,
                "limit": limit,
                "has_ocr": has_ocr,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Read a workspace concept graph or concept-centered subgraph from Neo4j."
)
async def get_concept_graph(
    workspace: str = "disruption",
    concept: str | None = None,
    depth: int = 2,
    limit: int = 50,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_concept_graph",
            json={
                "workspace": workspace,
                "concept": concept,
                "depth": depth,
                "limit": limit,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description=(
        "Compose a multi-workspace frontier brief from overview, trend, "
        "weak/emerging, and missing signals."
    )
)
async def get_frontier_brief(
    workspace: str | None = None,
    workspaces: list[str] | None = None,
    recent_limit: int = 8,
    clusters_limit: int = 8,
    missing_limit: int = 6,
    synthesize: bool = True,
) -> dict:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_frontier_brief",
            json={
                "workspace": workspace,
                "workspaces": workspaces,
                "recent_limit": recent_limit,
                "clusters_limit": clusters_limit,
                "missing_limit": missing_limit,
                "synthesize": synthesize,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description=(
        "Queue a URL for crawl4ai ingestion into the frontier pipeline. "
        "Requires an existing post_id in PostgreSQL."
    )
)
async def ingest_url(
    url: str,
    post_id: str,
    workspace: str = "disruption",
) -> dict:
    """Ingest a URL into the frontier pipeline."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/ingest_url",
            json={"url": url, "post_id": post_id, "workspace": workspace},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="List workspaces with categories, activity state, and cross-workspace bridges."
)
async def list_workspaces(
    active_only: bool = False,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_workspaces",
            json={"active_only": active_only},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="List source health with source_score, authority, content mode, and last run status."
)
async def list_sources_health(
    workspace: str | None = None,
    limit: int = 100,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_sources_health",
            json={"workspace": workspace, "limit": limit},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Get ingestion/enrichment pipeline status counts and recent posts."
)
async def get_pipeline_stats(
    workspace: str | None = None,
    recent_limit: int = 20,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_pipeline_stats",
            json={"workspace": workspace, "recent_limit": recent_limit},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Get a compact workspace overview with summary counts, top sources, recent posts, and clusters."
)
async def get_workspace_overview(
    workspace: str,
    recent_limit: int = 8,
    sources_limit: int = 8,
    clusters_limit: int = 6,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_workspace_overview",
            json={
                "workspace": workspace,
                "recent_limit": recent_limit,
                "sources_limit": sources_limit,
                "clusters_limit": clusters_limit,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="List semantic and/or trend clusters for a workspace."
)
async def list_clusters(
    workspace: str | None = None,
    kind: str = "all",
    pipeline: str = "stable",
    limit: int = 20,
    stages: list[str] | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_clusters",
            json={
                "workspace": workspace,
                "kind": kind,
                "pipeline": pipeline,
                "limit": limit,
                "stages": stages,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Get detailed source health, recent runs, and recent posts for a single source."
)
async def get_source_details(
    source_id: str,
    recent_runs_limit: int = 10,
    recent_posts_limit: int = 10,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_source_details",
            json={
                "source_id": source_id,
                "recent_runs_limit": recent_runs_limit,
                "recent_posts_limit": recent_posts_limit,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="List weak, emerging, or fading signals for a workspace."
)
async def list_emerging_signals(
    workspace: str | None = None,
    limit: int = 20,
    stages: list[str] | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_emerging_signals",
            json={"workspace": workspace, "limit": limit, "stages": stages},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="List under-covered topics detected by SearXNG gap analysis."
)
async def list_missing_signals(
    workspace: str | None = None,
    limit: int = 20,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_missing_signals",
            json={"workspace": workspace, "limit": limit},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Get full cluster details with scoring and explainability breakdown."
)
async def get_cluster_details(
    cluster_id: str,
    kind: str = "auto",
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_cluster_details",
            json={"cluster_id": cluster_id, "kind": kind},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Get a single missing-signal record with evidence URLs and opportunity text."
)
async def get_missing_signal_details(signal_id: str) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_missing_signal_details",
            json={"signal_id": signal_id},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Get metadata and representative evidence for a semantic or trend cluster."
)
async def get_cluster_evidence(
    cluster_id: str,
    kind: str = "auto",
    evidence_limit: int = 6,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_cluster_evidence",
            json={"cluster_id": cluster_id, "kind": kind, "evidence_limit": evidence_limit},
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description="Get persisted signal timeline points, breakpoints, and temporal score breakdown."
)
async def get_signal_timeline(
    entity_kind: str,
    entity_id: str,
    workspace: str | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_signal_timeline",
            json={"entity_kind": entity_kind, "entity_id": entity_id, "workspace": workspace},
        )
        r.raise_for_status()
        return r.json()


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
