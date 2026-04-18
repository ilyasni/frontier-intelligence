"""MCP Server — exposes Frontier Intelligence tools to Claude Projects."""
import logging
import sys

sys.path.insert(0, "/app")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Frontier Intelligence MCP", version="1.0.0")
app.mount("/metrics", make_asgi_app())

from shared.config import get_settings as _get_settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_settings().allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import tool routers
from mcp.tools.search_frontier import router as search_router
from mcp.tools.search_balanced import router as balanced_search_router
from mcp.tools.search_trend_clusters import router as trend_search_router
from mcp.tools.search_by_vision import router as vision_search_router
from mcp.tools.graph import router as graph_router
from mcp.tools.frontier_brief import router as brief_router
from mcp.tools.ingest_url import router as ingest_router
from mcp.tools.observability import router as observability_router
from shared.qdrant_sparse import HAS_SPARSE


def _search_frontier_description() -> str:
    if HAS_SPARSE:
        return (
            "Search frontier intelligence documents using hybrid vector search "
            "(dense embeddings + BM25 sparse via fastembed)"
        )
    return (
        "Search frontier intelligence documents using dense vector search only "
        "(BM25 sparse unavailable — fastembed not loaded in this image)"
    )


app.include_router(search_router, prefix="/tools/search_frontier", tags=["search"])
app.include_router(balanced_search_router, prefix="/tools/search_balanced", tags=["search"])
app.include_router(trend_search_router, prefix="/tools/search_trend_clusters", tags=["search"])
app.include_router(vision_search_router, prefix="/tools/search_by_vision", tags=["search"])
app.include_router(graph_router, prefix="/tools/get_concept_graph", tags=["graph"])
app.include_router(brief_router, prefix="/tools/get_frontier_brief", tags=["brief"])
app.include_router(ingest_router, prefix="/tools/ingest_url", tags=["ingest"])
app.include_router(observability_router, prefix="/tools", tags=["observability"])


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/tools")
async def list_tools():
    """List all available MCP tools."""
    return {
        "tools": [
            {
                "name": "search_frontier",
                "description": _search_frontier_description(),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query"},
                        "workspace": {"type": "string", "description": "Workspace ID (default: disruption)"},
                        "limit": {"type": "integer", "description": "Max results (default: 10)"},
                        "synthesize": {"type": "boolean", "description": "Use GigaChat to synthesize results (default: false)"},
                        "lang": {"type": "string", "description": "Optional language filter, e.g. ru or en"},
                        "days_back": {"type": "integer", "description": "Optional hard date filter in days"},
                        "valence": {"type": "string", "description": "Optional signal valence: positive | neutral | negative"},
                        "signal_type": {"type": "string", "description": "Optional signal type filter"},
                        "source_region": {"type": "string", "description": "Optional source region filter, e.g. ru | global | us"},
                        "entities": {"type": "array", "items": {"type": "string"}, "description": "Optional competitors/entities to surface in evidence"},
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "search_balanced",
                "description": "Balanced analytical search that returns growth signals, counter-signals, RU verification, competitor evidence, and known blind spots.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query"},
                        "workspace": {"type": "string", "description": "Workspace ID (default: disruption)"},
                        "limit": {"type": "integer", "description": "Max results per lane (default: 10)"},
                        "synthesize": {"type": "boolean", "description": "Use GigaChat to synthesize balanced output (default: true)"},
                        "lang": {"type": "string", "description": "Optional language filter, e.g. ru or en"},
                        "source_region": {"type": "string", "description": "Optional source region filter"},
                        "entities": {"type": "array", "items": {"type": "string"}, "description": "Optional competitors/entities to track"},
                        "days_back": {"type": "integer", "description": "Main search window in days (default: 7)"},
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "search_trend_clusters",
                "description": "Semantic search over stable trend clusters mirrored to the Qdrant trend_clusters collection.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query"},
                        "workspace": {"type": "string", "description": "Workspace ID (default: disruption)"},
                        "limit": {"type": "integer", "description": "Max results (default: 10)"},
                        "pipeline": {"type": "string", "description": "stable | reactive (default: stable)"},
                        "stages": {"type": "array", "items": {"type": "string"}, "description": "Optional signal stages: weak | emerging | stable | fading"},
                        "days_back": {"type": "integer", "description": "Optional detected_at date filter in days"},
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "search_by_vision",
                "description": "Search stored GigaChat Vision and OCR enrichments by labels, scenes, OCR text, and post preview.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Vision/OCR query; empty lists recent vision enrichments"},
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "limit": {"type": "integer", "description": "Max results (default: 20)"},
                        "has_ocr": {"type": "boolean", "description": "Optional OCR presence filter"},
                    },
                },
            },
            {
                "name": "get_concept_graph",
                "description": "Read a workspace concept graph or a concept-centered subgraph from Neo4j.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Workspace ID (default: disruption)"},
                        "concept": {"type": "string", "description": "Optional concept name to center the subgraph"},
                        "depth": {"type": "integer", "description": "Traversal depth 1-4 (default: 2)"},
                        "limit": {"type": "integer", "description": "Max edges to return (default: 50)"},
                    },
                },
            },
            {
                "name": "get_frontier_brief",
                "description": "Compose a multi-workspace frontier brief from overview, trend clusters, weak/emerging signals, and missing signals.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Single workspace ID"},
                        "workspaces": {"type": "array", "items": {"type": "string"}, "description": "Optional workspace IDs for cross-workspace brief"},
                        "recent_limit": {"type": "integer", "description": "Recent posts per workspace (default: 8)"},
                        "clusters_limit": {"type": "integer", "description": "Clusters/signals per workspace (default: 8)"},
                        "missing_limit": {"type": "integer", "description": "Missing signals per workspace (default: 6)"},
                        "synthesize": {"type": "boolean", "description": "Use GigaChat to synthesize the brief (default: true)"},
                    },
                },
            },
            {
                "name": "ingest_url",
                "description": (
                    "Queue a URL for crawl4ai on stream:posts:crawl; requires existing post_id in PostgreSQL"
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "HTTP(S) URL to crawl"},
                        "workspace": {"type": "string", "description": "Workspace ID (default: disruption)"},
                        "post_id": {
                            "type": "string",
                            "description": "Existing post id (crawl enrichment attaches to this post)",
                        },
                    },
                    "required": ["url", "post_id"],
                },
            },
            {
                "name": "list_workspaces",
                "description": "List workspaces with categories, activity state, and bridge metadata.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "active_only": {"type": "boolean", "description": "Only return active workspaces"},
                    },
                },
            },
            {
                "name": "list_sources_health",
                "description": "List source health, source_score, authority, content mode, and last run status.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "limit": {"type": "integer", "description": "Max sources to return (default: 100)"},
                    },
                },
            },
            {
                "name": "get_pipeline_stats",
                "description": "Get ingestion/enrichment pipeline status counts and recent posts.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "recent_limit": {"type": "integer", "description": "Max recent posts to return (default: 20)"},
                    },
                },
            },
            {
                "name": "get_workspace_overview",
                "description": "Get a compact workspace overview with summary counts, top sources, recent posts, and clusters.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Workspace ID"},
                        "recent_limit": {"type": "integer", "description": "Max recent posts to return (default: 8)"},
                        "sources_limit": {"type": "integer", "description": "Max top sources to return (default: 8)"},
                        "clusters_limit": {"type": "integer", "description": "Max semantic/trend clusters to return (default: 6)"},
                    },
                    "required": ["workspace"],
                },
            },
            {
                "name": "list_clusters",
                "description": "List semantic and/or trend clusters for a workspace. Emerging results default to signal_stage=emerging unless stages are explicitly provided.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "kind": {"type": "string", "description": "all | semantic | trend | emerging (default: all)"},
                        "pipeline": {"type": "string", "description": "Trend pipeline filter (default: stable)"},
                        "limit": {"type": "integer", "description": "Max clusters to return (default: 20)"},
                        "stages": {"type": "array", "items": {"type": "string"}, "description": "Optional signal stages for emerging results: weak | emerging | stable | fading"},
                    },
                },
            },
            {
                "name": "list_emerging_signals",
                "description": "List emerging signals for a workspace. Defaults to signal_stage=emerging; include stages explicitly for analyst/debug views.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "limit": {"type": "integer", "description": "Max signals to return (default: 20)"},
                        "stages": {"type": "array", "items": {"type": "string"}, "description": "Optional signal stages: weak | emerging | stable | fading"},
                    },
                },
            },
            {
                "name": "list_missing_signals",
                "description": "List externally active but under-covered topics detected via SearXNG gap analysis.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "limit": {"type": "integer", "description": "Max signals to return (default: 20)"},
                    },
                },
            },
            {
                "name": "get_cluster_details",
                "description": "Get full cluster details with scoring and explainability breakdown.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "string", "description": "Cluster or signal ID"},
                        "kind": {"type": "string", "description": "auto | semantic | trend | emerging | missing (default: auto)"},
                    },
                    "required": ["cluster_id"],
                },
            },
            {
                "name": "get_missing_signal_details",
                "description": "Get a single missing-signal record with external evidence URLs and opportunity text.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "signal_id": {"type": "string", "description": "Missing signal ID"},
                    },
                    "required": ["signal_id"],
                },
            },
            {
                "name": "get_source_details",
                "description": "Get detailed source health, recent runs, and recent posts for a single source.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "source_id": {"type": "string", "description": "Source ID"},
                        "recent_runs_limit": {"type": "integer", "description": "Max source runs to return (default: 10)"},
                        "recent_posts_limit": {"type": "integer", "description": "Max recent posts to return (default: 10)"},
                    },
                    "required": ["source_id"],
                },
            },
            {
                "name": "get_cluster_evidence",
                "description": "Get evidence, representative posts, and metadata for a semantic or trend cluster.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "string", "description": "Cluster ID"},
                        "kind": {"type": "string", "description": "auto | semantic | trend | emerging (default: auto)"},
                        "evidence_limit": {"type": "integer", "description": "Max evidence posts to return (default: 6)"},
                    },
                    "required": ["cluster_id"],
                },
            },
            {
                "name": "get_signal_timeline",
                "description": "Get persisted signal time-series points, breakpoints, and temporal score breakdown for a semantic, trend, or emerging entity.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "entity_kind": {"type": "string", "description": "semantic | trend | emerging"},
                        "entity_id": {"type": "string", "description": "Entity ID"},
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                    },
                    "required": ["entity_kind", "entity_id"],
                },
            },
        ]
    }


if __name__ == "__main__":
    import uvicorn
    from shared.config import get_settings
    settings = get_settings()
    uvicorn.run(app, host="0.0.0.0", port=settings.mcp_port)
