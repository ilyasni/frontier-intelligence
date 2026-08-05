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
from mcp.tools.threshold_proposals import router as threshold_proposals_router
from mcp.tools.graph_health import router as graph_health_router
from mcp.tools.editorial import router as editorial_router
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
app.include_router(threshold_proposals_router, prefix="/tools", tags=["rsi"])
app.include_router(graph_health_router, prefix="/tools", tags=["rsi"])
app.include_router(editorial_router, prefix="/tools", tags=["editorial"])


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
                        "include_bridges": {"type": "boolean", "description": "Also search workspaces declared in cross_workspace_bridges (default: false); every result then carries origin_workspace and bridged"},
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
                        "include_bridges": {"type": "boolean", "description": "Also brief workspaces declared in cross_workspace_bridges (default: false); every block then carries origin_workspace and bridged"},
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
            {
                "name": "list_threshold_proposals",
                "description": "RSI retrospective loop: list weak-signal threshold-change proposals (default status=pending) with rationale and evidence for human review.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "status": {"type": "string", "description": "pending | approved | rejected | superseded (default: pending)"},
                        "limit": {"type": "integer", "description": "Max proposals to return (default: 50)"},
                    },
                },
            },
            {
                "name": "approve_threshold_change",
                "description": "RSI gate: approve a weak-signal threshold proposal. Applies the new value to the workspace cluster_analysis override (effective next clustering run). This is the human gate that closes the retrospective loop.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "proposal_id": {"type": "string", "description": "Threshold proposal ID from list_threshold_proposals"},
                        "reviewed_by": {"type": "string", "description": "Who approved (default: operator)"},
                    },
                    "required": ["proposal_id"],
                },
            },
            {
                "name": "reject_threshold_change",
                "description": "RSI gate: reject a weak-signal threshold proposal (threshold unchanged).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "proposal_id": {"type": "string", "description": "Threshold proposal ID"},
                        "reviewed_by": {"type": "string", "description": "Who rejected (default: operator)"},
                        "note": {"type": "string", "description": "Optional reason for rejection"},
                    },
                    "required": ["proposal_id"],
                },
            },
            {
                "name": "list_underrated_signals",
                "description": "RSI contour B: weak candidates that a different-family novelty judge (DeepSeek) rated as genuinely novel — blind spots the primary Gemma/GigaChat stack under-rated. Ranked by judge novelty_score.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "days_back": {"type": "integer", "description": "Lookback window in days (default: 30)"},
                        "limit": {"type": "integer", "description": "Max signals to return (default: 30)"},
                    },
                },
            },
            {
                "name": "list_relevance_audit_sample",
                "description": "RSI contour C: sample of posts the Relevance Filter REJECTED (silent false-negative audit), ranked by score (closest to threshold first = most likely wrongly rejected). Review each and mark with mark_relevance_audit.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "days_back": {"type": "integer", "description": "Lookback window in days (default: 30)"},
                        "limit": {"type": "integer", "description": "Max rejected posts to sample (default: 20)"},
                    },
                },
            },
            {
                "name": "mark_relevance_audit",
                "description": "RSI contour C: record a human verdict on a rejected post. verdict=false_negative means it was wrongly rejected. Enough false_negatives auto-propose lowering the relevance threshold (review via list_threshold_proposals).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "post_id": {"type": "string", "description": "Post ID from list_relevance_audit_sample"},
                        "verdict": {"type": "string", "description": "false_negative | correct_reject"},
                        "reviewed_by": {"type": "string", "description": "Who reviewed (default: operator)"},
                        "note": {"type": "string", "description": "Optional note"},
                    },
                    "required": ["post_id", "verdict"],
                },
            },
            {
                "name": "get_graph_health",
                "description": "RSI contour D: Neo4j concept-graph health (concept count, orphan nodes, duplicate clusters, edge density) plus normalized duplicate-entity groups. Inspect before applying entity-resolution merges.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Workspace ID"},
                        "duplicates_limit": {"type": "integer", "description": "Max duplicate groups to return (default: 25)"},
                    },
                    "required": ["workspace"],
                },
            },
            {
                "name": "list_entity_merge_proposals",
                "description": "RSI contour D+: semantic concept-merge proposals (acronym↔expansion, e.g. LLM↔Large Language Model), confirmed by a different-family LLM judge. Review before approving the graph merge.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "status": {"type": "string", "description": "pending | approved | rejected (default: pending)"},
                        "limit": {"type": "integer", "description": "Max proposals to return (default: 50)"},
                    },
                },
            },
            {
                "name": "approve_entity_merge",
                "description": "RSI contour D+ gate: approve a semantic merge — merges the two concept nodes in Neo4j (canonical kept, the other becomes an alias). Irreversible graph mutation.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "proposal_id": {"type": "string", "description": "Proposal ID from list_entity_merge_proposals"},
                        "reviewed_by": {"type": "string", "description": "Who approved (default: operator)"},
                    },
                    "required": ["proposal_id"],
                },
            },
            {
                "name": "reject_entity_merge",
                "description": "RSI contour D+ gate: reject a semantic merge proposal (graph unchanged).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "proposal_id": {"type": "string", "description": "Proposal ID"},
                        "reviewed_by": {"type": "string", "description": "Who rejected (default: operator)"},
                        "note": {"type": "string", "description": "Optional reason"},
                    },
                    "required": ["proposal_id"],
                },
            },
            {
                "name": "export_inbox_cards",
                "description": "Editorial loop: pull the top signals for a workspace and render them as inbox cards (one topic line, one number traceable to a DB column, an empty question slot). Returns a fresh batch_id, the cards in the exact shape record_card_feedback accepts, and markdown to append to content/inbox.md. Writes no file — the local agent does that. Also returns 'axes' — which of relevance_at_pick / own_stake_at_pick actually exist for this card kind (both are NULL for emerging/trend/missing: no relevance column in those tables) — and, when the result is empty and the source supports it (missing only), 'last_run' so 'no gaps' is distinguishable from 'the analysis job is broken'.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Workspace ID"},
                        "limit": {"type": "integer", "description": "Cards to return, 2..10 (default: 5)"},
                        "source": {"type": "string", "description": "emerging | trend | missing (default: emerging)"},
                        "days_back": {"type": "integer", "description": "Lookback window in days (default: 14)"},
                    },
                    "required": ["workspace"],
                },
            },
            {
                "name": "record_card_feedback",
                "description": "Editorial loop: record which card of a weekly batch the author picked. One row per card, exactly one verdict=chosen per batch_id, the rest passed carrying the shared reason — the shared reason explains why the OTHERS were passed over, so it is never written to the chosen row. Omit chosen_entity_id for the half of a cross-workspace batch that holds no pick; a second chosen on the same batch_id is rejected with 409. A workspace that exists in config/workspaces.yml but was never bootstrapped into Postgres is rejected with 400 naming the bootstrap call, before anything is written. Append-only: re-posting the same batch_id is a no-op, so use a new batch_id to change a decision.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Workspace ID"},
                        "batch_id": {"type": "string", "description": "batch_id minted by export_inbox_cards"},
                        "cards": {"type": "array", "items": {"type": "object"}, "description": "2..10 cards as returned by export_inbox_cards (entity_kind, entity_id, title, metric_name, metric_value, relevance, own_stake, stake_quadrant, question, reason). Per-card 'reason' is optional and is the only way to say something about the chosen card itself — the batch-level reason never lands on it"},
                        "chosen_entity_id": {"type": "string", "description": "entity_id of the picked card; must match exactly one card. Omit when this call carries the chosen-less half of a cross-workspace batch"},
                        "reason": {"type": "string", "description": "Why the OTHERS were passed over. Stored on the passed rows only; the chosen row keeps NULL unless that card carries its own per-card reason"},
                        "reviewed_by": {"type": "string", "description": "Who reviewed (default: author)"},
                    },
                    "required": ["workspace", "batch_id", "cards"],
                },
            },
            {
                "name": "list_card_feedback",
                "description": "Editorial loop: accumulated card feedback. Title and the card number come from the stored snapshot, so rows survive the signal tables being rebuilt or wiped. Each row also carries batch_reason — its batch's shared 'why the others were passed over' — because the chosen row's own reason is NULL by design. The response includes axes_filled, the count of rows that actually have relevance_at_pick / own_stake_at_pick, so half-empty pairs do not accumulate unnoticed.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace": {"type": "string", "description": "Optional workspace ID filter"},
                        "limit": {"type": "integer", "description": "Max rows to return (default: 50)"},
                    },
                },
            },
        ]
    }


if __name__ == "__main__":
    import uvicorn
    from shared.config import get_settings
    settings = get_settings()
    uvicorn.run(app, host="0.0.0.0", port=settings.mcp_port)
