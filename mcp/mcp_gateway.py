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


def _raise_for_status_with_detail(response: httpx.Response) -> None:
    """raise_for_status, но с текстом detail из тела ответа.

    httpx собирает сообщение HTTPStatusError только из статуса и URL, тело он не читает:
    подсказка «этот воркспейс не забутстраплен, вызови POST /api/workspaces/bootstrap»
    до автора не доходит, он видит голое «Client error '400 Bad Request'». FastMCP
    показывает клиенту str(exc), поэтому detail надо положить в само сообщение.

    Тип исключения остаётся httpx.HTTPStatusError — то же, что бросал бы raise_for_status.
    """
    if not response.is_error:
        return
    detail: str | None = None
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        raw = payload.get("detail")
        if isinstance(raw, str) and raw.strip():
            detail = raw.strip()
    if detail is None:
        # Тело не в форме {"detail": "..."} — добавить нечего, поведение прежнее.
        response.raise_for_status()
        return
    raise httpx.HTTPStatusError(
        f"{response.status_code} {response.reason_phrase} from {response.url}: {detail}",
        request=response.request,
        response=response,
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


@mcp.tool(
    description=(
        "Editorial loop: pull top signals for a workspace as inbox cards (one topic line, "
        "one number traceable to a DB column, empty question slot). Returns batch_id, cards "
        "in the exact shape record_card_feedback accepts, and markdown for content/inbox.md. "
        "Writes no file. Also returns 'axes' — which of relevance_at_pick / own_stake_at_pick "
        "exist for this card kind (both NULL for emerging/trend/missing: those tables have no "
        "relevance column) — and, on an empty result from a source that exposes it (missing "
        "only), 'last_run', so 'no gaps' is distinguishable from 'the analysis job is broken'."
    )
)
async def export_inbox_cards(
    workspace: str,
    limit: int = 5,
    source: str = "emerging",
    days_back: int = 14,
) -> dict:
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/export_inbox_cards",
            json={
                "workspace": workspace,
                "limit": limit,
                "source": source,
                "days_back": days_back,
            },
        )
        r.raise_for_status()
        return r.json()


@mcp.tool(
    description=(
        "Editorial loop: record which card of a weekly batch the author picked. One row per "
        "card, exactly one chosen per batch_id, the rest passed carrying the shared reason. "
        "The shared reason explains why the OTHERS were passed over, so it is never stored on "
        "the chosen row — to say something about the picked card, put a per-card 'reason' on "
        "that card. Omit chosen_entity_id when this call carries the half of a cross-workspace "
        "batch that holds no pick — a second chosen on the same batch_id is rejected with 409. "
        "A workspace present in config/workspaces.yml but never bootstrapped into Postgres is "
        "rejected with 400 naming the bootstrap call, before anything is written. "
        "Append-only — re-posting the same batch_id is a no-op, use a new batch_id to "
        "change a decision."
    )
)
async def record_card_feedback(
    workspace: str,
    batch_id: str,
    cards: list[dict],
    chosen_entity_id: str | None = None,
    reason: str | None = None,
    reviewed_by: str = "author",
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/record_card_feedback",
            json={
                "workspace": workspace,
                "batch_id": batch_id,
                "cards": cards,
                "chosen_entity_id": chosen_entity_id,
                "reason": reason,
                "reviewed_by": reviewed_by,
            },
        )
        # Единственный инструмент, у которого 4xx-тело — инструкция к починке (400 «сначала
        # забутстрапь воркспейс», 409 «у батча уже есть chosen, пришли новый batch_id»),
        # а разметка пятёрки при этом набрана руками и теряется. Остальные инструменты
        # шлюза намеренно не трогаю — это отдельный проход.
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "Editorial loop: accumulated card feedback. Title and card number come from the stored "
        "snapshot, so rows survive the signal tables being rebuilt or wiped. Each row carries "
        "batch_reason — its batch's shared 'why the others were passed over' — because the "
        "chosen row's own reason is NULL by design. The response includes axes_filled: how many "
        "rows actually have relevance_at_pick / own_stake_at_pick."
    )
)
async def list_card_feedback(
    workspace: str | None = None,
    limit: int = 50,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_card_feedback",
            json={"workspace": workspace, "limit": limit},
        )
        r.raise_for_status()
        return r.json()


# ─────────────────────────────────────────────────────────────────────────────
# Контур RSI: чтение предложений и человеческий гейт.
#
# Эти десять инструментов существовали в REST (mcp/tools/graph_health.py и
# mcp/tools/threshold_proposals.py, подключены роутерами в mcp/server.py) и НЕ
# были выведены сюда. А единственный MCP-клиент ходит через шлюз, поэтому
# «одобрить» было физически не из чего: на 04.08.2026 в entity_merge_proposals
# висело 86 строк со статусом pending с 07.07, последнее одобрение — 05.07;
# у всех 26 911 строк relevance_decisions поле audit_status осталось NULL за всю
# историю. Контур самоулучшения был построен целиком и не мог сделать ни одного
# оборота из-за отсутствия обёрток.
#
# ВНИМАНИЕ. Четыре из десяти — ПИШУЩИЕ, и они серьёзнее всего, что было в шлюзе
# раньше (запись отзыва о карточке, постановка URL в очередь краула):
#   approve_entity_merge      — сливает два концепта в графе Neo4j;
#   reject_entity_merge       — закрывает предложение;
#   approve_threshold_change  — меняет порог детектора в workspaces.extra;
#   mark_relevance_audit      — пишет человеческий вердикт в relevance_decisions.
# Шлюз опубликован на 0.0.0.0:8102 без аутентификации — решение владельца от
# 04.08.2026 (хост в локальной сети). Условие пересмотра, записанное там же
# («новый пишущий инструмент серьёзнее записи отзыва»), этой правкой наступило.
# См. docs/AUDIT-2026-08-04.md, раздел «Принятые решения».
# ─────────────────────────────────────────────────────────────────────────────


@mcp.tool(
    description=(
        "RSI: concept-graph health — orphan ratio, duplicate clusters, density, plus the "
        "duplicate groups themselves for audit. Read-only; merging is a separate operator job."
    )
)
async def get_graph_health(workspace: str, duplicates_limit: int = 25) -> dict:
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/get_graph_health",
            json={"workspace": workspace, "duplicates_limit": duplicates_limit},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "RSI loop D+: semantic concept-merge proposals awaiting a human verdict "
        "(acronym vs expansion and similar). Defaults to pending."
    )
)
async def list_entity_merge_proposals(
    workspace: str | None = None,
    status: str = "pending",
    limit: int = 50,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_entity_merge_proposals",
            json={"workspace": workspace, "status": status, "limit": limit},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "RSI loop D+: APPROVE a concept merge. This MUTATES the Neo4j graph — the two "
        "concepts are merged into canonical + alias and cannot be split back automatically."
    )
)
async def approve_entity_merge(proposal_id: str, reviewed_by: str = "operator") -> dict:
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/approve_entity_merge",
            json={"proposal_id": proposal_id, "reviewed_by": reviewed_by},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(description="RSI loop D+: reject a concept-merge proposal. The graph is left untouched.")
async def reject_entity_merge(
    proposal_id: str,
    reviewed_by: str = "operator",
    note: str | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/reject_entity_merge",
            json={"proposal_id": proposal_id, "reviewed_by": reviewed_by, "note": note},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "RSI: threshold-change proposals for the weak-signal detector, awaiting a human "
        "verdict. Defaults to pending."
    )
)
async def list_threshold_proposals(
    workspace: str | None = None,
    status: str = "pending",
    limit: int = 50,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_threshold_proposals",
            json={"workspace": workspace, "status": status, "limit": limit},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "RSI loop B: weak candidates that a judge from a different model family considered "
        "novel — blind spots of the primary detector that were caught."
    )
)
async def list_underrated_signals(
    workspace: str | None = None,
    days_back: int = 30,
    limit: int = 30,
) -> dict:
    async with httpx.AsyncClient(timeout=45.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_underrated_signals",
            json={"workspace": workspace, "days_back": days_back, "limit": limit},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "RSI loop C: sample of rejected posts for manual audit, ordered by score — the ones "
        "closest to the threshold are the likeliest mistakes."
    )
)
async def list_relevance_audit_sample(
    workspace: str | None = None,
    days_back: int = 30,
    limit: int = 20,
) -> dict:
    async with httpx.AsyncClient(timeout=45.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/list_relevance_audit_sample",
            json={"workspace": workspace, "days_back": days_back, "limit": limit},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "RSI loop C: record a human verdict on a rejected post. WRITES to relevance_decisions. "
        "verdict must be 'false_negative' (rejected in error) or 'correct_reject'."
    )
)
async def mark_relevance_audit(
    post_id: str,
    verdict: str,
    reviewed_by: str = "operator",
    note: str | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/mark_relevance_audit",
            json={
                "post_id": post_id,
                "verdict": verdict,
                "reviewed_by": reviewed_by,
                "note": note,
            },
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(
    description=(
        "RSI: APPROVE a threshold change. This WRITES the new value into "
        "workspaces.extra->cluster_analysis and changes detector behaviour from the next run."
    )
)
async def approve_threshold_change(proposal_id: str, reviewed_by: str = "operator") -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/approve_threshold_change",
            json={"proposal_id": proposal_id, "reviewed_by": reviewed_by},
        )
        _raise_for_status_with_detail(r)
        return r.json()


@mcp.tool(description="RSI: reject a threshold-change proposal. Thresholds are left untouched.")
async def reject_threshold_change(
    proposal_id: str,
    reviewed_by: str = "operator",
    note: str | None = None,
) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{REST_BASE}/tools/reject_threshold_change",
            json={"proposal_id": proposal_id, "reviewed_by": reviewed_by, "note": note},
        )
        _raise_for_status_with_detail(r)
        return r.json()


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
