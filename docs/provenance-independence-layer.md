# Provenance & Independence Layer — Spec

Status: **Layers 1–3 + P2-heuristic implemented 2026-07-14** (see "Implementation
status" below); only P3 (research) remains. Prerequisite for a real (non-proxy)
signal-nature discriminator and for trustworthy source-diversity in cross-source fusion.

## Implementation status (2026-07-14)

Landed (additive — raw `source_count` / `source_diversity_score` untouched, so scoring
and promotion gates are unchanged):

- `shared/provenance.py` — pure core: `canonical_url`, `same_artifact_groups`,
  `echo_edges`, `independence_metrics` (deduped_source_count, distinct_voices,
  echo_ratio, arrival_dispersion, single_day_spike, independence_score). Fully unit
  tested (`tests/test_provenance.py`, 15 cases; verified locally).
- Clustering wiring (`worker/services/semantic_clustering.py`): provenance computed on
  member posts in `_semantic_results` and `_signal_results`; persisted on
  semantic_clusters / trend_clusters / emerging_signals; mirrored into the trend Qdrant
  payload. ECHO_OF edges collected per semantic cluster.
- Migration `storage/postgres/migrations/20260714_provenance_dedup.sql` + ORM columns.
- Neo4j (`worker/integrations/neo4j_client.py`): `upsert_echo_edges` writes
  `(:Document)-[:ECHO_OF {method,score,lag_hours}]->(:Document)` + Document.first_seen_at,
  wired best-effort (never fails clustering) in `run_semantic_clustering`;
  `count_originators` ready.
- MCP (`mcp/tools/observability.py`): new fields exposed in list_clusters /
  get_cluster_details (SELECT *) / get_cluster_evidence / get_signal_timeline.
- Joint enrichment prompt enum widened with `person|org` so originators get tagged.
- **Layer 3 originator fill** — `count_originators_batch` (Neo4j) + `_apply_originators`
  run as a best-effort post-pass in `run_semantic_clustering`, filling
  `distinct_originators` and recomputing `independence_score` (`compute_independence_score`)
  for semantic/trend/emerging rows. NOTE: Concept.category org/person only populates for
  posts enriched AFTER the prompt change, so `distinct_originators` stays 0/NULL for old
  posts until an enrichment reprocess; `independence_score` falls back to
  deduped_source_count meanwhile.
- **Merged clusters** now recompute provenance from unioned member posts in both merge
  paths (`_merge_semantic_candidates`, `_merge_signal_candidates`).

Deferred:

- P3 only — homoplasy CI/RI (needs a phylogenetic build), attractor-strength (needs
  transmission chains), and the second-model artifact cross-check (needs a second
  embedding model deployed). Each is blocked on data/infra, not code.

**Deploy order (matters):** apply the migration FIRST (idempotent, ADD COLUMN IF NOT
EXISTS), THEN rebuild+deploy worker and mcp images — the new INSERTs reference the new
columns. Verify by running one `run_semantic_clustering` and reading a multi-source
cluster via `get_cluster_evidence` (expect `deduped_source_count` <= `source_count`,
`echo_ratio` > 0 on re-syndicated clusters).

## Why this exists

Two gaps are actually one missing layer:

1. **`source_count` / `source_diversity_score` over-count independence.** They count
   distinct *feeds*, not distinct *originators/events*. Re-syndication reads as
   independent convergence. This distorts `signal_score`, `source_diversity`, emergence
   promotion and any downstream that trusts source multiplicity — across all workspaces,
   not just one lens.
2. **No provenance / lineage.** There is no "which came first / who echoed whom" relation.
   `get_concept_graph` exposes concept co-occurrence, not copy edges. So the
   replication-vs-attraction question in `signal-nature-lens.md` can only run on proxies.

Both are fixed by adding a cross-source dedup + first-seen layer over existing posts.

### Evidence (live probes, disruption workspace, 2026-07-14)

- Semantic cluster "UX Debt": `source_count=2` → one identical Medium URL through two
  tag-feeds (`rss_medium_design` + `rss_medium_ux`) → really 1 voice.
- Trend "fuel maps": `source_count=14`, `source_diversity_score=0.7368` → ~3 originating
  events, mostly one Sber press release echoed across feeds.
- Same article `3dnews.ru/1144859` ingested under two `source_id`s and never collapsed —
  current dedup (`guid_or_url` + `canonicalize_url`) is **within-source only**.

## What already exists (do not rebuild)

- Posts with `url`, `published_at`, `source_id`; dense (GigaR 2560d) + BM25 sparse
  embeddings in Qdrant; concept graph + clusters in Neo4j; NER/NEL in enrichment;
  per-source `linked_ratio` scalar; `get_signal_timeline` (per-window `source_ids` /
  `post_ids`), `get_cluster_evidence` (per-post `url` / `published_at` / `source_id`).
- Missing: cross-source canonical dedup, near-duplicate text grouping, provenance edges,
  originator-vs-feed resolution, exposed first-seen ordering.

## Design — five layers (build in order)

### Layer 1 — Cross-source canonical + near-duplicate dedup  (P0, fixes the bug)

- **Canonical URL** across sources: strip UTM/tracking params, unify scheme/host, fold
  AMP/mobile variants. Extend the existing `canonicalize_url` from within-source to a
  cross-source grouping key.
- **Near-duplicate text**, two stages:
  - *blocking*: MinHash/SimHash + LSH over shingles of `title` + `preview` → candidate pairs
    (cheap, scales).
  - *verify*: sentence-embedding cosine (reuse the GigaR dense vectors) with threshold
    **≥ 0.60** (the "Rewrite the News" news-reuse recipe); use RETSim-style resilient
    embeddings only if paraphrase robustness proves insufficient.
- Output: a `SAME_ARTIFACT` group. One canonical post = a "voice"; the rest are echoes.
- **Metric fix**: recompute `source_count` / `source_diversity_score` **after** collapsing
  echoes → distinct originating artifacts, not distinct feeds. This is the highest-value,
  cheapest win and unblocks trustworthy source-diversity for fusion work.
- Calibrate the 0.60 threshold on labeled data (below); flag a false-positive when a
  candidate *predates* its assigned "source".

### Layer 2 — First-seen / provenance edges (Neo4j)  (P1)

- New edge `(:Post)-[:ECHO_OF {method, score, lag}]->(:Post)` pointing from an echo to the
  earliest post in its `SAME_ARTIFACT` group (`min(published_at)` = candidate origin).
- Group-level `first_seen_at`. Optional directionality (news→blog vs blog→news, MemeTracker
  style) if source type is available.
- This is **"earliest in the same-material group"**, not proven copying. Honest limits:
  `published_at` is publication time, not discovery/copy time; timeline windows are daily,
  so genuinely simultaneous independent launches are indistinguishable from echoes.

### Layer 3 — Actor / originator resolution  (P1)

- From NER/NEL + keywords, extract **named actors** (orgs/products: Sber, Alfa, Yandex, 2GIS).
- "Independence" = count of distinct **originators**, not distinct feeds.
- **Circularity guard** (from the red-team): actor extraction runs through the same
  enrichment LLM whose priors are the artifact risk. Count an actor as an *originator* only
  when it is the subject/source of the item, not merely mentioned, and cross-check against
  Layer 1 — if all "actors" live in one `SAME_ARTIFACT` group, it is one material, not N.
- Reuse the entity-resolution planned for the RSI retro-loop (see `docs/rsi.md`) — do not
  build a second NEL path.

### Layer 4 — Independence & attractor metrics (optional)  (P2)

- `independence_score` per cluster = f(distinct originators after dedup, canonical-URL
  dedup ratio, arrival dispersion [penalize single-day multi-source spikes], source
  diversity as tiebreak).
- **Shared-trigger control** (theory's key caveat): mutual independence ≠ independence from
  a common upstream cause. Flag clusters whose burst starts ≤ N days after a known external
  release/event; a simultaneous cold-start of many "independent" actors is itself a
  shared-trigger suspicion, not proof of an attractor.
- Research-grade, later: phylomemetic **CI/RI** (homoplasy: low CI = independent
  convergence) over a concept-presence-by-cluster matrix; iterated-learning
  **attractor-strength** `l = I/(1−s)`, strength `= 1−s`.

### Layer 5 — Artifact cross-check (optional)  (P3)

- A second embedding model or ensemble-agreement score. Convergence that survives across
  models = corpus signal; convergence unique to `EmbeddingsGigaR` = the pipeline's own
  attractor. Today everything sits behind one embedding model and `coherence_score` is
  that model's own density, so the artifact arm of the lens is currently untestable.

## Expose in MCP

Add to `list_clusters` / `get_cluster_details`: `deduped_source_count`,
`distinct_originators`, `echo_ratio`, `first_seen_at`, `arrival_dispersion`, and
(when Layer 4 lands) `independence_score`. Once exposed, `signal-nature-lens.md` stops
being proxy-only — its Gate items become directly answerable from the tool instead of
hand-estimated.

## Calibration / validation

- Hand-label a set of clusters as known-independent vs known-syndicated.
- Measure dedup precision/recall across cosine thresholds; pick the threshold from data.
- Do not trust `independence_score` until validated on the labeled set. No external
  benchmark exists (MCP registry has zero provenance connectors; GDELT's MCP gives
  narrative clusters but no exposed originator/first-seen and only over public news).

## Priority order

- **P0**: Layer 1 canonical-URL dedup → immediate `source_count` fix.
- **P0/P1**: Layer 1 near-duplicate text (MinHash/LSH + cosine≥0.60 verify).
- **P1**: Layer 2 `ECHO_OF` edges + `first_seen`; Layer 3 originator count; recompute
  metrics + expose in MCP.
- **P2**: Layer 4 independence score + calibration + shared-trigger control.
- **P3**: Layer 4 homoplasy/attractor-strength; Layer 5 cross-model artifact check.

## Risks & anti-goals

- Do not treat named-actor count as independence without the Layer 1 cross-check — that
  reintroduces the same LLM prior as the confound.
- Do not surface proxy scores as facts; keep them inside the Confidence Model
  (`Гипотеза` / `Вероятно`), never `Подтверждено`.
- Do not pull in an external provenance connector — none fit this corpus.

## Relations

- Unblocks the real discriminator in `signal-nature-lens.md`
  (`.claude`/`.agents` × `visionary-designer`/`designer-ai-visionary`).
- Prerequisite for reliable `source_diversity` in `docs/trend-detection-future-roadmap.md`
  §4 (Cross-Source Fusion) — that fusion trusts source multiplicity this layer corrects.
- Reuse entity-resolution from `docs/rsi.md` (RSI retro-loop) for Layer 3.
