# Оценка текущего состояния и готовности к SaaS

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **9** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** оценка текущего состояния (факты, проверены по коду многоагентным аудитом).
> **Дата:** 2026-06-26 · Индекс: [README](./README.md)

## 1. Текущее состояние

Frontier Intelligence is a single-operator personal trend-intelligence platform that is analytically mature but commercially un-tenanted. It ingests signals from Telegram, RSS/Atom, web (crawl4ai), generic APIs, and email via a normalized multi-connector framework (~64 production-tested sources in config/sources.yml), enriches them through a sophisticated multi-provider LLM control plane (Wormsoft primary, OpenRouter, Polza, GigaChat via gpt2giga-proxy) doing relevance scoring, NER/NEL concept extraction, valence, 2560d GigaChat embeddings, and GigaChat Vision/OCR, then stores in PostgreSQL (canonical), Qdrant (hybrid dense+BM25), Neo4j (concept GraphRAG), and Cloud.ru S3. It produces semantic/trend clusters (reactive burst-score + stable HDBSCAN) framed as Insight->Opportunity->Concept, SearXNG-driven missing-signal gap analysis, and rare confirmed-spike Telegram alerts, all surfaced through ~30 MCP tools over an SSE/Streamable-HTTP gateway (:8102) consumed by Claude/Cursor — including premium-grade search_balanced (growth/counter-signal/RU-verify/competitor/blind-spots), get_frontier_brief, and a 12-tool human-gated RSI self-improvement loop. The product is organized as five curated, tuned 'workspaces' (disruption, ai_trends, ai_research, ai_products_media, design), each with relevance weights, design lenses, cross-workspace bridges, and clustering parameters — effectively ready-made intelligence feeds.

The entire commercial/tenancy/ops envelope, however, does not exist. Verified directly against the schema: there is NO users/accounts/orgs/subscriptions/api_keys/billing/quota table anywhere in init.sql or its 11 migrations (the only 'account' token is sources.tg_account_idx, a Telegram-session index); a 'workspace' is a shared curated topic-feed seeded from a checked-in YAML and UPSERTed via bootstrap, explicitly NOT a customer. All three HTTP surfaces (MCP REST :8100, MCP gateway :8102, Admin API :8101) bind 0.0.0.0 with ZERO authentication (verified: no Depends/HTTPBearer/api_key/Authorization in mcp/server.py) except one fail-open Alertmanager webhook token; the 'workspace' parameter is client-controlled free text defaulting to 'disruption', so any caller can read or mutate any workspace and trigger irreversible Neo4j entity merges. Tenancy is soft application-level WHERE workspace_id filtering with no Postgres RLS, a single shared Qdrant collection, and a single shared Neo4j graph. The metering substrate is unusually tenant-ready by design (ExecutionReceipt and CostAggregateState carry workspace_id; ProviderBudgetManager has a cost_workspace Redis scope) but is dead code in production: chat()/vision()/embed() in llm_router_client.py take no workspace_id (verified — only 3 occurrences in the whole file, none in the signatures) even though enrichment_task.py has it 28 times, so receipt.workspace_id is always empty; 'cost' is raw billable-token counts not money (no price-table applied); and all metering is volatile Redis with ~3-day TTL with no durable Postgres ledger. There is no payment integration of any kind (verified: zero YooKassa/Stripe/payment code). Deployment is a single-node Docker Compose stack with no HA, no backups/DR, no real CI/CD (one echo-only GitHub workflow), unversioned raw-SQL migrations applied to prod, secrets in a single plaintext .env, a global single-flight LLM pacing guard (~1 inflight call, no per-tenant fairness), shared Telegram accounts/xray egress across all workspaces, and no per-tenant observability. RU-residency is favorable by default (GigaChat Sber endpoint, Cloud.ru S3 ru-central-1) but there is no formal 152-FZ personal-data handling.

## 3. Готовность к SaaS

### Готово к переиспользованию

- Analytical engine and feed catalog: 5 curated/tuned workspaces + ~64 live-tested sources are packageable as subscribable intelligence feeds with no further analytical work
- ~30 MCP tools over a working SSE/Streamable-HTTP gateway (:8102) already consumed by Claude/Cursor — the entire resellable API surface exists, including search_balanced, get_frontier_brief, search_by_vision, get_concept_graph
- Multi-provider LLM router (Wormsoft->OpenRouter->Polza->GigaChat) with circuit breaker, fallback chains, and pooled platform keys — matches the confirmed PLATFORM-POOLED key model exactly
- Stateless worker on Redis consumer groups (group=enrichment_workers, consumer=worker-{uuid}, stale-pending reclaim) — already supports N replicas at the architecture level
- Human-gated RSI quality loop (12 MCP tools) — a defensible quality moat already implemented
- Real per-model price tables already fetched (wormsoft /api/money/token-pricing input/output/cache rates) — the data needed to convert tokens to RUB exists, just unapplied

### Требует доработки

- Tenancy: workspace_id is threaded through ~24 files and almost all core tables but means 'feed' not 'customer'; needs an account/tenant root above it and RLS or filter-chokepoint hardening (child tables source_checkpoints/source_runs/indexing_status/post_enrichments lack workspace_id and leak via JOIN)
- Metering: ExecutionReceipt/CostAggregateState/cost_workspace scope are tenant-ready but workspace_id is never propagated into chat/vision/embed (verified dead code) — a small wiring fix activates the whole latent path
- Cost accounting: 'cost' is billable-token counts, not money; a pricing resolver must map (provider,model,tokens)->RUB using the existing price snapshots, splitting estimated vs actual
- Quota/fairness: only global per-provider env caps and one global single-flight guard exist; needs per-tenant leaky-bucket (Redis primitive already present: WormsoftSharedGuard/openrouter_picker Lua reserve_slot) and per-workspace entitlements
- Auth seam: FastAPI router structure + the Alertmanager token-extraction pattern + gateway/REST separation give clean insertion points, but no actual auth dependency exists yet
- Observability: Prometheus/Alertmanager/Grafana wired but with zero per-tenant dimension; needs a workspace_id label (watch cardinality)

### Отсутствует (greenfield)

- Identity & auth: no users/accounts/orgs/api_keys/roles tables or any auth on customer or admin surfaces — fully greenfield
- Billing & payments: no YooKassa integration, no subscriptions/plans/entitlement tables, no invoicing, no НПД-receipt generation (самозанятый uses NPD receipts via FNS 'Мой налог'/partner API, not 54-FZ ККТ)
- Durable usage ledger: no Postgres usage-events table; all metering evaporates after ~3 days, cannot reconstruct a billing month
- Self-serve onboarding: no signup, no customer portal, no feed catalog UX; provisioning is manual YAML edit + bootstrap
- Multi-channel per-tenant notifications: alerts are Telegram-only single-recipient
- Backups/DR: no pg_dump, snapshot, or restore tooling anywhere; single local Docker volumes
- HA / horizontal data tier: single Postgres/Qdrant/Neo4j/Redis, single pinned ingest, no replication or failover
- CI/CD & staging: no test gate, no image registry/versioning, no staging, no rollback; deploy is rsync+rebuild-on-prod
- Secrets management: plaintext .env, no Vault/SOPS, no rotation; Grafana defaults to admin/admin
- 152-FZ controls: no PDn classification, retention/erasure, or routing policy preventing RU personal data from reaching non-RU providers (OpenRouter/Polza) or xray egress
- Revenue-cap guardrail: nothing enforces or even tracks aggregate revenue against the 2.4M RUB/year НПД ceiling

## 4. Приоритизированные гэпы

Severity: **blocker** = блокирует первого платного клиента · **major** = критично для устойчивости · **minor** = улучшение. Effort: S/M/L/XL.

| # | Severity | Effort | Слой | Гэп |
|---|---|---|---|---|
| 1 | blocker | M | llm-finops-controlplane | Cost is measured in raw billable tokens, not money; real per-model RUB price tables are fetched but never applied, so a tenant cannot be billed in rubles |
| 2 | blocker | M | llm-finops-controlplane | No durable usage/billing ledger: all metering in Redis with ~3-day TTL; a billing month cannot be reconstructed for invoicing/audit |
| 3 | blocker | M | ops-deploy-security | No backups or disaster recovery anywhere (no pg_dump/snapshot/restore); all tenant data in single local Docker volumes — for a paid product this is existential |
| 4 | blocker | M | llm-finops-controlplane | No per-tenant plan/quota/overage enforcement; only global per-provider env caps — one tenant can drain shared LLM budget and overage is undefined |
| 5 | blocker | L | serving-surface-auth | Zero authentication on all three surfaces (MCP REST :8100, MCP gateway :8102, Admin :8101); workspace is client-controlled free text so any caller reads/mutates any tenant and triggers irreversible Neo4j merges |
| 6 | blocker | L | tenancy-data-model | No identity/account/users/api_keys model exists at all (verified: zero such tables) — the SaaS root that everything hangs off is absent |
| 7 | blocker | S | llm-finops-controlplane | Per-tenant metering is dead code: workspace_id never propagated into chat/vision/embed despite ExecutionReceipt/cost_workspace being tenant-ready — zero tenant-attributed usage is ever written |
| 8 | blocker | L | billing-payments | No billing/payment integration: no YooKassa, no plans/subscriptions/entitlement tables, no НПД-receipt generation (самозанятый NPD receipts via FNS, not 54-FZ ККТ) |
| 9 | major | S | serving-surface-auth | No per-tenant rate limiting at the HTTP edge; a single client can exhaust LLM budget or DoS the pipeline (Redis leaky-bucket exists as a primitive but is not wired here) |
| 10 | major | L | compliance-legal | No 152-FZ personal-data controls: ingested Telegram content/handles/media may be RU PDn; non-RU providers (OpenRouter/Polza) and xray egress could route it abroad with no gating |
| 11 | major | M | ops-deploy-security | Secrets in single plaintext .env, no manager/rotation; Grafana defaults to admin/admin |
| 12 | major | L | ops-deploy-security | Single-node, no HA: data+workers+ingest+MCP+admin on one host; node failure = full outage; ingest pinned single-instance |
| 13 | major | S | billing-payments | No aggregate-revenue guardrail against the 2.4M RUB/year НПД ceiling — exceeding it forces re-registration as ИП/ООО (which then needs 54-FZ ККТ), so the cap must be tracked and capacity-limited |
| 14 | major | M | ingestion-sources | Shared Telegram accounts and shared xray egress across all tenants — one tenant's FloodWait/ban degrades ingestion for everyone |
| 15 | major | M | ops-deploy-security | Single-process APScheduler in admin runs all crons (clustering/RSI/alerts) — admin down = whole loop down; fragile for any SLA |
| 16 | major | M | tenancy-data-model | Qdrant single shared collection and Neo4j single shared graph isolated only by per-query predicate — one missing filter leaks all tenants' data |
| 17 | major | L | tenancy-data-model | Tenancy is soft WHERE workspace_id filtering with no RLS; child tables (source_checkpoints, source_runs, indexing_status, post_enrichments) lack workspace_id and leak via JOIN; cluster_runs/admin_manual_jobs allow NULL workspace_id |
| 18 | major | M | serving-surface-auth | Customer-facing read API and destructive admin/ops control plane (CRUD, pipeline triggers, RSI approve, entity merge) share the same non-existent auth posture and surfaces |
| 19 | major | M | ops-deploy-security | Global single-flight LLM guard (~1 inflight) with no per-tenant fairness; one busy workspace starves all others and adding workers does not help |
| 20 | major | M | ops-deploy-security | No real CI/CD, no staging, no rollback, unversioned raw-SQL migrations applied to prod; bad build hits prod directly (untenable when solo-operating paying tenants) |
| 21 | major | S | serving-surface-auth | Wildcard CORS and disabled MCP transport security (allowed_hosts/origins=['*'], DNS-rebinding off) enabling cross-origin/rebinding attacks combined with no auth |
| 22 | major | S | llm-finops-controlplane | No durable per-tenant usage idempotency/de-dup (HINCRBY day-keyed, shadow execs metered identically) — retries/reprocessing double-count and would mis-charge |
| 23 | minor | S | llm-finops-controlplane | Wormsoft has no live remaining-credit endpoint; platform-level budget is only estimable from a 1:1 token=credit approximation |
| 24 | minor | S | serving-surface-auth | Alertmanager webhook token fails open when unset — the lone auth control silently disables on misconfig |
| 25 | minor | M | tenancy-data-model | Workspace identity is a global human slug (PK 'disruption'); two tenants cannot both have a 'disruption' feed and natural keys lack an account dimension |
| 26 | minor | M | product-intent-roadmap | Provisioning is manual YAML edit + bootstrap, not self-serve; no signup or feed-catalog UX — contradicts the no-employees/solo-operable НПД constraint that demands automation |
| 27 | minor | M | product-intent-roadmap | Alerts/notifications are Telegram-only single-recipient with no per-tenant destinations or preferences |

## 5. Переиспользуемые активы

- workspace_id already threaded through ~24 files and almost every core Postgres table, Qdrant payload filter (KEYWORD index), and Neo4j root node — a real tenant column to build the account dimension on
- ExecutionReceipt + CostAggregateState (shared/llm_control_plane.py) already carry workspace_id, cost_estimate, actual_cost, cost_drift, prompt/completion/billable tokens — directly usable as the Postgres usage-event row schema
- ProviderBudgetManager cost_workspace Redis scope + snapshot_costs(workspace_ids=...) (worker/provider_budget_manager.py) — per-tenant metering plumbing already implemented, only needs workspace_id written
- wormsoft_limits._normalize_pricing() — the only real per-model input/output/cache RUB price table in the system; the missing piece to convert tokens to money
- openrouter_picker Lua atomic reserve_slot + WormsoftSharedGuard Redis-Lua slot/quarantine — production-grade rate-limit primitives reusable as per-tenant leaky buckets
- Qdrant filter chokepoints _build_payload_filter/_build_trend_filter (worker/integrations/qdrant_client.py) — single points to harden for hard tenant isolation, with existing contract tests (tests/test_qdrant_contracts.py) to extend
- Neo4j ensure_workspace_node + per-query {workspace_id:$ws} predicate (worker/integrations/neo4j_client.py) — graph tenant property already in place
- 5 curated/tuned workspaces (config/workspaces.yml) + ~64 live-tested sources (config/sources.yml, incl. a full T-Bank competitor-monitoring pack) — a ready-to-sell feed inventory and proven use case
- ~30 MCP tools over the working FastMCP gateway :8102 (search_frontier, search_balanced, get_frontier_brief, search_by_vision, get_concept_graph, trend clusters, missing signals) — the resellable API surface, already isolated from REST :8100 giving a clean auth seam
- Multi-LLM router with Wormsoft->OpenRouter->Polza->GigaChat fallback + provider circuit breaker — matches the platform-pooled-key model and is the abstraction point for per-tenant cost attribution
- Stateless worker on Redis consumer groups with stale-pending reclaim — supports horizontal scaling once the global LLM guard is replaced with per-tenant fairness
- bootstrap_configs.py YAML->Postgres UPSERT pattern — repurposable as the seed path for programmatic per-account provisioning
- FastAPI router structure (app.include_router) + Alertmanager token-extraction pattern (Basic/header/query) in admin/backend/routers/monitoring.py — a working pattern to seed a real auth dependency attached at include_router level
- workspaces.extra JSONB already stores per-workspace tuning that the RSI loop mutates — a working pattern for per-tenant config/entitlement overrides
- Idempotent init.sql + numbered migrations under storage/postgres/migrations — clean place to add accounts/users/RLS/usage-ledger migrations (Alembic is already in the SQLAlchemy 2.x stack)
- trend_alerts dedupe table + weekly-cap selection logic (admin scheduler) — reusable notification selection engine for multi-channel per-tenant delivery
- RU-resident infra defaults (GigaChat Sber endpoint, Cloud.ru S3 ru-central-1) — favorable 152-FZ starting point for the RU-first market
- Observability already wired (Prometheus/Alertmanager/Grafana frontier-runtime.json/frontier-rsi.json, MCP /metrics, stream-lag alerts) — needs only a tenant dimension, not a greenfield build
- Qdrant alias cutover + versioned backfill scripts — a zero-downtime reindex pattern reusable for migrations and embedding-version swaps

## 6. Ключевые риски

- НПД revenue cap (2.4M RUB/year) caps the achievable MRR at ~200K RUB/month; the business model must hit profitability inside that ceiling AND track aggregate revenue to avoid involuntarily breaching it (which forces ИП/ООО re-registration and then 54-FZ ККТ). This makes a small number of higher-priced B2B feed subscriptions far safer than high-volume low-ASP self-serve.
- No-employees constraint means everything must be solo-operable: the current hand-operated deploy (rsync+rebuild-on-prod, no CI/CD/backups/HA, single-process scheduler that takes down all crons) is the single biggest operational risk to a one-person paid SaaS — an outage or data loss with no DR is unrecoverable solo.
- Security exposure is acute: three 0.0.0.0-bound surfaces with zero auth and wildcard CORS/disabled DNS-rebinding protection mean that the moment any port is reachable, any party can read all intelligence, mutate config, and trigger irreversible Neo4j entity merges. This must be closed before any external tenant exists.
- Billing correctness risk: cost is token-counts not money, metering is dead code (workspace_id never propagated) and volatile (3-day Redis TTL), with no idempotency — so today the system literally cannot produce a defensible per-tenant invoice. Shadow executions are metered identically and would over-bill tenants.
- 152-FZ exposure: ingested Telegram content (author handles, message bodies, media) likely contains RU-citizen personal data; non-RU providers (OpenRouter, Polza) and xray egress can route it abroad with no gating, and there is no PDn classification/retention/erasure. For a RU-market product this is a legal/RKN-localization risk, not just a feature gap.
- Data-licensing risk: feeds are third-party RSS/scraped newsroom content; reselling synthesized intelligence over them may require a ToS/licensing review before commercial packaging.
- Product-model ambiguity (shared feeds vs private per-tenant workspaces) blocks the data-model migration — choosing wrong forces an expensive rework of the entire tenancy and metering design.
- Slug-PK migration risk: changing workspace.id from 'disruption' to account-namespaced/UUID breaks existing Claude/Cursor MCP clients that pass workspace='disruption' literally.

## 7. Вопросы, требующие решения владельца

_Часть уже закрыта ответами (гибрид / платформенные ключи / YooKassa / НПД). Ниже — то, что осталось; сведено в [10-decision-log](./10-decision-log.md)._

- Tenancy model: is a customer a single workspace, or an account that owns multiple workspaces? And are the 5 curated feeds SHARED (many tenants subscribe to the same 'ai_trends' data — simplest, fits the HYBRID curated-feed product) while the 'limited self-serve custom sources' on higher tiers get PRIVATE per-tenant workspaces? This single decision drives the entire data-model and metering migration.
- Isolation tier: shared DB + Postgres RLS (cheapest, pragmatic first step, fits a solo operator) vs schema-per-tenant vs db-per-tenant? Given НПД (low tenant count, must stay solo-operable), RLS seems indicated — confirm acceptable.
- Billing model: pass-through pooled-LLM cost + markup, fixed plan with included credits, or pure metered? This determines whether you need full plan/overage tables or just a usage ledger, and how the shared Wormsoft credit budget is fairly attributed across tenants.
- НПД receipt mechanism: should the system auto-generate NPD receipts via the FNS 'Мой налог' API / a partner aggregator (e.g. via YooKassa's самозанятый flow) on each YooKassa payment, and how should aggregate revenue be tracked/capped against the 2.4M RUB/year ceiling so you never breach it involuntarily?
- Should shadow executions (execution_role=shadow) and the RSI/platform-quality LLM spend be absorbed as platform cost rather than billed to tenants? Today they are metered identically and would over-bill.
- 152-FZ stance: must PDn-bearing payloads be pinned to GigaChat/RU-only providers, or may OpenRouter/Polza/xray egress receive them? And is RKN data-localization registration in scope for RU-citizen personal data?
- Auth/transport for MCP specifically: are Claude/Cursor reliably able to send a static bearer/API-key header on the streamable-HTTP session in your target deployment, or is a per-tenant gateway URL / token-in-path needed? This shapes the customer auth design.
- Which surface is the product: the MCP gateway (:8102) for agent/Claude consumption, a future REST/web API, or a customer portal? And should the admin API ever be customer-reachable (limited self-serve source management on higher tiers) or stay operator-only behind VPN?
- International-payments phase: when international payments arrive later, do you intend to outgrow НПД (re-register as ИП/ООО, accept 54-FZ ККТ and Stripe/Paddle), or keep НПД + YooKassa and treat international as out-of-scope until then? This affects how much to invest now in payment abstraction.
- Embeddings portability: embeddings are strict-GigaChat (2560d) with the multi-provider switch deferred — is RU-only embedding acceptable for the target market, or is a Western-provider embedding path + reindex migration needed (large effort) for any international expansion?
