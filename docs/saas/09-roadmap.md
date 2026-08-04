# Дорожная карта (поэтапный план)

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **6** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

# Phased, Solo-Operable Roadmap

Effort tags: **S** ≤0.5d · **M** ~1–2d · **L** ~3–5d · **XL** >1wk. Phases are ordered so the cheapest high-leverage and existential-risk wins land first. Each workstream lists its owning domain section.

---

## Phase 0 — Security lockdown + activate the latent meter (cheapest, highest leverage)
**Goal:** stop the bleeding (close fail-open holes, unpublish destructive surface) and light up the dead metering path *behind a flag, observe-only* — before any money or auth depends on it. None of this needs new tables.

| Workstream | Effort | MVP-blocking | Source |
|---|---|---|---|
| Unpublish MCP REST `:8100` host port (Docker-net only); pin gateway DNS-rebinding=ON; tighten CORS defaults to `[]`/known origins | S | **Yes** | auth §5, §3 |
| Fail-**closed** the alertmanager token (`monitoring.py` returns early when unset today) | S | **Yes** | auth §5, ops SEC.3 |
| Mandatory Grafana password (no `:-admin` default) + default-secret check in `check_env.py` | S | **Yes** | ops SEC.2 |
| Remove RSI/ops/ingest tools from the customer gateway toolset; scope `list_workspaces` to entitled set | M | **Yes** | auth §3.2 |
| **Thread `workspace_id`/`account_id` into `chat/vision/embed`** + set on all `ExecutionReceipt`s + mint `request_id`; forward through chains' `run()`/`_call()` (2 task call-sites). Ship behind `metering_enabled` flag, observe `cost_workspace` populate | M | **Yes** | metering A, ops HA.2, pricing #3 |
| Backfill `workspace_id` onto child tables (`source_checkpoints`, `source_runs`, `indexing_status`, `post_enrichments`) + writer wiring; route `cluster_runs`/`admin_manual_jobs` NULLs to `__system__` sentinel | M | **Yes** | tenancy §3 (OWNS); sources/ops consume |

**Exit/gate:** all 3 surfaces have no fail-open holes; `:8100` not reachable from the internet; `cost_workspace` Redis buckets populate with real tenant attribution (verified in Grafana) but enforce nothing yet; every workspace-scoped row has a real `workspace_id`.
**Dependencies:** none — this is the foundation. The child-table backfill is a prerequisite for both RLS (Phase 4) and erasure (Phase 5).

---

## Phase 1 — Tenancy + auth + identity foundation (MVP-blocking core)
**Goal:** a real identity model and server-resolved workspace so a credential maps to an entitled account. This is the hard blocker every other domain waits on.

| Workstream | Effort | MVP-blocking | Source |
|---|---|---|---|
| Adopt Alembic as ordering authority (baseline `stamp head`); each revision runs the idempotent raw-SQL file via `op.execute` | M | **Yes** | tenancy §7, ops CI.5 |
| Identity DDL: `accounts`, `users`, `memberships`, `api_keys`, `feeds`, `account_feeds`, `account_workspaces`; extend `workspaces` with `kind`/`owner_account_id` | M | **Yes** | tenancy §2 (OWNS) |
| Seed `feeds` for the 5 slugs; create the operator's internal account (`is_internal=TRUE`), grant all 5 feeds, mark `workspaces.kind` | S | **Yes** | tenancy §7 |
| API-key issue/verify/rotate/revoke helper (prefix + SHA-256(pepper‖secret)); `resolve_principal` + 60s Redis principal cache | M | **Yes** | auth §1, §2.4 (OWNS) |
| `Depends()` deps (`authenticate`/`require_read|write|operator`); attach `require_operator` to all Admin routers; Admin behind VPN/localhost | M | **Yes** | auth §3 |
| Gateway auth middleware (bearer + `/t/<token>/` fallback) + per-tool `workspace` override via `resolve_requested_workspace`/`resolve_workspace` | L | **Yes** | auth §2, tenancy §5 |
| Admin operator endpoints: issue/rotate/revoke keys + grant `account_feeds`/`account_workspaces` | M | **Yes** | auth §6 (#11) |

**Exit/gate:** the operator's own Claude/Cursor authenticates with an internal key (auth path exercised day one, no permanent unauth bypass); a customer key resolves to an account whose entitled feed set is enforced; cross-tenant read is impossible.
**Dependencies:** Phase 0 (child-table `workspace_id`).

---

## Phase 2 — Billing, payments + НПД (turns it into a business)
**Goal:** charge money, issue legally-required receipts, and make breaching the 2.4M cap structurally impossible.

| Workstream | Effort | MVP-blocking | Source |
|---|---|---|---|
| Pricing resolver: `llm_model_prices` table + `shared/llm_pricing.py` (tokens→RUB) from `wormsoft_limits._normalize_pricing`; adapters return RUB, `cost_currency='RUB'` | M | **Yes** | metering B, payments #9 |
| Durable ledger: `usage_events` + `usage_rollup_monthly` DDL; stream-emit from `record_execution_receipt`; **new ledger consumer** + nightly rollup job | L | **Yes** | metering C (OWNS) |
| Billing core DDL: `plans`, `subscriptions`, `payments`, `payment_webhook_events`, `receipts`, `entitlements`, `revenue_ledger`; seed plans from `config/plans.yml` | M | **Yes** | payments §1, pricing #1 |
| YooKassa provider (`PaymentProvider` protocol): create_checkout (save_method), charge_saved_method, parse_webhook (sig/IP verify), refund | L | **Yes** | payments §2, §5 |
| `routers/billing.py` (list plans, checkout, self-service, consent capture) + `routers/payment_webhooks.py` (idempotent inbox → state machine) | L | **Yes** | payments §3, §4 |
| `NpdReceiptIssuer` + receipt retrier (auto-issue on succeeded, cancel on refund) | L | **Yes** | payments §2.4 |
| **Pre-charge revenue-cap admission check** (in-tx, blocks any charge crossing 2.4M; soft 80% / hard 95%); recurrent-charge cron + dunning on the scheduler | M | **Yes** | payments §3, pricing §3.5 |
| Per-tenant quota/overage enforcement: tenant entitlement scope (monthly RUB, ~40d TTL, reconciled from Postgres) checked *before* global provider scope; quota middleware at gateway (FC allowance/grace/hard-stop → 402) | M | **Yes** | metering D, pricing #5 |

**Exit/gate:** a real YooKassa payment creates a subscription, writes `revenue_ledger`, auto-issues an НПД чек, and grants `account_feeds`; an automated charge can never cross the cap; a tenant past its hard cap is throttled, not the platform. **Enforcement ships LAST in this phase — never block tenants before the ledger is trusted (Phase 0 observe → Phase 2 enforce).**
**Dependencies:** Phase 1 (`accounts` FK, auth). Pricing/durable-ledger (first two rows) can start during Phase 1 since they only depend on Phase 0 wiring.

---

## Phase 3 — Self-serve sources + feed catalog (top-tier revenue feature)
**Goal:** browseable feed catalog for everyone + safe self-serve RSS/web/api sources for the Custom tier.

| Workstream | Effort | MVP-blocking | Source |
|---|---|---|---|
| `feed_catalog` DDL + `config/feeds.yml` + bootstrap; `browse_feeds`/`get_feed_overview` read tools (entitlement-gated teaser) | M | No (fast-follow) | sources §2 |
| `sources` ownership/tier/tos/approval columns + backfill (curated = platform) | S | No | sources §3.1 |
| `ingest/safe_fetch.py` SSRF guard (private-IP/IMDS/internal-service block) + redirect re-validation hook; forbid tenant `proxy_config` | M | No | sources §4.1 |
| `clamp_source_extra_for_tier` (per-tier `extra` clamp) at add + schedule-load; outbound leaky bucket (Lua) per account | M | No | sources §4.2–4.3 |
| `POST /me/sources` self-serve endpoint (type allowlist rss/api/web, quota, ToS, `approval_state` auto vs pending_review); robots.txt cache; abuse auto-throttle nightly job | M | No | sources §3.2, §4.4, §6 |
| Per-tenant `tenant_secrets` envelope-encrypted table (libsodium sealed box; data key in SOPS) for self-serve API bearer tokens; email stays disabled | M | No (gated to self-serve) | ops SEC.4 |

**Exit/gate:** a Custom-tier tenant adds an RSS source into a private workspace; web sources route through `pending_review`; SSRF/abuse blast radius is bounded; full vision and `xray` egress remain platform-only.
**Dependencies:** Phase 1 (entitlement), Phase 2 (tier limits).

---

## Phase 4 — Backups/DR + CI/CD + scheduler HA (the solo-operability hardening)
**Goal:** make the platform recoverable and deployable without hand-edits on prod. *Backups (B1) and secrets (B2) are existential and should overlap Phase 0–1 in practice (see milestone note); the rest is post-first-customer.*

| Workstream | Effort | MVP-blocking | Source |
|---|---|---|---|
| **B1 Backups/DR:** PG WAL archive → RU S3 + nightly `pg_dump`; Qdrant/Neo4j snapshots; S3 versioning; `restore_all.sh` + runbook; **monthly automated restore drill** | L | **Yes (existential)** | ops §2 |
| **B2 Secrets:** SOPS+age encrypted file, decrypt-on-deploy, rotation doc (Grafana/alertmanager fixes already in Phase 0) | M | **Yes** | ops §6 |
| **B3 CI/CD:** GitHub Actions lint+`pytest -m unit` gate; build/push tagged per-service images to RU registry; `deploy-tag.sh` (pull, no rebuild-on-prod, auto-rollback); staging stack (synthetic data only) | L | **Yes** | ops §5 |
| Postgres RLS: `app_visible_workspace`/`app_writable_workspace` helpers, ENABLE/FORCE RLS + policies on all ws-scoped tables, `frontier_app`/`frontier_migrate` roles, `apply_tenant_guc` middleware (GUC `app.account_id`/`app.is_operator`) | L | No (defence-in-depth) | tenancy §4 (OWNS); ops/legal consume |
| Scheduler HA: APScheduler → Postgres jobstore + Redis leader lock; later split `scheduler-runner` service; add revenue-cap-guard cron | M | Near-critical | ops §4 |
| Per-tenant fair-queue weighted leaky bucket replaces global single-flight (if not already shipped with metering enforcement) | M | No | ops HA.1, auth §4 |

**Exit/gate:** a disk loss is recoverable to ≤1h RPO with a tested restore; deploys pull tagged images and auto-rollback on health failure; admin restart no longer stops all crons; RLS fails closed even on a missed `WHERE`.
**Dependencies:** RLS needs Phase 0 child-table backfill + Phase 1 entitlement tables. CI/Alembic underpins all later migrations.

---

## Phase 5 — 152-FZ compliance + per-tenant observability
**Goal:** legal envelope for processing a second human's data for money, plus the visibility that keeps the operator out of the loop.

| Workstream | Effort | MVP-blocking | Source |
|---|---|---|---|
| PDn register doc + RU localization attestation; consent/privacy/оферта pages + `consent_version` checkout gate | M | **Yes (legal)** | payments §4, ops FZ.1 |
| **PDn routing gate** in `RoutingPolicyV2`: tag `pdn_class`; pin `pdn_high` to GigaChat (RU) or pseudonymize (NER strip) before non-RU OpenRouter/Polza egress | M | **Yes (legal)** | ops FZ.2, payments §4 |
| Per-account retention + purge cron + `DELETE /account/{id}/pdn` erasure cascade (PG/Qdrant/Neo4j/S3); keep `receipts`/`revenue_ledger` for statutory retention (anonymize account ref) | M | No (compliance) | ops FZ.3, payments §4 |
| Per-tenant observability: bounded `workspace` label on `/metrics` + `note_llm_cost` (allowlist, no `model` label); Grafana per-tenant dashboard; `GET /status` tenant self-service; aggregate-MRR-vs-cap board with 80%/hard-stop alerts | M | No | ops §8, metering E, pricing |
| `get_usage_summary` MCP tool (account-scoped) | S | No | metering E (#10) |

**Exit/gate:** no raw Telegram PDn leaves RU un-pseudonymized; an erasure request cascades across all stores; the operator sees MRR-vs-cap runway and per-tenant spend without manual queries.
**Dependencies:** PDn gate depends on Phase 0 metering wiring; erasure depends on Phase 0 child-table `workspace_id` + Phase 4 RLS.

---

## Later — international + scale (position now, build later)
Managed RU Postgres (PITR) + Redis; stateless replicas behind LB; ingest shard-lease leader-election (ship the shard-filter code now at N=1 so scaling is a replica-count change); USD pricing + Stripe/Paddle behind the existing `PaymentProvider`/`NpdReceiptIssuer` seams. **All of this is naturally gated behind the ИП/ООО migration event** (international acceptance + 54-FZ ККТ require leaving НПД anyway). One line in the deck, zero code now (`pricing-packaging-gtm` §5.4, §6; ops L2–L4).

---

## Explicit guardrail + migration milestones

- **M-CAP (Phase 2):** `revenue_ledger` + rolling-12-month sum live; soft alert at **1.92M (80%)** stops new signups + sets lowest plan `is_public=false`; hard stop at **~2.28M (95%)** blocks any charge crossing 2.4M; **pre-charge admission check is the involuntary-breach prevention** and must evaluate prospective annual prepays before capture (a single 599k Custom annual = 25% of the cap).
- **M-MIGRATE (Later, but documented at M-CAP):** trigger ИП/ООО migration when rolling-year sustains **>1.8M for 2 consecutive months** *or* the first enterprise buyer requires ИП invoicing (procurement may not contract with a самозанятый — this can force migration *before* the revenue cap). The `payments.provider`/`receipts.provider` columns + protocol seams absorb the added 54-FZ ККТ (`yookassa_kkt`) as a strategy swap, not a rewrite.

---

## Дополнение (раунд 3, 2026-06-26): инфраструктура и миграция embeddings

**Хостинг — решено:** целевой сценарий «один зарубежный регион + residential-прокси для Telegram» (см. [11-hosting-residency-egress](./11-hosting-residency-egress.md)). В Phase 0/4 это меняет ops-топологию: общий xray VLESS HTTP-failover удаляется; в ingest остаётся только residential-прокси для Telethon-логина; endpoint / store / compute — зарубежный узел.

### Workstream M-EMB — миграция embeddings off-GigaChat (+ реиндекс Qdrant)

**Провайдер — ✅ РЕШЕНО: Wormsoft `qwen/qwen3-embedding:8b`** (Qwen3-Embedding-8B — open-weights Apache-2.0, сильно мультиязычный RU+EN, ctx 16k). Покрывает сразу обе цели: уход с RU-only GigaChat **и** международную переносимость одним реиндексом.

| Шаг | Effort | MVP-блок? | Примечание |
|---|---|---|---|
| Снять с Wormsoft фактическую размерность вектора `qwen/qwen3-embedding:8b` (Qwen3-8B native = 4096, MRL-configurable) и формат ответа | S | да | определяет `EMBED_DIM` и dim Qdrant-коллекции; verify по факту API |
| Целевая размерность — **✅ 4096 (ЗАФИКСИРОВАНО владельцем)**. Verify, что Wormsoft реально отдаёт 4096 (не усечённый MRL по умолчанию) | S | готово | `EMBED_DIM=4096`; ~1.6× память Qdrant vs текущие 2560 — заложить в сайзинг узла |
| Реализовать `WormsoftAdapter.embed` (сейчас кидает `embeddings_not_supported`) против embeddings-эндпоинта Wormsoft (вероятно OpenAI-совместимый `/embeddings`); снять strict-GigaChat-only политику для family=embeddings | M | да | `worker/provider_adapters.py`, routing policy; маппинг `purpose=document/query` на instruction-aware формат Qwen3 (запрос с instruction, документ — без) |
| Сменить `EMBED_DIM`; завести версионированную Qdrant-коллекцию под новую модель | S | да | `.env`, `shared/config.py`, `storage/qdrant` |
| Полный реиндекс корпуса через alias-cutover (zero-downtime) | L | да | переиспользовать `server-qdrant-alias-cutover.sh` + versioned backfill |
| Контрактные тесты векторного стора + QA качества retrieval на новой модели | M | да | `tests/test_qdrant_contracts.py` + выборочный QA (ожидаемо качество ≥ EmbeddingsGigaR) |

**Зачем именно Qwen3-Embedding-8B:** (1) снимает жёсткую привязку к GigaChat; (2) **open-weights** → не лок на Wormsoft: ту же модель позже можно self-host рядом с зарубежным worker или взять у не-РФ провайдера — векторы воспроизводимы (те же веса) → **без второго реиндекса** (только verify паритет нормализации/пулинга/инструкции); (3) **мультиязычность** напрямую обслуживает международную фазу. **Не флаг, а миграция** — отдельный релиз с реиндексом.

> RTT-нюанс: Wormsoft — РФ-endpoint, поэтому *сейчас* зарубежный worker платит трансграничный RTT на эмбеддингах. Но т.к. модель open-weights, это решается ПОЗЖЕ со-локацией той же Qwen3-8B рядом с worker — **без смены векторного пространства и без ещё одной миграции**.

