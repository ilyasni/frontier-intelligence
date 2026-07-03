# Журнал решений и вопросы к владельцу

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

## Зафиксировано владельцем — обновление 2026-06-26 (раунд 2)

- **Тенантность: аккаунт владеет несколькими workspace.** ✅ ЗАФИКСИРОВАНО. Совпадает с [ARCH-RESOLVED] ниже и с дизайном [02-identity-tenancy](./02-identity-tenancy-data-isolation.md): `accounts` (1) → (N) `workspaces`; курируемые потоки — общие через `account_feeds`, кастомные приватные — через `account_workspaces`. Снимает развилку «тенант = один workspace или аккаунт».
- **Биллинг: фикс-план с включённой квотой + soft/hard overage.** ✅ ЗАФИКСИРОВАНО. Совпадает с дизайном [04-metering-billing](./04-metering-billing-engine.md) и [08-pricing](./08-pricing-packaging-gtm.md): включённая квота на тариф, soft cap = предупреждение, hard cap = блок (`quota_exhausted`), двухуровневый контроль (per-tenant + глобальный потолок провайдера). Снимает развилку «модель биллинга».
- **Юрлицо/статус ещё не оформлен** (самозанятый/НПД — план). Регистрация НПД + согласие/политика 152-ФЗ + уведомление РКН — **предусловие запуска платного контура** (Фаза 2). До оформления — только бесплатные/пилотные доступы без приёма платежей.
- **Хостинг/egress без VPN — ✅ РЕШЕНО (раунд 3): один зарубежный регион + residential-прокси для Telegram.** Владелец снял якоря: 152-локализация вынесена за скобки; MCP-endpoint может быть за рубежом (потребляется через Claude/ChatGPT); оплата хостинга решена (зарубежная карта). Общий xray VLESS HTTP-failover удаляется; Telegram-логин — через residential-прокси (анти-бан, не закон). Остаётся обязательным: уведомление РКН + трансграничная передача (ст.12). Детали — [11-hosting-residency-egress](./11-hosting-residency-egress.md).
- **Embeddings — ✅ РЕШЕНО: Wormsoft `qwen/qwen3-embedding:8b`** (Qwen3-Embedding-8B, open-weights, multilingual RU+EN, ctx 16k). Триггерит полный реиндекс Qdrant (alias-cutover). Размерность — **✅ 4096** (подтверждено владельцем 2026-06-26; `EMBED_DIM=4096`, ~1.6× память Qdrant vs 2560). Плюсы: open-weights → портируемо (self-host / не-РФ провайдер позже без второго реиндекса) и сразу мультиязычно под международную фазу. RTT с Wormsoft (РФ-endpoint) остаётся сейчас, но решается позже без смены векторов. Детали — [09-roadmap M-EMB](./09-roadmap.md).

---

# Consolidated Decision Log

Deduped across the 7 domains. Each entry: the resolved **RECOMMENDATION** and the tradeoff. Cross-domain conflicts I resolved as lead architect are marked **[ARCH-RESOLVED]**.

## Cross-domain conflicts resolved (read these first)

- **[ARCH-RESOLVED] `accounts.id` type — UUID (tenancy) vs TEXT `'acct_<ULID>'` (auth/payments/pricing).** RECOMMENDATION: **`accounts.id TEXT PRIMARY KEY` = `'acct_'+ULID`**, matching auth/payments/pricing (the majority and the FK target everyone else writes). Tradeoff: lose native UUID indexing ergonomics, but gain a human-greppable, sortable, prefix-typed key consistent with the existing `sources.id TEXT`/`workspaces.id TEXT` convention. **Every FK in tenancy's DDL (`memberships`, `api_keys`, `account_feeds`, `account_workspaces`, `workspaces.owner_account_id`) changes UUID→TEXT.** Use `pgcrypto` only for `users.id` (no cross-domain FK pressure) or also make it TEXT for uniformity — pick TEXT for uniformity.

- **[ARCH-RESOLVED] Entitlement table shape — three competing models.** Tenancy proposes `account_feeds` (read) + `account_workspaces` (owned private); auth proposes one `account_workspaces` with `access read|write`; payments proposes `entitlements(account_id, workspace_id, access_level)`. RECOMMENDATION: **adopt tenancy's two-table split as canonical** — `account_feeds` (read entitlement to shared feeds, granted/expired by billing) + `account_workspaces` (ownership of a private partition). Fold payments' `entitlements` into `account_feeds` (it is the same read-grant; `subscription_id`/`quota_limits`/`status`/`expires_at` columns move onto `account_feeds`). Map auth's `access='write'` to "owns a private workspace" (i.e. an `account_workspaces` row), not a column on the read table. Tradeoff: payments must rename its `entitlements` references to `account_feeds`; the benefit is one unambiguous source of truth that RLS (`app_visible_workspace`) and the gateway resolver both read.

- **[ARCH-RESOLVED] RLS scoping GUC — `app.workspace_id` (ops) vs `app.account_id`+`app.is_operator` (tenancy).** RECOMMENDATION: **tenancy's account-scoped model wins.** Scope RLS by account (the real tenant) via `app.account_id`/`app.is_operator`, with `app_visible_workspace(ws)` resolving entitled workspaces from `account_feeds`+`account_workspaces`. A single `app.workspace_id` GUC cannot express "this account may read 3 feeds" and breaks `get_frontier_brief`. Tradeoff: policies call a STABLE SQL helper per row (small cost) instead of a literal equality; worth it for correct multi-feed entitlement.

- **[ARCH-RESOLVED] Child-table `workspace_id` backfill — claimed by tenancy §3, sources #3, ops FZ.4.** RECOMMENDATION: **tenancy owns the migration** (`20260702_workspace_id_backfill.sql`); sources and ops consume it. Ship it in **Phase 0** (it blocks RLS, erasure, and per-tenant abuse accounting). One migration, not three.

- **[ARCH-RESOLVED] Ledger write path — sync insert vs async Redis-stream consumer.** RECOMMENDATION: **async stream consumer** (`stream:usage:events` → batch insert, `request_id` idempotency via `ON CONFLICT DO NOTHING`). Keeps the LLM hot path latency-neutral and gives at-least-once durability. Tradeoff: one more long-running process — a single point of failure flagged to ops; mitigated because the Redis stream buffers during downtime (recoverable) and the new `scheduler-runner` supervision (Phase 4) covers restart.

## Tenancy & data isolation

- **Private workspaces stay in the single Qdrant collection + single Neo4j graph** with mandatory filters; do NOT split per-account collections/databases at НПД scale. Tradeoff: relies on the filter being unbypassable (mitigated: entitlement 403s before the query). Revisit only for a contractual hard-isolation enterprise tier (then Neo4j multi-database is the documented path).
- **`cluster_runs`/`admin_manual_jobs` NULLs → `__system__` sentinel workspace**, kept nullable in schema, treated as operator-only under RLS. Confirm with RSI/ops that no cross-workspace job must remain truly account-agnostic.
- **Operator authenticates from day one** (internal account + `is_internal=TRUE` key) so there is no permanent unauth bypass to maintain. Tradeoff: operator must configure a key in their own Claude/Cursor — small, and it exercises the auth path immediately.
- **Entity-merge (irreversible Neo4j `merge_concepts`) gated to `principal.is_operator`**; never exposed to customer keys (scope `{search}`, not `{merge}`).

## Auth & serving surfaces

- **Reverse proxy/TLS:** add **Traefik** (not Caddy — forbidden by CLAUDE.md) as the single TLS ingress; required for the `/t/<token>/` path fallback and to redact token path segments from logs. Hard dependency for Phase 1 gateway auth.
- **Gateway-only at launch** (curated feeds consumed in Claude/Cursor); add a thin read REST facade later when a non-MCP customer appears.
- **Spend cap enforced per-account-per-month**, no per-workspace sub-cap (matches the small high-ASP book).
- **Operator auth on Admin = VPN/localhost primary + admin-scoped key defence-in-depth**; do not build full session/2FA now.

## Metering & billing engine

- **Bill per ACCOUNT; workspace is the cost-attribution dimension.** Ledger carries both; rollup PK `(account_id, billing_period, workspace_id, provider, task_family)`. Until accounts land, `account_id` nullable, roll up by `workspace_id`.
- **Manual `usd_rub_rate` in config, refreshed monthly, stored per price row** (audit column) so historical invoices reproduce. Live FX is over-engineering.
- **Soft-cap behavior: warn-and-continue + upsell** for paid tiers; only degrade-to-cheapest for tenants in overage with `overage_allowed=FALSE`. Don't silently ship worse intelligence to a paying customer.
- **Wormsoft cost: re-scale modeled per-token RUB to the flat plan price at period close** for COGS/margin accuracy (customers are billed plan price + overage, not raw Wormsoft tokens). Tradeoff: more moving parts, but accurate internal margin.
- **Prometheus: add the bounded `workspace` label (tens of series) but NEVER `model`;** Postgres `usage_events`/rollup is the SINGLE SOURCE OF TRUTH for invoices (Prometheus is lossy across restarts).

## Payments & legal (НПД)

- **NPD receipt channel: prefer `yookassa_selfemployed`** (one integration, YooKassa files the чек with ФНС); keep `fns_lknpd` behind the same `NpdReceiptIssuer` protocol as fallback.
- **Cap hard-stop ~95% (2.28M); pre-charge admission check refuses any charge crossing 2.4M.** A near-cap recurrent charge is **PAUSED + notify** (protects НПД status, customer keeps grace access), not pro-rated.
- **Plan/ASP shape: small number of higher-priced B2B tiers** (Pulse 3 900 / Pro 12 900 / Studio 29 900 / Custom 59 900 ₽/mo; annual = 10×). Lean higher, not lower — the cap rewards ASP. Pulse is a paid funnel (not free) to avoid low-intent floods; 14-day Pro trial is the acquisition hook.
- **FC weighting (heavy 1.0 / light 0.2) + overage 20 ₽/FC are first-pass;** instrument 1–2 months of real per-workspace usage before finalizing. Ship generous, tighten later.
- **Cap annual prepays to Pro/Studio only** (or operator-approve Custom annuals) so one 599k prepay can't blow cap headroom.

## Sources & feeds product

- **Web self-serve = `pending_review`** (one-click operator approve); auto-approve RSS + allowlisted-host API. Caps SSRF/abuse blast radius.
- **`api` sources may set `item_url_template` only if the template host passes the SSRF allowlist** and matches the index host (else operator approval); each item fetch counts against the leaky bucket.
- **Robots.txt `Disallow` = hard block** (refuse to enable the source), not soft-skip — keeps the operator clean under the ToS-attestation model.
- **Ship the shard-filter + lease code now at N=1** (single replica); scaling to N>1 is a pure replica-count change. Avoids over-building HA pre-revenue.
- **Curated-feed full access: Pro = all AI feeds, Business/Studio = + disruption/design;** `feed_catalog.min_tier` is the lever (final packaging is a billing decision).

## Ops, HA, DR, secrets, 152-FZ

- **Managed RU Postgres: recommend Yandex Managed PostgreSQL** for mature PITR/HA *if* Cloud.ru S3 cross-account works; else stay self-hosted single-node with WAL→S3 for MVP, migrate post-revenue. **Decide before the RLS work (Phase 4).**
- **Scheduler: APScheduler + Postgres jobstore + Redis leader lock first** (minimal new infra, reuses async stack); move to RQ only if cron contention appears.
- **PDn default: RU-pin `pdn_high` to GigaChat** (simplest, provably compliant); treat the NER pseudonymizer as a later cost optimization to re-enable cheaper non-RU providers. Confirm the cost delta justifies building the pseudonymizer in the MVP window.
- **Secrets: SOPS+age now** (encrypted-file-in-repo, no server to operate); defer Vault/Lockbox to scale.
- **Staging: same-host second compose project with isolated volumes + synthetic data** unless budget allows a cheap second RU VM.
- **Restore drill: monthly automated on an ephemeral node** (an untested backup is not a backup).

---

## Items the owner MUST verify with an accountant/lawyer/provider before charging the first ruble

1. **НПД receipt mechanism — does YooKassa самозанятый actually FILE the чек with ФНС, or must the operator confirm it in "Мой налог"?** Determines whether `yookassa_selfemployed` alone is sufficient or `fns_lknpd` is required. **(accountant + YooKassa)**
2. **YooKassa recurrent (автоплатёж) on a самозанятый/ИП account** — some самозанятый YooKassa products are payout-only and don't support saved-card autopay. If disabled, the recurrent cron falls back to monthly emailed checkout links. **Verify before committing the recurrent cron.** **(YooKassa)**
3. **Service framing** — the offer/чек must read "информационно-аналитические услуги", NOT "перепродажа доступа к API третьих лиц" or reselling goods (forbidden under НПД). Confirm exact OKVED/wording so no channel is deemed перепродажа товаров. **(lawyer/accountant)**
4. **НПД is taxed on GROSS received including YooKassa commission** — confirm the чек amount is the gross the buyer paid (not net of fee) and model the tax accordingly. **(accountant)**
5. **юрлицо sales under НПД** — allowed (6% tax) with INN capture, BUT income from a current/recent (2yr) employer is excluded, and some enterprise procurement won't contract with a самозанятый (no VAT, no закрывающие документы). **This may force ИП migration before the revenue cap.** Confirm whether target B2B logos accept самозанятый invoicing. **(accountant + sales)**
6. **РКН operator notification scope** — уведомление about processing ПДн (Telegram author handles, customer email, buyer INN) must be filed at/before launch. **(lawyer)**
7. **152-FZ / RKN data-residency** — RU PDn must stay in RU (already satisfied: GigaChat Sber, Cloud.ru S3 ru-central-1, Postgres on RU); the only gap is non-RU LLM hops (OpenRouter/Polza) + xray egress, closed by the PDn routing gate (Phase 5). Confirm the localization attestation is sufficient. **(lawyer)**
8. **Statutory retention vs erasure** — `receipts`/`revenue_ledger` must be retained for tax records even after a 152-FZ erasure request; confirm the retention period and the anonymization approach for the account reference. **(lawyer/accountant)**
9. **Data-licensing of resold feeds** — resell derived synthesis (trends/briefs) only; prefer link+summary over full third-party article text where licensing is unclear; ToS attestation shifts self-serve-source liability to the tenant. Confirm the posture is defensible for the curated corpus. **(lawyer)**
10. **Revenue-cap buffer** — confirm `NPD_CAP_HARD_STOP` at ~95% (2.28M) vs a more conservative threshold. **(owner/accountant)**

