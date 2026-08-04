# Ценообразование, упаковка и GTM в рамках НПД

<!-- audit-status:2026-08-04 -->
> **📐 ЗАМЫСЕЛ, НЕ РЕАЛИЗОВАНО · сверено 2026-08-04.**
> Замысел, а не описание системы: на дату сверки не реализован. Не читать как отчёт о готовом.
> Конкретных расхождений найдено: **7** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

# Pricing, Packaging & GTM under НПД

> Scope: how Frontier Intelligence is packaged into paid tiers, priced for positive gross margin, sold (ICP / wedge / MVP offer), billed via YooKassa, and kept structurally inside the self-employed (НПД) revenue ceiling of **2 400 000 ₽/year ≈ 200 000 ₽/mo MRR**, with no employees and a documented exit ramp to ИП/ООО.
>
> This section is decision-complete on the commercial model. It depends on the Identity/Auth, Metering & Billing, and Tenancy-Isolation domains for enforcement; it does **not** re-specify those — it specifies what they must enforce.

---

## 0. Grounding facts (why this shape, not a generic SaaS)

Three codebase realities drive every decision below:

1. **Real platform LLM cost is low and flat.** Per `docs/llm-cost-strategy.md`, the *entire* current pipeline (15 000 posts/mo, 5 workspaces, ~30 MCP tools) runs on **~3 600 ₽/mo** under the recommended S4 hybrid (Wormsoft Simple flat **1 500 ₽/mo** for text+embeddings + GigaChat-2-Pro packs for vision), scaling to only **~9–12k ₽/mo at 3 000 posts/day**. The expensive ingest/enrichment work is **shared across all tenants** (one curated corpus), so the marginal LLM cost of an *additional read-only subscriber* is essentially their MCP-call synthesis only — `mcp_synthesis` is budgeted at 4 000 in / 1 500 out tokens per call (`docs/llm-cost-strategy.md` token table). This is the central unit-economics fact: **shared-corpus subscriptions have ~90%+ gross margin.**

2. **Per-tenant metering is already half-built.** `worker/provider_budget_manager.py` already implements a `cost_workspace` FinOps scope (`_finops_scopes`, lines 244–255) and `snapshot_costs(workspace_ids=...)` (line 824). `shared/llm_control_plane.py` `ExecutionReceipt.workspace_id` (line 486) and `ProviderExecutionRequest` carry the field — it is simply never populated on the request path. So **token→workspace attribution is a wiring job, not a rebuild** — which makes per-tenant included-credit quotas cheap to ship. Real RUB prices to convert tokens→money already arrive via `admin/backend/services/wormsoft_limits.py::_normalize_pricing()` (the `/api/money/token-pricing` table) — fetched but never applied.

3. **The НПД cap forces a B2B, low-customer-count shape.** At a 200k ₽/mo MRR ceiling, **high-volume low-ASP self-serve is mathematically the wrong model** — it maxes the cap with hundreds of accounts to support solo. A **small number of higher-ASP B2B feed subscriptions** hits the same revenue with 1–2 orders of magnitude fewer customers to operate, which is the only viable shape for a one-person, no-employees business. The math in §3 proves this.

---

## 1. Packaging model

The product is **hybrid** (confirmed owner decision): a shared curated-intelligence layer everyone gets, plus limited self-serve custom sources on the top tier. Packaging maps cleanly onto existing assets:

- **Feeds = the 5 curated workspaces** already in `config/workspaces.yml` (`disruption`, `ai_trends`, `ai_research`, `ai_products_media`, `design`). Each is a ready-made intelligence product. "Number of feeds" in a plan = number of `workspace_id`s the tenant's API key may read.
- **MCP tools = the product surface** (~30 tools in `mcp/mcp_gateway.py`). Tiering gates *which tools* a key may call: basic search vs. the high-value synthesis tools (`search_balanced`, `get_frontier_brief`, `get_concept_graph`).
- **LLM allowance = included synthesis credits**, metered per `workspace_id` via the existing budget manager once `workspace_id` is propagated.
- **Custom sources = the self-serve RSS/web/api connectors** (`shared/source_definitions.py::validate_source_payload`, `ingest/sources/base.py`). Telegram and email stay platform-curated-only (confirmed: plaintext IMAP creds unsafe; Telegram FloodWait blast-radius).
- **Alert channels = the existing `trend_alerts` dedupe + weekly-cap selection** logic, delivered to a tenant-configured webhook/email/Telegram-DM.

### 1.1 The "credit" unit (customer-facing)

Tokens are not a sellable unit. Define a **Frontier Credit (FC)** as the customer-facing abstraction, mapped internally to real cost:

- **1 FC ≈ 1 "heavy synthesis call"** = one `get_frontier_brief` / `search_balanced` / `search_trend_clusters` invocation (the `mcp_synthesis` budget: 4 000 in + 1 500 out tokens).
- **Light calls** (`search_frontier`, `get_concept_graph`, `get_source_details`, vision/observability reads) = **0.2 FC** each (no or cheap LLM).
- This decouples the price list from per-token volatility while keeping a clean internal conversion: at S4 blended cost, a heavy call costs the platform **well under 1 ₽** of LLM. FC pricing is therefore **margin, not cost recovery** — credits exist for fairness/abuse-limiting, not to break even on LLM.

> Decision: **included-credits model, NOT cost-plus-markup metering.** Rationale: LLM cost is so low relative to ASP that pure pooled-cost+markup would underprice the product and expose raw provider economics. Included credits + hard cap = predictable margin and predictable customer bill. Pooled-cost+markup is kept only as the *internal* accounting basis for the НПД cap guardrail and for overage pricing (§4).

---

## 2. Tier table

Four tiers. Three are **shared-corpus** (huge margin, the volume of the business); the top tier adds the only marginally-expensive feature (private workspace + self-serve ingest).

| | **Pulse** (read-only) | **Pro** (analyst) | **Studio** (team) | **Custom** (private) |
|---|---|---|---|---|
| **Target ASP/mo** | 3 900 ₽ | 12 900 ₽ | 29 900 ₽ | 59 900 ₽ |
| **Annual (−2 mo)** | 39 000 ₽ | 129 000 ₽ | 299 000 ₽ | 599 000 ₽ |
| **Feeds (workspaces)** | 1 of 5 | 3 of 5 | all 5 | all 5 + **1 private** |
| **MCP tools** | `search_frontier`, `get_concept_graph`, observability reads | + `search_balanced`, `get_frontier_brief`, `search_trend_clusters`, `search_by_vision` | all read tools, cross-workspace `get_frontier_brief` | all + ingest-mgmt tools |
| **Heavy-call allowance (FC/mo)** | 200 | 1 000 | 3 000 | 6 000 |
| **Seats (API keys)** | 1 | 2 | 5 | 10 |
| **Custom sources (RSS/web/api)** | — | — | — | **up to 15** self-serve |
| **Private workspace** | — | — | — | 1 (isolated `workspace_id`) |
| **Alert channels** | 1 (weekly brief, 1 channel) | 3 (daily + threshold alerts) | 5 (+ webhook) | 10 (+ webhook) |
| **Brief cadence** | weekly | daily | daily + on-demand | daily + on-demand |
| **Support** | email, best-effort | email, 2 biz-day | priority email, 1 biz-day | priority + onboarding call |
| **Overage** | hard stop + upsell | soft (grace 20%) → metered | soft (grace 20%) → metered | soft (grace 20%) → metered |

**Notes on the gating:**
- The **wedge tools live behind Pro**: `search_balanced` (growth / counter-signal / RU-verify / **competitor monitoring** / blind-spot detection) and `get_frontier_brief` are the differentiated, hard-to-replicate value. Pulse is deliberately a "taste" tier that drives upgrade.
- **Custom** is the only tier that consumes a *new* `workspace_id` + runs self-serve ingest, i.e. the only tier with non-trivial marginal cost (its own enrichment volume). It is priced to absorb that (see §3.4).
- **No tier exposes destructive admin/ops tools** — the customer key namespace must be separated from admin (`:8101`) per the Auth domain. Tiering here assumes customer keys can only reach the read/ingest-mgmt MCP surface.

---

## 3. Unit economics & cap-aware capacity model

All costs blended from `docs/llm-cost-strategy.md` (S4 hybrid). YooKassa fee assumed **~3.5%** of GMV. НПД tax (4% B2C / 6% B2B) is paid by the operator out of revenue, **not** a COGS line, but it *does* eat the cap headroom — see §3.5.

### 3.1 Marginal LLM cost per tier (the only meaningful COGS)

Shared ingest/enrichment (~3 600 ₽/mo) is a **fixed platform cost amortized across all tenants**, not per-customer. Per-customer marginal cost = their MCP synthesis calls:

```
Heavy call (mcp_synthesis): 4 000 in + 1 500 out tokens.
At S4 blended text cost (Wormsoft Simple flat-rate text is effectively
~0 marginal until the 500k-credits/5h bucket is exhausted; GigaChat Pro
fallback ~500 ₽/1M tokens):
   worst-case money-priced heavy call ≈ (5 500 / 1e6) * 500 ₽ ≈ 2.75 ₽
   realistic (mostly Wormsoft flat-rate) heavy call ≈ 0.3–0.8 ₽
Use a conservative planning figure: 2.0 ₽ / heavy call (FC).
```

| Tier | Included FC/mo | Marginal LLM COGS @2₽/FC | YooKassa fee | **Total COGS** | **ASP** | **Gross margin** |
|---|---:|---:|---:|---:|---:|---:|
| Pulse | 200 | 400 ₽ | 137 ₽ | ~540 ₽ | 3 900 ₽ | **86%** |
| Pro | 1 000 | 2 000 ₽ | 452 ₽ | ~2 450 ₽ | 12 900 ₽ | **81%** |
| Studio | 3 000 | 6 000 ₽ | 1 047 ₽ | ~7 050 ₽ | 29 900 ₽ | **76%** |
| Custom | 6 000 | 12 000 ₽ | 2 097 ₽ | ~14 100 ₽ | 59 900 ₽ | **76%** |

Plus the **shared platform floor** of ~3 600 ₽/mo (ingest+enrichment) and fixed infra (server, S3, proxies) — call it **~8 000–10 000 ₽/mo all-in fixed**. With even ~5 paying customers this floor is fully covered; it is irrelevant to per-tier margin and shrinks as a % of revenue with each customer.

> **Conclusion: every tier clears 75%+ gross margin.** The included-credits model is safe — even a tenant who fully exhausts their allowance every month is highly profitable. Overage (§4) is therefore about fairness and abuse, not cost recovery.

### 3.2 Cap-aware capacity: max customers per tier

The hard constraint is **MRR ≤ 200 000 ₽/mo** (and annual revenue ≤ 2 400 000 ₽). Max customers if the book were 100% one tier:

| Tier | ASP/mo | Max customers @ 200k MRR cap | Realistic operable count (solo) |
|---|---:|---:|---:|
| Pulse | 3 900 ₽ | **51** | tight to support solo at scale |
| Pro | 12 900 ₽ | **15** | comfortable |
| Studio | 29 900 ₽ | **6** | very comfortable |
| Custom | 59 900 ₽ | **3** | very comfortable |

**This is the core strategic proof.** To fill the cap you need *either* ~51 Pulse accounts *or* ~3 Custom accounts. A solo operator under НПД (no employees) cannot support 51 accounts' worth of onboarding/support/abuse-handling, but trivially supports 3–15. **Therefore steer the book toward Pro/Studio/Custom; treat Pulse as a self-serve, low-touch funnel entry, not the revenue base.**

### 3.3 Recommended target book (the "safe shape")

A blended book that sits at **~70–75% of the cap** (deliberate headroom for a soft-landing before the cap, see §3.5):

```
  3 × Custom   @ 59 900 = 179 700  ← too hot alone, shown for illustration
  --- recommended blend ---
  1 × Custom   @ 59 900 =  59 900
  3 × Studio   @ 29 900 =  89 700
  2 × Pro      @ 12 900 =  25 800
  --------------------------------
  Total MRR             ≈ 175 400 ₽/mo   (≈ 2 104 800 ₽/yr — 88% of cap)
  ~6 invoices, solo-operable, ~78% blended gross margin
```

Target steady-state: **6–10 active B2B subscriptions**, MRR held at **~150–175k ₽** with a hard guardrail at 190k (§3.5). Pulse accounts are *funnel*, capped in count so they never crowd out higher-ASP slots.

### 3.4 Custom-tier marginal-cost sanity check

Custom adds a private `workspace_id` with up to 15 self-serve sources. At the project's own scaling figures, even a busy private workspace of 15 sources is a fraction of the 64-source shared corpus, i.e. **< ~1 000 ₽/mo** of additional ingest+enrichment LLM. Folded into the 59 900 ₽ ASP this is noise — Custom still clears ~74% margin after its private ingest. Self-serve sources must be **quota-bounded** (`shared/source_quality.py` health/yield gating already exists) so a tenant can't point 15 firehoses at the pipeline; cap per-source daily post volume in the plan entitlement.

### 3.5 НПД cap guardrail (hard requirement)

A solo НПД business that crosses 2.4M ₽/yr **loses НПД status retroactively** — this is a business-ending risk, not a billing nicety. Required mechanics (owned by Metering & Billing domain, specified here):

- A durable **`revenue_ledger`** (Postgres, not Redis — the existing budget Redis has ~3-day TTL and cannot reconstruct a billing year). Every YooKassa-confirmed payment writes an immutable row.
- A **rolling 12-month revenue sum** computed on each successful charge.
- **Soft guard at 1 920 000 ₽ (80%)**: stop accepting *new* subscriptions and annual prepays; alert the operator.
- **Hard guard at 2 280 000 ₽ (95%)**: block all renewals/charges that would cross 2.4M; queue affected renewals to next NPD year or to the ИП migration path.
- Annual prepays are the riskiest cap event (one 599k ₽ Custom annual is 25% of the cap in one transaction) — the guard must evaluate **prospective** revenue *before* capturing an annual payment.

```sql
-- owned by Metering & Billing domain; cited here as the cap-guard contract
CREATE TABLE revenue_ledger (
    id              BIGSERIAL PRIMARY KEY,
    account_id      UUID NOT NULL,              -- FK -> accounts (Identity domain)
    yookassa_payment_id TEXT NOT NULL UNIQUE,   -- idempotency
    amount_rub      NUMERIC(12,2) NOT NULL,
    kind            TEXT NOT NULL,              -- 'subscription' | 'annual_prepay' | 'overage' | 'refund'
    npd_receipt_id  TEXT,                       -- FNS "Мой налог" / YooKassa самозанятый receipt
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    npd_year        SMALLINT NOT NULL           -- bucket for the 2.4M/yr ceiling
);
CREATE INDEX revenue_ledger_year_idx ON revenue_ledger (npd_year, captured_at);
-- rolling-year guard: SUM(amount_rub) WHERE kind!='refund' AND captured_at > now()-interval '12 months'
```

### 3.6 Pooled-cost+markup vs included-credits — the decision

| Option | Verdict |
|---|---|
| **Pooled cost + % markup** (bill tokens→RUB via `_normalize_pricing`, add markup) | **Rejected as the customer model.** LLM cost is so low that markup pricing would yield ~tens of ₽/mo bills — absurd vs. the value delivered, and it exposes raw provider economics. **Kept internally** as the accounting basis for the cap guardrail and overage rate. |
| **Included credits + hard/soft overage** | **Chosen.** Predictable customer bill, predictable margin, fairness-limiting. Credits are denominated in heavy-call FC (§1.1), not tokens. |

---

## 4. Billing mechanics (YooKassa tie-in)

Confirmed: **YooKassa (ЮKassa), самозанятый flow** — NPD receipts via FNS "Мой налог" / YooKassa самозанятый, **NOT** 54-FZ ККТ (that obligation only appears on the ИП/ООО migration path). Specified here as the commercial contract; implementation is the Metering & Billing domain.

### 4.1 Recurrent monthly

- YooKassa **recurring payments** (saved-method / `payment_method_id`): first charge with `save_payment_method=true`, subsequent monthly charges are server-initiated.
- On each successful capture: (1) write `revenue_ledger` row, (2) issue NPD receipt (самозанятый), (3) reset the tenant's monthly FC allowance, (4) re-enable the API key if it was overage-suspended.
- **Idempotency**: YooKassa `Idempotence-Key` header + `yookassa_payment_id UNIQUE` (§3.5) — never double-count toward the cap on webhook retries.

### 4.2 Included quota + soft/hard overage

Drives directly off the existing per-workspace metering once `workspace_id` is propagated:

```
on each MCP call:
  fc_cost   = 1.0 if heavy_tool else 0.2
  used      = ProviderBudgetManager.snapshot_costs(workspace_ids=[ws]) -> month-to-date FC
              (NOTE: requires a durable monthly ledger; the 3-day-TTL Redis
               keys in provider_budget_manager.py are NOT sufficient for a
               billing month — Metering domain must add a Postgres usage roll-up)
  allowance = plan.included_fc
  if used + fc_cost <= allowance:          -> allow
  elif used <= allowance * 1.20 (Pro+):    -> allow, flag soft-overage   (grace band)
  else:                                     -> hard stop -> 402 + upsell  (Pulse: no grace)
```

- **Soft overage (Pro/Studio/Custom)**: 20% grace band, then **metered overage** billed next cycle at **20 ₽/FC** (a deliberate ~10× markup over the ~2 ₽ COGS — overage should *nudge to upgrade*, not be a profit center). Overage writes a `kind='overage'` ledger row → counts toward the cap.
- **Hard stop (Pulse)**: no grace; returns a 402 with an upgrade CTA. Pulse is the funnel — friction here is intentional.
- **Enforcement point**: the gating decision belongs at the MCP gateway auth/quota middleware (Auth + Metering domains), keyed by the resolved `workspace_id`/`account_id` from the API key — **not** the client-supplied workspace string (today's leak).

### 4.3 Annual prepay (cashflow smoothing under the cap)

- Offer **annual = 10× monthly** (2 months free). Improves retention and front-loads cash.
- **Cap interaction (critical):** an annual prepay lands as a single large `revenue_ledger` row in *this* NPD year. The cap guard (§3.5) must run **before capture**; if the prospective rolling-year sum would exceed the hard guard, decline the annual and offer monthly instead. Do **not** spread an annual across NPD years for cap purposes — under НПД, revenue is recognized on receipt.

---

## 5. Positioning, ICP & MVP offer

### 5.1 ICP (in priority order)

1. **Strategy / innovation / competitive-intelligence teams** (corporate strategy, R&D foresight, venture/PE scouting) — they already pay for trend reports and have budget at the 13–60k ₽/mo range. **Primary ICP.**
2. **Design & futures studios** (the `design` + `disruption` workspaces map exactly; the project's own `visionary-designer` Claude Project is the dog-fooded reference customer).
3. **AI-product teams** (`ai_trends`, `ai_products_media`, `ai_research`) needing to track model/product launches and competitor moves without a dedicated analyst.

All three are **B2B, low-count, high-ASP** — exactly the shape the НПД cap rewards (§3.2).

### 5.2 Wedge

The defensible, hard-to-replicate hooks, in order:
- **Competitor-monitoring pack** (`search_balanced` competitor/RU-verify lenses) — the T-Bank-style "watch these players" job-to-be-done, delivered as a standing feed + alerts.
- **Frontier briefs** (`get_frontier_brief`, cross-workspace) — a weekly/daily synthesized briefing that replaces an analyst's manual scan.
- **Blind-spot / counter-signal detection** (`search_balanced` blind-spots) — "what is the market *not* talking about" — a genuinely differentiated output most aggregators can't produce.
- **RU-first credibility**: GigaChat Sber endpoint + Cloud.ru S3 ru-central-1 = a 152-FZ-friendly story for Russian enterprise buyers that Western tools can't match.

### 5.3 MVP offer (sell this first)

**"Competitive & Frontier Brief subscription"** — launch with **Pro (12 900 ₽/mo)** as the hero offer, Pulse as the free-ish/cheap funnel, Custom as a hand-sold anchor:

- Pre-sell **3–5 Pro/Studio logos** from the operator's existing design/AI network before building the billing rails (the corpus already works; only auth+metering+YooKassa block monetization).
- One **Custom anchor** (a studio or strategy team wanting a private workspace) underwrites the build and validates the top tier.
- MVP scope to monetize = the three BLOCKERS the audit named: (1) identity + API keys, (2) auth on the MCP gateway with server-resolved `workspace_id`, (3) YooKassa recurrent + `revenue_ledger` + per-workspace quota wiring. Everything else (RLS, per-tenant Qdrant, HA) is fast-follow.

### 5.4 Later international phase (position now, don't build now)

- Keep the **provider/embedding abstraction seam** (`worker/llm_router_client.py`, `worker/provider_adapters.py`) so a non-RU embedding/LLM lane can be added without a rewrite — but do **not** build it now.
- International = a **later phase requiring ИП/ООО anyway** (foreign-currency acceptance + likely 54-FZ ККТ once off НПД), so it is naturally gated behind the cap-migration event (§6).
- Positioning when it arrives: the same competitor-monitoring/frontier-brief wedge, USD pricing (~$149 Pro / ~$349 Studio), Stripe/Paddle rails added alongside YooKassa. **One line in the deck today; zero code.**

---

## 6. Cap-exit ramp (ИП/ООО migration trigger)

A one-line growth policy, because hitting the cap is a *success* that must not become a *crisis*:

- When the rolling-year revenue sustains **> 1 800 000 ₽ for 2 consecutive months** *and* demand exists beyond the cap, trigger the documented migration to **ИП (УСН "доходы" 6%)**.
- ИП migration unlocks: higher/uncapped revenue, ability to hire/contract, but **adds 54-FZ ККТ obligation** (online-касса / fiscal receipts) — replacing the YooKassa самозанятый receipt flow with a 54-FZ-compliant one (YooKassa supports both; it is a config + receipt-fiscalization change, not a rewrite).
- This is why §3.3 holds the target book at ~75–88% of the cap with a hard guard at 95%: it buys time to execute the migration *before* an involuntary cap breach.

---

## 7. Sequenced change list (commercial layer only)

Tags: S ≤ 0.5d · M ~1–2d · L ~3–5d · XL > 1 week. These are the **pricing/packaging** changes; auth/identity/RLS are owned by their domains and listed here only as dependencies.

| # | Change | Effort | Depends on |
|---|---|---|---|
| 1 | **Plan catalog** (`config/plans.yml` + Postgres `plans` table): tier → {feeds[], allowed_mcp_tools[], included_fc, seats, custom_source_cap, alert_channels}. UPSERT via existing `bootstrap_configs.py` YAML→Postgres pattern. | M | Identity (accounts/subscriptions) |
| 2 | **FC accounting**: classify each MCP tool as heavy(1.0)/light(0.2) in the plan catalog; map tool name → FC weight at the gateway. | S | Auth (key→account resolution) |
| 3 | **Propagate `workspace_id` into the request path** so `ExecutionReceipt.workspace_id` (already defined, `shared/llm_control_plane.py:486`) is populated and the existing `cost_workspace` scope (`provider_budget_manager.py:244`) fills. Pure wiring. | M | — (reuses existing schema) |
| 4 | **Durable monthly usage roll-up** (Postgres) — the Redis FinOps keys have ~3-day TTL (`provider_budget_manager.py:454,511`) and cannot back a billing month; add a daily roll-up job from `snapshot_costs(workspace_ids=...)` into a `usage_monthly(account_id, ws, npd_month, fc_used)` table. | L | Metering & Billing |
| 5 | **Quota middleware** at MCP gateway: on each call, enforce tool-allowed + FC allowance/grace/hard-stop (§4.2), return 402+upsell on hard stop. | M | Auth (server-resolved ws), #1–#4 |
| 6 | **Apply `_normalize_pricing()` table** to convert tokens→RUB for the *internal* cap-guard accounting basis (it is fetched but never applied today — `wormsoft_limits.py:62`). | S | — |
| 7 | **`revenue_ledger` + rolling-year cap guard** (§3.5) with soft(80%)/hard(95%) thresholds and prospective-annual check. | L | Metering & Billing, YooKassa |
| 8 | **YooKassa recurrent** integration: first charge w/ saved method, monthly server-initiated charge, webhook → ledger + NPD receipt + allowance reset; idempotency key. | XL | Metering & Billing |
| 9 | **Annual prepay** SKU (10× price) with pre-capture cap-guard evaluation (§4.3). | M | #7, #8 |
| 10 | **Custom-tier self-serve source quota**: per-plan cap (≤15 sources, per-source daily-volume bound) enforced via existing `shared/source_quality.py` + `validate_source_payload`. | M | Tenancy (private workspace), Self-serve ingest |
| 11 | **Pricing/landing page + 3–5 pre-sold logos** (GTM, non-code). | M | — |

**Reusable assets leaned on (by path):**
- `worker/provider_budget_manager.py` — `cost_workspace` scope + `snapshot_costs(workspace_ids=...)` (metering core, ~done).
- `shared/llm_control_plane.py` — `ExecutionReceipt`/`ProviderExecutionRequest` carry `workspace_id`/`cost_estimate`/`actual_cost` (tenant-ready schema).
- `admin/backend/services/wormsoft_limits.py::_normalize_pricing()` — the only real RUB price table (for cap-guard accounting basis).
- `config/workspaces.yml` — the 5 sellable feeds.
- `config/sources.yml` — 64-source shared corpus (37 RSS / 15 web / 9 TG / 3 api) = the fixed-cost amortization base.
- `shared/source_definitions.py`, `shared/source_quality.py`, `ingest/sources/base.py` — self-serve custom-source validation + quota gating for Custom tier.
- `admin/backend/bootstrap_configs.py` — YAML→Postgres UPSERT pattern for the plan catalog.

---

## 8. Open commercial decisions

(Carried in the `open_decisions` field for the owner.)


---

## Открытые решения по этому разделу

- Final price points: I recommend Pulse 3 900 / Pro 12 900 / Studio 29 900 / Custom 59 900 ₽/mo (annual = 10×). These hit 75%+ margin and let ~6-10 customers fill the cap. Owner should validate against 2-3 target buyers' willingness-to-pay before locking — RU B2B intel buyers may bear higher (Pro 14 900-19 900). Lean higher, not lower: the НПД cap rewards higher ASP.
- Pulse positioning: free trial vs. paid 3 900 ₽. Recommendation: paid (not free) to avoid a flood of low-intent accounts that cost solo support time and crowd cap-irrelevant slots; offer a 14-day Pro trial instead as the acquisition hook. Confirm.
- FC weighting (heavy=1.0 / light=0.2) and overage rate (20 ₽/FC). These are first-pass; recommend instrumenting 1-2 months of real per-workspace usage (once #3 wiring lands) before finalizing, since true call-mix is unknown. Ship with generous allowances first, tighten later.
- NPD-vs-B2B receipt nuance: самозанятый can invoice юрлица (6% tax) but some enterprise procurement won't contract with a самозанятый (no VAT, no закрывающие документы in the form they expect). This may force ИП earlier than the cap does. Owner should confirm whether target B2B logos accept самозанятый invoicing — if not, the migration ramp (§6) trigger is 'first enterprise that requires ИП', not the revenue cap.
- Custom-tier private-workspace isolation depends on the Tenancy domain delivering at least logical workspace isolation (and ideally a private Qdrant collection) before selling it. If hard isolation slips, gate Custom behind a 'private workspace = logically-filtered, not physically-isolated' disclosure, or delay the Custom tier to fast-follow. Decide sell-now-with-caveat vs. delay.
- Annual prepay cap risk: a single 599k Custom annual is 25% of the 2.4M cap in one transaction. Recommend capping annual prepays to Pro/Studio only (or requiring operator manual approval for Custom annuals) so one big prepay can't blow the cap-guard headroom. Confirm policy.

## Зависимости от других разделов

- Identity & Auth (accounts / api_keys / subscriptions tables; server-resolved workspace_id from API key — the quota and ledger logic is meaningless until the workspace string is no longer client-controlled free text)
- Metering & Billing (durable Postgres usage roll-up replacing 3-day-TTL Redis; revenue_ledger + rolling-year NPD cap guard; YooKassa recurrent + самозанятый receipts; idempotency)
- Tenancy & Isolation (private workspace_id for the Custom tier; per-plan feed/tool entitlement enforcement; self-serve source isolation so a Custom tenant's firehose can't degrade the shared corpus)
- Observability (per-tenant usage metrics — current MCP /metrics has no per-tenant dimension; needed for overage/upgrade nudges and cap monitoring)
- Self-serve ingest safety (RSS/web/api only, quota-bounded via source_quality; Telegram/email stay platform-curated-only)

