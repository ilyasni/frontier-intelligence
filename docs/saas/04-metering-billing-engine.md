# Учёт потребления и биллинговый движок

<!-- audit-status:2026-08-04 -->
> **📐 ЗАМЫСЕЛ, НЕ РЕАЛИЗОВАНО · сверено 2026-08-04.**
> Замысел, а не описание системы: на дату сверки не реализован. Не читать как отчёт о готовом.
> Конкретных расхождений найдено: **3** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

## Usage Metering & Billing Engine

### 0. Why this section exists

Frontier Intelligence already ships a *FinOps-shaped* control plane: every LLM call ends in an `ExecutionReceipt` that carries `workspace_id`, `budget_class`, `cost_estimate`, `actual_cost`, `cost_drift`, `prompt/completion/billable_tokens` and `cost_currency`, and that receipt is funneled through `LLMRouterClient._record_finops_receipt` → `ProviderBudgetManager.record_execution_receipt` (`worker/llm_router_client.py:1381`, `worker/provider_budget_manager.py:535`). The Redis FinOps layer even pre-builds a per-tenant `cost_workspace` scope (`_finops_scopes` / `_finops_key`, `worker/provider_budget_manager.py:195-255`) and `snapshot_costs(..., workspace_ids=...)` already iterates it (`:824-922`).

**The entire engine is dead in exactly four places**, and this design lights them up:

1. `workspace_id` is `""` on every `ProviderExecutionRequest` and therefore every `ExecutionReceipt` — the chains and `chat()/vision()/embed()` never pass it (`worker/llm_router_client.py:459`, `:915`, `:2315`). So the `cost_workspace` Redis bucket is never written.
2. "Cost" is a **raw token count**, not money: `_usage_cost_estimate` returns `response.usage.billable_tokens` and `embed()` sets `estimated_cost = float(billable_tokens)` with `cost_currency="credits"` (`worker/provider_adapters.py:66-72`, `worker/llm_router_client.py:574`). The only real RUB price table — `wormsoft_limits._normalize_pricing` → `{model: {input, output, cache}}` (`admin/backend/services/wormsoft_limits.py:62-77`) — is fetched and displayed but never multiplied by tokens.
3. The ledger is **volatile**: all FinOps state is day-bucketed Redis hashes with `ttl = 3*24*3600` (`worker/provider_budget_manager.py:454`, `:567`). A billing month cannot be reconstructed.
4. Admission control (`allow_reservation`, `:385`) is keyed **per provider globally** — there is no per-tenant entitlement, only `.env` provider day-caps in `_scope_limits` (`:130-168`).

This section delivers: (a) propagation, (b) a RUB pricing resolver, (c) a durable Postgres ledger + monthly rollup, (d) two-level quota/overage, (e) per-tenant FinOps snapshot + dashboards, and the Wormsoft shared-credit attribution rule.

> **Dependency note:** this engine consumes an `accounts` / `subscriptions` / `entitlements` identity model that does **not yet exist** (no users/accounts/orgs/api_keys tables anywhere). The DDL below references `accounts(id)` and `subscriptions` as FKs; those tables are owned by the *Identity & Tenancy* and *Plans & Subscriptions* sections. Until they land, `account_id` is nullable and we attribute by `workspace_id` only (every workspace already FK-references `workspaces(id TEXT PRIMARY KEY)`).

---

### A. Activate the latent path — thread `workspace_id` into the receipt

**Problem.** `event.workspace_id` is available at the top of enrichment (`worker/tasks/enrichment_task.py:583` `await self._get_workspace(event.workspace_id)`), but the chains call `self.client.chat(...)` / `self.gigachat.embed(...)` with no tenant context, and the router builds `ProviderExecutionRequest(... )` with the default `workspace_id=""`.

**Design.** Add an explicit, optional `workspace_id` (plus `account_id`, `billing_period`, `execution_role`) keyword to the three public router entrypoints and carry it into the request and the receipt. Do **not** use a contextvar as the primary mechanism (the worker is async + `asyncio.gather`-bounded, `enrichment_task.py:528`, and shadow tasks run detached, `:1480`) — pass it explicitly. A contextvar is acceptable only as a *fallback default* for code paths not yet migrated.

```python
# worker/llm_router_client.py — signature changes (S each)
async def chat(self, system, user, *, task="chat", pro=False,
               model_override=None, provider_override=None, max_tokens=1024,
               workspace_id: str = "", account_id: str = "",
               billing_period: str = "", execution_role: str = EXECUTION_ROLE_PRIMARY): ...
async def vision(self, image_bytes, prompt="", *, quality_tier="standard",
                 workspace_id: str = "", account_id: str = "", billing_period: str = ""): ...
async def embed(self, text, *, purpose="document",
                workspace_id: str = "", account_id: str = "", billing_period: str = ""): ...
```

Each builds the request with the tenant set, e.g. in `chat()` at `worker/llm_router_client.py:2315`:

```python
resolved_request = ProviderExecutionRequest(
    system=system, user=user, task=task, task_family=task_family,
    model=effective_model, max_tokens=max_tokens, pro=(pro if index == 0 else False),
    workspace_id=workspace_id,
    metadata={"account_id": account_id, "billing_period": billing_period or _current_period()},
)
```

Then set it on **every** `ExecutionReceipt(...)` constructed in `chat`/`vision`/`embed` (the success receipt, the per-attempt failed receipt, and the terminal failure receipt — there are 3 per method):

```python
receipt = ExecutionReceipt(
    ..., execution_role=execution_role,
    workspace_id=workspace_id, budget_class=candidate.budget_class,
)
```

`_finalize_execution_receipt` (`:1336`) already back-fills `workspace_id` from `budget_attribution` and seeds `budget["workspace_id"]`, and `record_execution_receipt` already reads `receipt.workspace_id` (`worker/provider_budget_manager.py:546`). So once the receipt carries it, the `cost_workspace` Redis scope lights up with **zero** changes to the budget manager for part (a).

**Chain plumbing.** Each chain holds `self.client` and calls `.chat(...)` with no tenant (`worker/chains/relevance_chain.py:200`, `concept_chain.py:76`, `valence_chain.py:81`, `relevance_concepts_chain.py:76`). The cleanest seam: pass tenant into `run()` (which already takes `workspace_name`, `relevance_chain.py:240`) and forward it through `_call`. Concretely, add `workspace_id`/`account_id` params to each chain's `run()` and `_call()`, and in `enrichment_task.process_event` pass `event.workspace_id` (+ the looked-up `ws` account fields from `_get_workspace`, `:583`). The `embed` call at `enrichment_task.py:737` becomes:

```python
vector = await self.gigachat.embed(
    embed_text, purpose="document",
    workspace_id=event.workspace_id, account_id=ws.get("account_id", ""),
)
```

The novelty/entity RSI chains call `self.wormsoft.chat` / `self.polza.chat` **directly** (`novelty_judge_chain.py:89/107`, `entity_equivalence_chain.py:74/85`) — these bypass the router. They must pass `execution_role="shadow"` or a new `task_family`/`budget_class="platform"` so the ledger can exclude them from billable totals (see part C). If they cannot route through `LLMRouterClient`, they must at minimum emit a receipt with `workspace_id=""` (platform attribution).

**Vision worker.** The vision path (`vision_task`) is a separate consumer of `STREAM_VISION` carrying its own event with `workspace_id`; thread it the same way into `router.vision(..., workspace_id=event.workspace_id)`.

**Effort: M** (signature + ~6 receipt constructions in router; ~5 chain `run()`/`_call` edits; 2 task call-sites). No schema change. This is the highest-leverage, lowest-risk change and should ship first behind a `metering_enabled` flag.

---

### B. Pricing Resolver — tokens → RUB

**Goal.** Map `(provider, model, prompt_tokens, completion_tokens, cached_tokens)` → RUB, split **estimated** (pre-call, from request token budget) vs **actual** (post-call, from `response.usage`). Replace the `billable_tokens`-as-cost convention without breaking the existing `actual_units`/budget reservation flow (which can keep using token counts as its internal unit).

**Token shape available.** `GigaChatUsage` exposes `prompt_tokens`, `completion_tokens`, `precached_prompt_tokens` (the cached signal), and `billable_tokens == total_tokens` (`worker/llm_types.py:8-17`). So cached input = `precached_prompt_tokens`; fresh input = `prompt_tokens - precached_prompt_tokens`.

**Price table sources (all per-1K or per-1M token, RUB):**
- **Wormsoft** — live from `wormsoft_limits._normalize_pricing` → `{model: {input, output, cache}}` (`admin/backend/services/wormsoft_limits.py:62-77`), cached in Redis `admin:wormsoft_limits:last_ok`. This is the *only real, fetched* table.
- **OpenRouter (paid), Polza, GigaChat** — no per-call price endpoint in-repo. Add a **config-driven YAML** `config/llm_pricing.yml`, bootstrapped to Postgres via the existing `bootstrap_configs.py` YAML→UPSERT pattern, with manual RUB prices (GigaChat per Sber tariff; OpenRouter/Polza converted from USD at a configured `usd_rub_rate`).

**Pricing table (durable, config-overridable):**

```sql
-- migration 20260701_llm_pricing.sql  (style mirrors 20260418_trend_alerts.sql)
CREATE TABLE IF NOT EXISTS llm_model_prices (
    provider            TEXT NOT NULL,
    model               TEXT NOT NULL,
    input_rub_per_1k    NUMERIC(18,8) NOT NULL DEFAULT 0,
    output_rub_per_1k   NUMERIC(18,8) NOT NULL DEFAULT 0,
    cached_rub_per_1k   NUMERIC(18,8) NOT NULL DEFAULT 0,
    embed_rub_per_1k    NUMERIC(18,8) NOT NULL DEFAULT 0,
    currency            TEXT NOT NULL DEFAULT 'RUB',
    source              TEXT NOT NULL DEFAULT 'config',   -- 'wormsoft_api' | 'config' | 'manual'
    usd_rub_rate        NUMERIC(12,4),                     -- audit: rate used if converted
    effective_from      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, model, effective_from)
);
CREATE INDEX IF NOT EXISTS idx_llm_prices_lookup
    ON llm_model_prices(provider, model, effective_from DESC);
```

We keep history (`effective_from` in the PK) so an invoice for a past month resolves the price that was in effect then — critical for НПД audit defensibility.

**Resolver (new `shared/llm_pricing.py`):**

```python
# returns RUB (Decimal). All token args are post-tokenizer counts.
def resolve_cost_rub(
    provider: str, model: str, *,
    prompt_tokens: int, completion_tokens: int, cached_tokens: int,
    task_family: str, at: float | None = None,
    prices: PriceTable,
) -> Decimal:
    p = prices.lookup(provider, model, at)   # latest row with effective_from <= at
    if p is None:
        return Decimal(0)                    # cost_quality='unpriced' (see ledger)
    if task_family == TASK_FAMILY_EMBEDDINGS:
        return (Decimal(prompt_tokens)/1000) * p.embed_rub_per_1k
    fresh_in = max(0, prompt_tokens - cached_tokens)
    return ( (Decimal(fresh_in)/1000)        * p.input_rub_per_1k
           + (Decimal(cached_tokens)/1000)   * p.cached_rub_per_1k
           + (Decimal(completion_tokens)/1000)* p.output_rub_per_1k )
```

**Estimated vs actual:**
- **Estimated (pre-call):** computed at reservation time from `budget_text` token budget (`relevance_chain.py:261`) + `max_tokens` for completion + assume `cached_tokens=0`. Feeds `cost_estimate` and the *soft pre-admission* quota check (part D).
- **Actual (post-call):** computed in `estimate_cost(...)` from `response.usage`. **Change `worker/provider_adapters.py:_usage_cost_estimate`** so adapters return RUB instead of `billable_tokens`:

```python
def _usage_cost_estimate(response, *, provider, model, task_family, prices):
    u = response.usage
    return float(resolve_cost_rub(provider, model,
        prompt_tokens=u.prompt_tokens, completion_tokens=u.completion_tokens,
        cached_tokens=u.precached_prompt_tokens, task_family=task_family, prices=prices))
```

Each adapter's `estimate_cost` (`provider_adapters.py:253,525,651,756`) gains the `prices` table (injected once at `LLMRouterClient.__init__`, refreshed alongside `refresh_runtime_overrides`). Set `receipt.cost_currency="RUB"`. `cost_drift = actual - estimate` is already computed in `_finalize_execution_receipt:1347` — it now reports **money drift in RUB**, which is exactly the FinOps signal we want.

**Wormsoft has no per-token billing API — only a credit-window plan** (the `alerting_note` in `wormsoft_limits.py:207` confirms "no account-level remaining-credit endpoint"). So for Wormsoft, `resolve_cost_rub` is a **modeled/derived** cost, not an invoice line. We still record it (source=`wormsoft_api` pricing applied to our token counts) and **reconcile** against the flat plan price in part F.

**Effort: M** (resolver + pricing table + YAML + bootstrap wiring + adapter changes + price-history loader). Embeddings are GigaChat-only (`POLICY_MODE_STRICT`, `llm_control_plane.py:722`), so embed pricing needs exactly one row.

---

### C. Durable Ledger — append-only `usage_events` + monthly rollup

**Write path.** The natural emission point is `ProviderBudgetManager.record_execution_receipt` (`worker/provider_budget_manager.py:535`) — it already receives the finalized receipt with all fields. But that method is sync-Redis and hot. To avoid blocking the worker on Postgres, **emit to a Redis Stream** `stream:usage:events` from `record_execution_receipt` (one `XADD`, fire-and-forget) and run a dedicated **async ledger consumer** (`worker/usage_ledger_consumer.py`, mirrors the enrichment consumer-group pattern) that batch-inserts into Postgres. This keeps the LLM hot path latency-neutral and gives durable at-least-once delivery; idempotency is enforced by the DB.

```sql
-- migration 20260701_usage_ledger.sql
CREATE TABLE IF NOT EXISTS usage_events (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id        TEXT NOT NULL,                 -- UNIQUE idempotency key
    workspace_id      TEXT NOT NULL REFERENCES workspaces(id),
    account_id        TEXT,                           -- FK accounts(id) once it exists
    billing_period    TEXT NOT NULL,                  -- 'YYYY-MM' (UTC), denormalized for rollup
    provider          TEXT NOT NULL,
    model             TEXT NOT NULL,
    task              TEXT NOT NULL,
    task_family       TEXT NOT NULL,                  -- text_generation|vision_generation|embeddings
    execution_role    TEXT NOT NULL DEFAULT 'primary',-- primary|shadow
    status            TEXT NOT NULL,                  -- ok|error
    billable          BOOLEAN NOT NULL DEFAULT TRUE,  -- FALSE for shadow/RSI/platform spend
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    cached_tokens     INTEGER NOT NULL DEFAULT 0,
    billable_tokens   INTEGER NOT NULL DEFAULT 0,
    cost_estimate_rub NUMERIC(18,8) NOT NULL DEFAULT 0,
    cost_rub          NUMERIC(18,8) NOT NULL DEFAULT 0,  -- actual
    cost_drift_rub    NUMERIC(18,8) NOT NULL DEFAULT 0,
    cost_quality      TEXT NOT NULL DEFAULT 'priced',    -- priced|unpriced|modeled(wormsoft)
    currency          TEXT NOT NULL DEFAULT 'RUB',
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_usage_events_request UNIQUE (request_id)
);
CREATE INDEX IF NOT EXISTS idx_usage_events_period
    ON usage_events(account_id, billing_period) WHERE billable;
CREATE INDEX IF NOT EXISTS idx_usage_events_ws_period
    ON usage_events(workspace_id, billing_period, occurred_at DESC);
```

`request_id` must be a stable UUID minted **once per top-level LLM call**. Mint it in `chat/vision/embed`, stamp it into `request.metadata["request_id"]` and `receipt.budget_attribution["request_id"]`, and carry it on the stream message. `ON CONFLICT (request_id) DO NOTHING` makes the consumer idempotent under stream redelivery (no double-billing).

**Billable attribution rule (the task's exclusion requirement):**

```
billable = (execution_role != 'shadow')
       AND (task_family in ('text_generation','vision_generation','embeddings'))
       AND (workspace_id is a real tenant workspace, not platform/RSI)
       AND (task not in RSI_TASKS: novelty_judge, entity_equivalence, threshold_audit, ...)
```

Shadow spend is already isolated (`EXECUTION_ROLE_SHADOW`, `llm_router_client.py:1539`) and capped via `llm_runtime_shadow_daily_request_*` (`provider_budget_manager.py:161`). RSI chains (novelty/entity, part A) get `billable=FALSE` and attribute to a reserved `workspace_id='__platform__'`. Non-billable rows are still written (for cost visibility) but excluded from every tenant total.

**Monthly rollup** (the billing snapshot; refreshed by the existing single-process APScheduler in admin, and recomputed on-demand at invoice time):

```sql
CREATE TABLE IF NOT EXISTS usage_rollup_monthly (
    account_id        TEXT NOT NULL,
    billing_period    TEXT NOT NULL,                 -- 'YYYY-MM'
    workspace_id      TEXT NOT NULL,
    provider          TEXT NOT NULL,
    task_family       TEXT NOT NULL,
    request_count     BIGINT NOT NULL DEFAULT 0,
    success_count     BIGINT NOT NULL DEFAULT 0,
    prompt_tokens     BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    billable_tokens   BIGINT NOT NULL DEFAULT 0,
    cost_rub          NUMERIC(20,6) NOT NULL DEFAULT 0,
    cost_drift_rub    NUMERIC(20,6) NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, billing_period, workspace_id, provider, task_family)
);

-- idempotent recompute for a period (run nightly + at close):
INSERT INTO usage_rollup_monthly AS r
  (account_id, billing_period, workspace_id, provider, task_family,
   request_count, success_count, prompt_tokens, completion_tokens,
   billable_tokens, cost_rub, cost_drift_rub, updated_at)
SELECT account_id, billing_period, workspace_id, provider, task_family,
       count(*), count(*) FILTER (WHERE status='ok'),
       sum(prompt_tokens), sum(completion_tokens), sum(billable_tokens),
       sum(cost_rub), sum(cost_drift_rub), NOW()
FROM usage_events
WHERE billing_period = :period AND billable
GROUP BY account_id, billing_period, workspace_id, provider, task_family
ON CONFLICT (account_id, billing_period, workspace_id, provider, task_family)
DO UPDATE SET request_count=EXCLUDED.request_count,
   success_count=EXCLUDED.success_count, prompt_tokens=EXCLUDED.prompt_tokens,
   completion_tokens=EXCLUDED.completion_tokens, billable_tokens=EXCLUDED.billable_tokens,
   cost_rub=EXCLUDED.cost_rub, cost_drift_rub=EXCLUDED.cost_drift_rub, updated_at=NOW();
```

**НПД guardrail (per the legal constraint).** A platform-wide view powers the 2.4M RUB/yr cap monitor — this is the *price the tenant pays*, not our cost, so it must read from the *subscription/invoice* tables (other section), but the rollup gives the COGS side for margin. Add a cheap aggregate the dashboard can poll:

```sql
CREATE OR REPLACE VIEW v_platform_cogs_ytd AS
SELECT substr(billing_period,1,4) AS year, sum(cost_rub) AS llm_cogs_rub
FROM usage_rollup_monthly GROUP BY 1;
```

**Effort: L** (ledger table + rollup table + stream emit in `record_execution_receipt` + new consumer service + rollup job + request_id minting). The consumer is a new long-running process — call this out to the Ops section because it adds a single-point-of-failure unless supervised (Redis stream buffers during downtime, so it is recoverable).

---

### D. Quota / Overage — per-tenant admission, two-level enforcement

**Current state.** `allow_reservation` (`worker/provider_budget_manager.py:385`) checks only `runtime_usage/runtime_model/runtime_task_family/runtime_execution_role` scopes — all **per provider, global**, sourced from `.env` (`_scope_limits:130`). No tenant scope exists in admission, even though the cost side already has `cost_workspace`.

**Design — add a tenant entitlement scope and check it FIRST.** Introduce a per-(account, billing_period) reservation scope counting **money** (RUB) against the plan entitlement, evaluated *before* the existing global provider scopes. The global provider ceiling stays as the second gate (protects the platform from any single tenant when an entitlement is misconfigured). Both must pass.

```python
# worker/provider_budget_manager.py — new scope + new key dimension (M)
def _entitlement_key(account_id, *, period, scope="entitlement_spend"):
    return f"llm:entitlement:{scope}:{account_id}:{period}"   # period = YYYY-MM, NOT day-bucketed

async def allow_tenant_reservation(self, *, account_id, billing_period,
                                   estimated_cost_rub, entitlement) -> tuple[bool, str]:
    # entitlement = {soft_cap_rub, hard_cap_rub, overage_allowed}
    redis = await self._client()
    spent = float(await redis.hget(self._entitlement_key(account_id, period=billing_period),
                                   "committed_cost_rub") or 0.0)
    projected = spent + float(estimated_cost_rub)
    if entitlement.hard_cap_rub and projected > entitlement.hard_cap_rub \
            and not entitlement.overage_allowed:
        return False, "quota_exhausted"                 # ERROR_QUOTA_EXHAUSTED (control_plane:65)
    if entitlement.soft_cap_rub and projected > entitlement.soft_cap_rub:
        return True, "quota_soft_warn"                  # allow, but warn + flag for upsell
    return True, "ok"
```

Wire it into `LLMRouterClient` immediately *before* `_allow_budget_reservation` in all three methods (`chat` `:2386`, `vision` `:964`, `embed` `:524`). On `quota_exhausted` the request is skipped exactly like the existing caps — it joins `skipped_candidates`, produces a terminal receipt with `fallback_reason="quota_exhausted"`, and surfaces `READINESS_QUOTA_EXHAUSTED` (`derive_provider_readiness`, `llm_control_plane.py:165`). On `quota_soft_warn` the call proceeds but the router emits a `note_llm_throttle_event`/routing event the dashboard turns into an upsell prompt.

**Entitlement period bucket is monthly, not daily.** Critically, the entitlement key uses `billing_period` (YYYY-MM) and must **persist for the whole month** — set TTL ≈ 40 days, *not* the 3-day TTL used for the volatile day buckets. The authoritative monthly spend is the Postgres rollup (part C); the Redis entitlement counter is a fast in-period cache that the ledger consumer **reconciles** after each batch insert (`HSET committed_cost_rub = SUM(cost_rub) for period`) so a Redis flush self-heals from Postgres.

**Commit the money on the entitlement scope.** In `commit()` (`:486`) / `record_execution_receipt`, after computing actual RUB, `HINCRBYFLOAT` the entitlement key by `cost_rub`. Reserve on estimate, reconcile to actual on commit (the reserve/commit/release pattern already exists for tokens, `:427-658`).

**Entitlement config** (owned by Plans section, consumed here):

```sql
CREATE TABLE IF NOT EXISTS account_entitlements (
    account_id      TEXT NOT NULL,
    billing_period  TEXT NOT NULL,
    plan_code       TEXT NOT NULL,
    soft_cap_rub    NUMERIC(18,6),      -- warn threshold (e.g. 80% of included)
    hard_cap_rub    NUMERIC(18,6),      -- block threshold (included + overage allowance)
    overage_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (account_id, billing_period)
);
```

**Two-level guarantee, summarized:** (1) tenant entitlement (money, monthly, per account) — fairness + revenue protection; (2) global provider ceiling (requests/day, per provider, `.env`) — platform protection + noisy-neighbor cap. A call must pass both. This also directly mitigates the "global single-flight LLM guard, no per-tenant fairness" MAJOR gap, because a tenant that exhausts its entitlement stops competing for the shared inflight slot.

**Effort: M** (new entitlement scope/key/methods + 3 router wire-ins + commit reconcile + the Plans-owned entitlement table read). Reuses the entire reserve/commit/release machinery.

---

### E. Per-tenant FinOps snapshot + dashboards

**Snapshot.** `snapshot_costs(..., workspace_ids=[...])` (`worker/provider_budget_manager.py:824`) already emits the `cost_workspace` scope — it just needs the caller to pass the active workspace list. The admin FinOps endpoint should pass `workspace_ids` from `SELECT id FROM workspaces`. For durable history use the Postgres rollup (part C) as the source of truth; use the Redis snapshot only for the live current-day view.

**Metrics cardinality (the explicit warning).** `note_llm_cost` labels today are `[service, provider, task_family, execution_role]` (`shared/metrics.py:540, 87`). Adding a `workspace` label is bounded — there are only ~5 curated workspaces and the product targets a *small number of high-ASP B2B subscriptions* (НПД cap), so worst case is tens of series, not thousands. **Add `workspace` as a label but NEVER add `model`** (model × provider × workspace would explode). Concretely:

```python
# shared/metrics.py — bump labelset (S)
LLM_COST_ACTUAL_TOTAL = Counter("frontier_llm_cost_actual_rub_total", ...,
    ["service", "provider", "task_family", "execution_role", "workspace"])
def note_llm_cost(..., workspace: str = ""):
    labels = {..., "workspace": workspace or "__none__"}
```

Pass `finalized.workspace_id` from `_record_finops_receipt` (`llm_router_client.py:1383`). Guard cardinality with a `MAX_WORKSPACE_LABELS` allowlist (active billable workspaces only); anything else collapses to `__other__`. Per-tenant Prometheus series feeds Grafana; the authoritative tenant invoice always comes from Postgres, not Prometheus (Prometheus is lossy/rounded).

**Dashboards.**
- **Admin (operator) usage dashboard:** new FastAPI router (mirrors `admin/backend/routers/monitoring.py` structure) exposing `GET /api/usage/accounts/{id}?period=YYYY-MM` → rollup rows + drift + `cost_quality` breakdown, and a platform `GET /api/usage/cogs` → `v_platform_cogs_ytd` + НПД cap proximity. Reuse the `_assert_alertmanager_token` token pattern (`monitoring.py`) but **fail CLOSED** — the existing helper fails *open when unset*, which is unacceptable on a revenue surface; this must require a token.
- **Tenant-facing usage view (MCP/Admin read-only):** a `get_usage_summary` MCP tool (joins the ~30 existing tools) returning the tenant's own `usage_rollup_monthly` filtered to their entitled workspaces, plus % of plan consumed and soft-cap warnings — drives self-serve upsell.

**Effort: S–M** (label bump = S; admin router + view = M; MCP tool = S, depends on MCP-auth from the Identity section to scope the caller to their own account).

---

### F. Shared Wormsoft credit attribution (no balance API)

**Problem.** Wormsoft sells a flat credit-window plan (`subscription-limits`, `_normalize_plans`, `wormsoft_limits.py:50-59`) and exposes **no remaining-credit endpoint** (`alerting_note`, `:207`). We pool one Wormsoft key across all tenants. So Wormsoft spend cannot be billed per token from an invoice — it must be **attributed**.

**Attribution rule (proportional, token-weighted):**
1. The pricing resolver still computes a **modeled** RUB per Wormsoft call using the live `_normalize_pricing` per-model table applied to our measured tokens, written to the ledger with `cost_quality='modeled'`. This is the *fair-share weight*, not an invoice.
2. The **true** Wormsoft cost for a period is the flat plan price (`plan.price`, `wormsoft_limits.py:46`) × windows consumed — a platform fixed cost.
3. At period close, **re-scale** modeled Wormsoft RUB so the per-tenant shares sum to the actual flat plan spend:
   `tenant_wormsoft_rub = flat_plan_rub × (tenant_modeled_wormsoft_rub / Σ all_modeled_wormsoft_rub)`.
   Persist the scaling factor on the rollup as an audit field; the reconciliation job (part C's nightly recompute) applies it for `provider='wormsoft'`.
4. Because Wormsoft is the **primary** text provider (Wormsoft→OpenRouter→Polza→GigaChat), most billable text spend flows here, so this re-scaling is the dominant accuracy lever. The platform absorbs the gap between modeled and flat (it is a fixed-cost arbitrage, tracked as platform margin, not billed to tenants).
5. Live pressure (no balance API): keep the existing signal — monitor 429 bursts + fallback spikes via `WormsoftCreditGuard` (`worker/wormsoft_credit_guard.py`, already invoked at `_record_finops_receipt:1393`) and the credit-window counters (`add_credit_usage`, `provider_budget_manager.py:274`). The `cost_workspace` scope now lets us see *which tenant* is driving the 429s.

**Effort: M** (reconciliation/re-scale step in the rollup job + plan-price ingestion from `_normalize_plans` + audit field). Pure post-processing; no hot-path change.

---

### G. Sequenced change list

| # | Change | Files / symbols | Effort |
|---|--------|-----------------|--------|
| 1 | Thread `workspace_id`/`account_id` into `chat/vision/embed` + set on all `ExecutionReceipt`s; mint `request_id` | `worker/llm_router_client.py` (`:459,915,2315`, 6 receipts) | M |
| 2 | Forward tenant through chains' `run()`/`_call()` + 2 task call-sites | `worker/chains/*`, `worker/tasks/enrichment_task.py:737`, vision_task | M |
| 3 | Pricing table + resolver + YAML + bootstrap | new `shared/llm_pricing.py`, `config/llm_pricing.yml`, `bootstrap_configs.py`, mig `20260701_llm_pricing.sql` | M |
| 4 | Adapters return RUB; set `cost_currency='RUB'` | `worker/provider_adapters.py:_usage_cost_estimate` + 4 `estimate_cost` | S |
| 5 | `usage_events` + `usage_rollup_monthly` DDL | mig `20260701_usage_ledger.sql` | S |
| 6 | Stream-emit from `record_execution_receipt`; new ledger consumer; rollup job | `worker/provider_budget_manager.py:535`, new `worker/usage_ledger_consumer.py`, admin cron | L |
| 7 | Tenant entitlement scope + admission wire-in + commit reconcile | `worker/provider_budget_manager.py` (new methods), router (3 sites) | M |
| 8 | `workspace` label on `note_llm_cost` (no model label) + allowlist | `shared/metrics.py:540,87`, `llm_router_client.py:1383` | S |
| 9 | Admin usage router (token fail-CLOSED) + `v_platform_cogs_ytd` + НПД monitor | new `admin/backend/routers/usage.py` | M |
| 10 | `get_usage_summary` MCP tool (account-scoped) | MCP gateway | S |
| 11 | Wormsoft proportional re-scale in rollup job | rollup job, `_normalize_plans` ingest | M |
| 12 | Adopt Alembic for these migrations (currently raw-SQL unversioned) | `storage/postgres/migrations/` | M |

**Recommended order:** 1 → 2 (light up the latent path behind a flag, observe `cost_workspace` populate) → 5 → 6 (durable ledger before money matters) → 3 → 4 (switch to RUB) → 8 → 9 (visibility) → 7 (enforcement, last — never block tenants before the ledger is trusted) → 11 → 10. Ship 1+2 first; everything else is additive.


---

## Открытые решения по этому разделу

- account_id grain: bill per ACCOUNT (one customer, possibly many entitled workspaces) vs per WORKSPACE. RECOMMENDATION: account is the billing entity; workspace is the cost-attribution dimension. The ledger carries both, the rollup PK is (account_id, billing_period, workspace_id, ...). Blocked on the Identity section delivering accounts/subscriptions — until then account_id is nullable and we roll up by workspace_id.
- Ledger write path: synchronous Postgres insert inside record_execution_receipt vs the proposed async Redis-stream consumer. RECOMMENDATION: async stream consumer — keeps the LLM hot path latency-neutral and gives at-least-once durability with request_id idempotency. Cost: one more long-running process (Ops/HA concern). If a second process is unacceptable solo-ops, fall back to a synchronous best-effort insert with a Redis spill-buffer.
- Pricing precision for OpenRouter/Polza (USD-denominated, no in-repo RUB endpoint): pin a manual usd_rub_rate in config vs fetch a live FX rate. RECOMMENDATION: manual config rate refreshed monthly, stored on each price row (usd_rub_rate audit column) so historical invoices are reproducible. Live FX is over-engineering at НПД scale.
- Soft-cap behavior on quota_soft_warn: warn-and-continue vs degrade-to-cheapest-provider (force GigaChat). RECOMMENDATION: warn-and-continue + upsell prompt for paid tiers; only degrade for tenants in overage with overage_allowed=FALSE. Avoids silently shipping worse intelligence to a paying customer.
- Wormsoft cost truth: bill tenants the MODELED per-token RUB (simple, may over/under-recover vs flat plan) vs the proportional RE-SCALE to the flat plan price (accurate platform COGS, more moving parts). RECOMMENDATION: re-scale at period close for COGS/margin accuracy; tenants are billed on plan price + metered overage, not raw Wormsoft tokens, so the re-scale only affects internal margin reporting not customer invoices.
- Prometheus workspace label vs Postgres-only tenant cost: RECOMMENDATION: add the bounded workspace label (tens of series max under НПД) for live Grafana, but treat Postgres usage_events/rollup as the SINGLE SOURCE OF TRUTH for any invoice — Prometheus counters are lossy across restarts and must never feed billing.

## Зависимости от других разделов

- Identity & Tenancy (accounts/users/api_keys + MCP/Admin auth): supplies accounts(id) FK, scopes the get_usage_summary MCP tool to the caller's account, and provides the authenticated workspace_id that this engine bills against (today workspace is client-controlled free text — billing on it is unsafe until auth lands).
- Plans & Subscriptions: owns plan_code, included quotas, overage policy, and the account_entitlements table this engine READS in part D; also owns YooKassa invoice/receipt generation that consumes usage_rollup_monthly + the НПД 2.4M cap guardrail.
- Postgres tenancy / RLS: usage_events/usage_rollup must be covered by the same row-level isolation as posts; tenant-facing reads must be RLS-scoped to the account's entitled workspaces.
- Ops / HA & Scheduler: the new ledger consumer and the nightly rollup/reconcile job add processes to a currently single-process APScheduler + single-node setup; needs supervision/restart so a downed consumer doesn't silently stop billing (Redis stream buffers but must be drained).
- Migrations/CI: this section adds 3 migrations to an unversioned raw-SQL prod flow; depends on adopting Alembic (available, unused) so billing schema changes are reviewable and rollback-able.

