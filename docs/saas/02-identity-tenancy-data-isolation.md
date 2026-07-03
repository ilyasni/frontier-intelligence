# Идентичность, мультитенантность и изоляция данных

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

## Identity, Tenancy & Data Isolation

### 0. Starting point (what the code actually is today)

The system has **soft tenancy keyed on a global `workspace_id` slug** and **zero identity**. Concretely:

- `workspaces` is the only tenant-ish table. Its PK is a free-text slug (`id TEXT PRIMARY KEY`) — `storage/postgres/init.sql:4` and `shared/models/workspace.py:11`. The five slugs (`disruption`, `ai_trends`, `ai_research`, `ai_products_media`, `design`) come from `config/workspaces.yml` and are bootstrapped by `admin/backend/services/bootstrap_configs.py`.
- `workspace` is **client-controlled free text defaulting to `"disruption"`** on every MCP tool: `shared/search_contracts.py:24` (`workspace: str = Field(default="disruption")`), echoed verbatim in the gateway signature `mcp/mcp_gateway.py:38` and the tool input schemas in `mcp/server.py:83`. Any caller can read/mutate any workspace.
- There are **no `accounts`, `users`, `memberships`, `api_keys`, `subscriptions`, `entitlements`** tables anywhere.
- `workspace_id` is enforced at the storage chokepoints already: Postgres FK on most tables, Qdrant `_build_payload_filter` / `_build_trend_filter` (`worker/integrations/qdrant_client.py:115,154`), Neo4j `ensure_workspace_node` + `{workspace_id}` on every `Concept`/`Document` MERGE (`worker/integrations/neo4j_client.py:23,76,85`). **The filter is correct; the input is untrusted.**
- Child tables **lack `workspace_id`**: `source_checkpoints`, `source_runs` (PK/FK on `source_id` only — `init.sql:40,51`), `indexing_status` (PK `post_id` — `init.sql:91`), `post_enrichments` (FK `post_id` only — `init.sql:317`). And `cluster_runs.workspace_id` / `admin_manual_jobs.workspace_id` are **nullable** (`init.sql:265,283`).
- Migrations are **unversioned raw SQL** applied in filename order (`storage/postgres/migrations/*.sql`), idempotent (`IF NOT EXISTS`). Alembic is available but unused. We keep raw-SQL idempotent files (lowest-friction, matches the existing operator muscle memory) but **introduce an ordering manifest** so a fresh DB and prod converge.

The strategy below is shaped by the НПД constraints (solo operator, no employees, ~10–30 B2B feed subscribers, not thousands of self-serve users). That tenant count is the single most important design input: it makes **shared-DB + RLS + shared Qdrant/Neo4j with hardened filters** the correct first step, and makes per-account collections/databases an over-build we explicitly defer.

---

### 1. Entity model

Three layers, cleanly separated so the slug never has to change:

1. **Identity** — `accounts` (the paying tenant / billing subject), `users` (a human login), `memberships` (user↔account with a role), `api_keys` (machine credential bound to an account).
2. **Catalog** — `feeds` (a sellable curated intelligence product) decoupled from the physical `workspaces` slug.
3. **Entitlement** — `account_feeds` (which accounts may *read* which shared feeds) and `account_workspaces` (which workspaces an account *owns*, for private self-serve sources).

**The key modelling decision (HYBRID model):** a `workspace` is no longer "the tenant". It becomes a *physical data partition* that is either **`shared`** (platform-owned curated feed, read by many accounts through `account_feeds`) or **`private`** (owned by exactly one account through `account_workspaces`, created when that account adds self-serve RSS/web/api sources).

```
accounts ──< memberships >── users
   │
   ├──< api_keys
   │
   ├──< account_feeds >── feeds ──1:1── workspaces (kind='shared')   ← READ entitlement, many accounts
   │
   └──< account_workspaces ─1:1─ workspaces (kind='private')         ← OWNED partition, one account
```

- The 5 existing slugs become `workspaces.kind='shared'`, each wrapped by one `feeds` row. Today's "owner" gets an `accounts` row entitled to all of them, so nothing the operator does breaks.
- A self-serve customer who adds a custom RSS source gets a freshly minted `workspaces` row with `kind='private'`, `owner_account_id` set, and a generated non-guessable slug (e.g. `acct_7Gk2…__custom`). Telegram sources are never offered here (platform-curated-only); email is never offered (plaintext IMAP creds in `sources.extra` — confirmed unsafe).

---

### 2. SQL DDL — new tables

New migration `storage/postgres/migrations/20260701_identity_tenancy.sql` (idempotent, raw SQL to match the existing convention; Alembic equivalent in §7). UUID PKs via `pgcrypto`/`gen_random_uuid()`.

```sql
-- 20260701_identity_tenancy.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── accounts: the paying tenant / billing subject ────────────────────────────
CREATE TABLE IF NOT EXISTS accounts (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug          TEXT NOT NULL UNIQUE,              -- short stable handle, e.g. 'acme'
    display_name  TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active','suspended','closed')),
    billing_email TEXT,
    -- НПД revenue-cap guardrail lives in billing domain; mirror a flag here:
    is_internal   BOOLEAN NOT NULL DEFAULT FALSE,    -- TRUE = operator's own account
    extra         JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── users: a human login (email/password or magic-link) ──────────────────────
CREATE TABLE IF NOT EXISTS users (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email          CITEXT NOT NULL UNIQUE,           -- needs CREATE EXTENSION citext
    password_hash  TEXT,                             -- argon2id; NULL if magic-link only
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active','disabled')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── memberships: user ↔ account with a role ──────────────────────────────────
CREATE TABLE IF NOT EXISTS memberships (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id  UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id     UUID NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    role        TEXT NOT NULL DEFAULT 'member'
                CHECK (role IN ('owner','admin','member','viewer')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, user_id)
);
-- Note: 'operator' (platform super-admin for /ops) is NOT a membership role;
-- it is a property of the operator's own internal account + a separate ops auth path (§5).

-- ── api_keys: machine credential (prefix + hashed secret) ─────────────────────
-- Token shown ONCE at creation as  fk_<prefix>.<secret> ; we store only the hash.
CREATE TABLE IF NOT EXISTS api_keys (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id   UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    prefix       TEXT NOT NULL UNIQUE,               -- 8-char public lookup key, indexed
    secret_hash  TEXT NOT NULL,                      -- argon2id/sha256-hmac of the secret half
    name         TEXT NOT NULL DEFAULT 'default',
    scopes       TEXT[] NOT NULL DEFAULT '{search}', -- e.g. {search, ingest}
    last_used_at TIMESTAMPTZ,
    expires_at   TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(prefix) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_api_keys_account ON api_keys(account_id);

-- ── feeds: a sellable curated intelligence product (catalog) ─────────────────
CREATE TABLE IF NOT EXISTS feeds (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug          TEXT NOT NULL UNIQUE,              -- public product handle, e.g. 'ai-trends'
    workspace_id  TEXT NOT NULL REFERENCES workspaces(id), -- 1:1 to a kind='shared' workspace
    display_name  TEXT NOT NULL,
    description   TEXT,
    is_listed     BOOLEAN NOT NULL DEFAULT TRUE,     -- visible in the catalog
    min_tier      TEXT,                              -- coordinated with billing/plans
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── account_feeds: READ entitlement (shared feed → many accounts) ────────────
CREATE TABLE IF NOT EXISTS account_feeds (
    account_id  UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    feed_id     UUID NOT NULL REFERENCES feeds(id)    ON DELETE CASCADE,
    source      TEXT NOT NULL DEFAULT 'subscription'  -- 'subscription' | 'trial' | 'comp'
                CHECK (source IN ('subscription','trial','comp')),
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ,                          -- NULL = until subscription lapses
    PRIMARY KEY (account_id, feed_id)
);

-- ── account_workspaces: OWNED private partition (one account) ────────────────
CREATE TABLE IF NOT EXISTS account_workspaces (
    workspace_id TEXT PRIMARY KEY REFERENCES workspaces(id) ON DELETE CASCADE,
    account_id   UUID NOT NULL REFERENCES accounts(id)      ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_account_workspaces_account ON account_workspaces(account_id);
```

**Workspace gets an account dimension without changing its PK** — additive columns only:

```sql
-- same migration: extend workspaces, do NOT touch the PK
ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'shared'
        CHECK (kind IN ('shared','private')),
    ADD COLUMN IF NOT EXISTS owner_account_id UUID REFERENCES accounts(id);
-- invariant (enforced in app + a trigger): kind='private' ⇒ owner_account_id NOT NULL;
--                                          kind='shared'  ⇒ owner_account_id IS NULL.
CREATE INDEX IF NOT EXISTS idx_workspaces_owner ON workspaces(owner_account_id)
    WHERE owner_account_id IS NOT NULL;
```

**Effort: M** (DDL + SQLAlchemy models mirroring `shared/models/workspace.py` style; new `shared/models/account.py`, `user.py`, `membership.py`, `api_key.py`, `feed.py`, `entitlement.py`).

---

### 3. Backfilling `workspace_id` onto child tables + fixing NULLs

This must land **before** RLS, because RLS policies need the column to exist and be populated. Migration `20260702_workspace_id_backfill.sql`:

```sql
-- 20260702_workspace_id_backfill.sql  (idempotent)

-- 3a. add the column nullable, backfill from the parent, then enforce NOT NULL --
ALTER TABLE source_checkpoints ADD COLUMN IF NOT EXISTS workspace_id TEXT;
UPDATE source_checkpoints sc
   SET workspace_id = s.workspace_id
  FROM sources s
 WHERE s.id = sc.source_id AND sc.workspace_id IS NULL;

ALTER TABLE source_runs ADD COLUMN IF NOT EXISTS workspace_id TEXT;
UPDATE source_runs sr
   SET workspace_id = s.workspace_id
  FROM sources s
 WHERE s.id = sr.source_id AND sr.workspace_id IS NULL;

ALTER TABLE indexing_status ADD COLUMN IF NOT EXISTS workspace_id TEXT;
UPDATE indexing_status ix
   SET workspace_id = p.workspace_id
  FROM posts p
 WHERE p.id = ix.post_id AND ix.workspace_id IS NULL;

ALTER TABLE post_enrichments ADD COLUMN IF NOT EXISTS workspace_id TEXT;
UPDATE post_enrichments pe
   SET workspace_id = p.workspace_id
  FROM posts p
 WHERE p.id = pe.post_id AND pe.workspace_id IS NULL;

-- 3b. enforce FK + NOT NULL once backfill is clean (guarded so re-run is safe) --
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM source_checkpoints WHERE workspace_id IS NULL) THEN
    ALTER TABLE source_checkpoints
        ALTER COLUMN workspace_id SET NOT NULL,
        ADD CONSTRAINT IF NOT EXISTS source_checkpoints_ws_fk
            FOREIGN KEY (workspace_id) REFERENCES workspaces(id);
  END IF;
  -- repeat the same guarded block for source_runs, indexing_status, post_enrichments
END $$;

CREATE INDEX IF NOT EXISTS idx_source_checkpoints_ws ON source_checkpoints(workspace_id);
CREATE INDEX IF NOT EXISTS idx_source_runs_ws        ON source_runs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_indexing_status_ws    ON indexing_status(workspace_id);
CREATE INDEX IF NOT EXISTS idx_post_enrichments_ws   ON post_enrichments(workspace_id);

-- 3c. fix nullable workspace_id on cluster_runs / admin_manual_jobs ------------
-- These have orphan/system rows. Route system-owned rows to a sentinel workspace.
INSERT INTO workspaces (id, name, description, kind)
     VALUES ('__system__', 'System / cross-workspace jobs', 'Sentinel for ops rows', 'shared')
ON CONFLICT (id) DO NOTHING;

UPDATE cluster_runs       SET workspace_id = '__system__' WHERE workspace_id IS NULL;
UPDATE admin_manual_jobs  SET workspace_id = '__system__' WHERE workspace_id IS NULL;
-- keep them nullable in schema for now (some cross-workspace jobs are legitimately
-- account-agnostic); RLS treats '__system__' as operator-only (§4).
```

The writer code paths must also start *setting* `workspace_id`. The parent is always in scope where these are written: `ingest/source_runtime.py` writes `source_runs`/`source_checkpoints` (it already has the `Source`), the worker writes `indexing_status`/`post_enrichments` with the `Post` in hand. **Effort: M** (DDL is S; the ~6 writer call-sites that must add `workspace_id=...` are the work; verify via `Grep` for `INSERT INTO source_runs`, `INSERT INTO post_enrichments`, etc.).

---

### 4. Postgres isolation — shared-DB + Row-Level Security (RLS)

**Decision: shared database + RLS, scoped by a per-request session GUC.** This is the pragmatic solo-operable choice for ≤ low-hundreds of tenants: one DB to back up, one schema to migrate, defence-in-depth that survives a forgotten `WHERE workspace_id = …`. We do **not** use schema-per-tenant or DB-per-tenant (operationally heavier, not justified at НПД scale).

We scope RLS by **account**, not raw workspace, because that is the real tenant. A small SQL helper resolves "which workspaces may this account touch" from the two entitlement tables.

```sql
-- 20260703_rls.sql  (idempotent)

-- GUCs set per request by middleware:
--   app.account_id   = current account UUID (empty for operator/system path)
--   app.is_operator  = 'on' when the operator/ops principal is acting

-- helper: workspaces readable by the current account (owned private + entitled shared)
CREATE OR REPLACE FUNCTION app_visible_workspace(ws TEXT) RETURNS BOOLEAN AS $$
  SELECT
    current_setting('app.is_operator', true) = 'on'
    OR EXISTS ( -- owned private workspace
         SELECT 1 FROM account_workspaces aw
          WHERE aw.workspace_id = ws
            AND aw.account_id = NULLIF(current_setting('app.account_id', true),'')::uuid)
    OR EXISTS ( -- entitled shared feed
         SELECT 1 FROM feeds f
           JOIN account_feeds af ON af.feed_id = f.id
          WHERE f.workspace_id = ws
            AND af.account_id = NULLIF(current_setting('app.account_id', true),'')::uuid
            AND (af.expires_at IS NULL OR af.expires_at > NOW()));
$$ LANGUAGE sql STABLE;

-- write visibility is stricter: only owned private workspaces (shared feeds are read-only
-- to customers; only the operator/system writes shared data).
CREATE OR REPLACE FUNCTION app_writable_workspace(ws TEXT) RETURNS BOOLEAN AS $$
  SELECT
    current_setting('app.is_operator', true) = 'on'
    OR EXISTS (
         SELECT 1 FROM account_workspaces aw
          WHERE aw.workspace_id = ws
            AND aw.account_id = NULLIF(current_setting('app.account_id', true),'')::uuid);
$$ LANGUAGE sql STABLE;

-- enable + policy on every workspace-scoped table. Example for posts:
ALTER TABLE posts ENABLE ROW LEVEL SECURITY;
ALTER TABLE posts FORCE ROW LEVEL SECURITY;   -- applies even to table owner role
CREATE POLICY posts_read  ON posts FOR SELECT USING (app_visible_workspace(workspace_id));
CREATE POLICY posts_write ON posts FOR ALL    USING (app_writable_workspace(workspace_id))
                                         WITH CHECK (app_writable_workspace(workspace_id));
-- repeat for: sources, source_checkpoints, source_runs, indexing_status,
--             post_enrichments, trend_clusters, semantic_clusters, emerging_signals,
--             signal_time_series, trend_alerts, missing_signals, media_objects,
--             media_groups, cluster_runs, admin_manual_jobs.
```

**App role must not be a superuser / table owner** (RLS is bypassed by `BYPASSRLS`/owner unless `FORCE`). Two DB roles: `frontier_app` (NOLOGIN base, no `BYPASSRLS`, used by worker/MCP/admin-customer paths) and `frontier_migrate` (runs DDL/migrations, owns tables). The worker's enrichment writes run as `frontier_app` with `app.is_operator='on'` (it legitimately writes shared curated data on the platform's behalf).

**Session-setting middleware** (FastAPI, shared by MCP REST `:8100` and Admin `:8101`). Authentication (resolving the API key → account) is owned by the Auth domain; this middleware only *applies* the resolved principal to the DB session via `SET LOCAL` inside the request transaction:

```python
# shared/tenancy.py  — used by mcp/server.py and admin/backend/main.py
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

async def apply_tenant_guc(session: AsyncSession, principal) -> None:
    """principal: resolved by auth layer. SET LOCAL is transaction-scoped,
    so it cannot leak across pooled connections."""
    await session.execute(
        text("SET LOCAL app.account_id = :acct"),
        {"acct": str(principal.account_id) if principal.account_id else ""},
    )
    await session.execute(
        text("SET LOCAL app.is_operator = :op"),
        {"op": "on" if principal.is_operator else "off"},
    )
```

Critical implementation notes:
- Use **`SET LOCAL`** (transaction-scoped), and ensure every request runs inside a transaction (`async with session.begin():`). Today many handlers use bare `AsyncSession(get_engine())` (e.g. `mcp/tools/search_frontier.py:65`) and raw `text()` queries — those must move to a request-scoped session that has `apply_tenant_guc` called first. **Never** use `SET` (session-scoped) on a pooled async connection — it leaks to the next tenant.
- The worker is **not** request-scoped; it sets `app.is_operator='on'` once per unit-of-work session (it's trusted infrastructure writing platform data). Customer-driven self-serve ingest (private workspaces) sets `app.account_id` to the owning account.

**Effort: L** (policies are mechanical S–M; the real work is routing every handler through a request-scoped, GUC-stamped session and converting the ad-hoc `AsyncSession(get_engine())` + raw `text()` call-sites in `mcp/tools/*` and `admin/backend/routers/*`). This is the highest-leverage isolation work and the one most likely to surface latent leaks.

---

### 5. The workspace-slug-PK migration problem (don't break Claude/Cursor)

**Problem:** existing Claude/Cursor MCP clients pass the slug literally (`workspace="disruption"`, default in `shared/search_contracts.py:24`, `mcp/mcp_gateway.py:38`). We must add an account dimension without forcing every client to change its config string.

**Decision: keep the slug as the workspace PK forever; bind the account at the credential, not in the slug.** The API key *is* the account. The flow becomes:

1. Client sends its API key (header `Authorization: Bearer fk_<prefix>.<secret>` for REST `:8100`; for the MCP gateway `:8102`, the key travels as a header negotiated at MCP connect / a `?key=` on the SSE URL — Auth domain decides the exact carrier).
2. Auth resolves `api_key.prefix → account_id`. Middleware stamps `app.account_id`.
3. The `workspace` argument is still accepted, but it is now **validated against entitlement** instead of trusted: `app_visible_workspace(workspace)` must be true, else `403`. The default `"disruption"` is honoured only if the account is entitled to it (the operator's internal account is, so the owner's own clients keep working unchanged).
4. **Slug resolution for collisions:** shared feed slugs stay globally unique (today's 5). Private workspace slugs are namespaced and non-guessable (`acct_<shortid>__<label>`), so a customer's `workspace="acme__competitors"` cannot collide with a shared slug and cannot be guessed to reach another account's data (and RLS would block it anyway).

This means **zero breaking change for the operator's existing clients** (same slugs, now gated by an entitlement that the internal account already has), and a clean path for paying customers (their key implies their account; they pass the feed slug they bought, or their private slug).

A thin **resolver helper** centralises this so tools don't each re-implement it:

```python
# shared/tenancy.py
async def resolve_workspace(session, principal, requested_slug: str | None) -> str:
    slug = (requested_slug or principal.default_workspace or "disruption").strip()
    ok = await session.scalar(text("SELECT app_visible_workspace(:s)"), {"s": slug})
    if not ok:
        raise PermissionError(f"workspace '{slug}' not entitled for this account")
    return slug
```

Every tool replaces its raw `req.workspace` use with `resolve_workspace(...)`. **Effort: M** (one helper + ~10 tool call-sites in `mcp/tools/*`; the Qdrant/Neo4j filters downstream already take the resolved slug unchanged).

---

### 6. Qdrant + Neo4j isolation

**Decision: keep the single shared Qdrant collection and single Neo4j graph; harden the existing filter chokepoints to be unbypassable. Defer per-account collections/databases.** Rationale: at НПД tenant scale, per-collection/per-database multiplies operational surface (backup, schema, warmup) for no security gain that RLS-style filtering doesn't already give — *provided* the filter is mandatory and the input is the RLS-validated slug from §5.

**Qdrant hardening** (`worker/integrations/qdrant_client.py`):
- The `workspace_id` `must` condition already exists and is correct (`_build_payload_filter:125`, `_build_trend_filter:162`, KEYWORD-indexed at `:42,:56`). The fix is **upstream**: the `workspace_id` passed in must be the §5 resolved+entitled slug, never raw client text. Add an assertion at the chokepoint that `workspace_id` is non-empty and one slug only (reject `MatchAny` over multiple workspaces unless the caller is operator) so a future refactor can't silently widen scope.
- For **private** self-serve workspaces, the payload filter is identical — isolation is by slug, enforced by entitlement before the query is built. No new collection needed.
- Cross-workspace tools (`get_frontier_brief`) must expand to *only the entitled set*: pass `MatchAny(any=entitled_slugs)` where `entitled_slugs` comes from `app_visible_workspace` over the account's feeds — not a hardcoded `cross_workspace_bridges` list.

**Neo4j hardening** (`worker/integrations/neo4j_client.py`):
- Every `Concept`/`Document` MERGE is already scoped `{workspace_id: $ws}` (`:85,:111,:161`). Same principle: the `$ws` must be the resolved slug.
- **The irreversible-merge risk** (entity merges via `merge_concepts` at `:267`) must be gated: those are RSI/operator tools and must require `principal.is_operator` (or the owning account for a private workspace). Wire this into the §5 resolver before any write/merge tool runs. Do **not** expose entity-merge tools to customer API keys (scope `{search}`, not `{merge}`).
- Keep one Neo4j database. If a future enterprise tier needs hard graph isolation, the migration path is Neo4j *multi-database* keyed per private workspace — but that is post-НПД and explicitly out of scope now.

**Effort: M** (assertions + entitled-set expansion in `get_frontier_brief`; operator-gate on merge tools).

---

### 7. Migration order (Alembic) and rollout sequence

Adopt Alembic now (it's installed, unused) as the **ordering authority**, but let each revision execute the idempotent raw-SQL file via `op.execute(open(...).read())` so we keep the existing review-friendly SQL and the operator's `psql` muscle memory. `alembic stamp head` on the current prod DB to baseline, then:

```
rev 0001  baseline            -> alembic stamp (existing init.sql state, no-op)
rev 0002  20260701_identity_tenancy.sql      (accounts/users/memberships/api_keys/
                                              feeds/account_feeds/account_workspaces +
                                              workspaces.kind/owner_account_id)
rev 0003  data: seed feeds for the 5 shared slugs; create operator internal account;
          INSERT account_feeds for all 5 to the operator account; mark workspaces.kind
rev 0004  20260702_workspace_id_backfill.sql (child-table columns + NULL fixes + sentinel)
rev 0005  20260703_rls.sql                   (helpers, ENABLE/FORCE RLS, policies, DB roles)
rev 0006  app cutover: middleware GUC + resolve_workspace + writer workspace_id set
```

**Order rationale:** identity tables (0002) before any data that references accounts (0003); backfill (0004) before RLS (0005) so policies see populated columns; RLS enabled (0005) only after the app can stamp the GUC in a feature-flagged read-only dry-run, then flip enforcement. **Roll RLS out with `app.is_operator` defaulting to `'on'`** for all existing service identities first (no behavioural change), then progressively flip customer paths to scoped accounts. This makes the cutover reversible at every step.

**Effort per rev:** 0002 M, 0003 S, 0004 M, 0005 L, 0006 L. Total domain: **XL** but cleanly staged; each rev is independently shippable and the system stays solo-operable throughout (no tenant > the operator until customers are onboarded).

---

### 8. Why this stays solo-operable

- One DB, one Qdrant collection, one Neo4j graph — one backup, one migration path. No per-tenant infra to provision by hand.
- RLS is **defence-in-depth, not the only line** — the resolver (§5) already 403s before the query runs, so a missed policy fails closed at the app layer too.
- The entitlement tables are tiny and entirely automatable from the billing/subscription webhook (YooKassa) — `INSERT account_feeds` on payment, `expires_at` on lapse. No manual provisioning.
- Reuses what exists: Qdrant/Neo4j filters, `provider_budget_manager`'s `cost_workspace` scope (for the metering domain), the `workspaces` table and its bootstrap. We add columns and policies, not a parallel data model.


---

## Открытые решения по этому разделу

- API-key carrier on the MCP gateway (:8102): the SSE/Streamable-HTTP transport in mcp/mcp_gateway.py currently uses FastMCP with allowed_hosts=['*'] and no auth hook. Recommendation: terminate auth at a thin reverse proxy / FastMCP middleware that reads a header and maps it to account before proxying to REST :8100 — exact carrier (header vs ?key= on SSE URL) is owned by the Auth/Edge domain; this design assumes the resolved principal arrives at apply_tenant_guc.
- Whether private self-serve workspaces should ever become a separate Qdrant collection at higher tiers. Recommendation: NO for НПД scale (single collection + mandatory filter is sufficient and cheaper to operate); revisit only if an enterprise tier with contractual hard-isolation appears, at which point Neo4j multi-database + per-collection is the documented path.
- Disposition of legitimately cross-workspace system rows in cluster_runs / admin_manual_jobs: this design routes NULLs to a '__system__' sentinel workspace and keeps the column nullable. Recommendation: confirm with the RSI/ops domain that no cross-workspace job needs to remain truly account-agnostic; if some do, treat '__system__' as operator-only under RLS (already specified).
- Whether the operator's own clients should authenticate at all in phase 1. Recommendation: issue the operator an internal account + api_key now (is_internal=TRUE, entitled to all 5 feeds) and run the operator's Claude/Cursor with that key, so the auth path is exercised from day one and there is no permanently unauthenticated bypass to maintain.

## Зависимости от других разделов

- Auth/Edge domain: must resolve api_keys.prefix -> account and produce the `principal` (account_id, is_operator, scopes, default_workspace) that apply_tenant_guc consumes; must add auth to MCP REST :8100, MCP gateway :8102, Admin :8101 (all currently 0.0.0.0, wildcard CORS, DNS-rebinding off).
- Billing/Subscriptions/YooKassa domain: owns writing account_feeds (grant on payment, expires_at on lapse) and the НПД 2.4M RUB revenue-cap guardrail referenced by accounts.is_internal / status; feeds.min_tier coordinates with the plan catalog.
- Per-tenant metering/FinOps domain: depends on workspace_id finally being propagated into the LLM request path (currently dead — shared/llm_control_plane.py ProviderExecutionRequest.workspace_id at :545 is never populated); this design's resolved-slug becomes the workspace_id that worker/provider_budget_manager.py's cost_workspace scope (:247) records.
- Self-serve sources domain: depends on account_workspaces + workspaces.kind='private' to create per-account private partitions for RSS/web/api sources (Telegram and email excluded); reuses shared/source_definitions.validate_source_payload.
- Observability domain: per-tenant metrics need the resolved account_id/workspace_id as a label; MCP /metrics currently has no per-tenant dimension.

