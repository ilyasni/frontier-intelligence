# Аутентификация, авторизация и поверхности обслуживания

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **6** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

# Authentication, Authorization & Serving Surfaces

## 0. Current reality (file-grounded)

Three surfaces exist today, all bound `0.0.0.0`, all unauthenticated, all trusting a client-supplied `workspace` string that defaults to `"disruption"`:

| Surface | Port | Process | File | Auth today |
|---|---|---|---|---|
| MCP REST (internal API) | `8100` | FastAPI | `mcp/server.py` (host published `8100:8100`, `docker-compose.yml:442`) | none; CORS `allow_origins=settings.allowed_origins` which defaults to `["*"]` (`shared/config.py:345`) |
| MCP Gateway (SSE / streamable-HTTP) | `8102` | FastMCP | `mcp/mcp_gateway.py` (published `8102:8102`, `docker-compose.yml:470`) | none; `allowed_hosts=["*"]`, `allowed_origins=["*"]`, `enable_dns_rebinding_protection=False` (`mcp/mcp_gateway.py:22-26`) |
| Admin / Ops | `8101` | FastAPI | `admin/backend/main.py` (published `8101:8101`, `docker-compose.yml:603`) | none; wildcard CORS (`admin/backend/main.py:38-43`); only the alertmanager webhook has a token, and it **fails open** when unset (`admin/backend/routers/monitoring.py:63-67`) |

Every MCP tool takes a free-text `workspace` param defaulting to `"disruption"` (e.g. `mcp/mcp_gateway.py:38, 76, 215`; validated only as a plain string in `shared/search_contracts.py:24`). The gateway forwards it verbatim to REST, which passes it straight into the Qdrant filter chokepoint `_build_payload_filter(workspace_id, …)` (`worker/integrations/qdrant_client.py:115-125`). There is **no identity model and no `api_keys` table** anywhere. This section closes that gap.

### Design stance
- **Trust boundary is moved server-side.** The client-supplied `workspace` becomes a *hint*, not authority. The edge resolves the caller → an allowed-workspace set and **overrides** the param.
- **Two physically separable surfaces:** a customer **read surface** (the gateway + a thin authenticated REST facade) and an operator **ops surface** (Admin `:8101` + the raw REST `:8100`), the latter never exposed to customers (VPN/localhost only).
- **Reuse, don't rebuild.** The metering/quota Redis primitives (`worker/provider_budget_manager.py`, `admin/backend/services/openrouter_picker.py`, `worker/wormsoft_guard.py`) already implement reserve/commit and Lua single-flight; we add a *tenant edge guard* of the same shape. Postgres identity tables are new but small.

---

## 1. Tenant identity & API-key model

### 1.1 Tables (new Postgres DDL)

Schema lives with the rest in `storage/postgres/init.sql` (idempotent `CREATE TABLE IF NOT EXISTS` style, consistent with that file). Per project convention `workspace_id TEXT REFERENCES workspaces(id)` everywhere. Because the project still applies raw SQL migrations (Alembic is available but unused — a MAJOR gap flagged elsewhere), ship this as a new migration file `storage/postgres/migrations/20260701_tenancy_identity.sql`.

```sql
-- ─── tenant accounts (a paying customer / org; solo-operator = self-employed) ───
CREATE TABLE IF NOT EXISTS accounts (
    id              TEXT PRIMARY KEY,              -- 'acct_' + ULID
    display_name    TEXT NOT NULL,
    contact_email   TEXT NOT NULL,
    plan_code       TEXT NOT NULL DEFAULT 'trial', -- FK target in Plans/Billing section
    status          TEXT NOT NULL DEFAULT 'active' -- active | suspended | closed
        CHECK (status IN ('active','suspended','closed')),
    pdn_consent_at  TIMESTAMPTZ,                   -- 152-FZ consent capture (see Legal section)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── which workspaces (curated feeds) an account may read ───
-- An account is entitled to N curated workspaces; entitlement is the ONLY source
-- of truth for workspace access (the client-supplied workspace param is ignored).
CREATE TABLE IF NOT EXISTS account_workspaces (
    account_id    TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    workspace_id  TEXT NOT NULL REFERENCES workspaces(id),
    access        TEXT NOT NULL DEFAULT 'read'    -- read | write (write = self-serve custom sources, higher tiers)
        CHECK (access IN ('read','write')),
    granted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, workspace_id)
);
CREATE INDEX IF NOT EXISTS idx_account_workspaces_ws ON account_workspaces(workspace_id);

-- ─── API keys: store PREFIX + HASHED secret, never the secret itself ───
CREATE TABLE IF NOT EXISTS api_keys (
    id            TEXT PRIMARY KEY,               -- 'key_' + ULID
    account_id    TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    prefix        TEXT NOT NULL UNIQUE,           -- public lookup handle, e.g. 'fk_live_7Qa3' (shown in UI)
    secret_hash   BYTEA NOT NULL,                 -- sha256(pepper || secret); pepper from env, not in DB
    scopes        TEXT[] NOT NULL DEFAULT '{read}', -- subset of {read, write, admin}
    label         TEXT,                           -- human label set at issue time
    status        TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active','revoked')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at  TIMESTAMPTZ,
    expires_at    TIMESTAMPTZ,                    -- NULL = no expiry; set on rotation grace window
    revoked_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_api_keys_account ON api_keys(account_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(prefix) WHERE status = 'active';
```

### 1.2 Key format, hashing, lifecycle

- **Wire format:** `fk_live_<prefix4>_<secret32>` (e.g. `fk_live_7Qa3_9f2b…`). The `prefix` column stores `fk_live_7Qa3`; `secret_hash = sha256(SERVER_PEPPER || secret32)`. SHA-256 (not bcrypt) is correct here because the secret is a 32-byte high-entropy random token, not a low-entropy password — fast verification on the hot path, no brute-force surface. `SERVER_PEPPER` is read `os.environ["FRONTIER_API_KEY_PEPPER"]` (KeyError if unset — matches the project's "no hardcoded secrets" rule in `CLAUDE.md`).
- **Lookup:** parse prefix from the token → single indexed `SELECT … WHERE prefix=$1 AND status='active'` → constant-time compare `sha256(pepper||presented)` against `secret_hash`. The **plaintext secret is returned exactly once** at issue time and never persisted.
- **Issue / rotate / revoke** (operator-only endpoints on Admin, §3):
  - *Issue*: insert row, return plaintext once.
  - *Rotate*: issue a new key for the same account; set `expires_at = NOW() + interval '7 days'` on the old key (overlap window so a live integration doesn't break), then it auto-expires.
  - *Revoke*: `UPDATE … SET status='revoked', revoked_at=NOW()` — takes effect on next request (no caching beyond the §2.4 short TTL).
- **Scopes:** `read` (all search/brief/list tools), `write` (self-serve custom RSS/web/api source registration on higher tiers — gated, not yet built), `admin` (operator-only; never granted to customers).

**Effort: M** (DDL + a `shared/auth/api_keys.py` verify/issue helper + 4 admin endpoints).

---

## 2. Gateway auth (`:8102`) + server-side workspace resolution

### 2.1 How Claude/Cursor present the credential

MCP streamable-HTTP/SSE clients (Claude Desktop, Cursor) support a static `Authorization` header on the MCP server config. The supported, preferred path:

```jsonc
// Claude/Cursor MCP server entry
{
  "frontier": {
    "url": "https://feeds.<domain>/mcp",
    "headers": { "Authorization": "Bearer fk_live_7Qa3_9f2b…" }
  }
}
```

**Fallback for clients with unreliable header support** (some MCP clients drop custom headers on SSE reconnect): a **per-tenant gateway URL with the token in the path**:

```
https://feeds.<domain>/t/fk_live_7Qa3_9f2b…/mcp
```

The token-in-path is extracted by an ASGI middleware before FastMCP routing and treated identically to the bearer. (TLS-terminated at the reverse proxy so the token is never in cleartext on the wire; we accept that path tokens appear in proxy access logs and therefore disable URL logging for `/t/*` at the proxy and mark the access log to redact that path segment.)

### 2.2 Wrapping FastMCP with auth (the gateway is not a plain FastAPI app)

`mcp/mcp_gateway.py` uses `FastMCP(...).run(transport="streamable-http")`, which exposes a Starlette ASGI app via `mcp.streamable_http_app()`. We **stop calling `mcp.run()` directly** and instead mount the app behind a Starlette `BaseHTTPMiddleware` that authenticates *before* the MCP protocol layer, then run it with uvicorn. Sketch (replaces `mcp/mcp_gateway.py:439-441`):

```python
import os, contextvars
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

# request-scoped resolved identity, read by tool wrappers
current_principal: contextvars.ContextVar["Principal"] = contextvars.ContextVar("principal")

class GatewayAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        path = request.url.path
        token = _bearer(request.headers.get("authorization"))
        if token is None and path.startswith("/t/"):          # token-in-path fallback
            token, request.scope["path"] = _split_path_token(path)
        principal = await resolve_principal(token)            # §2.3; None on bad/missing
        if principal is None:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        current_principal.set(principal)
        return await call_next(request)

app = mcp.streamable_http_app()
app.add_middleware(GatewayAuthMiddleware)
# DNS-rebinding + host/origin pinning re-enabled — see §5
```

Each tool wrapper then **ignores the inbound `workspace` arg and substitutes the resolved one**. Concretely, `search_frontier` (`mcp/mcp_gateway.py:36-66`) changes from trusting `workspace: str = "disruption"` to:

```python
@mcp.tool(...)
async def search_frontier(query: str, workspace: str | None = None, limit: int = 10,
                          synthesize: bool = False, **kw) -> dict:
    p = current_principal.get()
    ws = resolve_requested_workspace(p, workspace)   # §2.3 — raises if not entitled
    enforce_edge_quota(p, ws, cost_class="read" if not synthesize else "synthesize")  # §4
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(f"{REST_BASE}/tools/search_frontier",
            json={"query": query, "workspace": ws, ...,
                  # propagate identity to REST for metering (§ Metering section)
                  } ,
            headers={"X-Frontier-Account": p.account_id, "X-Frontier-Workspace": ws})
        r.raise_for_status(); return r.json()
```

The REST base (`:8100`) is **not reachable from the customer network** (§3), so it trusts these `X-Frontier-*` headers; the gateway is the only ingress that sets them.

### 2.3 Server-side workspace resolution (never trust the client param)

```python
@dataclass
class Principal:
    account_id: str
    key_id: str
    scopes: frozenset[str]
    workspaces: frozenset[str]          # from account_workspaces
    plan_code: str

async def resolve_requested_workspace(p: Principal, requested: str | None) -> str:
    # No requested ws → default to the account's primary (first entitled, deterministic order)
    if not requested:
        if not p.workspaces:
            raise PermissionError("no_workspace_entitlement")
        return sorted(p.workspaces)[0]
    if requested not in p.workspaces:
        raise PermissionError(f"workspace_forbidden:{requested}")  # → 403 to client
    return requested
```

For multi-workspace tools (`get_frontier_brief` accepts `workspaces: list[str]`, `mcp/mcp_gateway.py:182-203`) the resolver **intersects** the requested list with `p.workspaces` and drops the rest (never errors on extras → silently scoped). This single rule kills the cross-tenant read/merge vulnerability: a caller can no longer set `workspace="<someone else's feed>"`.

### 2.4 Principal cache

`resolve_principal(token)` hits Postgres once, then caches the resolved `Principal` in Redis under `auth:principal:<sha256(token)>` with a **60 s TTL** (short enough that revoke/rotate take effect quickly, long enough to keep the hot path off Postgres). On revoke we also `DEL` that key for instant effect. Reuses the existing `redis.asyncio` client pattern already used throughout (`mcp/tools/search_frontier.py:46`).

**Effort: L** (middleware + token-in-path split + per-tool wrapper edits across `mcp/mcp_gateway.py` + `resolve_principal`/`resolve_requested_workspace` + cache).

---

## 3. Trust-tier split: customer read surface vs operator ops surface

| Capability | Surface | Network exposure | Auth |
|---|---|---|---|
| search_frontier / search_balanced / search_trend_clusters / search_by_vision / get_concept_graph / get_frontier_brief / list_* / get_* (read) | **Gateway `:8102`** | Public (TLS via reverse proxy) | API key `read` scope, workspace-scoped (§2) |
| Raw MCP REST `:8100` | internal | **Docker network only — unpublish the host port** | header-trusted from gateway |
| All Admin CRUD: workspaces/sources/pipeline/posts/media/settings/graph/clusters (`admin/backend/main.py:58-68`) | **Admin `:8101`** | **VPN / localhost only** | operator session (see below) |
| RSI gates (`approve_threshold_change`, `reject_threshold_change`, `mark_relevance_audit`, `approve_entity_merge` — irreversible Neo4j merge — `reject_entity_merge`) | operator only | VPN/localhost | operator |
| Pipeline triggers, `ingest_url` | operator only | VPN/localhost | operator |
| Alertmanager webhook | Admin `:8101` | internal (Prometheus→Admin) | token, **fail-closed** (§5) |

**Concrete changes:**

1. **Unpublish `:8100` from the host.** In `docker-compose.yml:441-442` remove the `ports: ["8100:8100"]` mapping for `mcp`; the gateway reaches it via the `frontier-net` service DNS `http://mcp:8100` (already its `MCP_REST_BASE`, `docker-compose.yml:468`). REST is now reachable only inside the compose network. **Effort: S.**

2. **Remove the destructive/operator tools from the customer toolset.** The gateway today registers RSI + ops tools that must NOT be customer-callable. The following `@mcp.tool` registrations stay only on an **operator gateway profile** (or move to Admin) and are **removed from the public gateway**:
   - `ingest_url` (`mcp/mcp_gateway.py:212`) — write/crawl trigger.
   - The RSI write gates exposed via REST routers (`mcp/server.py:62-63`, `mcp/tools/threshold_proposals.py`, `mcp/tools/graph_health.py`): `approve_threshold_change`, `reject_threshold_change`, `mark_relevance_audit`, `approve_entity_merge` (**irreversible graph mutation**), `reject_entity_merge`.
   - Ops/introspection tools that leak operational internals and other tenants' source config: `list_sources_health`, `get_source_details`, `get_pipeline_stats`, `list_missing_signals`/`get_missing_signal_details` (SearXNG gap analysis = competitive ops signal), `list_threshold_proposals`, `list_underrated_signals`, `list_relevance_audit_sample`, `get_graph_health`, `list_entity_merge_proposals`.

   **Customer-allowed toolset (read, workspace-scoped):** `search_frontier`, `search_balanced`, `search_trend_clusters`, `search_by_vision`, `get_concept_graph`, `get_frontier_brief`, `get_workspace_overview`, `list_clusters`, `list_emerging_signals`, `get_cluster_details`, `get_cluster_evidence`, `get_signal_timeline`, and a scoped `list_workspaces` that returns **only the caller's entitled** workspaces (today `list_workspaces` returns all — `mcp/mcp_gateway.py:230` — it must filter to `p.workspaces`). **Effort: M.**

3. **Operator auth on Admin `:8101`.** Admin stays behind VPN/localhost (network-level primary control) and additionally requires a single operator credential — an `admin`-scoped API key reusing the same `api_keys` machinery, checked by a `require_operator` dependency at `include_router` level (§3.1). This avoids a second auth system. **Effort: M.**

### 3.1 `Depends()` auth dependency attached at `include_router` level

FastAPI lets you attach dependencies to a whole router group via `dependencies=[Depends(...)]` on `include_router`, so every route inherits it with zero per-handler edits. Applied to the customer REST facade and to Admin:

```python
# shared/auth/deps.py
from fastapi import Depends, Header, HTTPException, Request

async def authenticate(request: Request,
                       authorization: str | None = Header(None)) -> Principal:
    token = _bearer(authorization)
    principal = await resolve_principal(token)        # Redis-cached, §2.4
    if principal is None:
        raise HTTPException(401, "unauthorized")
    return principal

def require_scope(scope: str):
    async def _dep(p: Principal = Depends(authenticate)) -> Principal:
        if scope not in p.scopes:
            raise HTTPException(403, f"missing_scope:{scope}")
        return p
    return _dep

require_read     = require_scope("read")
require_write    = require_scope("write")
require_operator = require_scope("admin")
```

```python
# admin/backend/main.py — attach at router-include level (one line each)
from shared.auth.deps import require_operator

app.include_router(ws_router,        prefix="/api/workspaces", dependencies=[Depends(require_operator)])
app.include_router(src_router,       prefix="/api/sources",    dependencies=[Depends(require_operator)])
app.include_router(pipeline_router,  prefix="/api/pipeline",   dependencies=[Depends(require_operator)])
# … posts, albums, media, settings, graph, search, clusters …
# monitoring router keeps its OWN token gate for the alertmanager webhook (§5)
app.include_router(monitoring_router, prefix="/api/monitoring", dependencies=[Depends(require_operator)])
```

For the **gateway**, FastMCP isn't a router-tree so the equivalent is the middleware in §2.2 (auth happens once per request before tool dispatch) plus the per-tool `current_principal.get()` substitution.

---

## 4. Per-tenant rate limiting + LLM-spend quota at the edge

Two distinct cost classes (the audit confirms reads are cheap, `synthesize=true`/`ingest` are expensive — they spend LLM tokens). We meter both at the **gateway edge** using the same Redis Lua reserve pattern proven in `admin/backend/services/openrouter_picker.py:48-95` and `worker/wormsoft_guard.py:12-31`.

### 4.1 Two-tier edge guard

- **Tier A — request rate (leaky/fixed-window bucket), per (account, cost_class):** cheap-read bucket vs expensive bucket with separate limits. Keys mirror the existing convention (`or:rpm:{model}:{minute}` → `edge:rpm:{account}:{class}:{minute}`):

```
edge:rpm:<account>:read:<YYYYMMDDHHMM>        # e.g. 120/min
edge:rpm:<account>:synthesize:<YYYYMMDDHHMM>  # e.g. 10/min
edge:rpd:<account>:synthesize:<YYYYMMDD>      # daily synthesize cap
```

- **Tier B — LLM-spend quota, per (account, billing-month):** the durable spend budget. Today cost is tracked per-provider globally in Redis with ~3-day TTL (`worker/provider_budget_manager.py:454`) and as raw billable-token counts, not money — both flagged as blockers in adjacent sections (durable ledger + token→RUB conversion live in the Metering/Billing sections). The **edge guard reads the month-to-date spend** for the account and rejects expensive calls once the plan's monetary cap is exhausted:

```
edge:spend:<account>:<YYYYMM>   # month-to-date RUB, authoritative copy in Postgres ledger
```

### 4.2 Edge reserve Lua (same shape as `_RESERVE_SLOT_LUA`)

```lua
-- KEYS[1]=rpm_key KEYS[2]=rpd_key KEYS[3]=spend_key
-- ARGV: rpm_limit, rpd_limit, rpm_ttl, rpd_ttl, spend_cap, est_cost
local rpm = tonumber(redis.call('GET', KEYS[1]) or '0')
if rpm >= tonumber(ARGV[1]) then return {'edge_rpm_throttle', rpm} end
local rpd = tonumber(redis.call('GET', KEYS[2]) or '0')
if rpd >= tonumber(ARGV[2]) then return {'edge_rpd_cap', rpd} end
local spend = tonumber(redis.call('GET', KEYS[3]) or '0')
if (spend + tonumber(ARGV[6])) > tonumber(ARGV[5]) then return {'edge_spend_cap', spend} end
rpm = redis.call('INCR', KEYS[1]); if rpm == 1 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end
rpd = redis.call('INCR', KEYS[2]); if rpd == 1 then redis.call('EXPIRE', KEYS[2], ARGV[4]) end
return {'ok', rpm}
```

`enforce_edge_quota(p, ws, cost_class)` (called in each tool wrapper, §2.2) runs this script. On a non-`ok` reason it raises → the tool returns `{"error":"rate_limited","reason":"edge_spend_cap","retry_after":…}` (MCP-friendly structured error, never a 5xx). Spend is *reserved* with an estimate pre-call; the actual RUB is reconciled post-call by the metering layer (which finally populates `workspace_id`/`account_id` into `record_execution_receipt` — the dead field at `worker/provider_budget_manager.py:200,244-254`). Plan limits (`rpm_limit`, `spend_cap`) come from the plan row keyed by `p.plan_code` (Plans section).

### 4.3 Why edge, not provider-level

The existing guards (`openrouter_picker`, `WormsoftSharedGuard`, `provider_circuit_breaker`) are **per-provider global** — they protect the upstream LLM account from 429s but give **no per-tenant fairness** (single ~1-inflight Wormsoft slot = noisy-neighbor risk, a MAJOR gap). The edge guard adds the tenant dimension *in front of* those, so one tenant can't exhaust the shared single-flight slot. We keep the provider guards unchanged downstream.

**Effort: M** (one Lua script + `enforce_edge_quota` helper + plan-limit lookup; reuses `shared/redis_client.get_client`).

---

## 5. Hardening: CORS, DNS-rebinding, fail-closed alert token

1. **Tighten CORS to known origins** (three files). Replace the `["*"]` default. `shared/config.py:345` default `allowed_origins` becomes `[]` (deny by default); real origins set via `ALLOWED_ORIGINS` env. The customer surface is header-bearer (no browser → CORS mostly irrelevant), so the only legitimate browser origin is the operator Admin SPA — restrict Admin CORS (`admin/backend/main.py:38-43`) to the operator console origin(s) and **drop `allow_methods=["*"]`/`allow_headers=["*"]`** to an explicit list. Gateway (`mcp/mcp_gateway.py`) sets `allowed_origins` to the known MCP client origins / the public feeds domain. **Effort: S.**

2. **Re-enable DNS-rebinding protection on the gateway.** Flip `mcp/mcp_gateway.py:22-26`:

```python
transport_security=TransportSecuritySettings(
    allowed_hosts=[os.environ["MCP_GATEWAY_PUBLIC_HOST"]],   # e.g. "feeds.<domain>"
    allowed_origins=[f"https://{os.environ['MCP_GATEWAY_PUBLIC_HOST']}"],
    enable_dns_rebinding_protection=True,
)
```

This blocks the local-network DNS-rebinding class of attack that `enable_dns_rebinding_protection=False` currently allows. **Effort: S.**

3. **Fail-closed the alertmanager token.** `_assert_alertmanager_token` (`admin/backend/routers/monitoring.py:63-67`) returns early (allows the call) when `alertmanager_webhook_token` is empty. Change to **reject when unset**:

```python
def _assert_alertmanager_token(request: Request) -> None:
    expected = get_settings().alertmanager_webhook_token.strip()
    if not expected:
        raise HTTPException(503, "alertmanager_token_not_configured")  # fail CLOSED
    ...
```

Make `ALERTMANAGER_WEBHOOK_TOKEN` a required env in the Admin compose block. **Effort: S.**

---

## 6. Sequenced change list

| # | Change | Files | Effort |
|---|---|---|---|
| 1 | Add `accounts` / `account_workspaces` / `api_keys` DDL | new `storage/postgres/migrations/20260701_tenancy_identity.sql` | M |
| 2 | API-key issue/verify/rotate/revoke helper (prefix+hash+pepper) | new `shared/auth/api_keys.py` | M |
| 3 | `resolve_principal` + Redis 60s principal cache | new `shared/auth/principal.py` | S |
| 4 | `Depends()` deps: `authenticate` / `require_read|write|operator` | new `shared/auth/deps.py` | S |
| 5 | Gateway auth middleware + token-in-path fallback + per-tool `workspace` override | `mcp/mcp_gateway.py` (all `@mcp.tool` + `__main__`) | L |
| 6 | Remove ops/RSI/ingest tools from customer toolset; scope `list_workspaces` | `mcp/mcp_gateway.py`, `mcp/server.py` | M |
| 7 | Edge two-tier rate + spend guard (Lua) | new `shared/auth/edge_guard.py` | M |
| 8 | Attach `require_operator` to all Admin routers | `admin/backend/main.py:58-68` | S |
| 9 | Fail-closed alertmanager token | `admin/backend/routers/monitoring.py:63-67` | S |
| 10 | Unpublish REST `:8100` host port; pin gateway DNS-rebinding; tighten CORS defaults | `docker-compose.yml:441-442,464-475`, `mcp/mcp_gateway.py:22-26`, `admin/backend/main.py:38-43`, `shared/config.py:345` | S |
| 11 | Admin operator endpoints to issue/rotate/revoke keys + grant `account_workspaces` | new `admin/backend/routers/tenants.py` | M |

**Rollout order:** 1→2→3→4 (identity foundation) → 8/9/10 (lock down ops + close fail-open holes; low risk, immediate security win) → 5/6/7 (customer gateway auth + scoping + edge quota) → 11 (operator tooling to onboard the first paying tenant).

---

## 7. Net security outcome

After these changes: every customer call carries a hashed API key resolved to an `account` with an explicit entitled-workspace set; the client-supplied `workspace` is overridden server-side (cross-tenant read/merge closed); the destructive surface (`ingest_url`, RSI approve, irreversible entity merges, all Admin CRUD) is operator-only behind VPN/localhost and an `admin` scope; expensive `synthesize`/spend is metered per tenant at the edge in front of the existing provider guards; and the three fail-open holes (wildcard CORS, disabled DNS-rebinding, fail-open alert token) are closed.

---

## Открытые решения по этому разделу

- Reverse proxy / TLS terminator choice: CLAUDE.md explicitly forbids reusing Caddy from the old project, and no nginx/traefik exists in the stack today. RECOMMENDATION: add Traefik (or nginx) as the single TLS ingress in front of gateway :8102 and a customer REST facade; it also lets us redact /t/<token>/ path segments from access logs. This is a hard dependency for the token-in-path fallback and for not exposing uvicorn directly.
- Customer REST facade vs gateway-only: should we expose a plain authenticated REST API (not just MCP) for non-Claude customers, or is MCP-over-SSE the sole product surface for now? RECOMMENDATION: gateway-only at launch (the confirmed product is curated feeds consumed in Claude/Cursor); add a thin read REST facade later behind the same require_read dep when a non-MCP customer appears.
- Spend-cap enforcement granularity: enforce the LLM-spend cap per-account-per-month only, or also a per-workspace sub-cap? RECOMMENDATION: per-account-per-month at launch (matches the small number of higher-priced B2B subscriptions implied by the NPD 2.4M cap); per-workspace sub-caps are over-build for now.
- Operator auth strength on Admin: rely on VPN/localhost + a single admin-scoped API key, or add a stronger second factor? RECOMMENDATION: VPN/localhost is the primary control and is sufficient for a solo operator; the admin-scoped key is the defence-in-depth layer. Do not build a full session/2FA system now (automation-first, solo-operable).

## Зависимости от других разделов

- Plans, Quotas & Tiers — supplies plan_code rows with rpm/rpd/spend limits read by the edge guard (§4) and the read/write access tiers; account_workspaces entitlement maps to which curated feeds a plan includes.
- Per-tenant Metering & Usage Ledger — must populate workspace_id/account_id into ProviderExecutionRequest/ExecutionReceipt (today dead, worker/provider_budget_manager.py:200) and provide the durable month-to-date RUB spend that the edge spend-cap reads; converts billable tokens to money via wormsoft_limits pricing.
- Billing & Payments (YooKassa, self-employed/NPD) — owns accounts.plan_code lifecycle, suspend/close status transitions that this layer enforces (suspended account → 403), and the 2.4M RUB/yr revenue guardrail.
- Networking / Ops (VPN, reverse proxy, TLS) — provides the VPN/localhost boundary for the ops surface and the TLS ingress required by gateway bearer + token-in-path fallback; also the unpublishing of REST :8100.
- 152-FZ / Legal & Data Residency — defines pdn_consent capture on accounts and which workspaces/providers a tenant may use (gates non-RU OpenRouter/Polza routing for PDn-bearing tenants), constraining workspace entitlement.
- Multi-tenant Data Isolation (Postgres RLS / Qdrant / Neo4j) — the server-side workspace resolution here is the enforcement point that must align with whatever isolation (RLS policies, per-workspace Qdrant filter) that domain hardens downstream.

