# Модель источников и потоков (гибрид)

<!-- audit-status:2026-08-04 -->
> **📐 ЗАМЫСЕЛ, НЕ РЕАЛИЗОВАНО · сверено 2026-08-04.**
> Замысел, а не описание системы: на дату сверки не реализован. Не читать как отчёт о готовом.
> Конкретных расхождений найдено: **5** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

## Sources & Feeds Product Model (Hybrid)

### 0. Scope and grounding

This section converts the existing single-operator ingest layer into a **hybrid two-sided sources product**:

1. **Curated intelligence feeds** — the 5 existing workspaces (`disruption`, `ai_trends`, `ai_research`, `ai_products_media`, `design`, defined in `config/workspaces.yml`) become subscribable, browse-able "feeds." This is a packaging layer over `list_workspaces` / `get_workspace_overview` — zero pipeline change.
2. **Self-serve custom sources** — higher tiers may add `rss` / `api` / guarded `web` sources only. `telegram` stays **platform-curated-only** (shared operator account pool, FloodWait blast radius); `email` is **excluded from self-serve** entirely (plaintext IMAP creds live in `extra.fetch.password`, see `shared/source_definitions.py` `SOURCE_DEFAULT_OVERRIDES[SOURCE_TYPE_EMAIL]`).
3. **Safety** — SSRF/private-IP/allowlist guards, per-tier quotas, abuse/cost control via existing `source_runs` counters + a new Redis outbound leaky bucket.
4. **Ingest scaling** — shard structured connectors (rss/web/api/email) across replicas by consistent-hash on `source_id` with a Redis distributed lock; keep Telegram on a single dedicated worker with the operator-owned account pool.
5. **Data-licensing posture** — resell derived synthesis (trends), prefer link+summary over full-text where licensing is unclear, ToS attestation on self-serve, honor robots + conditional-GET.

The whole design is **automation-first / solo-operable** (НПД, no employees) and adds NO destructive surface to the customer path.

---

### 1. What exists today (reusable assets — cite-grounded)

| Asset | Path | Reuse role |
|---|---|---|
| `sources` table (per-source config, `extra` JSONB, `proxy_config`, `source_authority`/`source_score`) | `storage/postgres/init.sql:18-38` | Add ownership/tier columns; no rewrite |
| `validate_source_payload()` (type-gated, normalizes `extra`, requires url/tg_channel/imap host) | `shared/source_definitions.py:355-378` | **The** add-source validator — wrap, do not replace |
| `RSS_PRESETS` (43 curated feeds) + `SOURCE_DEFAULT_OVERRIDES` | `shared/source_definitions.py:24-278` | Self-serve RSS preset picker |
| `source_runs` / `source_checkpoints` (per-source run metering: fetched/emitted/status/error) | `storage/postgres/init.sql:40-62`, `ingest/source_runtime.py` | Quota + abuse counters (already populated by `AbstractSource.run`, `ingest/sources/base.py:120-160`) |
| `source_quality.py` (authority/health/yield/freshness composite + `recommend_content_mode`) | `shared/source_quality.py` | Customer-facing source health card; auto-throttle bad self-serve sources |
| `build_httpx_client` (per-source proxy, conditional GET, retries, canonical-URL dedupe) | `ingest/sources/base.py:432-518` | SSRF guard injection point (single chokepoint) |
| Admin add/list/toggle/delete source API | `admin/backend/routers/sources.py` | Already calls `validate_source_payload`; split into customer vs ops surface |
| `list_workspaces` / `get_workspace_overview` MCP tools | `mcp/tools/observability.py:142-155, 327-396` | Feed-catalog browse + per-feed overview, already aggregate counts/clusters/sources |
| Source catalog endpoint (presets + starter bundles) | `admin/backend/routers/sources.py:131-179` | Seed for `feed_catalog` + self-serve preset menu |
| In-process Telegram run lock + 2-account `AccountRotator` | `ingest/main.py:32,56-69,144-148`, `ingest/account_rotator.py` | Move to dedicated single-instance TG worker (keep as-is) |
| APScheduler reload loop | `ingest/main.py:151-185` | Add shard ownership filter to `load_sources` |

**Known gaps this design closes:** `source_checkpoints` / `source_runs` lack `workspace_id` (leak via JOIN through `sources`); `sources` has no ownership/tenant/tier columns; ingest is single `ingest-0` with an `asyncio.Lock` global to one process (no horizontal scale); no SSRF guard (any `web`/`api` URL is fetched verbatim by `fetch_url_content`, `ingest/sources/base.py:503-518`); no outbound rate limit.

---

### 2. Curated Feed Catalog (the product surface for shared feeds)

A **feed** is a curated workspace exposed as a subscribable SKU. We do NOT expose `workspaces` directly to customers (it carries internal `relevance_weights`, `cluster_analysis` thresholds, `cross_workspace_bridges`). We add a thin presentation table that maps 1 feed → 1 workspace.

#### 2.1 DDL — `feed_catalog`

```sql
-- storage/postgres/migrations/20260701_feeds_catalog.sql
CREATE TABLE IF NOT EXISTS feed_catalog (
    id              TEXT PRIMARY KEY,                       -- 'feed_ai_trends'
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    slug            TEXT NOT NULL UNIQUE,                    -- 'ai-trends'
    title           TEXT NOT NULL,                           -- customer-facing
    tagline         TEXT,                                    -- one-liner
    long_description TEXT,
    topics          JSONB DEFAULT '[]',                      -- ['llm','agents','inference']
    cover_signal_types JSONB DEFAULT '[]',                   -- from source expected_signal_types
    visibility      TEXT NOT NULL DEFAULT 'public'
                    CHECK (visibility IN ('public','beta','private','retired')),
    min_tier        TEXT NOT NULL DEFAULT 'pro'              -- see tier matrix §5
                    REFERENCES plan_tiers(code),
    sort_rank       INTEGER DEFAULT 100,
    is_listed       BOOLEAN DEFAULT TRUE,                    -- show in browse
    sample_locked   BOOLEAN DEFAULT TRUE,                    -- gate full feed behind sub
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_feed_catalog_listed
    ON feed_catalog(is_listed, sort_rank) WHERE is_listed = TRUE;
CREATE INDEX IF NOT EXISTS idx_feed_catalog_workspace
    ON feed_catalog(workspace_id);
```

`plan_tiers` / `feed_subscriptions` (who is subscribed to which feed) are owned by the **Identity & Billing** domain (see Dependencies). This domain only needs `min_tier` as a FK and an entitlement check helper it can call.

#### 2.2 Bootstrap seed (one-time, idempotent)

Seed `feed_catalog` from `config/feeds.yml` (new), mirroring the existing `bootstrap_*_from_config` UPSERT pattern (`admin/backend/services/bootstrap_configs.py`). Initial rows:

```yaml
# config/feeds.yml
feeds:
  - id: feed_disruption
    workspace_id: disruption
    slug: disruption
    title: "Disruption Intelligence"
    tagline: "Frontier signals for visionary design & strategic disruption"
    topics: [technology, design, business_models, mobility, ai]
    min_tier: business
    visibility: public
  - id: feed_ai_trends
    workspace_id: ai_trends
    slug: ai-trends
    title: "AI Trends"
    tagline: "LLMs, agents, inference, applied research"
    topics: [llm, agents, inference, research]
    min_tier: pro
    visibility: public
  - id: feed_ai_research   { workspace_id: ai_research,  min_tier: pro }
  - id: feed_ai_products   { workspace_id: ai_products_media, min_tier: pro }
  - id: feed_design        { workspace_id: design,        min_tier: pro }
```

#### 2.3 Customer-facing browse/subscribe surface

Two new **read-only** MCP/REST tools (customer surface, distinct from admin):

- `browse_feeds(topic?, tier?)` → joins `feed_catalog` (where `is_listed AND visibility IN ('public','beta')`) with a **derived freshness/volume snapshot** from `get_workspace_overview` (reuse `mcp/tools/observability.py:327`). Returns `{feed_id, slug, title, tagline, topics, min_tier, entitled: bool, sample: {latest_trend_titles[], post_count_7d, source_count, freshness}}`.
- `get_feed_overview(feed_id)` → entitlement-gated wrapper over `get_workspace_overview(workspace=feed.workspace_id)`. If not entitled and `sample_locked`, return a **teaser** (top 3 trend titles + counts, no post bodies / no `search_*`).

Subscription itself (`subscribe_feed`/`unsubscribe_feed`) is a thin call into Identity & Billing's `feed_subscriptions`; this domain provides `feed_is_entitled(account_id, feed_id) -> bool` used by every search tool's workspace resolver.

**Effort: feed_catalog DDL + seed = S; browse/overview tools = M; entitlement gate wiring into search tools = M (touches the workspace-resolver chokepoint, coordinate with Auth/Tenancy).**

---

### 3. Self-serve custom sources (higher tiers)

#### 3.1 Source ownership model — DDL delta on `sources`

```sql
-- storage/postgres/migrations/20260701_source_ownership.sql
ALTER TABLE sources
    ADD COLUMN IF NOT EXISTS ownership     TEXT NOT NULL DEFAULT 'platform'
        CHECK (ownership IN ('platform','tenant')),
    ADD COLUMN IF NOT EXISTS owner_account_id TEXT,                 -- FK -> accounts(id), nullable for platform
    ADD COLUMN IF NOT EXISTS created_by    TEXT,                    -- account_id or 'operator'
    ADD COLUMN IF NOT EXISTS tier_required TEXT,                    -- min tier that may run/keep this source
    ADD COLUMN IF NOT EXISTS tos_attested_at TIMESTAMPTZ,           -- §6 ToS attestation
    ADD COLUMN IF NOT EXISTS approval_state TEXT NOT NULL DEFAULT 'auto'
        CHECK (approval_state IN ('auto','pending_review','approved','rejected','suspended'));

CREATE INDEX IF NOT EXISTS idx_sources_owner
    ON sources(owner_account_id) WHERE ownership = 'tenant';
CREATE INDEX IF NOT EXISTS idx_sources_ownership_enabled
    ON sources(ownership, is_enabled) WHERE is_enabled = TRUE;

-- Backfill: all existing 64 curated sources are platform-owned
UPDATE sources SET ownership = 'platform', created_by = 'operator'
WHERE ownership IS NULL OR created_by IS NULL;
```

**Close the workspace_id leak on child tables** (required before RLS / per-tenant abuse accounting — coordinate with Tenancy domain but ship the column here):

```sql
ALTER TABLE source_runs        ADD COLUMN IF NOT EXISTS workspace_id TEXT;
ALTER TABLE source_checkpoints ADD COLUMN IF NOT EXISTS workspace_id TEXT;
-- backfill from parent
UPDATE source_runs r        SET workspace_id = s.workspace_id FROM sources s WHERE s.id = r.source_id AND r.workspace_id IS NULL;
UPDATE source_checkpoints c SET workspace_id = s.workspace_id FROM sources s WHERE s.id = c.source_id AND c.workspace_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_source_runs_ws ON source_runs(workspace_id, started_at DESC);
```
`ingest/source_runtime.py` `start_run`/`finish_run`/`upsert_checkpoint` then write `workspace_id` (the runner already has `self.workspace_id`, `ingest/sources/base.py:50`). **Effort S.**

#### 3.2 Add-source flow (self-serve) — reuse `validate_source_payload`

New customer endpoint `POST /me/sources` (distinct from admin `POST /sources`, `admin/backend/routers/sources.py:254`). Sequence:

```
1. resolve account + tier (Auth/Tenancy)                     [dependency]
2. reject if source_type NOT IN ('rss','api','web')          -> 422 self_serve_type_forbidden
       (telegram/email are platform/operator-only)
3. quota check: count tenant sources for account vs tier cap (§5)  -> 429 quota_exceeded
4. validate_source_payload(type, url, None, extra)           [reuse shared/source_definitions.py:355]
5. SSRF guard on url + extra.fetch.item_url_template (§4)    -> 422 url_not_allowed
6. clamp extra against tier policy (§4.3): full_content,
       max_items_per_run, min cron interval, vision.mode,
       max_media_bytes                                        (server overrides client)
7. require tos_attested == true                              -> 422 tos_required
8. INSERT with ownership='tenant', owner_account_id, created_by,
       tier_required=tier, approval_state =
         'auto'            for rss + known-good api hosts
         'pending_review'  for web + unknown api hosts (operator one-click approve)
9. enqueue async reachability smoke (one run via source_runtime) before is_enabled flips TRUE
```

The `web` type defaults to `pending_review` because `WebSource` performs **link-following hydration** (`ingest/sources/web_source.py:69-80` → `fetch_url_content`), which is the highest SSRF/abuse surface. RSS/API with allowlisted hosts auto-approve.

**Effort: customer add-source endpoint + tier clamp = M; approval_state workflow + smoke = M.**

---

### 4. Safety for self-serve (SSRF / quotas / abuse-cost)

#### 4.1 SSRF / private-IP / allowlist guard

A single new module `ingest/safe_fetch.py`, injected at the **one chokepoint** every structured connector funnels through: `build_httpx_client` + `http_get_with_retries` (`ingest/sources/base.py:432-500`). Guard logic (applied to the initial URL **and every redirect hop** and every hydration URL):

```python
# ingest/safe_fetch.py  (pseudocode)
BLOCKED_NETS = [ip_network(n) for n in (
    "127.0.0.0/8","10.0.0.0/8","172.16.0.0/12","192.168.0.0/16",
    "169.254.0.0/16","::1/128","fc00::/7","fe80::/10","0.0.0.0/8","100.64.0.0/10")]
BLOCKED_HOSTS = {"localhost","metadata.google.internal","169.254.169.254"}  # cloud IMDS
INTERNAL_NAMES = {"redis","qdrant","neo4j","xray","postgres","admin","mcp","worker",
                  "gpt2giga-proxy","searxng","crawl4ai"}  # docker-compose service names

def assert_url_allowed(url, *, ownership):
    if ownership == "platform": return            # operator-curated, trusted
    p = urlparse(url)
    if p.scheme not in ("http","https"): raise UrlNotAllowed("scheme")
    host = p.hostname.lower()
    if host in BLOCKED_HOSTS or host in INTERNAL_NAMES: raise UrlNotAllowed("internal_host")
    if p.port in (6379,5432,7687,6333,10808,8090,8100,8101,8102): raise UrlNotAllowed("internal_port")
    for fam, _, _, _, sockaddr in socket.getaddrinfo(host, None):   # resolve ALL A/AAAA
        ip = ip_address(sockaddr[0])
        if any(ip in net for net in BLOCKED_NETS) or not ip.is_global:
            raise UrlNotAllowed("private_ip")     # blocks DNS-rebind to RFC1918
```

Wiring: `build_httpx_client` gains `event_hooks={"request":[_assert_request_allowed]}` so **redirect targets are re-validated** (defeats redirect-to-internal). Tenant sources are additionally pinned to `follow_redirects` with a hop cap (3). Platform sources bypass (they legitimately use the `xray` SOCKS5 egress, `config/sources.yml` `proxy_config`). **Tenant sources are forbidden from setting `proxy_config`** (strip it server-side in step 6 of §3.2) — only the operator may route a source through `xray`.

**Effort: M.** Single module + 1 event hook + ~10 unit tests (rebind, redirect, IMDS, internal service name).

#### 4.2 Outbound leaky bucket (per-account, cross-replica)

Reuse the existing Lua-on-Redis pattern from `worker/wormsoft_guard.py` (single-flight Lua) — implement a token/leaky bucket via `self.redis.eval` (`shared/redis_client.py` exposes raw `self.redis`). Key: `lb:out:{account_id}` (and a global `lb:out:platform`). Checked once per outbound HTTP request inside `safe_fetch`:

```lua
-- leaky bucket: KEYS[1]=bucket, ARGV: capacity, leak_per_sec, now_ms, cost
local data = redis.call('HMGET', KEYS[1], 'level', 'ts')
local level = tonumber(data[1]) or 0
local ts    = tonumber(data[2]) or tonumber(ARGV[3])
level = math.max(0, level - (tonumber(ARGV[3]) - ts)/1000 * tonumber(ARGV[2]))
if level + tonumber(ARGV[4]) > tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'level', level + tonumber(ARGV[4]), 'ts', ARGV[3])
redis.call('PEXPIRE', KEYS[1], 600000)
return 1
```

If `0` (bucket full) → the run yields/short-circuits and is recorded as a soft error in `source_runs` (back-pressure, not data loss). **Effort: M.**

#### 4.3 Per-tier policy clamp (the enforcement of §5)

A pure function `clamp_source_extra_for_tier(tier, extra) -> extra` applied at add/update time AND re-applied at schedule-load (defense-in-depth, since `extra` is JSONB and could be tampered if any write path is missed). It clamps, against the §5 matrix: `parse.full_content`, `fetch.max_items_per_run`, `fetch.timeout_sec`, `vision.mode`/`vision.max_media_bytes`, and floors `schedule_cron` to the tier minimum interval (parse via existing `cron_to_minutes`, `ingest/scheduler.py:33`). **Effort: S.**

#### 4.4 Abuse auto-throttle (reuse `source_quality`)

A nightly operator-free job evaluates tenant sources via `source_quality_payload` (`shared/source_quality.py:195`). If `operational_status.state == 'critical'` for N consecutive days OR `signal_yield` ~0 with high fetch volume (cost with no value) → auto `is_enabled=FALSE` + `approval_state='suspended'` + notify owner. This reuses `recommend_content_mode` to auto-downgrade `web` sources from `full-content` to `listing-only` on repeated 403/429 (already computed at `shared/source_quality.py:114-139`). **Effort: M.**

---

### 5. Per-tier quota matrix

Quotas live in `plan_tiers.limits` JSONB (owned by Identity/Billing) but the **source/feed dimension is specified here** and enforced by §4.3:

| Limit (key in `plan_tiers.limits`) | Free | Pro | Business | Notes |
|---|---|---|---|---|
| Curated feeds (read) | 1 (sample-locked) | 1 full | all 5 | `feed_catalog.min_tier` |
| `self_serve_enabled` | no | yes | yes | gates `POST /me/sources` |
| `max_tenant_sources` | 0 | 5 | 25 | count `ownership='tenant'` per account |
| Allowed self-serve types | — | rss, api | rss, api, web | web = `pending_review` |
| `min_cron_minutes` | — | 60 | 30 | floor `schedule_cron` |
| `max_items_per_run` | — | 30 | 80 | clamp `fetch.max_items_per_run` |
| `hydration_full_content` | — | off (force `false`) | allowed (web only, capped) | clamp `parse.full_content` |
| `vision_mode` cap | — | `skip` | `ocr_only` | clamp `vision.mode`; **`full` vision = platform-only** (GigaChat Vision cost) |
| `max_media_bytes` | — | 0 | 3_000_000 | clamp `vision.max_media_bytes` |
| `outbound_req_per_min` (leaky bucket capacity) | 0 | 60 | 180 | §4.2 |
| `s3_media_budget_mb_month` | 0 | 0 | 200 | tenant media S3 cap |
| ToS attestation required | — | yes | yes | `tos_attested_at` |

Rationale honoring НПД: high-ASP **Business** tier (all curated feeds + 25 self-serve) is the revenue driver; Free is a sample funnel; Pro is the entry self-serve tier with vision off (the expensive path). Vision `full` and `proxy_config`/`xray` egress are **never** self-serve.

---

### 6. Data-licensing posture

Encoded as defaults + attestation, not just docs:

1. **Resell derived synthesis only.** Customer-facing search already returns clusters/trends with previews capped at 180–280 chars (`mcp/tools/observability.py:122-129, 300-304`). Keep full post bodies internal; the product SKU is the *synthesis* (`get_frontier_brief`, trend clusters), not raw third-party article text.
2. **Link + summary preferred over full-text for tenant sources.** Tier policy (§5) forces `parse.full_content=false` on Pro and caps it on Business. `recommend_content_mode` already biases `web` to `listing-only` and rss/api to `summary-only` on friction (`shared/source_quality.py:114-139`).
3. **ToS attestation.** `POST /me/sources` requires `tos_attested=true` (stored as `sources.tos_attested_at`): *"I have the legal right to ingest and process this URL."* Operator is shielded; liability sits with the asserting tenant.
4. **Honor robots + conditional-GET.** `build_httpx_client` already does conditional GET via `source_checkpoints.etag`/`last_modified` (`ingest/sources/base.py`, `source_runtime.py`). Add a robots.txt fetch+cache (Redis, 24h TTL) for **tenant `web` sources** in `safe_fetch`; respect `Disallow` for the configured User-Agent (`_DEFAULT_USER_AGENT`, `ingest/sources/base.py:28`). Platform sources keep current behavior. **Effort: M.**
5. **Retention/erasure hook.** `DELETE /me/sources/{id}` cascades (`source_checkpoints`/`source_runs` already `ON DELETE CASCADE`, `init.sql:41,53`); add a tenant-source posts purge job (delete `posts WHERE source_id=? AND ownership-derived`) for erasure requests — coordinate with 152-FZ/PDn domain.

---

### 7. Ingest scaling (sharding structured connectors)

**Today:** one `ingest-0` runs `AsyncIOScheduler`; all sources scheduled in one process; Telegram serialized by an in-process `asyncio.Lock` (`ingest/main.py:32,144-148`). This cannot scale and FloodWait on the shared account blasts all TG sources.

**Target topology (two service classes):**

```
ingest-structured  (N replicas, stateless, horizontally scalable)
    └─ rss / web / api / email connectors
    └─ shard ownership via consistent-hash(source_id) % N  +  Redis lease lock
telegram-ingest    (exactly 1 replica, dedicated)
    └─ telegram connectors only
    └─ operator-owned AccountRotator pool (existing code, unchanged)
```

#### 7.1 Sharding mechanism (consistent-hash + Redis lease)

Each `ingest-structured` replica gets `SHARD_INDEX` (0..N-1) and `SHARD_COUNT=N` (env, from compose `deploy.replicas` + ordinal). `load_sources` (`ingest/scheduler.py:10`) gains a filter so each replica only schedules sources it owns:

```sql
-- per-replica source load (deterministic ownership)
WHERE s.is_enabled = TRUE AND w.is_active = TRUE
  AND s.source_type <> 'telegram'                               -- telegram on dedicated worker
  AND (hashtext(s.id) & 2147483647) % :shard_count = :shard_index
```

`hashtext` is deterministic in Postgres, so ownership is stable as sources move; on replica count change, only ~1/N sources reassign (consistent-hash property). A **Redis lease lock** (`SET lock:run:{source_id} {replica} NX PX <interval_ms>`, reusing the `self.redis.eval` pattern) guards against a brief double-ownership window during rescale — a replica acquires the per-source lease before `source.run()` and skips if held. This makes the scheduler safe even if two replicas momentarily compute overlapping ownership.

#### 7.2 Telegram isolation

`telegram-ingest` keeps the existing single-instance design verbatim — `build_rotator` (2 operator accounts, `ingest/main.py:56-69`), `_telegram_run_lock`, per-source `tg_account_idx`. Because Telegram is **platform-curated-only** (no self-serve), its source set is small and operator-controlled, so a single instance is correct and FloodWait is contained to platform feeds (never customer-triggered). The account pool stays operator-owned (sessions in `sessions/`, server-only per project rules).

**Effort: shard filter in `load_sources` + env wiring = M; Redis lease lock = M; split compose into `ingest-structured` (replicas) + `telegram-ingest` = M (coordinate with Ops/HA domain).**

---

### 8. Sequenced change list (with effort)

| # | Change | Files | Effort | Depends on |
|---|---|---|---|---|
| 1 | `feed_catalog` DDL + `config/feeds.yml` + bootstrap UPSERT | new migration, `config/feeds.yml`, `bootstrap_configs.py` | S | — |
| 2 | `sources` ownership/tier/tos/approval columns + backfill | new migration | S | — |
| 3 | `workspace_id` on `source_runs`/`source_checkpoints` + writer wiring | migration, `ingest/source_runtime.py` | S | — |
| 4 | `browse_feeds` / `get_feed_overview` read tools (reuse `get_workspace_overview`) | `mcp/tools/`, `mcp/server.py` | M | 1, Auth (entitlement) |
| 5 | `ingest/safe_fetch.py` SSRF guard + redirect re-validation hook | new module, `ingest/sources/base.py` | M | — |
| 6 | `clamp_source_extra_for_tier` + apply at add & schedule-load | new helper, `sources.py`, `ingest/main.py` | S | Billing (tier limits shape) |
| 7 | `POST /me/sources` self-serve endpoint (type allowlist, quota, ToS, approval_state) | new customer router | M | 2,5,6, Auth |
| 8 | Outbound leaky bucket (Lua) in `safe_fetch` | `safe_fetch.py` | M | 5 |
| 9 | robots.txt fetch+cache for tenant `web` sources | `safe_fetch.py` | M | 5 |
| 10 | Abuse auto-throttle nightly job (reuse `source_quality`) | new job, scheduler | M | 2,3 |
| 11 | Consistent-hash shard filter + Redis lease + compose split | `scheduler.py`, `main.py`, compose | M–L | Ops/HA |
| 12 | Tenant-source erasure/purge job | new job | M | 152-FZ domain |

Total roughly: 3×S + 6×M + 1×(M–L). Solo-operable; no change requires more than one service to be touched in lockstep except #11 (compose topology).

---

### 9. Concrete schema sketch (consolidated)

```sql
-- 1. Feed catalog (customer-facing packaging over workspaces)
feed_catalog(id PK, workspace_id FK->workspaces, slug UNIQUE, title, tagline,
             topics jsonb, min_tier FK->plan_tiers, visibility, is_listed, sample_locked, sort_rank)

-- 2. Source ownership (on existing sources table)
sources += (ownership 'platform'|'tenant', owner_account_id FK->accounts,
            created_by, tier_required, tos_attested_at, approval_state)

-- 3. Tenant accounting (close the leak)
source_runs        += workspace_id
source_checkpoints += workspace_id

-- 4. Owned by Identity/Billing, referenced here:
plan_tiers(code PK, limits jsonb)            -- limits.* keys per §5
feed_subscriptions(account_id, feed_id, status)  -- entitlement source of truth
```

Redis keys introduced: `lb:out:{account_id}` / `lb:out:platform` (leaky bucket), `lock:run:{source_id}` (shard lease), `robots:{host}` (24h cache).

---

## Открытые решения по этому разделу

- Auto-approve vs always-review for self-serve `web` sources: I recommend `pending_review` for web + unknown-host api (one-click operator approve), auto-approve rss + allowlisted api hosts. This caps SSRF/abuse blast radius while keeping the common case (RSS) friction-free.
- Whether `api` self-serve sources may set arbitrary `fetch.item_url_template` (HN-style fan-out, see config/sources.yml api_hn_topstories). Recommend: allow but run the template host through the same SSRF allowlist and count each item fetch against the leaky bucket; reject templates whose host differs from the index URL host unless operator-approved.
- Tier where curated-feed full access begins. Recommend Pro = all AI feeds, Business = + disruption/design (broader) — but final price/tier packaging is a Billing decision; this design only fixes `feed_catalog.min_tier` as the lever.
- Shard count N for ingest-structured at launch. Recommend N=1 now (single replica, but already running the shard-filter + lease code so scaling to N>1 is a pure replica-count change with no code edit). Avoids over-building HA before paying customers exist.
- Robots.txt enforcement strictness for tenant web sources. Recommend respect Disallow as a hard block (refuse to enable the source) rather than soft-skip, to keep the operator legally clean under the ToS-attestation model.

## Зависимости от других разделов

- Identity & Tenancy / Auth — provides accounts(id), tier resolution, and the workspace/feed entitlement check that browse_feeds/get_feed_overview and every search tool's workspace resolver must call; also owns Postgres RLS that the new workspace_id columns on source_runs/source_checkpoints enable.
- Billing & Plans — owns plan_tiers (the limits JSONB whose source/feed keys are specified in the §5 matrix) and feed_subscriptions (entitlement source of truth); feed_catalog.min_tier and sources.tier_required FK into it.
- LLM metering & FinOps — vision-tier caps (vision_mode/max_media_bytes per tier) must align with per-tenant cost metering; the leaky bucket bounds outbound fetch cost but GigaChat Vision/embedding cost is metered in the worker (workspace_id propagation).
- Ingest HA / Ops — the ingest-structured replica split + telegram-ingest dedicated instance is a docker-compose / deployment-topology change; coordinate the consistent-hash shard env wiring and Redis lease with the HA/scheduler-resilience workstream.
- 152-FZ / PDn & Data Retention — tenant-source erasure/purge job, robots/ToS legal posture, and ensuring self-serve URLs (especially web hydration) do not route RU personal data through non-RU egress (tenant proxy_config is forbidden, which helps).

