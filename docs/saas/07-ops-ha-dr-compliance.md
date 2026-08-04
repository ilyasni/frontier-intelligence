# Эксплуатация, HA, DR, CI/CD, секреты и 152-ФЗ

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **6** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

# Ops, Reliability & 152-FZ Compliance Envelope

> Scope: the operational envelope that lets **one person under НПД (самозанятый)** run Frontier Intelligence as a paid SaaS — automated, recoverable, and legally resident in RU. No employees are permitted, so every recovery and deploy path must be a script or a managed service, never a heroic manual procedure. The current hand-ops model (`scripts/server-deploy-rebuild.sh` rebuilds images **on prod** after `rsync`; a single-process APScheduler in `admin/backend/scheduler.py` owns every cron; **zero** backup scripts exist in the repo) is the single largest existential risk and the focus of the MVP-blocking work below.

## 0. Verified ground truth (what exists today)

| Concern | Evidence (file) | State |
|---|---|---|
| Deploy = rebuild-on-prod | `scripts/server-deploy-rebuild.sh` (2-phase `docker compose build … up -d --force-recreate`), `scripts/server-build-stack.sh` | Code is baked into images; rsync+restart does NOT update code (per MEMORY `ops_code_deploy_needs_image_rebuild`). No registry, no tags, no rollback. |
| Migrations | `scripts/apply-postgres-migrations.sh` loops raw `*.sql` through `psql -v ON_ERROR_STOP=1`; 13 files in `storage/postgres/migrations/`; `init.sql` is idempotent DDL | **No Alembic** (`alembic.ini` absent). No down-migrations, no version table, no dry-run. |
| Scheduler | `admin/backend/scheduler.py` → `AsyncIOScheduler`, ~16 crons (clustering, signal, RSI: novelty-judge/relevance-audit/graph-maint/entity-resolution, trend-alerts, xray-health, OpenRouter reconcile) all in one process, `max_instances=1`, in-memory jobstore | Admin down ⇒ **all** crons down. No external jobstore, no leader election. `reconcile_running_manual_jobs()` only flips orphaned rows to `error` on restart. |
| Backups / DR | repo-wide search for `backup`/`restore`/`pg_dump`/`snapshot`/`dump` | **Nothing.** `postgres_data`, `qdrant_data`, `neo4j_data` are local Docker volumes (`docker-compose.yml` L751-761). Single-node. RPO = ∞, RTO = ∞. |
| CI | `.github/workflows/sources-rollout-helper.yml` only (a manual `workflow_dispatch` annotator) | **No lint/test/build CI.** `pyproject.toml` defines ruff/black/mypy/pytest but nothing runs them in CI. |
| Secrets | single `.env` on server (`docker-compose.yml` interpolates `${POSTGRES_PASSWORD}`, `${NEO4J_PASSWORD}`, `${GIGACHAT_CREDENTIALS}`, all provider keys, `${S3_*}`); Grafana defaults to `admin` (`GF_SECURITY_ADMIN_PASSWORD:-admin`, L738); admin alertmanager token **fails open** (`monitoring.py` L66-67 `if not expected: return`) | Plaintext, unrotated, single blast radius. |
| Per-tenant metering | `worker/llm_router_client.py` `chat()`/`vision()`/`embed()` take **no** `workspace_id` param; `receipt.workspace_id` resolves to `""` at L1353; `ProviderBudgetManager._finops_scopes` HAS a `cost_workspace` scope (L244-254) but it is never populated | Dead-code tenant FinOps. Redis keys ~3-day TTL (L454, L567) — cannot reconstruct a billing month. |
| LLM concurrency guard | `worker/wormsoft_guard.py` `_RESERVE_SLOT_LUA` keys `wormsoft:last_started_at` **globally** (no workspace) | Global single-flight ⇒ one tenant's enrichment starves all. |
| Reusable fair-queue primitive | `admin/backend/services/openrouter_picker.py` `_RESERVE_SLOT_LUA` (KEYS=health/rpm/rpd, atomic `INCR`+TTL windows, L48-95) | Re-keyable per workspace — the basis for per-tenant fair queueing. |
| Residency (favorable) | `shared/s3.py` `region_name=settings.s3_region` (default `ru-central-1`); GigaChat Sber endpoints in `shared/config.py` (L44-51) | Primary stores already RU. **But** OpenRouter/Polza + `xray` VLESS egress (`docker-compose.yml` L199-227) route data abroad with no PDn gating. |
| RLS | repo-wide `ROW LEVEL SECURITY`/`CREATE POLICY` search | **None.** Tenancy is soft (app-level `workspace_id` filter only). |

This section assumes the **Identity & Auth** and **Billing/Metering** domains land the tables they own (`accounts`, `api_keys`, `subscriptions`, `usage_events`, `tenant_secrets`). Where this section needs them it states the contract.

---

## 1. MVP-blocking vs later (priority spine)

**MVP-BLOCKING (cannot take a paying customer without these):**
1. **B1 — Backups/DR** (existential; a disk loss today = total business loss, no customer data recoverable).
2. **B2 — Secrets hardening** (close fail-open admin auth, non-default Grafana, encrypt per-tenant integration secrets at rest — required before any customer secret touches the DB).
3. **B3 — CI gate + image registry + tagged deploy + Alembic** (a solo operator must not hand-edit prod SQL or rebuild on prod; a bad migration with no rollback is unrecoverable solo).
4. **B4 — Per-tenant LLM fair queueing + durable usage ledger** (without it the platform-pooled key is a DoS/cost-bomb surface and billing month cannot be reconstructed — co-owned with Metering domain).
5. **B5 — 152-FZ minimum** (PDn classification + RU-pinned routing policy for PDn payloads + RKN localization attestation + erasure-by-account) — legally blocking the moment a second human's data (a customer + their Telegram handles) is processed for money.

**LATER (scale/robustness, not first-customer-blocking):**
6. **L1 — Scheduler HA** (externalize APScheduler) — single-operator tolerable at 1 node short-term, but on the near critical path because admin-down silently stops billing-cap and alert crons.
7. **L2 — Stateful tier to managed RU Postgres/Redis + read replica**.
8. **L3 — Multiple stateless replicas behind LB; ingest leader-election/sharding**.
9. **L4 — Per-tenant observability (workspace label + cardinality guard) + tenant status page**.

---

## 2. (a) Backups & DR — **B1, existential, effort L**

### Target RPO/RTO (solo-operable)
| Store | Method | RPO | RTO | Off-node target |
|---|---|---|---|---|
| PostgreSQL (canonical) | nightly `pg_dump -Fc` + hourly WAL archive (`archive_command` → RU object storage) or managed PITR | ≤ 1h (WAL) / 24h (dump-only fallback) | ≤ 1h | Cloud.ru S3 `ru-central-1`, versioned bucket, distinct from media bucket |
| Qdrant | native snapshot API (`POST /collections/{c}/snapshots`) nightly, downloaded + shipped | ≤ 24h (rebuildable from PG+embeddings if lost) | ≤ 4h | same RU bucket, `qdrant/` prefix |
| Neo4j | `neo4j-admin database dump` nightly (offline dump of a stopped DB or online `backup` on Enterprise; Community = dump on a scheduled brief pause window) | ≤ 24h (rebuildable from PG concepts) | ≤ 4h | RU bucket `neo4j/` prefix |
| S3 media / vision-summaries | enable **bucket Versioning** + lifecycle (noncurrent expiry 90d); optional cross-bucket replication within `ru-central-1` | 0 (versioned) | minutes | in-place |
| `.env` / SOPS secrets | encrypted file committed to private repo + copy in RU password manager | 0 | minutes | see §6 |

> Qdrant and Neo4j are **derived** stores (rebuildable from Postgres canonical + GigaChat re-embedding), so their RPO can be looser; **Postgres is the only store whose loss is unrecoverable**, hence WAL archiving is the one non-negotiable.

### New asset: `scripts/backup/backup_all.sh` (run by host cron + the externalized scheduler, see §7)
```bash
#!/usr/bin/env bash
set -euo pipefail
cd /opt/frontier-intelligence
STAMP=$(date -u +%Y%m%dT%H%M%SZ); DEST="s3://${BACKUP_BUCKET}/${STAMP}"
# 1. Postgres (custom format, compressed, single tx-consistent dump)
docker compose exec -T postgres pg_dump -Fc -U "$POSTGRES_USER" "$POSTGRES_DB" \
  | mc pipe "ru/${BACKUP_BUCKET}/${STAMP}/postgres/frontier.dump"
# 2. Qdrant snapshot (per collection alias)
for col in $(curl -s http://127.0.0.1:6333/collections | jq -r '.result.collections[].name'); do
  sid=$(curl -sX POST "http://127.0.0.1:6333/collections/${col}/snapshots" | jq -r '.result.name')
  curl -s "http://127.0.0.1:6333/collections/${col}/snapshots/${sid}" \
    | mc pipe "ru/${BACKUP_BUCKET}/${STAMP}/qdrant/${col}.snapshot"
done
# 3. Neo4j dump (brief pause window; Community)
docker compose exec -T neo4j neo4j-admin database dump neo4j --to-stdout \
  | mc pipe "ru/${BACKUP_BUCKET}/${STAMP}/neo4j/neo4j.dump"
# 4. SOPS-encrypted secrets snapshot
mc cp secrets.enc.yaml "ru/${BACKUP_BUCKET}/${STAMP}/secrets/"
# 5. Retention manifest + GFS prune (keep 7 daily, 4 weekly, 6 monthly)
scripts/backup/prune_gfs.py "ru/${BACKUP_BUCKET}"
```
WAL archiving (continuous, the actual RPO driver) is set on the Postgres side:
```ini
# postgresql.conf overlay (mounted as docker-compose.host-fixes.yml fragment)
wal_level = replica
archive_mode = on
archive_command = 'mc pipe ru/%BACKUP_BUCKET%/wal/%f < %p'
```

### Restore runbook — new doc `docs/dr-restore-runbook.md`
Single-command bootstrap so a solo operator can rebuild on a fresh node:
```bash
scripts/backup/restore_all.sh --stamp 20260626T030000Z --target-node new-host
# → provisions stack from tagged images (NOT rebuild), restores PG dump+WAL replay,
#   imports Qdrant snapshots, loads Neo4j dump, verifies row/point/node counts,
#   runs scripts/server_checks.sh smoke, flips DNS.
```

### Periodic restore drill — **mandatory, automated** (effort S)
A monthly scheduled cloud agent / CI job spins an ephemeral node, restores the latest backup, runs `scripts/server_checks.sh` + a row-count assertion against canonical counts, and posts pass/fail to the alert Telegram. **An untested backup is not a backup** — for a solo operator the drill is the only thing that catches a silently broken `archive_command`.

### Change list
| # | Change | Effort |
|---|---|---|
| B1.1 | `scripts/backup/backup_all.sh` + `prune_gfs.py` (GFS retention) | M |
| B1.2 | Enable Postgres WAL archiving overlay + create versioned RU `BACKUP_BUCKET` | S |
| B1.3 | Enable S3 media bucket Versioning + lifecycle | S |
| B1.4 | `scripts/backup/restore_all.sh` + `docs/dr-restore-runbook.md` | M |
| B1.5 | Monthly automated restore drill (cloud agent / CI) + alert | S |

---

## 3. (b) HA & scaling — **mix of B4 (queue) now, L2/L3 later**

### Topology: separate stateless from stateful
- **Stateless (replicable):** `worker`, `mcp`, `mcp-gateway`, `admin` (HTTP API portion), `crawl4ai`, `searxng`. These hold no durable state (worker is stateless per CLAUDE.md; MCP reads stores). They can run ≥2 replicas behind a load balancer.
- **Stateful (single-writer / managed):** `postgres`, `redis`, `qdrant`, `neo4j`, `xray`, `ingest` (holds Telethon session + checkpoint cursor).

### Per-tenant fair queueing — **B4, effort M, replaces global single-flight**
Replace the global `worker/wormsoft_guard.py` single-flight (`wormsoft:last_started_at`, no tenant dimension) with a **weighted leaky bucket keyed by `workspace_id`**, reusing the proven `_RESERVE_SLOT_LUA` reserve/INCR/TTL pattern from `admin/backend/services/openrouter_picker.py` (L48-95). New module `worker/tenant_fair_queue.py`:
```lua
-- KEYS[1]=bucket:{provider}:{workspace_id}  ARGV: now_ms, capacity, refill_per_sec, cost, weight
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or tonumber(ARGV[2])      -- capacity
local last   = tonumber(b[2]) or tonumber(ARGV[1])
local refill = (tonumber(ARGV[1]) - last)/1000.0 * tonumber(ARGV[3]) * tonumber(ARGV[5]) -- weighted by tier
tokens = math.min(tonumber(ARGV[2]), tokens + refill)
if tokens < tonumber(ARGV[4]) then
  return {'throttled', tostring(tokens)}                 -- caller backs off / 429s
end
redis.call('HMSET', KEYS[1], 'tokens', tostring(tokens - tonumber(ARGV[4])), 'ts', ARGV[1])
redis.call('EXPIRE', KEYS[1], 3600)
return {'ok', tostring(tokens - tonumber(ARGV[4]))}
```
- `weight` comes from the tenant's subscription tier (Metering domain), so a paid B2B feed tenant gets a larger refill than a trial — **fairness + tier QoS in one primitive**.
- The global guard stays as an **outer** provider-protection layer (protects the upstream from the pool); the per-tenant bucket is the **inner** fairness layer. Both are Redis-Lua, cross-worker coordinated.

### Per-tenant metering propagation — **B4, co-owned with Metering, effort M**
Thread `workspace_id` through the router so the *already-built* FinOps scope (`ProviderBudgetManager._finops_scopes` `cost_workspace`, L244-254) finally populates:
- Add `workspace_id: str` param to `LLMRouterClient.chat()` (L2204), `.vision()` (L831), `.embed()` (L422), and to `ProviderExecutionRequest`/`ExecutionReceipt` construction so `receipt.workspace_id` (read at `llm_router_client.py` L1353) is non-empty.
- Then `record_execution_receipt` writes the `cost_workspace` Redis scope; a new durable sink flushes those to a Postgres `usage_events` ledger (Metering domain owns the table) on a short interval, defeating the ~3-day Redis TTL (L567). Apply the real `wormsoft_limits._normalize_pricing()` RUB table (input/output/cache, L62-77) to convert tokens→money at flush time — closing the "cost = raw token counts" gap.

### Managed/replicated RU stateful tier — **L2, effort L**
- Move Postgres to a managed RU PITR-capable service (Cloud.ru/Yandex Managed PostgreSQL) → outsources WAL/PITR/failover, removing the single-node SPOF for the one unrecoverable store. Keep `DATABASE_URL` injection identical (`docker-compose.yml` L156/240/400/488) — only the host changes.
- Redis → managed RU Redis with replica + persistence; keep `REDIS_URL`.
- Qdrant/Neo4j stay self-hosted single-node initially (derived/rebuildable); add a replica only when read QPS demands.

### Ingest leader-election / sharding — **L3, effort M**
`ingest` is `HOSTNAME: ingest-0` (`docker-compose.yml` L184) with a shared 1-2 Telegram accounts and per-source checkpoints (`source_checkpoints`, `source_runs`). FloodWait on the shared account blasts all sources. Mitigation: shard sources across N ingest replicas by a stable hash of `source_id` written to a Redis `ingest:lease:{shard}` key (leader-election via `SET NX PX`), so a crashed replica's shard is re-leased. Telegram stays platform-curated-only (per owner decision), so account count stays small; sharding bounds the FloodWait blast radius rather than scaling throughput.

| # | Change | Effort | Priority |
|---|---|---|---|
| HA.1 | `worker/tenant_fair_queue.py` weighted leaky bucket (reuse picker Lua) | M | B4 |
| HA.2 | Thread `workspace_id` into router chat/vision/embed + durable `usage_events` flush + apply RUB pricing | M | B4 |
| HA.3 | Stateless replicas (≥2 worker/mcp/admin) behind LB | M | L3 |
| HA.4 | Managed RU Postgres (PITR) + Redis | L | L2 |
| HA.5 | Ingest shard-lease leader election | M | L3 |

---

## 4. (c) Scheduler HA — **L1, near-critical, effort M**

`admin/backend/scheduler.py` runs ~16 crons in one `AsyncIOScheduler` with an **in-memory** jobstore. Admin restart/crash = every cron silently stops — including `urgent_trend_alerts` (customer value) and, after Metering lands, the **revenue-cap guardrail** cron (НПД 2.4M check). For a solo operator this silent failure mode is dangerous.

**Phase 1 (cheap, do with CI work):** give APScheduler a **persistent Postgres jobstore** + a Redis `SET NX PX` **leader lock** so exactly one admin replica runs the scheduler (others run API only). This survives restart (job state in PG) and allows ≥2 admin replicas without double-firing. The existing per-job `asyncio.Lock` + DB `admin_manual_jobs` single-flight guard (`scheduler.py` L286-330) stays as the inner mutex.

**Phase 2 (real HA):** externalize to a durable queue/worker — recommended **Redis + RQ-Scheduler** or **APScheduler-with-PG-jobstore behind the leader lock** (avoid introducing Celery's broker weight for a solo op). Crons enqueue jobs; a separate `scheduler-runner` service consumes them, so admin API and cron execution are decoupled and independently restartable. Each cron job becomes idempotent (already mostly true — jobs are workspace-scoped and locked).

| # | Change | Effort |
|---|---|---|
| SCH.1 | APScheduler → Postgres jobstore + Redis leader lock (`SET NX PX`) | S |
| SCH.2 | Split scheduler into `scheduler-runner` service; crons enqueue, runner consumes | M |
| SCH.3 | Add revenue-cap-guard cron (НПД 2.4M; hard-stop new charges) — depends on Metering | S |

---

## 5. (d) CI/CD + migrations — **B3, effort L**

### Pipeline (GitHub Actions, replacing the lone annotator workflow)
```yaml
# .github/workflows/ci.yml
name: ci
on: { push: {}, pull_request: {} }
jobs:
  lint-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install ruff black isort pytest pydantic pydantic-settings
      - run: ruff check . && black --check . && isort --check-only .
      - run: pytest -m unit            # unit marker exists in pyproject.toml
  build-push:
    needs: lint-test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: |   # build & push versioned images per service to a RU registry
          TAG=$(git rev-parse --short HEAD)
          for s in worker mcp mcp-gateway admin ingest crawl4ai gpt2giga-proxy; do
            docker build -f $(svc_dockerfile $s) -t $REGISTRY/frontier-$s:$TAG .
            docker push $REGISTRY/frontier-$s:$TAG
          done
```
- **Registry:** a RU-hosted registry (Cloud.ru Container Registry / self-hosted Harbor on the same node initially) — keeps images RU-resident, consistent with 152-FZ posture.
- **Deploy by pulling tags, not rebuilding on prod.** New `scripts/deploy-tag.sh`:
```bash
TAG=$1
sed -i "s/:.*$/:$TAG/" .env.images   # IMAGE_TAG pin
docker compose --profile core --profile worker --profile mcp --profile admin pull
docker compose up -d --no-build       # images already built in CI
scripts/server_checks.sh || scripts/deploy-tag.sh $PREV_TAG   # auto-rollback
```
This **retires** `scripts/server-deploy-rebuild.sh` and `server-build-stack.sh` as the primary path (kept only as the AppArmor/BuildKit break-glass per `docs/ops-server-troubleshooting.md`). Note: per MEMORY `ops_admin_image_needs_worker_deps`, the admin image must carry all `worker.services.*` deps (pipeline_jobs imports at module level) — the CI build matrix must build admin from the same base, and the health gate must verify admin after deploy.

### Staging stack — **B3, effort M**
A second compose project (`COMPOSE_PROJECT_NAME=frontier-staging`, separate volumes, separate workspaces, **synthetic data only — no customer PDn**) on the same or a cheap second node. CI deploys tag → staging → smoke → manual promote to prod. Solo-operable: one `deploy-tag.sh --env staging`.

### Alembic adoption — **B3, effort M, reversible migrations**
Replace the raw-SQL loop (`scripts/apply-postgres-migrations.sh`) with Alembic:
1. `alembic init migrations_alembic`; `alembic stamp head` against current schema (baseline = today's `init.sql` + 13 applied files).
2. New changes go through `alembic revision --autogenerate` with **hand-reviewed up + down**.
3. CI runs `alembic upgrade head` against an ephemeral PG (catches broken migrations before prod).
4. Deploy runs `alembic upgrade head` (forward) gated, with `alembic downgrade -1` as the documented rollback — closing the "unversioned, irreversible SQL on prod" gap.

| # | Change | Effort |
|---|---|---|
| CI.1 | `ci.yml` lint (ruff/black/isort) + `pytest -m unit` gate | S |
| CI.2 | Build & push versioned per-service images to RU registry | M |
| CI.3 | `scripts/deploy-tag.sh` pull-tagged + auto-rollback; retire rebuild-on-prod | M |
| CI.4 | Staging stack (synthetic data) + promote flow | M |
| CI.5 | Alembic baseline + autogenerate + up/down + CI upgrade check | M |

---

## 6. (e) Secrets — **B2 (subset MVP-blocking), effort M**

### Off single plaintext `.env`
- Adopt **SOPS + age** (RU-friendly, no cloud KMS dependency): commit `secrets.enc.yaml` (encrypted) to the private repo; decrypt on the server into runtime env at deploy time (`sops -d secrets.enc.yaml > .env.runtime` in `deploy-tag.sh`, `0600`, tmpfs-mounted). Rotation = re-encrypt + redeploy; a documented quarterly rotation for `POSTGRES_PASSWORD`, `NEO4J_PASSWORD`, provider keys, `GIGACHAT_CREDENTIALS`, `S3_*`. This keeps the single-operator workflow (no Vault server to babysit) while removing plaintext-at-rest.
- (Scale option L: HashiCorp Vault or Yandex Lockbox; deferred — too heavy for solo MVP.)

### Close the open holes — **MVP-blocking**
1. **Grafana default creds** (`docker-compose.yml` L738 `:-admin`): make `GRAFANA_PASSWORD` mandatory (no default) and add a startup check in `scripts/check_env.py` that **fails** if any secret equals a known default.
2. **Admin alertmanager fail-open** (`monitoring.py` L66-67): change `if not expected: return` to **fail-closed** (raise 403 / refuse to boot) so an unset token never silently disables auth. This is the same anti-pattern the Auth domain must eradicate on all 3 surfaces.
3. **Per-tenant integration secrets at rest** (B2, hard requirement before self-serve sources): the owner decision allows self-serve RSS/web/api sources but Email is unsafe due to plaintext IMAP creds in `sources.extra`. For the allowed connectors, any tenant-supplied secret (e.g., API bearer token) goes into a dedicated `tenant_secrets` table **encrypted with a per-row envelope** (libsodium sealed box; the data key in SOPS, not in the DB):
```sql
CREATE TABLE tenant_secrets (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id  TEXT NOT NULL REFERENCES workspaces(id),
  account_id    UUID NOT NULL,              -- from Identity domain
  kind          TEXT NOT NULL,              -- 'api_bearer' | 'web_basic' ...
  ciphertext    BYTEA NOT NULL,             -- libsodium sealed box
  nonce         BYTEA NOT NULL,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  rotated_at    TIMESTAMPTZ
);
```
Email self-serve stays disabled (honor owner decision: plaintext IMAP unsafe).

| # | Change | Effort | Priority |
|---|---|---|---|
| SEC.1 | SOPS+age encrypted secrets file + decrypt-on-deploy + rotation doc | M | B2 |
| SEC.2 | Mandatory Grafana pwd + default-secret check in `check_env.py` | S | B2 |
| SEC.3 | Alertmanager auth fail-**closed** (`monitoring.py`) | S | B2 |
| SEC.4 | `tenant_secrets` envelope-encrypted table for self-serve connectors | M | B2 |

---

## 7. (f) 152-FZ + data residency — **B5, effort L**

### PDn classification
The system processes **personal data of RU citizens** the moment it ingests Telegram: author handles/usernames, message bodies (may name people), media (faces/text). Customer account data (email, payment identity) is also PDn. Classify in a register `docs/152fz-pdn-register.md`:
| Category | Where stored | PDn? | Basis |
|---|---|---|---|
| Telegram author handle / channel | `posts`, `concepts`, Neo4j nodes | Yes (indirect) | legitimate interest (public channel) — but still RU-localize |
| Message body / media | `posts.content`, `media_urls`, S3 | Yes (may contain) | same |
| Customer email / billing identity | `accounts` (Identity domain) | Yes (direct) | contract / consent |

### Routing POLICY: pin PDn-bearing payloads to RU providers or pseudonymize
Today text/vision can route to OpenRouter/Polza (non-RU) and `xray` egresses abroad with **no gating**. Add a **PDn gate** in `RoutingPolicyV2` candidate selection (`shared/llm_control_plane.py`):
- Tag each `ProviderExecutionRequest` with `pdn_class` (derived from task: enrichment of raw post body = `pdn_high`; cluster-label synthesis over aggregated/anonymized text = `pdn_low`).
- `pdn_high` ⇒ candidate list filtered to **RU-resident providers only (GigaChat)**, regardless of the normal Wormsoft→OpenRouter→Polza chain. Embeddings are already GigaChat-only (RU), so they are compliant by construction.
- **OR** pseudonymize before any non-RU hop: a pre-processor strips/【MASK】s handles, @mentions, emails, phone numbers (reuse NER from the concepts stage) so only de-identified text leaves RU. Pseudonymization is the lever that lets the cheaper non-RU providers stay in the chain for non-sensitive synthesis without violating localization.
```python
# shared/llm_control_plane.py — candidate filter (sketch)
def _apply_pdn_gate(candidates, pdn_class, pseudonymized: bool):
    if pdn_class == "pdn_high" and not pseudonymized:
        return [c for c in candidates if c.provider == PROVIDER_GIGACHAT]  # RU only
    return candidates
```
The `xray` egress (`docker-compose.yml` L199-227) must never carry raw PDn either — it is for source fetching, but any fetched content that becomes a PDn payload must be classified before LLM routing.

### RKN localization (Art. 18.5)
The **primary database of RU-citizen PDn must be in RU** — already satisfied (Postgres on RU node / managed RU PG, S3 `ru-central-1`, GigaChat Sber). Document this in a localization attestation; the only compliance *gap* is the non-RU LLM hops above (fixed by the PDn gate) — they are *processing*, not the *primary store*, but localization + cross-border transfer rules still require either RU-pinning or pseudonymization, which the gate delivers.

### Retention / erasure per account
- Add `retention_policy` (days) per workspace/account and a scheduled **purge job** (new cron, runs in the externalized scheduler) that deletes posts/enrichments/Qdrant points/Neo4j nodes older than the policy, scoped by `workspace_id`.
- **Right-to-erasure** endpoint: `DELETE /account/{id}/pdn` cascades across Postgres (`posts`, `post_enrichments`, `concepts`), Qdrant (`_build_payload_filter` by `workspace_id`), Neo4j (workspace subgraph), and S3 media — driven by `workspace_id` which is already the partition key everywhere except the child tables flagged below.
- **Blocker dependency:** child tables `source_checkpoints`, `source_runs`, `indexing_status`, `post_enrichments` LACK `workspace_id` (verified in `init.sql` — they FK to `sources`/`posts` only). Erasure-by-`workspace_id` must JOIN through parents; safer to **add `workspace_id` columns** (denormalized, backfilled via Alembic migration) so deletion and RLS are clean. This is shared with the Tenancy domain.

### Postgres RLS (defense-in-depth for tenancy + erasure correctness)
Soft tenancy (app-level filter only) is a 152-FZ and security risk (one bug = cross-tenant PDn leak). Add **RLS** keyed by a `SET app.workspace_id` session GUC, enabled per tenant-scoped table, so even a missed `WHERE workspace_id=` cannot leak:
```sql
ALTER TABLE posts ENABLE ROW LEVEL SECURITY;
CREATE POLICY ws_isolation ON posts
  USING (workspace_id = current_setting('app.workspace_id', true));
```
(Co-owned with Tenancy/Data domain; listed here because it is the technical guarantee behind the erasure + localization claims.)

| # | Change | Effort | Priority |
|---|---|---|---|
| FZ.1 | PDn register doc + localization attestation | S | B5 |
| FZ.2 | PDn gate in `RoutingPolicyV2` (RU-pin `pdn_high`) + NER pseudonymizer for non-RU hops | M | B5 |
| FZ.3 | Per-account retention + purge cron + `DELETE /account/{id}/pdn` cascade | M | B5 |
| FZ.4 | Backfill `workspace_id` on child tables (Alembic) | S | B5 (dep) |
| FZ.5 | Postgres RLS on tenant-scoped tables | M | later/B5 |

---

## 8. (g) Per-tenant observability + tenant status — **L4, effort M**

- **Workspace label:** add a `workspace` label to MCP `/metrics` (currently no per-tenant dimension) and to LLM-cost metrics (`note_llm_cost` in `llm_router_client.py` L1383 already receives the receipt — emit `workspace_id` as a label once HA.2 populates it). **Cardinality guard:** only label metrics by `workspace_id` for active paying tenants (bounded ~tens under НПД, not unbounded) and bucket the rest as `workspace="other"` to keep Prometheus series count safe — the НПД revenue cap conveniently bounds tenant count.
- **Per-tenant dashboards:** Grafana row templated by `workspace` var: ingestion lag, enrichment latency, LLM spend (RUB, from the new ledger), error rate, quota headroom.
- **Tenant status page:** a read-only `GET /status` (per `api_key` → `workspace_id`) surfacing last-ingest time, queue depth, monthly LLM spend vs plan, source health (reuse `shared/source_quality.py` authority/health/yield). Lets customers self-diagnose without operator involvement — essential for solo-operability.
- **Revenue-cap board:** a single global panel tracking aggregate MRR against the **2.4M RUB/yr НПД cap** with alerting at 80% and a hard-stop hook (the SCH.3 guard) — the legal guardrail must be observable.

| # | Change | Effort |
|---|---|---|
| OBS.1 | `workspace` label on MCP `/metrics` + LLM-cost metrics, with cardinality guard | M |
| OBS.2 | Per-tenant Grafana dashboard + RUB spend panel | S |
| OBS.3 | `GET /status` tenant self-service endpoint | M |
| OBS.4 | Aggregate-MRR-vs-НПД-cap panel + 80%/hard-stop alerts | S |

---

## 9. Minimal solo-operable target topology (MVP)

```
                         ┌─────────────────────────────┐
   Internet (Claude/     │  Reverse proxy + LB (RU)    │  TLS, per-key auth (Auth domain),
   Cursor, customers) ──▶│  Caddy/Traefik or nginx      │  rate-limit, DNS-rebind guard for MCP
                         └──────────────┬───────────────┘
        ┌───────────────────────────────┼───────────────────────────────┐
        │ STATELESS (≥2 replicas, from TAGGED images pulled from RU reg) │
        │  worker × N (per-tenant fair queue)   mcp / mcp-gateway × 2     │
        │  admin-api × 2   crawl4ai   searxng                              │
        └───────────────────────────────┬───────────────────────────────┘
        ┌───────────────────────────────┼───────────────────────────────┐
        │ SINGLE-WRITER / MANAGED-RU STATEFUL                            │
        │  Managed RU Postgres (PITR, WAL→RU S3)   Managed RU Redis      │
        │  Qdrant (snapshot→RU S3)   Neo4j (dump→RU S3)                   │
        │  ingest (shard-lease)   xray (egress, PDn-gated)                │
        │  scheduler-runner (leader-locked)                              │
        └───────────────────────────────┬───────────────────────────────┘
                         ┌──────────────┴───────────────┐
                         │ Cloud.ru S3 ru-central-1      │  media + versioned backups
                         │ (versioned, off-node, RU)     │  + WAL archive + SOPS secrets
                         └───────────────────────────────┘
   GigaChat (RU)  ◀── PDn-high routes pinned here ──┐
   OpenRouter/Polza (non-RU) ◀── only pseudonymized / pdn_low ──┘
```
Everything in this topology is reachable by **one operator** via `deploy-tag.sh`, `backup_all.sh`/`restore_all.sh`, and a leader-locked scheduler — no step requires a second human or a hand-edit on prod.

## 10. Sequenced rollout (effort-tagged)
1. **Week 1 (B1):** WAL archiving + `backup_all.sh` + restore runbook + first drill. *(L)* — stop being one disk failure from extinction.
2. **Week 1-2 (B2):** SOPS, fail-closed admin auth, mandatory Grafana pwd, `tenant_secrets`. *(M)*
3. **Week 2-3 (B3):** CI lint+test gate, registry+tagged deploy+rollback, Alembic baseline, staging. *(L)* — retire rebuild-on-prod.
4. **Week 3-4 (B4):** per-tenant fair queue + `workspace_id` propagation + durable `usage_events` + RUB pricing. *(M)* — co-deliver with Metering.
5. **Week 4-5 (B5):** PDn register, RU-pin routing gate + pseudonymizer, retention/erasure + child-table `workspace_id` backfill. *(L)*
6. **Post-MVP (L1-L4):** scheduler externalization, managed RU PG/Redis, stateless replicas+LB, ingest sharding, RLS, per-tenant observability + status + НПД-cap board.


---

## Открытые решения по этому разделу

- Managed RU Postgres provider choice (Cloud.ru vs Yandex Managed PostgreSQL): recommend Yandex Managed PostgreSQL for mature PITR/HA if Cloud.ru S3 cross-account works; otherwise stay self-hosted single-node with WAL→S3 for MVP and migrate post-revenue. Decision can wait until B1 ships, but pick before B5 RLS work.
- Scheduler externalization target (RQ-Scheduler vs APScheduler+PG-jobstore+leader-lock): recommend APScheduler+Postgres-jobstore+Redis leader lock first (minimal new infra, reuses existing async stack) and only move to RQ if cron contention appears. Owner should confirm appetite for adding RQ as a dependency.
- PDn handling default for non-RU providers: recommend RU-pinning pdn_high to GigaChat by default (simplest, provably compliant) and treat the NER pseudonymizer as an optimization to re-enable cheaper non-RU providers later — confirm whether the cost delta justifies building the pseudonymizer in the MVP window or deferring it.
- Secrets manager: recommend SOPS+age now (no server to operate, solo-friendly) and defer Vault/Lockbox to scale phase — confirm the owner accepts an encrypted-file-in-repo model vs wanting a managed KMS from day one.
- Staging node: recommend a single cheap second RU VM (or same-node second compose project with isolated volumes + synthetic data) — confirm budget appetite for a dedicated staging host vs same-host project isolation.
- Restore-drill cadence and runner: recommend monthly automated drill on an ephemeral node via a scheduled cloud agent — confirm whether monthly is acceptable for the targeted RTO or quarterly suffices to save compute.

## Зависимости от других разделов

- Identity & Auth — owns accounts/api_keys and must land fail-closed auth on MCP REST :8100, MCP gateway :8102, Admin :8101 (this section's secrets fail-closed fix and tenant status endpoint depend on per-key→workspace_id resolution)
- Billing & Metering — co-owns per-tenant usage ledger (usage_events table), RUB pricing application from wormsoft_limits._normalize_pricing, subscription tier weights consumed by the fair-queue, and the НПД 2.4M revenue-cap guard cron
- Tenancy & Data model — owns Postgres RLS rollout, the workspace_id backfill on child tables (source_checkpoints/source_runs/indexing_status/post_enrichments), and Qdrant/Neo4j per-tenant isolation that the erasure cascade relies on
- Product/Self-serve sources — defines which connectors (RSS/web/api) accept tenant secrets, gating the tenant_secrets encrypted-at-rest table; Email stays disabled by owner decision
- Provider/LLM routing — the PDn gate is implemented in shared/llm_control_plane.py RoutingPolicyV2 and the workspace_id threading touches worker/llm_router_client.py chat/vision/embed, overlapping with the LLM-routing owner

