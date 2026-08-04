# Security and Git Preflight

<!-- audit-status:2026-08-04 -->
> **📌 ИСТОРИЧЕСКИЙ СНИМОК · сверено 2026-08-04.**
> Датированный снимок/решение своего момента. Ценен как история — описанием сегодняшнего состояния не является.
> Конкретных расхождений найдено: **3** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).

Last audit: 2026-04-18.

Goal: prepare the repository for the first git init/publish without leaking server-only secrets or runtime data.

After the first server-backed commit and push to `origin`, the **shared**
history lives in git. The Windows workspace should follow **`git pull
--ff-only`** (or re-clone) before large edits so it does not diverge from the
server branch. Treating an unsynced local tree as authoritative over `origin`
is unsafe.

## What Was Checked

- Local working tree under `D:\Workspace\frontier-intelligence`
- Live server tree under `/opt/frontier-intelligence`
- Docker Compose published ports and service status
- Secret-looking values in code/config/docs, excluding `.env`, sessions, caches, and legacy archives
- Git/rsync ignore rules for server-only files

No real secret values should be pasted into issues, chats, PRs, or docs. For diagnostics, report only variable names and whether they are set.

## Findings Fixed

- `.gitignore` and `.rsync-exclude` had mojibake comments and over-broad storage exclusions. They now keep schema/helper files trackable while excluding runtime data, caches, sessions, private keys, `.env*`, and local AI/editor tooling.
- `.env.example` contained concrete Xray/VLESS/Reality example values. They were replaced with placeholders.
- `searxng/settings.yml` contained a hardcoded `server.secret_key`. The real file is now treated as server-local/ignored, and `searxng/settings.example.yml` is the tracked template.
- Added `scripts/server-ensure-searxng-secret.sh` to create or rotate a SearXNG `secret_key` on the server without printing it.
- The legacy archive `docs/old_docs*/` contains old sample passwords and historical commands. It is excluded from git/rsync by default; delete it separately if it is no longer needed as migration reference.

SearXNG requires `server.secret_key` in `settings.yml`; the upstream docs also mark example keys as "change this": https://docs.searxng.org/admin/settings/settings.html

## Server Exposure Snapshot

Updated **2026-08-04**. The previous snapshot (2026-04-17) listed every service in one
flat row as "published on all interfaces". That stopped being true after two separate
changes to the auth posture, and the document said nothing about either — which is the
part that mattered: this is the one place the attack surface gets judged from, and it
showed symmetry where there is none. The obligation to update it after a posture change
is stated at the bottom of this file; it was not honoured twice.

Reality now — three interfaces, not one:

**Published on `0.0.0.0` (the whole `192.168.31.0/24` segment can reach them):**

| Service | Auth | Note |
|---|---|---|
| `admin:8101` | HTTP Basic **or** cookie session | `/api/health`, `/api/auth/login` and the Alertmanager webhook are public by design; the webhook carries its own Basic credential. Plain HTTP — the session cookie is `httponly` + `samesite=lax` and **not** `secure`, because there is no TLS termination in the stack (Caddy was deliberately excluded) |
| `grafana:3000` | own login | not on default credentials (`admin:admin` → 401) |
| `mcp-gateway:8102` | **none at all** | see the residual-risk note below |

**Bound to `127.0.0.1` (reachable only through an SSH tunnel):**
`mcp:8100`, `prometheus:9090`, `alertmanager:9093`, `qdrant:6333`, `neo4j:7474/7687`,
`paddleocr:8008`, `gpt2giga-proxy:8090`.

`mcp:8100` moved to loopback on 2026-08-03 (branch `security/mcp-rest-loopback-only`,
commit `d90d5cd`) for one stated reason: it has no authentication of any kind.

**Residual risk, accepted knowingly — `mcp-gateway:8102`.** The gateway carries the same
absence of authentication as the REST port that was closed for it, and since 2026-08-04
it also exposes the RSI approval loop: `approve_entity_merge` (merges two concepts in the
Neo4j graph and does not split them back automatically), `approve_threshold_change`
(rewrites a detector threshold), `reject_entity_merge`, `mark_relevance_audit`, plus
`record_card_feedback` and `ingest_url`. Anything on the LAN segment can read the whole
knowledge base and invoke those writes without a credential. `ingest_url` is guarded
against SSRF by `assert_public_http_url`; nothing else is guarded.

The owner reviewed this twice on 2026-08-04 — the second time with the full list of write
tools in hand — and decided to leave it as is: the host sits on a local network with no
untrusted parties. **Do not reopen this without a change to the network contour** (a port
forwarded outward, or access to this network from untrusted devices). The reasoning is
recorded in [AUDIT-2026-08-04.md, section 8](./AUDIT-2026-08-04.md#8-принятые-решения).

**No host firewall.** `ufw status` → `inactive`, `iptables -S` → `-P INPUT ACCEPT`. The
open ports above are open to the segment with nothing in front of them. Accepted on the
same grounds and by the same decision.

Verify the snapshot rather than trusting it — the command below is the source of truth:

```bash
ssh frontier-intelligence "docker ps --format '{{.Names}}\t{{.Ports}}' | sort"
```

## Server-Only Files

Never commit or sync from local to git:

- `.env`, `.env.*` except committed examples
- `sessions/`, `*.session`, `*.session-journal`
- `searxng/settings.yml`
- database dumps, local Docker data directories, Grafana/Prometheus data
- private keys/certs: `*.pem`, `*.key`, `*.p12`, `*.pfx`, `id_rsa*`, `id_ed25519*`
- local tooling: `.agents/`, `.claude/`, `.cursor/`, `.vscode/`, `AGENTS.md`, `CLAUDE.md`
- legacy/bulky docs: `docs/old_docs*/`, `docs/chatgpt/designer-ai-visionary/books/`

## Before First Git Init

Preferred now: run from the server workspace:

```bash
cd /opt/frontier-intelligence
git init
git status --ignored --short
git add -n .
```

Expected: ignored output includes `.env`, sessions, caches,
`searxng/settings.yml`, local tooling, old docs/books archives, and runtime
storage data.

Legacy local-only path, if the server baseline has already been cloned/pulled:

```powershell
git init
git status --ignored --short
git add -n .
```

Expected: ignored output includes `.env`, sessions, caches, `searxng/settings.yml`, local tooling, and the old docs/books archives.

Run a secret scan before the first real `git add`:

```powershell
rg -n "(password|passwd|secret|token|api[_-]?key|access[_-]?key|client[_-]?secret|authorization|bearer|credential|webhook)" `
  --glob "!docs/old_docs ilyasni-telegram-assistant.git/**" `
  --glob "!docs/chatgpt/designer-ai-visionary/books/**" `
  --glob "!**/__pycache__/**" `
  --glob "!.venv/**" `
  --glob "!.pytest_cache/**" `
  --glob "!*.pyc" `
  --glob "!.env" `
  --glob "!*.env" `
  .
```

False positives in tests are acceptable when they use obvious dummy values like `secret`, `abc`, or `super-secret-token`.

## Server After Sync

If `searxng/settings.yml` is missing or still has a placeholder:

```bash
cd /opt/frontier-intelligence
bash scripts/server-ensure-searxng-secret.sh
docker compose --profile searxng --profile worker up -d --force-recreate searxng
```

After changing firewall, compose ports, or auth posture, update this document and `docs/ops-server-troubleshooting.md`.
