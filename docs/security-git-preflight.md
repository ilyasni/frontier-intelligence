# Security and Git Preflight

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

As of 2026-04-17, these services were running and published on all interfaces by Docker Compose:

- `admin:8101`
- `mcp:8100`
- `mcp-gateway:8102`
- `gpt2giga-proxy:8090`
- `grafana:3000`
- `prometheus:9090`
- `alertmanager:9093`
- `qdrant:6333`
- `neo4j:7474/7687`
- `paddleocr:8008`

This is acceptable only inside a trusted LAN/VPN or behind host firewall rules. Do not expose these ports directly to the internet without VPN or authenticated reverse proxy.

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
