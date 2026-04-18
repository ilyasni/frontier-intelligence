# Server-First Git Workflow

Last updated: 2026-04-18.

This project now treats the live server tree as the first git baseline:

```bash
/opt/frontier-intelligence
```

The server currently runs the production Docker stack, has the latest code,
and contains server-only runtime state. Git must track code, docs, templates,
Docker/compose files, scripts, migrations, and tests. Git must not track live
secrets, sessions, database volumes, or local editor/agent tooling.

## Source Of Truth

- Server working tree: canonical baseline for the initial git commit.
- Git remote: canonical source after the first push.
- Local Windows workspace: development checkout after cloning/pulling from git.

Do not make the local workspace authoritative until it has been refreshed from
the server-backed git history.

## Server-Only Files

These must remain ignored/untracked:

- `.env`, `.env.*` except committed example files.
- `sessions/`, `*.session`, `*.session-journal`.
- `searxng/settings.yml`; commit only `searxng/settings.example.yml`.
- Runtime Docker volume data, dumps, sqlite/db files, Prometheus/Grafana data.
- Private keys and certs: `*.pem`, `*.key`, `*.p12`, `*.pfx`, `id_rsa*`,
  `id_ed25519*`.
- Local tooling: `.agents/`, `.claude/`, `.cursor/`, `.vscode/`, `AGENTS.md`,
  `CLAUDE.md`.
- Legacy and bulky reference archives ignored by `.gitignore`.

## First Baseline

Run on the server:

```bash
cd /opt/frontier-intelligence
git init
git config user.name "Frontier Intelligence"
git config user.email "frontier-intelligence@local"
git status --ignored --short
git add -n .
git add .
git status --short
```

Before the real commit, verify that the staged list does not include `.env`,
`sessions/`, `searxng/settings.yml`, storage data, private keys, or old/bulky
archives.

Then:

```bash
git commit -m "Initial server baseline"
```

## Remote Push

After creating a private remote repository:

```bash
cd /opt/frontier-intelligence
git remote add origin <private-repo-url>
git branch -M main
git push -u origin main
```

Use a private repository. This system includes internal operational context and
should not be public.

## Local Development After Baseline

Once the remote exists, recreate or refresh the local workspace from git:

```powershell
cd D:\Workspace
git clone <private-repo-url> frontier-intelligence-git
```

If reusing the existing local folder, first make a backup, then compare it
against the freshly cloned tree. Do not push the older local tree over the
server baseline.

Recommended flow:

```powershell
git checkout -b feature/<name>
python -m pytest -q
git add .
git commit -m "<change summary>"
git push
```

Deploy from the server with fast-forward pulls only:

```bash
cd /opt/frontier-intelligence
git fetch origin
git status --short
git pull --ff-only origin main
bash scripts/server-build-stack.sh worker mcp crawl4ai
COMPOSE_PROFILES=core,ingest,xray,worker,crawl,paddleocr,mcp,admin \
  docker compose up -d --force-recreate worker mcp crawl4ai
```

## Server Hotfix Rule

Emergency edits on the server are allowed only to restore production. After a
hotfix:

```bash
cd /opt/frontier-intelligence
git diff
git status --short
git add <changed-files>
git commit -m "Hotfix <short description>"
git push
```

Then pull the same commit locally before continuing development.

## Runtime Checks

Useful checks after deploy or hotfix:

```bash
curl http://localhost:8100/healthz
curl http://localhost:8100/tools
docker compose exec -T redis redis-cli XINFO GROUPS stream:posts:reindex
docker compose exec -T redis redis-cli XINFO GROUPS stream:posts:vision
```

Qdrant document search uses `frontier_docs` with dense 2560d vectors plus
sparse BM25; trend cluster search uses `trend_clusters`.
