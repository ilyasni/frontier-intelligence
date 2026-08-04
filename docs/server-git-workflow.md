# Server-First Git Workflow

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **4** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).

Last updated: 2026-05-05.

This project now treats the live server tree as the first git baseline:

```bash
/opt/frontier-intelligence
```

The server currently runs the production Docker stack, has the latest code,
and contains server-only runtime state. Git must track code, docs, templates,
Docker/compose files, scripts, migrations, and tests. Git must not track live
secrets, sessions, database volumes, or local editor/agent tooling.

## Source Of Truth

- Server working tree: canonical baseline for the **initial** git commit (done).
- Git remote (`origin`): shared history after the first push (ongoing).
- Local Windows workspace: rsync-synced development tree. In the current setup, git operations happen on the server unless and until the local workspace is recreated from the remote repository.

Do not treat an outdated local tree as authoritative over `origin`; refresh
from git before large edits or rsync pushes.

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

If you want a full local git checkout, recreate or refresh the local workspace from git:

```powershell
cd D:\Workspace
git clone <private-repo-url> frontier-intelligence-git
```

If reusing the existing local folder, first make a backup, then compare it
against the freshly cloned tree. Do not push the older local tree over the
server baseline.

Current operational flow for this project:

```powershell
# edit locally
.\scripts\sync-push.ps1

# commit / branch / push on the server
ssh frontier-intelligence
cd /opt/frontier-intelligence
git status --short
git commit ...
git push
```

If the local workspace is later converted into a real git checkout, the recommended flow becomes:

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
bash scripts/server-prepare-base-images.sh worker mcp crawl4ai
bash scripts/server-build-stack.sh worker mcp crawl4ai
COMPOSE_PROFILES=core,ingest,xray,worker,crawl,paddleocr,mcp,admin \
  docker compose up -d --force-recreate --wait worker mcp crawl4ai
```

Important: `worker`, `admin`, `mcp`, `ingest`, `crawl4ai`, `paddleocr`, and
`gpt2giga-proxy` bake source code into the image with `COPY`. After source
changes, `docker compose restart <service>` is **not enough**. Rebuild the
image and recreate the container from the updated server tree.

Important: `rsync` pushes the bytes from the local working tree as-is. It does
not re-normalize line endings on the server. Keep server-side `*.sh` scripts in
`LF`, otherwise `bash` on the server can fail with errors such as
`pipefail\r: invalid option name` after sync.

## Workspace Hygiene

Keep the repository root small and predictable.

Allowed in the root:

- project manifests and top-level config (`docker-compose.yml`, `pyproject.toml`, `.env.example`, `.gitignore`);
- short operator docs and project instructions;
- stable source directories (`admin/`, `worker/`, `shared/`, `docs/`, `scripts/`, and so on).

Do not leave ad-hoc artifacts in the root:

- temporary images and smoke-test payloads;
- downloaded vendor bundles or web-inspection dumps such as `wormsoft_*.js`;
- one-off scratch files like `test_write.txt`;
- local `.env` copies.

Temporary investigation files should go under:

```text
tmp/
```

`tmp/` is intentionally excluded from rsync pushes so ad-hoc local artifacts do
not leak into the server tree. In git-managed checkouts you may keep a tracked
placeholder such as `tmp/.gitkeep`, but temporary payloads themselves should
stay untracked.

Server-only backups such as `.env` snapshots should live **outside** the repo tree,
for example under `~/frontier-backups/frontier-intelligence/`, not in
`/opt/frontier-intelligence/`.

If the server has unstable access to Docker Hub, set `PYTHON_BASE_IMAGE` in the
server `.env` to a reachable mirror/internal registry for Python-based
COPY-build services, then rerun the deploy workflow.

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
curl http://localhost:8101/api/health
curl http://localhost:8100/tools
docker compose exec -T redis redis-cli XLEN stream:posts:reindex
docker compose exec -T redis redis-cli XINFO GROUPS stream:posts:reindex
docker compose exec -T redis redis-cli XINFO GROUPS stream:posts:vision
```

Qdrant document search uses `frontier_docs` with dense 2560d vectors plus
sparse BM25; trend cluster search uses `trend_clusters`.
