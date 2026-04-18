# Сервер: AppArmor, сборка, health, PaddleOCR офлайн

Кратко по типичным проблемам на хосте `frontier-intelligence` (Docker + Compose).

## 1. Сборка: `apparmor failed to apply profile` при `RUN` в Dockerfile

**Практика:** отключить BuildKit и старый путь CLI для compose build.

```bash
cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
export COMPOSE_PROFILES=core,ingest,worker,crawl,paddleocr,mcp,admin
```

Дальше либо скрипт:

```bash
bash scripts/server-build-stack.sh worker crawl4ai
bash scripts/server-build-stack.sh   # все перечисленные сервисы с build
```

либо вручную:

```bash
docker compose -f docker-compose.yml build worker crawl4ai
docker compose -f docker-compose.yml up -d --force-recreate worker crawl4ai
```

Если сборка всё ещё падает — на уровне **демона Docker** иногда помогает `default-security-opts` (см. документацию Docker для твоего дистрибутива); это уже администрирование хоста, не репозиторий.

## 2. `docker compose exec` / health «unhealthy» из‑за AppArmor

В репозитории добавлен **`docker-compose.host-fixes.yml`**: для core-сервисов задаётся `security_opt: apparmor:unconfined`.

```bash
docker compose -f docker-compose.yml -f docker-compose.host-fixes.yml up -d
```

**Риск:** ниже уровень изоляции процессов. Используй точечно.

## 3. Healthcheck в основном `docker-compose.yml`

- **PostgreSQL:** `pg_isready` с `$${POSTGRES_USER}` / `$${POSTGRES_DB}` — переменные разворачиваются **внутри контейнера** (Compose), а не только из `.env` на этапе парсинга одной строки.
- **Qdrant:** образ без `curl`/`wget` в финальном слое; цепочка `curl || wget || bash /dev/tcp` + entrypoint образа на **bash**.
- **gpt2giga-proxy:** проверка **liveness** через `socket.connect` к `8090`, без вызова GigaChat (иначе при недоступности API контейнер помечался бы unhealthy).
- **Neo4j:** `127.0.0.1`, цепочка `wget || curl || bash /dev/tcp`, увеличен `start_period`.

После правок пересоздай контейнеры, чтобы подтянуть новый healthcheck:

```bash
docker compose -f docker-compose.yml up -d --force-recreate postgres redis qdrant neo4j gpt2giga-proxy
```

(при необходимости добавь второй `-f docker-compose.host-fixes.yml`).

## 4. PaddleOCR без доступа к `paddleocr.bj.bcebos.com`

1. Один раз скачай веса на машине с доступом к BCE (или из уже работающего контейнера с сетью), скопируй каталоги в volume **`paddleocr_models`** (в compose смонтирован в `/root/.paddleocr`).
2. В **`.env` на сервере** задай пути (пример в `.env.example`):

   - `LOCAL_OCR_DET_MODEL_DIR`
   - `LOCAL_OCR_REC_MODEL_DIR`
   - `LOCAL_OCR_CLS_MODEL_DIR`

3. `docker compose --profile core --profile paddleocr up -d paddleocr`

4. **`GET /healthz`** — liveness (процесс жив). **`GET /readyz`** — readiness: **200** только после успешной загрузки весов; иначе **503** (`models_not_loaded` или текст ошибки). Docker healthcheck в compose бьёт в `/readyz`; при `LOCAL_OCR_PRELOAD=false` до первого OCR контейнер может оставаться not ready после `start_period`.

API при ошибке инференса отдаёт **503** `model_unavailable` или **500** `ocr_failed` (см. `services/paddleocr/app/server.py`).

## 5. Деплой правок кода (локальный репо ≠ live)

Если в логах worker всё ещё «fastembed not available», а в репозитории уже есть `fastembed` в `worker/Dockerfile`, значит **на сервере крутится старый образ**. Нужны **sync кода** и **пересборка** затронутых сервисов.

**Практика (Docker Compose):** `docker compose build [SERVICE…]` пересобирает образы; `up -d --force-recreate` поднимает контейнеры с новым образом ([документация Compose](https://github.com/docker/compose/blob/main/docs/reference/compose_up.md) — recreation при смене конфигурации/образа).

```bash
cd /opt/frontier-intelligence
# после rsync / git pull
bash scripts/server-deploy-rebuild.sh
```

Вручную (эквивалент):

```bash
export COMPOSE_PROFILES=core,worker,mcp,crawl,paddleocr
docker compose -f docker-compose.yml build --pull worker mcp crawl4ai paddleocr
docker compose -f docker-compose.yml up -d --force-recreate worker mcp crawl4ai paddleocr
```

Проверки:

- `curl -sS --max-time 15 -w '\nHTTP %{http_code}\n' http://127.0.0.1:8008/readyz` — **не должно зависать**: при preload в фоне сначала часто **503** `not_ready`, после загрузки весов — **200**.
- worker: в логах нет строки `fastembed not available` (или реже один раз при старте до импорта — смотри полный стартовый лог).
- crawl4ai: при stale pool в логах есть `crawl4ai DB save stale connection, dispose pool and retry`.
- `curl -sS "http://127.0.0.1:8101/api/pipeline/stats"` и `curl -sS "http://127.0.0.1:6333/collections/frontier_docs"`.

## 6. Проверки после деплоя

- `bash scripts/server_checks.sh`
- `bash scripts/verify_worker_crawl_images.sh` — образ не старше файлов в `/opt/frontier-intelligence/…`
- `bash scripts/server-ensure-searxng-secret.sh` — если впервые поднимается SearXNG или менялся `searxng/settings.yml`.

## 6.1. Security exposure check before git/publish

На текущем сервере часть сервисов проброшена на `0.0.0.0`: `admin:8101`, `mcp:8100`, `mcp-gateway:8102`, `gpt2giga-proxy:8090`, `grafana:3000`, `prometheus:9090`, `alertmanager:9093`, `qdrant:6333`, `neo4j:7474/7687`, `paddleocr:8008`.

Это нормально только для закрытого LAN/VPN-контура. Перед публикацией репозитория или открытием хоста наружу:

- проверь firewall/security group на сервере;
- не публикуй Admin/MCP/Qdrant/Neo4j/Grafana напрямую в интернет без VPN/reverse-proxy auth;
- держи `.env`, `sessions/`, `searxng/settings.yml` и дампы БД вне git и rsync;
- после изменения `searxng/settings.example.yml` не затирай боевой `searxng/settings.yml` — он server-local.

## 7. GigaChat rollout: frequent issues

- `No such model` in `worker`:
  check `curl http://127.0.0.1:8090/v1/models` and do not keep unsupported model aliases in env. On the current server contour the safe baseline is `GigaChat-2`.
- `/tokens/count -> 404`:
  current `gpt2giga-proxy` may not support the endpoint. Token counting must degrade to best-effort without breaking enrichment.
- `AsyncOpenAI.copy() got an unexpected keyword argument 'extra_headers'`:
  the current `openai==1.54.4` plus proxy pair may reject `extra_headers`. Session cache should be considered optional and auto-disabled after the first incompatibility.
- Frequent `429 Too Many Requests`:
  keep `INDEXING_MAX_CONCURRENCY=1` on this contour and raise only after observing stable live traffic.

## 8. HTTP sources require VPN / proxy

Some external HTTP sources are reachable only through the server VPN contour.

Current production example:

- `rss_medium_*` must use `proxy_config={"type":"socks5","host":"xray","port":10808}`
- `ingest` must be rebuilt with the `xray` profile, not only `core`

Safe rebuild path:

```bash
cd /opt/frontier-intelligence
bash scripts/server-build-ingest-fix.sh
```

If a source works without proxy locally but fails on the server with network or reachability errors, first test the same source through `xray` before changing parser logic.

## 9. `source_runs` stuck in `running`

Historical `running` rows can appear after hard restarts or container recreation.

Current behavior:

- stale `running` rows older than 180 minutes are closed automatically at ingest startup
- when a source starts a new run, any older unfinished `running` rows for the same source are closed automatically

If you still need a manual check:

```sql
SELECT status, COUNT(*)
FROM source_runs
GROUP BY status
ORDER BY status;
```

## 10. Feed HTML breaks dashboard tables

Symptom:

- dashboard or pipeline tables render broken rows
- previews contain raw HTML fragments from RSS bodies, most commonly from Medium-style feeds

Current fixes:

- ingest normalizes RSS HTML to plain text before storing new posts
- admin backend strips tags in preview fields
- admin frontend escapes preview rendering

If historical rows are still dirty, use the maintenance script:

```bash
python scripts/backfill_posts_plaintext.py --workspace-id disruption --source-like 'rss_medium_%'
```

Or do a targeted SQL cleanup for the affected source family if a one-off server-side fix is faster.

## 11. Replay a bad pipeline window after `402 Payment Required`

If GigaChat balance was exhausted for a short interval and some posts were incorrectly written as `dropped`, replay the exact time window from the workstation:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\server-reprocess-window.ps1 `
  -Server frontier-intelligence `
  -StartUtc 2026-04-01T01:15:00Z `
  -EndUtc 2026-04-01T01:30:00Z `
  -DryRun
```

Then run the same command without `-DryRun`.

Notes:

- The script selects posts by `indexing_status.updated_at` and `embedding_status`.
- Default status is `dropped`; keep it narrow unless you have a reason to replay a broader class.
- Use UTC boundaries to avoid timezone confusion during incident review.
