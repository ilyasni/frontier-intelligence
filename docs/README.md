# Frontier Intelligence

> **Переписан 2026-08-04 против работающего стека.** Аудит нашёл в прежней редакции
> 35 расхождений — неверные схемы таблиц, мёртвые имена коллекций Qdrant, несуществующие
> метки и связи Neo4j, выключенные xray-профили, несуществующие профили compose,
> реактивный пайплайн без единой строки продюсера. Разделы про БД, Qdrant, Neo4j,
> Current Runtime, Workspace, MCP Tools, Processing Pipeline, Admin UI, статус, архитектуру,
> профили compose и структуру репозитория переписаны по коду и проверены на живом сервере.
>
> Там, где прежний текст врал, стоит пометка-предупреждение — чтобы читатель, помнящий
> старую версию, понимал, что изменилось. Метод и полный разбор:
> [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).
>
> **Принцип, принятый при переписывании:** схемы БД, деревья каталогов и списки файлов
> здесь больше не дублируются — только назначение плюс указание, где смотреть источник
> правды и какой командой свериться. Копия расходится с оригиналом всегда, вопрос лишь
> в сроке.

## Recent Docs

- **Аудит документации и стека 2026-08-04** — что чему не соответствовало, маршрут работ
  (заходы 0–5) и журнал принятых решений: [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).
  Разделы этого файла про БД, Qdrant, Neo4j, xray, профили compose, MCP-инструменты,
  Trend Detection, Admin UI и структуру репозитория переписаны по коду в тот же день.
- **Незаконченный функционал** — 52 пункта с доказательствами:
  [TODO-UNFINISHED.md](./TODO-UNFINISHED.md).
- **RSI — рекурсивное самоулучшение детектора** (Фаза 0 + контуры A/B/C/D/D+, связка B→A, человек-гейты, кроны, MCP-инструменты, метрики/алерты): [rsi.md](./rsi.md) (2026-06-26).
- Rollout summary for the multi-connector production upgrade:
  [CHANGELOG-2026-03-28.md](./CHANGELOG-2026-03-28.md)
- Source connector model, runtime tables, proxy rules, and live starter bundle:
  [source-connectors-runbook.md](./source-connectors-runbook.md)
- Server-side operational notes and common container issues:
  [ops-server-troubleshooting.md](./ops-server-troubleshooting.md)
- End-to-end pipeline verification checklist:
  [pipeline-e2e-checklist.md](./pipeline-e2e-checklist.md)
- Security and first-git preflight:
  [security-git-preflight.md](./security-git-preflight.md)
- Server-first git workflow:
  [server-git-workflow.md](./server-git-workflow.md)
- Remaining operational follow-ups for the multi-LLM rollout:
  [TODO_MULTI_LLM_ROLLOUT.md](./TODO_MULTI_LLM_ROLLOUT.md)
- Единый аудит оркестратора LLM (as-is/to-be, best practices, roadmap):
  [llm-orchestrator-audit.md](./llm-orchestrator-audit.md)
- Operational runbook по классам LLM-alerts (trigger/diagnosis/action/rollback):
  [runbooks/llm-orchestrator-alerts.md](./runbooks/llm-orchestrator-alerts.md)
- SLI/SLO для text/vision/embeddings и error-budget эскалации:
  [sre/llm-orchestrator-sli-slo.md](./sre/llm-orchestrator-sli-slo.md)
- Шаблон еженедельного audit snapshot (включая delivery и post-deploy verification):
  [audit/llm-orchestrator-weekly-snapshot.md](./audit/llm-orchestrator-weekly-snapshot.md)
- Обязательный Context7-gate для policy/routing изменений:
  [llm-orchestrator-context7-gate.md](./llm-orchestrator-context7-gate.md)
- Runtime mode overlays:
  [runtime-modes.md](./runtime-modes.md)
- Workspace hygiene and temporary-file policy live in:
  [server-git-workflow.md](./server-git-workflow.md)

## Current Runtime

- **LLM — `openai` SDK напрямую** (`openai==1.54.4`). Ни LangChain, ни gigachain в проекте
  нет и вводить их не планируется: `worker/gigachat_client.py:1` прямо документирует отказ.
  Маршрутизация — собственный control plane (`shared/llm_routing.py`,
  `shared/llm_control_plane.py`) поверх четырёх провайдеров: wormsoft, polza, openrouter, gigachat.
- **Живая таблица маршрутов — `admin_runtime_settings.llm_routing_v1`**, hot-reload без
  пересборки. Значения `GIGACHAT_MODEL_*` из `.env` — только основа, поверх которой работает
  роутер; читать их как исчерпывающее описание продакшн-маршрутизации нельзя.
- **GigaChat больше не обслуживает основную цепочку обогащения.** `relevance`, `concepts`,
  `valence` ушли на wormsoft (фолбэки — polza и gigachat). За GigaChat остались эмбеддинги
  (`EmbeddingsGigaR`, режим strict) и `mcp_synthesis`. Ходит он через `gpt2giga-proxy` на 8090.
- `GigaChat-2-Lite` не использовать в рантайме, пока он не появится в `GET /v1/models`
  на активном прокси.
- `X-Session-ID` и `/tokens/count` — best-effort оптимизации. Если пара SDK/прокси их
  отвергает, конвейер обязан продолжать работать без кэша и подсчёта токенов.
- **`INDEXING_MAX_CONCURRENCY=4.**` Значение поднимали 1 → 2 → 4 в июле, расшивая
  пропускную способность enrichment. Прежняя рекомендация «держать 1 на этом контуре»
  недействительна.
- **Egress наружу только через xray.** Прямого выхода в интернет у хоста нет:
  `api.telegram.org` с хоста отвечает `Network is unreachable`, работает socks5
  `xray:10808` — и это имя резолвится только внутри docker-сети.

### XRAY: реестр профилей

Продакшн использует реестр профилей, а не единственный `XRAY_VLESS_*`. Файлы на сервере:

```
/opt/frontier-intelligence/runtime/xray-profiles.json
/opt/frontier-intelligence/runtime/xray-active-profile.txt
/opt/frontier-intelligence/runtime/xray-previous-profile.txt
/opt/frontier-intelligence/runtime/xray-reload.trigger
```

> **Порядок отказоустойчивости здесь намеренно не перечислен.** Он меняется при каждом
> переключении, и зафиксированный в документе список немедленно устаревает: на 04.08.2026
> все три профиля, перечисленные в прежней редакции как активная цепочка, были
> **выключены**, а работал не упомянутый в ней вовсе `profile_e_tls_tm_ws`.
> Смотреть текущее состояние надо в реестре:
> ```bash
> ssh frontier-intelligence \
>   "python3 -c \"import json;[print(p['name'], p['enabled'], p['priority']) \
>    for p in json.load(open('/opt/frontier-intelligence/runtime/xray-profiles.json'))['profiles']]\""
> ```
> Выбирается включённый профиль с наименьшим `priority`.

### Быстрая проверка HTTP (с хоста)

Единого API на `:8000` в текущем compose **нет**.

```bash
curl -sS http://127.0.0.1:8100/healthz         # MCP
curl -sS http://127.0.0.1:8101/api/health      # Admin
curl -sS http://127.0.0.1:8100/metrics/        # метрики MCP; БЕЗ слэша отдаёт 307
```

Прочее: PaddleOCR `http://127.0.0.1:8008/readyz`, Prometheus `9090`, Alertmanager `9093`,
Grafana `3000`, Qdrant `6333`, Neo4j `7474`.

> **`401` на `/api/*` — это норма, а не поломка.** Все эндпоинты admin, кроме
> `/api/health` и `/api/auth/login`, закрыты cookie/Basic-авторизацией. Прежняя редакция
> предлагала блок команд `curl` по `/api/monitoring/xray/*` как рабочую процедуру —
> в написанном виде он не выполняется ни одной строкой. Управлять профилями xray
> и запускать проверки нужно из авторизованной сессии админки либо с явными
> учётными данными.

### Управление xray через Admin API

Все вызовы требуют авторизации (см. выше). Эндпоинты:

```
GET  /api/monitoring/xray/health              текущее здоровье
POST /api/monitoring/xray/health/run          прогнать проверку сейчас
GET  /api/monitoring/xray/health/history      история
GET  /api/monitoring/xray/profiles            реестр профилей
GET  /api/monitoring/xray/remediation/history история переключений
POST /api/monitoring/xray/remediate/switch    переключить на профиль
POST /api/monitoring/xray/remediate/rollback  вернуть предыдущий
```

Здоровье двухслойное: `transport` — общие пробы через `socks5://xray:10808`,
`source_smoke` — реальные URL источников, которые важны для приёма. Автоматическое
переключение локально для `admin` и срабатывает только после настроенной серии
деградаций и с учётом cooldown.

### Urgent Trend Alerts

Daily digests are intentionally disabled. The admin scheduler only sends Telegram alerts for rare, confirmed stable trend spikes:

- `ADMIN_TREND_ALERT_CRON=25 * * * *` checks once per hour.
- `TREND_ALERT_MIN_SIGNAL_SCORE=0.80`, `TREND_ALERT_MIN_DOC_COUNT=5`, `TREND_ALERT_MIN_SOURCE_COUNT=3` select strong confirmed clusters.
- `TREND_ALERT_MAX_PER_7D=2` caps delivery to roughly 0-2 urgent messages per week.
- `trend_alerts` in PostgreSQL stores sent alerts and deduplicates by `workspace_id + cluster_key + alert_kind`.
- Manual check: `POST http://127.0.0.1:8101/api/pipeline/run-urgent-trend-alerts?dry_run=true`
  — **требует авторизации**, голый `curl` вернёт `401 {"detail":"unauthorized"}`.

С 04.08.2026 у алертинга два независимых пути доставки: штатный вебхук в `admin`
и прямой receiver `telegram-direct` в Alertmanager, который ходит в Telegram сам.
Критические правила идут обоими сразу, поэтому падение `admin` больше не глушит
сообщение о падении `admin`. Плюс dead man's switch: правило `FrontierWatchdog`
и внешний наблюдатель `scripts/alert-watchdog.sh` в cron.

---

Персональная система мониторинга и синтеза трендов.  
Собирает сигналы из множества источников, обогащает через собственный мульти-провайдерный
LLM-роутер (wormsoft / polza / openrouter / GigaChat), хранит в векторной базе и графе знаний,
отдаёт через MCP-шлюз в Claude Code и Claude Desktop.

> **Один сервис — несколько рабочих пространств (workspace).**  
> Клиент подключается к одному MCP-шлюзу, но работает со своим изолированным срезом данных.  
> Настройки — источники, категории, веса, расписания — правятся через Admin UI,
> с оговоркой про bootstrap из YAML (см. раздел про Admin UI).

---

## Концепция Workspace

Один стек, **шесть** изолированных пространств. Источник правды — `config/workspaces.yml`
(и таблица `workspaces` в БД, куда его накатывает bootstrap).

| workspace | Тематика | Потребитель |
|---|---|---|
| `disruption` | automotive, HMI, future mobility, EV | Claude Project: visionary-designer |
| `ai_trends` | LLM, агенты, AI-инструменты, инференс | внешний MCP-клиент ai-researcher (опционально) |
| `ai_research` | исследовательский контур AI | — |
| `ai_products_media` | AI-продукты и медиа | — |
| `design` | дизайн-системы, UX-паттерны, визуальная культура | внешний MCP-клиент design-director (опционально) |
| `auto_hmi` | автомобильный HMI (заведён 03.08.2026) | наполняется: включён 1 источник из 10 |

> Прежняя редакция перечисляла три пространства и называла потребителя «Codex Project».
> И то и другое неверно: пространств шесть, а MCP отдаётся HTTP-шлюзом на `:8102`
> и прописан в Claude Code и Claude Desktop.

**Изоляция на уровне данных, а не инфраструктуры:**
- каждый документ, тренд и концепт помечен `workspace_id`;
- Qdrant — **одна коллекция на модель эмбеддингов**, изоляция payload-фильтром
  по `workspace_id` при каждом запросе (не отдельная коллекция на пространство);
- Neo4j: `(:Workspace)` — узел-корень пространства;
- PostgreSQL: `workspace_id TEXT NOT NULL` во всех основных таблицах.

**Cross-workspace сигналы.** Поле `workspaces.cross_workspace_bridges` заполнено у пяти
пространств из шести, пишется тремя путями и редактируется в админке.

> **Но потребителя у него нет.** Ни один поисковый инструмент мостами выдачу не расширяет:
> `list_workspaces` и `get_workspace_overview` только показывают значение,
> `get_frontier_brief` берёт строго переданный список пространств. Утверждение
> «тренд из `ai_trends` может попасть в `disruption`, если релевантен обоим»
> описывает замысел, а не поведение. В `auto_hmi` поле сознательно оставлено пустым
> с комментарием «их не читает никто».

## Статус реализации

Снимок на 04.08.2026, сверен с работающим стеком. Столбец «Предел» обязателен: почти всё
в этой таблице работает не «вообще», а до конкретной границы, и именно граница устаревает
первой. Числа по корпусу (сколько постов, кластеров, источников) намеренно не переносятся
в таблицу — они меняются ежедневно; под таблицей лежат команды, которыми любое такое
утверждение пересчитывается за минуту.

| Компонент | Состояние | Предел — где именно кончается «работает» |
|---|---|---|
| Core infra: postgres, redis, qdrant, neo4j, gpt2giga-proxy | Работает | Профиль `core`. Все 18 сервисов объявлены с `profiles:`, без `COMPOSE_PROFILES` голый `up` поднимает ноль |
| Ingest: `telegram`, `rss`, `web`, `api` | Работает | Живых типов четыре. `email` объявлен в `CANONICAL_SOURCE_TYPES`, но источников этого типа в БД нет. `habr` — deprecated-алиас, а не тип |
| Ingest: ротация Telegram-аккаунтов | Работает частично | Слотов два (`TG_API_ID_0/1`), авторизованная сессия одна — `sessions/account_0.session`. Переключаться при FloodWait фактически некуда |
| Worker: relevance → concepts → valence → embeddings → Qdrant | Работает | Hybrid dense 2560d + BM25 sparse (fastembed). Коллекция одна на модель эмбеддингов, изоляция воркспейсов — payload-фильтром, не отдельными коллекциями |
| Worker: Neo4j concept graph | Работает на запись | Инлайн-`MERGE` из enrichment; отдельного `graph_task` в `worker/tasks/` нет. Ни `DELETE`, ни `REMOVE` в `worker/integrations/neo4j_client.py` не встречается; единственная операция, убирающая узлы, — `apoc.refactor.mergeNodes` в `merge_duplicate_entities`, а она вызывается только при `apply=True`, тогда как плановое `run_graph_maintenance_job` идёт с `apply=False`. Дропнутый пост исчезает из Qdrant, но остаётся в графе |
| Worker: vision (S3, GigaChat Vision, PaddleOCR) | Работает на телеграм-корпусе | `media_urls` заполняет только `telegram_source`; rss/web/api его не заполняют. Отдельного album assembler нет — альбомы схлопываются в ingest (`telegram_source`), строку `media_groups` заводит enrichment, vision её дозаполняет (`assembled`, `vision_summary_s3_key`) |
| Semantic clusters | Кластеры есть во всех шести воркспейсах | Наполнение несопоставимое: от десятков тысяч в `disruption` до одного кластера в `auto_hmi`. Алгоритм — связные компоненты графа косинусной близости (`_connected_components`, DFS), не HDBSCAN |
| Stable trend clusters | Работает, но наполняется практически только `disruption` | В четырёх остальных воркспейсах — единицы кластеров (2–6 на каждый), у `design` последний `detected_at` — 14.07; в `auto_hmi` не создано ни одного. PostgreSQL канонический, Qdrant `trend_clusters_active` — вторичный векторный индекс |
| Emerging signals | Работает | Считаются в том же прогоне `signal_analysis`, что и stable-кластеры |
| Missing signals | Работает по расписанию | Gap-анализ вызывается внутри `run_signal_analysis` (cron `20 */8 * * *`), а не отдельным заданием: у самостоятельного `run_missing_signals` расписания нет, он только ручной из админки. Ошибка gap-анализа гасится `try/except` и даёт пустой список, не срывая прогон |
| Urgent trend alerts | Работает | Только Telegram, только подтверждённые всплески stable-трендов, cron `25 * * * *`, отбор по `signal_stage='stable'` + `has_recent_change_point`, два потолка — `TREND_ALERT_MAX_PER_RUN` и `TREND_ALERT_MAX_PER_7D` (оба по 2). Ежедневного дайджеста нет |
| Admin UI: workspaces, sources, pipeline, search, clusters, graph, media | Работает | 11 роутеров в `admin/backend/routers/`, все подключены в `admin/backend/main.py` (сверка: `grep -c include_router admin/backend/main.py`). Всё, кроме `/api/health`, закрыто cookie/Basic-авторизацией: голый `curl` получает `401 {"detail":"unauthorized"}` |
| MCP: поиск, наблюдаемость, кластеры, здоровье источников, `ingest_url`, редакторская обратная связь | Работает | REST-слой на `mcp:8100` (loopback), MCP-шлюз на `:8102` — Streamable HTTP. Шлюз опубликован наружу и не имеет аутентификации вообще |
| Prometheus + Grafana | Работает | Два дашборда: `frontier-runtime.json`, `frontier-rsi.json`. Алерты — `prometheus/alerts.yml` + textfile-коллектор |
| crawl4ai, SearXNG, PaddleOCR, xray | Работают как вспомогательные | crawl4ai читает `stream:posts:crawl` (внешние ссылки из постов), а не забирает web-источники: те тянет ingest через httpx + BeautifulSoup |

Четыре утверждения прошлой версии были неверны — если они помнятся, это не аберрация памяти:

> **`habr` не тип источника.** `shared/source_definitions.py:12` держит его как
> `deprecated alias kept for compatibility` и канонизирует в `rss`;
> `ingest/sources/habr_source.py` — класс без тела (`class HabrSource(RSSSource): pass`);
> строк с `source_type='habr'` в БД ноль. Зато в прежнем списке отсутствовал реально
> работающий тип `api` — на нём висят коннекторы Hacker News, дающие второй по объёму
> корпус после rss.

> **Шлюз на `:8102` — не SSE.** `mcp/mcp_gateway.py:1` — «Streamable HTTP транспорт»,
> запуск `mcp.run(transport="streamable-http")`, эндпоинт `/mcp`. Клиент, настроенный
> на SSE, не подключится.

> **Vision обслуживает малую долю корпуса.** `media_urls` в событие кладёт только
> `ingest/sources/telegram_source.py`; у rss, web и api постов с `has_media` — ноль.
> На 04.08.2026 vision пройден примерно у 1,6% постов, и это потолок конструкции,
> а не отставание очереди.

> **Стабильные тренды — это про `disruption`.** Из 393 строк `trend_clusters` 376 лежат
> в `disruption`; в `design` последний кластер задетектирован 14.07, в `ai_trends` — 28.07.
> Утверждение «работает» верно для механики, но не для наполнения остальных пространств.

**Чем сверить, не полагаясь на этот текст.** Все запросы читающие:

```bash
ssh frontier-intelligence
cd /opt/frontier-intelligence
export COMPOSE_PROFILES="core,ingest,xray,worker,crawl,paddleocr,mcp,admin,searxng,monitor"

# какие типы источников живы и сколько включено
docker compose exec -T postgres psql -U frontier -d frontier -c \
  "SELECT source_type, count(*) total, count(*) FILTER (WHERE is_enabled) enabled
     FROM sources GROUP BY 1 ORDER BY 2 DESC;"

# доля постов с медиа по типам — потолок vision
docker compose exec -T postgres psql -U frontier -d frontier -c \
  "SELECT s.source_type, count(*) posts, count(*) FILTER (WHERE p.has_media) with_media
     FROM posts p JOIN sources s ON s.id = p.source_id GROUP BY 1 ORDER BY 2 DESC;"

# где на самом деле формируются стабильные тренды
docker compose exec -T postgres psql -U frontier -d frontier -c \
  "SELECT workspace_id, count(*), max(detected_at) FROM trend_clusters GROUP BY 1 ORDER BY 2 DESC;"

# насколько неравномерно наполнены воркспейсы
docker compose exec -T postgres psql -U frontier -d frontier -c \
  "SELECT workspace_id, count(*) FROM semantic_clusters GROUP BY 1 ORDER BY 2 DESC;"

# стадии индексации, включая vision
docker compose exec -T postgres psql -U frontier -d frontier -c \
  "SELECT vision_status, count(*) FROM indexing_status GROUP BY 1 ORDER BY 2 DESC;"
```

Источники правды, которые тут сознательно не продублированы: перечень воркспейсов —
`config/workspaces.yml`, перечень источников — таблица `sources` (в `config/sources.yml`
лежит её частичное зеркало), маршруты LLM — `admin_runtime_settings.llm_routing_v1`,
расписания заданий — дефолты полей `admin_*_cron` в `shared/config.py`.

---

## Архитектура

```
┌─── SOURCES — контейнер ingest ───────────────────────────────────────────┐
│                                                                          │
│ telegram      rss / atom      web                api                     │
│ Telethon      feedparser      httpx + bs4        JSON/CSV + cursor       │
│ MTProxy       RSS-пресеты     селекторы в        только Hacker News      │
│ / SOCKS5                      source.extra                               │
│                                                                          │
│ Часть источников ходит наружу через xray (sources.proxy_config).         │
│ Расписание опроса — cron в config/sources.yml и в таблице sources.       │
└──────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
             Redis Stream: stream:posts:parsed
             {content, source_id, workspace_id, media_urls, linked_urls}
                                 │
┌─── PROCESSING — контейнер worker, три консьюмера в одном процессе ───────┐
│                                                                          │
│  ┌── EnrichmentTask ← stream:posts:parsed ────────────────────────────┐  │
│  │ relevance → concepts → valence — три отдельных LLM-вызова:         │  │
│  │ совместный relevance_concepts есть в коде, но выключен             │  │
│  │ (GIGACHAT_RC_JOINT_ENABLED=false)                                  │  │
│  │ → embed (EmbeddingsGigaR, 2560d) → Qdrant upsert                   │  │
│  │ → Neo4j upsert_concepts — инлайн, только запись                    │  │
│  │ → posts.tags — производное от concepts (weight >= 3), не вызов     │  │
│  │                                                                    │  │
│  │ LLM: свой роутер поверх openai SDK. Основной провайдер             │  │
│  │ wormsoft, фолбэки polza и GigaChat. Живая таблица маршрутов —      │  │
│  │ admin_runtime_settings.llm_routing_v1, hot-reload.                 │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌── VisionTask ← stream:posts:vision ────────────────────────────────┐  │
│  │ S3 → GigaChat Vision (labels, scene, design signals)               │  │
│  │ → PaddleOCR → post_enrichments(kind='vision'), posts.vision_labels │  │
│  │ → событие в stream:posts:reindex — в Qdrant пишет ReindexTask,     │  │
│  │   сам VisionTask Qdrant не трогает                                 │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌── ReindexTask ← stream:posts:reindex ──────────────────────────────┐  │
│  │ продюсеры события: vision_task (reason=vision), crawl4ai           │  │
│  │ (reason=crawl), ops-скрипт enqueue_reindex_enriched_posts.py.      │  │
│  │ Кнопка «reprocess» в админке шлёт пост не сюда, а заново           │  │
│  │ в stream:posts:parsed                                              │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
        │                  │                        │
        │                  │                        ▼
        │                  │      stream:posts:crawl → контейнер crawl4ai
        │                  │      (внешние ссылки, найденные в постах)
        │                  ▼
        │      stream:posts:enriched — консьюмерских групп нет
        ▼
┌─── ANALYTICS — APScheduler в контейнере admin, задания в субпроцессах ───┐
│                                                                          │
│ 35 3 * * *     semantic clustering — связные компоненты графа            │
│                косинусной близости (не HDBSCAN)                          │
│ 20 */8 * * *   signal analysis: stable trend clusters + emerging         │
│                signals + missing signals (SearXNG gap-анализ)            │
│ 25 * * * *     urgent trend alerts → Telegram                            │
│ раз в сутки    retrospective, novelty judge, relevance audit,            │
│                graph maintenance, entity resolution — контуры RSI        │
│                                                                          │
│ Плюс служебные джобы: source scores (17 */6), обновление лимитов         │
│ провайдеров, xray health. Полный список — admin/backend/scheduler.py.    │
│                                                                          │
│ Расписания — дефолты shared/config.py, в .env не переопределены.         │
└──────────────────────────────────────────────────────────────────────────┘
        │
   ┌────┴──────────┬────────────────┬──────────────────┐
   ▼               ▼                ▼                  ▼
 Qdrant          Neo4j          PostgreSQL       S3 / Cloud.ru
 dense 2560d     Concept        23 таблицы       медиа-файлы
 + BM25 sparse   Document       posts, кластеры, + vision_summary
 одна коллекция  Workspace      enrichments,     (gzip)
 на модель,      MENTIONS       indexing_status
 алиасы          RELATED_TO
 *_active,       CONTAINS
 фильтр по       ECHO_OF
 workspace_id
   │               │                │
   └───────────────┴────────────────┴──────────────────┐
                                                       ▼
┌─── MCP ──────────────────────────────────────────────────────────────────┐
│                                                                          │
│ mcp:8100       REST-слой, ~32 POST-роута (mcp/tools/*.py)                │
│ mcp-gateway    Streamable HTTP на /mcp, 22 инструмента —                 │
│ :8102          обёртки поверх REST. Аутентификации нет.                  │
│                                                                          │
│ Инструменты workspace-aware; синтез внутри части из них идёт             │
│ маршрутом mcp_synthesis (сейчас GigaChat-2-Pro).                         │
└──────────────────────────────────────────────────────────────────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
            Claude Code                   Claude Desktop
        http://<host>:8102/mcp        http://<host>:8102/mcp

  Воркспейсов шесть: disruption, ai_trends, ai_research,
  ai_products_media, design, auto_hmi — config/workspaces.yml
```

Чего на схеме нет и не должно быть:

> **Клиенты — Claude, а не Codex.** Прежняя версия заканчивалась тремя «Codex Project»
> и тремя воркспейсами. Рабочих клиентов два — Claude Code и Claude Desktop, оба ходят
> в шлюз `:8102`; воркспейсов шесть (`config/workspaces.yml`). Шлюз слушает LAN-адрес,
> поэтому из проектов на claude.ai он недоступен принципиально.

> **LangChain в стеке нет.** Прежняя схема подписывала PROCESSING как
> «LangChain + GigaChat via gpt2giga-proxy», а цепочку — как `RunnableBranch`.
> Ни `langchain`, ни `gigachain` не ставит ни один Dockerfile стека и ни один
> `requirements*.txt` — в коде остались только упоминания словом (комментарий
> в `worker/gigachat_client.py`, устаревшие `.cursor/rules/`);
> обращения к моделям идут через `openai` SDK и собственный роутер
> (`shared/llm_routing.py`, `shared/llm_control_plane.py`). GigaChat остался фолбэком
> и обслуживает эмбеддинги и `mcp_synthesis`, основная цепочка обогащения — на wormsoft.

> **HDBSCAN, community detection и album assembler не реализованы.** Кластеризация —
> связные компоненты графа косинусной близости; в Neo4j нет ни Louvain/Leiden,
> ни иерархических summary, поэтому подпись «GraphRAG community detection» описывала
> замысел; альбомы схлопываются прямо в ingest, отдельного четырёхфазного сборщика нет.

> **«Reactive-пайплайн каждые 30 минут» не существует.** Столбец `trend_clusters.pipeline`
> принимает единственное значение `stable`; `burst_score` — поле внутри того же прогона,
> а не отдельный контур. Реальные каденции — на схеме.

Один хвост, который видно только по Redis: `stream:posts:enriched` пишется на каждом
успешном обогащении, но консьюмерских групп у него нет (`XINFO GROUPS` возвращает пусто).
Поток существует как точка расширения и как удобный след для отладки, потребителя у него
сейчас нет.

## Технологический стек

| Компонент | Образ / Библиотека | Роль |
|---|---|---|
| **LLM-роутинг** | `openai==1.54.4` + свой control plane | Маршрутизация по wormsoft / polza / openrouter / gigachat, таблица маршрутов в `admin_runtime_settings.llm_routing_v1` |
| **gpt2giga-proxy** | локальная сборка, порт 8090 | GigaChat через OpenAI-совместимый интерфейс: эмбеддинги и `mcp_synthesis` |
| **Redis** | redis:7-alpine | Event bus (Streams), кэш эмбеддингов TTL 7д |
| **PostgreSQL** | postgres:16-alpine | 23 таблицы: посты, обогащения, кластеры, контур управления |
| **Qdrant** | qdrant/qdrant:v1.17.0 | Hybrid search: dense 2560 Cosine + sparse BM25, доступ через алиасы |
| **Neo4j** | neo4j:5.15-community | Граф ко-встречаемости концептов |
| **S3 / Cloud.ru** | boto3, path-style | Медиафайлы, vision summaries gzip, ночные бэкапы |
| **crawl4ai** | локальная сборка | Web-краулинг источников |
| **SearXNG** | searxng/searxng | Self-hosted поиск для missing signals |
| **Prometheus + Grafana** | standard | Метрики и дашборды, Alertmanager с двумя путями доставки |
| **xray** | локальная сборка | Единственный egress наружу (socks5 `xray:10808`) |
| **Admin UI** | FastAPI + Vue 3 (CDN, без сборки) | Управление: workspace, источники, темы, FinOps |

> **Чего в стеке нет, вопреки прежней редакции.** Ни LangChain, ни gigachain не установлены
> ни в одном образе — `worker/gigachat_client.py:1` прямо документирует отказ от них.
> Neo4j используется как граф ко-встречаемости: **NER/NEL и community detection
> не реализованы** (упоминаний `gds.`, Louvain или Leiden в коде нет).
> Фронт админки — Vue 3 с vue-router, а не Vanilla JS.

**Не используется из telegram-assistant:**
Supabase стек (kong/postgrest/studio/meta), Caddy, JWT/QR-auth, Mini App,
мультитенантность (RLS), SaluteSpeech.

> Telegram Bot API — **исключение**: он используется в проде для доставки алертов
> (`admin/backend/services/telegram_alerts.py` и прямой receiver Alertmanager).
> Запрет касается бота как пользовательского интерфейса, а не как транспорта уведомлений.

---

## Admin UI — центр управления

Сервис `admin` — FastAPI-бэкенд плюс SPA на **Vue 3 + vue-router**. Библиотеки лежат
в репозитории (`admin/frontend/vendor/vue.global.prod.js`, `vue-router.global.prod.js`),
подключаются обычными `<script>`, сборки нет: экраны — нативные ES-модули с ленивым
`() => import()`. Порт 8101 опубликован на `0.0.0.0`.

> **Слово «CDN» в таблице стека выше — неточность: из сети ничего не тянется.**
> `admin/frontend/index.html` подключает `/static/vendor/vue.global.prod.js` и
> `/static/vendor/vue-router.global.prod.js` — оба файла лежат в репозитории и
> отдаются самим admin'ом. `admin/frontend/js/app.js` строит
> `createRouter(createWebHashHistory())`. Дорефакторинговый монолит сохранён как
> `admin/frontend-legacy.html` — он лежит **вне** смонтированного каталога
> `admin/frontend` и наружу не отдаётся (`GET /static/frontend-legacy.html` → 404,
> проверено 2026-08-04).

Роутинг — **hash-history**, адреса выглядят как `http://<host>:8101/#/sources`.
Catch-all `@app.get("/{path:path}")` (`admin/backend/main.py`) отдаёт `index.html` на всё,
что не перехвачено раньше него, — а раньше зарегистрированы ручки `/api/*`, монтирование
`/static` и обработка `/metrics`. Поэтому «глубокие» ссылки без `#` не работают:
`GET /sources` возвращает 200 и Dashboard, а не экран источников.

**Где источник правды**

| Что | Файл | Команда сверки |
|---|---|---|
| Маршруты SPA | `admin/frontend/js/app.js` | `grep -oE "path: '[^']+'" admin/frontend/js/app.js` |
| Пункты меню | `admin/frontend/js/components/AppLayout.js` (`const NAV`) | `grep -A2 "section:" admin/frontend/js/components/AppLayout.js` |
| Подключённые API-роутеры | `admin/backend/main.py` | `grep include_router admin/backend/main.py` |
| Ручки конкретного роутера | `admin/backend/routers/*.py` | `grep -oE '@router\.[a-z]+\("[^"]*"' admin/backend/routers/sources.py` |

На 2026-08-04: 11 маршрутов фронта и 11 подключённых API-роутеров (каталог
`admin/backend/routers/` — 11 модулей плюс `__init__.py`). Числа здесь не дублируются
списком намеренно: они меняются, а команды выше отвечают всегда.

### Разделы

Один маршрут — один экран из `admin/frontend/js/views/` (в каталоге есть ещё `LoginView.js`
и `Placeholder.js` — они маршрутов не имеют).

| Маршрут | Экран | Основной API |
|---|---|---|
| `/` | Dashboard: сводка по workspace | `/api/pipeline/stats` |
| `/pipeline` | Статус scheduler'а, Redis-стримы, ручной запуск джобов, reprocess поста | `/api/pipeline/*` |
| `/sources` | Список источников, добавление, toggle, vision, telegram-handle, bootstrap | `/api/sources/*` |
| `/posts` | Посты и карточка поста | `/api/posts` |
| `/albums` | Медиа-группы Telegram | `/api/albums` |
| `/media` | Медиафайлы по sha256 | `/api/media` |
| `/clusters` | Семантические кластеры, тренды, emerging и missing signals, timeline | `/api/clusters/*` |
| `/graph` | Граф концептов (Neo4j, рендер через `vendor/cytoscape.min.js`) | `/api/graph` |
| `/search` | `search_frontier` и `search_balanced` | `/api/search`, `/api/search/balanced` |
| `/workspaces` | Создание и правка workspace, toggle, bootstrap | `/api/workspaces/*` |
| `/settings` | Четыре вкладки: Runtime и роутинг · Провайдеры и FinOps · Xray · Система | `/api/settings/*`, `/api/monitoring/xray/*` |

> **Маршрута `/signals` нет — ни в текущем фронте, ни в сохранённом `frontend-legacy.html`.**
> В прежней версии раздела на него вела ссылка из таблицы «Что управляется через UI
> (не через конфиги)». Catch-all `{ path: '/:pathMatch(.*)*', redirect: '/' }` молча уводит
> такой адрес на Dashboard — отсюда и ощущение, что «раздел был, но пропал».

> **DLQ-экрана в UI нет.** Прежний текст обещал на `/pipeline` раздел
> «DLQ: проблемные документы». Ни `PipelineView.js`, ни `routers/pipeline.py` слова `dlq`
> не содержат. Живых вхождений во всей админке два, и оба — не экран: `routers/settings.py`
> отдаёт `vision_dlq_stream` как read-only значение конфига на вкладку «Система», а
> `frontend-legacy.html` упоминает DLQ в подсказках к тем же полям. `VISION_DLQ_STREAM` —
> это Redis-стрим, читать его нужно `redis-cli`.

Ручной запуск джобов живёт на `/pipeline`; набор кнопок — `const JOBS` в
`admin/frontend/js/views/PipelineView.js` (на 2026-08-04 их четыре). Три из них —
`Signal analysis`, `Missing signals`, `Semantic clusters` — уходят через `launch_manual_job`
в субпроцесс `python -m admin.backend.manual_jobs`, чтобы CPU-bound работа не блокировала
event loop admin. Четвёртая, `Refresh source scores`, субпроцесса не создаёт:
`POST /api/pipeline/refresh-source-scores` вызывает `refresh_source_scores_job` прямо
в процессе admin.

### Авторизация

HTTP-middleware `_auth` в `admin/backend/main.py` и его предикат `_path_needs_auth`:
закрыто всё под `/api/`, кроме `/api/health`, `/api/auth/login` и
`/api/monitoring/alertmanager/webhook`; отдельно пропускается любой запрос методом
`OPTIONS`. `/metrics` открыт. Статика и сам SPA публичны — форма входа рисуется
до авторизации.

Два способа: cookie-сессия `fadmin_session` (HMAC на `ADMIN_PASSWORD`, TTL 7 дней) — для
браузера, и HTTP Basic — для curl и интеграций. 401 отдаётся JSON'ом **без**
`WWW-Authenticate`, чтобы браузер не подвешивал XHR нативным диалогом. Поэтому голый
`curl http://127.0.0.1:8101/api/sources` → `401 {"detail":"unauthorized"}` — это норма.

### Что действительно управляется через UI

| Настройка | Где | Пишет ли UI |
|---|---|---|
| Workspace: имя, категории, design lenses, bridges, `extra` | `/workspaces` | да (`POST`, `PATCH`) |
| Workspace: `relevance_threshold` | `/workspaces` | да, **сливается** в `relevance_weights` |
| Workspace: активность | `/workspaces` | да (`PATCH /toggle`) |
| Источник: создание и удаление | `/sources` | да |
| Источник: вкл/выкл | `/sources` | да (`PATCH /toggle`) — но см. предупреждение ниже |
| Источник: vision, telegram-handle | `/sources` | да |
| Источник: url, cron, `proxy_config`, `tg_account_idx` | `/sources` | **только при создании**, отдельного `PATCH` нет |
| Runtime-mode и политика роутинга v2 | `/settings` → Runtime | да, пишется в `admin_runtime_settings` (+ зеркало в Redis) |
| Legacy `llm_routing_v1` (per-task) | `/settings` → Runtime | **нет, только чтение** — см. предупреждение ниже |
| Xray: ремедиация, failover, rollback | `/settings` → Xray | да |
| Провайдеры, бюджеты, circuits, FinOps | `/settings` → Провайдеры | нет, только чтение |
| Пороги indexing/vision/relevance, интеграции, секреты | `/settings` → Система | нет, только чтение `shared/config.Settings` |
| Пороги missing signals (`MISSING_SIGNALS_*`), окна trend-alert | — | нет; переменные окружения в `docker-compose.yml` |
| Темы для gap-анализа | — | нет; темы предлагает LLM, фолбэк — `design_lenses` + `categories` (`worker/services/missing_signals.py`) |

> **Три ключа `admin_runtime_settings` — не три редактируемых экрана.** Живых ключей
> в таблице ровно три: `runtime_mode`, `llm_control_plane_policy_v2`, `llm_routing_v1`.
> Первые два UI пишет (`POST /api/settings/runtime-mode`, `POST /api/settings/policy`),
> а `llm_routing_v1` показывает только на чтение — в `RuntimeRoutingTab.js` этот блок
> так и подписан: «Унаследованный per-task роутинг — только для справки». Ручка
> `POST /api/settings/llm-routing` в API есть, контрола под неё в UI нет; менять
> per-task маршруты приходится запросом мимо интерфейса.

**API workspaces:** `PATCH /api/workspaces/{id}` — частичное обновление (Pydantic
`exclude_unset`); `relevance_threshold` сливается в JSON `relevance_weights` через
`merge_relevance_weights`, категорийные веса (`technology`, `design`, …) не затираются.
`POST /api/workspaces` при upsert делает то же слияние, но в его `ON CONFLICT DO UPDATE`
**нет `is_active`** — активность через этот вызов не меняется. Меняют её `PATCH /toggle`,
bootstrap из YAML и — если положить поле в тело — тот же `PATCH /api/workspaces/{id}`
(`is_active` объявлен в `WorkspaceUpdate`); из UI это поле не отправляется.

**Приёмка после смены порога или категорий:** логи worker на `relevance_category_unknown`;
`GET /api/pipeline/stats`. Кэш workspace в worker живёт до 90 с
(`worker/tasks/enrichment_task.py`, `_get_workspace`) — раньше этого срока изменение
не проявится.

**Кластеры vs category:** семантические `trend_clusters` и поле `posts.category` — разные оси;
кластер не подменяется ненадёжной классификацией поста. Обоснование — в докстринге
`shared/models/trend_cluster.py` (сам модуль описателен и в рантайме не используется).

**Типы источников в форме добавления:** `telegram`, `rss`, `web`, `api`, `email`
(`shared/source_definitions.CANONICAL_SOURCE_TYPES`). `habr` — не тип, а устаревший алиас
`rss` плюс набор пресетов `RSS_PRESETS`.

### `config/*.yml` и БД: две асимметрии, обе кусаются

> **Утверждение «`config/` содержит только дефолты при первом запуске, дальше источник
> правды — БД» неверно.** `admin/backend/services/bootstrap_configs.py` делает
> `INSERT ... ON CONFLICT (id) DO UPDATE SET ... is_enabled = EXCLUDED.is_enabled`
> (и то же для `schedule_cron`, `proxy_config`, `extra`, `source_authority`, а у воркспейсов —
> для `is_active`). Любой вызов `POST /api/sources/bootstrap` перезаписывает БД из YAML.

Из этого следуют два несимметричных перекоса.

**1. YAML сильнее БД там, где пересекается.** Источник, выключенный руками через
`PATCH /api/sources/{id}/toggle`, будет молча включён обратно ближайшим bootstrap'ом, если в
`config/sources.yml` у него `is_enabled: true` (или ключ вовсе отсутствует — `bootstrap_configs.py`
читает его как `bool(src.get("is_enabled", True))`, то есть «нет ключа» = включён).
Ответ ручки при этом успешный. **Включение или выключение источника — два действия:
`PATCH /toggle` и правка `config/sources.yml`.** Регрессию ловит
`tests/test_sources_config_contract.py`, но узко: `test_auto_batch_enabled_flags_match_the_recorded_state`
сверяет флаги только у батча `auto_*` (десять источников). Остальные записи `sources.yml`
не подстрахованы ничем — за ними следит только дисциплина.

**2. БД шире YAML там, где не пересекается.** `config/sources.yml` — частичное зеркало,
а не полный список: bootstrap только вставляет и обновляет, ничего не удаляет.

Замер 2026-08-04: в БД 247 источников (202 включённых), в `sources.yml` — 168 (133 включённых),
79 записей существуют только в БД, ни одной — только в YAML. Расхождений по `is_enabled`
в этот день нет, но это следствие дисциплины, а не механизма. Пересчитать:

```bash
docker exec frontier-intelligence-postgres-1 sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c \
   "SELECT count(*), count(*) FILTER (WHERE is_enabled) FROM sources"' </dev/null

docker exec frontier-intelligence-admin-1 python -c \
  "import yaml; d=yaml.safe_load(open('/app/config/sources.yml'))['sources']; \
   print(len(d), sum(1 for s in d if s.get('is_enabled', True)))" </dev/null
```

`</dev/null` обязателен: `docker exec` без него вычитывает остаток скрипта из stdin.

### `config/` запечён в образ

У контейнера `admin` смонтированы ровно два bind-mount'а: `/runtime` и
`/app/admin/frontend` (ro). Каталог `config/` в списке отсутствует — он попадает внутрь
строкой `COPY config/ /app/config/` в `admin/Dockerfile`.

```bash
docker inspect frontier-intelligence-admin-1 \
  --format '{{range .Mounts}}{{.Destination}} {{end}}'
# → /runtime /app/admin/frontend
```

Практические следствия:

- Правка `config/*.yml` локально плюс rsync **не меняет ничего**, пока файл не окажется
  внутри контейнера. Варианты: `docker cp config/sources.yml <admin>:/app/config/` (быстро,
  живёт до пересоздания контейнера) либо пересборка образа — `bash scripts/server-build-stack.sh admin`.
- `COPY config/ /app/config/` есть ровно в двух Dockerfile'ах — `admin/` и `mcp/`. Читают
  эти файлы тоже только двое: `admin/backend/services/bootstrap_configs.py`
  (`CONFIG_DIR = Path("/app/config")`) и `mcp/guards.py` (`/app/config/workspaces.yml`).
  `worker` и `ingest` YAML-конфиги не читают вообще — они работают от таблиц
  `sources` / `workspaces`. Сверить:
  `grep -l "COPY config" */Dockerfile`.
- `docker cp` в admin **не** доезжает до mcp — это два независимых экземпляра файла.
  Замер 2026-08-04: `workspaces.yml` на хосте 11789 байт, в `admin` — те же 11789,
  в `mcp` — 9701 (копия суточной давности).
- Правки фронта, наоборот, применяются мгновенно: `admin/frontend` смонтирован с хоста,
  а FastAPI отдаёт статику с диска и ставит `Cache-Control: no-cache, must-revalidate`.
- Правки Python в `admin/backend/` требуют пересборки образа — код запечён, `restart` его
  не обновит.

> **Каталог `/app/config` внутри `worker` и `ingest` всё-таки есть — и он мёртвый.**
> В их Dockerfile'ах `COPY config/` нет; файлы достались от более ранних сборок и с тех пор
> не обновлялись: `sources.yml` там 24 282 байта от 3 мая против 80 334 от 3 августа
> у `admin` и `mcp`. Ни одна строка в `worker/` и `ingest/` эти файлы не открывает.
> Правка там не влияет ни на что — сверять и чинить конфиг нужно в `admin` и `mcp`.

---

## База данных

> **Схема здесь намеренно не дублируется.** Источник правды — `storage/postgres/init.sql`
> и `storage/postgres/migrations/*.sql`; живая база сверяется так:
> ```bash
> docker exec -it frontier-intelligence-postgres-1 \
>   sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\d posts"'
> ```
> До 04.08.2026 в этом разделе лежали рукописные `CREATE TABLE`, разошедшиеся с базой
> по именам колонок, типам и самому составу таблиц: фигурировали несуществующие
> `post_media_map` и `media_group_items`, `sources.type`/`enabled`/`schedule` вместо
> реальных `source_type`/`is_enabled`/`schedule_cron`, `UUID PRIMARY KEY` вместо `TEXT`.
> По такому описанию нельзя было написать ни одного рабочего запроса. Поэтому теперь
> здесь инвентарь назначений, а не копия DDL — копия неизбежно расходится снова.

### PostgreSQL — 23 таблицы

Первичные ключи везде **`TEXT`**, не UUID. `workspace_id TEXT NOT NULL` — в каждой
основной таблице; дочерние (`indexing_status`, `post_enrichments`, `source_checkpoints`,
`source_runs`) его не несут и скоупятся через родителя.

**Приём и обогащение**

| Таблица | Назначение | Значимые ограничения |
|---|---|---|
| `workspaces` | Пространства. Пер-воркспейсные настройки живут в `extra.cluster_analysis` (пороги, окна, `semantic_cluster_max_posts`) | — |
| `sources` | Источники. Расписание в `schedule_cron`, вкл/выкл в `is_enabled`, прокси в `proxy_config` | `source_type ∈ telegram, rss, web, api, email, habr` |
| `source_checkpoints` | Курсоры инкрементального опроса: `cursor_json`, `etag`, `last_seen_published_at` | — |
| `source_runs` | Журнал прогонов источников | `status ∈ running, success, error` |
| `posts` | Документы. Категория — `category TEXT` плюс `tags JSONB`; `grouped_id` — **TEXT**; `url` отдельной колонкой **нет**, он в `extra` | — |
| `post_enrichments` | Результаты обогащения, тяжёлое тело — в S3 по `s3_key` | `kind ∈ concepts, vision, tags, crawl, valence` |
| `indexing_status` | Статусы конвейера по посту | `embedding_status ∈ pending, done, dropped, error`; `graph_status` и `vision_status ∈ pending, done, skipped, error` |
| `media_objects` | Медиа по содержимому. **Первичный ключ — сам `sha256`**, отдельного `id` нет | — |
| `media_groups` | Телеграм-альбомы: `assembled`, `item_count`, `vision_summary_s3_key` | — |
| `relevance_decisions` | Решения о релевантности с обоснованием. `audit_status` — под человеческую разметку RSI | — |

**Аналитический слой**

| Таблица | Назначение | Значимые ограничения |
|---|---|---|
| `semantic_clusters` | Семантические кластеры окна | — |
| `trend_clusters` | Стабильные тренды. `title_ru` — русский заголовок | `pipeline ∈ reactive, stable` — но **пишется только `stable`**, продюсера для `reactive` нет |
| `emerging_signals` | Ранние сигналы | `signal_stage ∈ weak, emerging, stable, fading` |
| `signal_time_series` | Оконные ряды по сущностям | `entity_kind ∈ semantic, trend, emerging` |
| `weak_signal_snapshots` | Снимки слабых сигналов + вердикт судьи (`judge_verdict`) | — |
| `cluster_runs` | Журнал прогонов кластеризации: пороги, сводка, метрики качества | `status ∈ running, success, error` |
| `missing_signals` | Пробелы, найденные через SearXNG | — |
| `trend_alerts` | Отправленные срочные алерты, дедуп по `workspace_id + cluster_key + alert_kind` | `status ∈ pending, sent, error` |

**Управление и обратная связь**

| Таблица | Назначение | Значимые ограничения |
|---|---|---|
| `threshold_proposals` | Предложения изменить пороги (контур RSI), человеческий гейт | `status ∈ pending, approved, rejected, superseded` |
| `entity_merge_proposals` | Предложения слить сущности графа | то же |
| `card_feedback` | Редакторская разметка карточек | `entity_kind ∈ post, semantic, trend, emerging, missing`; `verdict ∈ chosen, passed` |
| `admin_manual_jobs` | Ручные запуски задач из админки | — |
| `admin_runtime_settings` | Живые настройки с hot-reload, в том числе таблица маршрутизации LLM `llm_routing_v1` | **Не создаётся ни `init.sql`, ни миграциями** — единственная таблица вне DDL-контура |

**Слой провенанса.** Миграция `20260714_provenance_dedup.sql` добавила по шесть колонок
в `semantic_clusters`, `trend_clusters` и `emerging_signals`: `deduped_source_count`,
`distinct_voices`, `echo_ratio`, `arrival_dispersion`, `distinct_originators`,
`independence_score`. Значение по умолчанию `0.0`, исторические строки не бэкфилились —
`0.0` означает «не измерено», а не «полностью синдицировано». Считает их
`shared/provenance.py`, который импортируется ровно одним модулем
(`worker/services/semantic_clustering.py`) и до поисковых поверхностей не доходит.

**Учёта миграций нет.** Ни Alembic, ни таблицы `schema_migrations`: применённость
проверяется наличием объектов, а не журналом. Список миграций — `storage/postgres/migrations/`.

### Qdrant — коллекции

**Одна коллекция на модель эмбеддингов, а не по коллекции на воркспейс.** Изоляция —
payload-фильтром по `workspace_id` в каждом запросе (`worker/integrations/qdrant_client.py`).
Имя строится как `{база}__{версия}__{семейство индекса}` в `shared/embedding_models.py`.

Обращаться нужно **через алиасы** — прямое имя коллекции в коде считается ошибкой:

| Алиас | Указывает на | Векторы |
|---|---|---|
| `frontier_docs_active` | `frontier_docs__embeddingsgigar__dense_2560` | dense 2560 Cosine + sparse (BM25) |
| `trend_clusters_active` | `trend_clusters__embeddingsgigar__dense_2560` | dense 2560 Cosine, sparse нет |

> **Осторожно: базовые коллекции живы, но мертвы.** `frontier_docs` и `trend_clusters`
> остались после перехода на версионированные имена, не являются целями ни одного алиаса
> и не обновляются с момента cutover. Прямой запрос по такому имени тихо вернёт срез
> полугодовой давности. Скрипты перехода — `scripts/qdrant_alias_cutover.py`
> и `scripts/qdrant_backfill_versioned.py`.

Коллекция личного корпуса (`own_corpus`, вторая ось `own_stake`) **не создана**:
код готов, флаг `OWN_STAKE_ENABLED=false`, `scripts/index_own_corpus.py` в проде
не запускался. С 04.08.2026 `init_storage.py` её больше и не заводит.

### Neo4j — граф концептов

Это граф **ко-встречаемости концептов**, а не провенанса. Что в нём реально есть:

| Метка | Что | Порядок |
|---|---|---|
| `Concept` | Извлечённые концепты | сотни тысяч |
| `Document` | Документы | ~190 тыс. |
| `Workspace` | Корни пространств | 7 (шесть рабочих плюс мусорный узел `id='test'`) |

| Связь | Что означает |
|---|---|
| `MENTIONS` | документ упоминает концепт |
| `RELATED_TO` | ко-встречаемость концептов, со счётчиком |
| `CONTAINS` | воркспейс содержит концепт |
| `ECHO_OF` | слой провенанса: перепечатка/эхо |

> **Чего в графе нет, вопреки прежнему описанию.** Метки `Source` и `TrendCluster`
> существуют только потому, что их породили констрейнты из
> `storage/neo4j/constraints.cypher` — узлов ноль. Метки `Album` нет даже там.
> Связей `FROM_SOURCE`, `EVOLVED_FROM`, `BRIDGES` не существует:
> `CALL db.relationshipTypes()` возвращает ровно четыре типа из таблицы выше.
> Community detection, Louvain/Leiden и иерархические summary из GraphRAG
> **не реализованы** — упоминаний `gds.` в коде нет.

**Из графа ничего не удаляется.** У `Neo4jFrontierClient` нет ни одного метода удаления:
когда пост признан нерелевантным и вычищен из Qdrant, его `(:Document)` с рёбрами
остаётся навсегда, а счётчики `RELATED_TO` продолжают его учитывать.

## MCP Tools

Инструменты живут в **двух контурах**, и это не одно и то же множество:

| Контур | Что это | Адрес | Сколько инструментов |
|---|---|---|---|
| REST | FastAPI-приложение `mcp/server.py`, роутеры из `mcp/tools/*.py` | `mcp:8100`, наружу — только `127.0.0.1:8100` | **32** |
| MCP-шлюз | `mcp/mcp_gateway.py` — тонкие обёртки `@mcp.tool`, ходящие в REST по HTTP | `0.0.0.0:8102/mcp` | **22** |

Шлюз — строгое подмножество REST: все 22 его имени есть среди 32 REST-имён, обратного
включения нет. MCP-клиенты (Claude Code, Claude Desktop) подключены к `:8102` — то есть
**видят 22 из 32**. Сколько всего клиентов реально подключено к шлюзу, изнутри стека
не проверяется: аутентификации нет и учёта подключений он не ведёт.

### Источник правды и как сверить

Списки инструментов меняются чаще, чем этот файл. Не верь таблицам — спрашивай стек:

```bash
# 32 из REST. Ответ — объект {"tools": [...]}, длину брать у списка внутри:
# len() от самого объекта вернёт 1 (число ключей), а не число инструментов.
curl -sS http://127.0.0.1:8100/tools | python3 -c \
  "import sys,json;print(len(json.load(sys.stdin)['tools']))"

# 22 из шлюза — счётчик по коду; отдельного эндпоинта со списком у него нет
grep -c '@mcp.tool' /opt/frontier-intelligence/mcp/mcp_gateway.py
```

Регистрация REST-инструментов — `app.include_router(...)` в `mcp/server.py`:

| Модуль | Семейство | Инструментов | Есть в шлюзе |
|---|---|---|---|
| `mcp/tools/search_*.py`, `graph.py`, `frontier_brief.py`, `ingest_url.py` | поиск, граф, brief, ingest_url | 7 (по одному на модуль) | да |
| `mcp/tools/observability.py` | workspaces, sources, clusters, signals, timeline | 12 | да |
| `mcp/tools/editorial.py` | редакционная петля карточек | 3 | да |
| `mcp/tools/graph_health.py` | здоровье графа, слияние сущностей | 4 | **нет** |
| `mcp/tools/threshold_proposals.py` | пороги, недооценённые сигналы, аудит релевантности | 6 | **нет** |

### Разрыв REST ↔ шлюз: 10 инструментов клиенту недоступны

> **В шлюз не выведены два целых модуля.** `mcp/mcp_gateway.py` содержит ровно 22 `@mcp.tool`,
> и ни одного из перечисленных ниже среди них нет. По REST (`curl .../tools`) они отвечают,
> в MCP-клиенте их не существует — это не сбой подключения и не кэш клиента.

Из `graph_health.py`: `get_graph_health`, `list_entity_merge_proposals`,
`approve_entity_merge`, `reject_entity_merge`.

Из `threshold_proposals.py`: `list_threshold_proposals`, `list_underrated_signals`,
`list_relevance_audit_sample`, `mark_relevance_audit`, `approve_threshold_change`,
`reject_threshold_change`.

Практическое следствие: RSI-петля (одобрение слияний сущностей и изменений порогов)
из Claude Code **не управляется** — только прямым `curl` в `:8100` с сервера.

> **Прежняя редакция этого раздела перечисляла 19 инструментов одной таблицей.**
> Недоставало 13: три редакционных (`export_inbox_cards`, `record_card_feedback`,
> `list_card_feedback` — они в шлюзе есть) и десять выше.

### Транспорт

Шлюз работает по **Streamable HTTP**, а не по SSE: `mcp/mcp_gateway.py` заканчивается
`mcp.run(transport="streamable-http")`, docstring говорит то же. Строки `transport="sse"`
в файле нет.

> **Формулировка «SSE gateway» неверна** и была источником ложных диагнозов при отладке
> подключения. Голый `GET http://127.0.0.1:8102/mcp` отдаёт `406`, а не поток событий —
> это нормальный ответ Streamable HTTP на запрос без нужного `Accept`, а не поломка.

### Доступ и аутентификация

- `mcp:8100` опубликован **только на loopback** (`127.0.0.1:8100->8100/tcp` в `docker compose ps`).
- `mcp-gateway:8102` опубликован на `0.0.0.0` и **не имеет аутентификации вообще**:
  в `mcp/server.py` нет ни одной auth-зависимости, а `TransportSecuritySettings` шлюза
  заданы как `allowed_hosts=["*"]`, `allowed_origins=["*"]`,
  `enable_dns_rebinding_protection=False`.

Это осознанное решение владельца: хост живёт в локальной сети, наружу не проброшен.
Учитывать при этом надо, что через `:8102` доступны **пишущие** инструменты —
`ingest_url` (кладёт задание в `stream:posts:crawl`) и `record_card_feedback`
(пишет в `card_feedback`). Остальные пишущие (`approve_*`/`reject_*`, `mark_relevance_audit`)
остались за REST-контуром и потому недостижимы снаружи. См.
[docs/TODO-UNFINISHED.md](TODO-UNFINISHED.md).

### Общие правила инструментов

- `workspace` принимают 17 из 22 инструментов шлюза. Пять не принимают, и это не упущение:
  они адресуются идентификатором сущности (`get_cluster_details`, `get_cluster_evidence`,
  `get_missing_signal_details`, `get_source_details`) либо не привязаны к пространству
  (`list_workspaces`).
- В Qdrant ходят три поисковых инструмента из четырёх: `search_frontier`, `search_balanced`
  (переиспользует `run_search_request` из `search_frontier`) и `search_trend_clusters` —
  hybrid dense + sparse с payload-фильтром по `workspace_id`. **`search_by_vision` — исключение:**
  это сырой SQL по `post_enrichments` в PostgreSQL, Qdrant там не участвует.
- Hybrid — свойство образа, а не гарантия: `mcp/server.py` строит описание `search_frontier`
  через `HAS_SPARSE` и при отсутствии fastembed честно объявляет «dense only». В работающем
  стеке 04.08.2026 `curl .../tools` отдаёт вариант с «dense embeddings + BM25 sparse».
- Имена коллекций конфигурируемы (`QDRANT_COLLECTION`, `QDRANT_TRENDS_COLLECTION` и их
  `*_ALIAS`), захардкоженных имён в инструментах нет: поиск по строковым литералам
  `grep -rE "['\"](frontier_docs|trend_clusters)['\"]" mcp/ --include=*.py` даёт ноль
  (без кавычек в шаблоне — 19 попаданий, все в текстах описаний). Значения — `shared/config.py`.
- `synthesize=true` поддерживают ровно три инструмента: `search_frontier`, `search_balanced`,
  `get_frontier_brief`.

> **Синтез не привязан к GigaChat намертво.** Модель берётся из маршрута `mcp_synthesis`
> в `admin_runtime_settings.llm_routing_v1` с hot-reload. На 04.08.2026 там действительно
> `gigachat / GigaChat-2-Pro`, но это состояние таблицы, а не свойство кода: маршрут
> меняется без пересборки. Сверка:
>
> ```bash
> docker compose exec -T postgres psql -U frontier -d frontier -Atc \
>   "select value from admin_runtime_settings where key='llm_routing_v1'" </dev/null \
>   | python3 -m json.tool | grep -A4 mcp_synthesis
> ```

- Admin UI ходит в REST-контур **узко**: `admin/backend/routers/search.py` дёргает
  `search_frontier` и `search_balanced` через `settings.mcp_internal_url`
  (`http://mcp:8100`). Остальные 30 инструментов админка не вызывает.

> **Прежняя формулировка «Admin UI тоже ходит в эти tools» преувеличивала.**
> `grep -rl --include=*.py mcp_internal_url admin/` даёт два файла, и во втором
> (`routers/settings.py`) URL только выводится в дампе конфигурации, вызовов нет.
> Фильтр по `*.py` обязателен: без него в выдачу попадают ещё четыре `__pycache__/*.pyc`.

### Известная ловушка параметра

`search_trend_clusters` объявляет `pipeline: Literal["stable", "reactive"] | None = "stable"`,
и значение `"reactive"` проходит валидацию без ошибки — но **в данных его не существует**:

```bash
docker compose exec -T postgres psql -U frontier -d frontier -Atc \
  "select pipeline, count(*) from trend_clusters group by 1" </dev/null
# 04.08.2026: единственная строка stable|393
```

Вызов с `pipeline="reactive"` возвращает `{"results": []}` и эхо фильтра в
`applied_filters` — пустой результат, а не ошибку.

> **Reactive-ветки детектора нет в коде — это не «временно не запускается».**
> Единственное место, которое пишет в `trend_clusters`, — `worker/services/semantic_clustering.py`,
> и оно проставляет `"pipeline": "stable"` константой. В коде и схеме слово `reactive`
> встречается только декларативно — объявление типа выше, текст описания в `mcp/server.py`,
> `CHECK (pipeline IN ('reactive', 'stable'))` в `storage/postgres/init.sql` и комментарий
> в `shared/models/trend_cluster.py`. Ни одной ветки, которая бы его писала, нет; сверка:
>
> ```bash
> grep -rE "reactive" --include=*.py --include=*.sql --include=*.yml .
> ```
>
> (в `docs/` упоминаний больше — но это тексты, не код).
> Раздел **Trend Detection** ниже по этому же файлу описывает «Reactive (cron 30 мин)»
> как работающий контур — это описание замысла, кода за ним нет.

---

## Processing Pipeline

### As-built: событийная цепочка

Всё обогащение — один процесс `worker` (`worker/main.py`), который держит три консьюмера
в одном event-loop: `EnrichmentTask`, `VisionTask`, `ReindexTask`. Четвёртый консьюмер живёт
в отдельном сервисе `crawl4ai`.

```
ingest (telegram / rss / web / api / email)
  медиа → S3, s3_keys кладутся в media_urls события
  → stream:posts:parsed
       │
       └→ EnrichmentTask
            ├ post в PostgreSQL, indexing_status=pending
            ├ has_media → stream:posts:vision      (публикует ИМЕННО enrichment, не ingest)
            ├ relevance → нерелевантный: indexing_status=dropped, точка в Qdrant удаляется
            └ релевантный: concepts → valence → embedding текста поста
                 ├ Qdrant upsert, Neo4j concepts
                 ├ post_enrichments (concepts, tags, valence)
                 ├ linked_urls → stream:posts:crawl
                 └ stream:posts:enriched   ← пишется, но НИКЕМ не читается

VisionTask:  stream:posts:vision  → S3 → vision-модель (+ PaddleOCR)
                                  → post_enrichments(kind='vision') → stream:posts:reindex
crawl4ai:    stream:posts:crawl   → crawl
                                  → post_enrichments(kind='crawl')  → stream:posts:reindex
ReindexTask: stream:posts:reindex → enriched-текст → Qdrant patch + Neo4j concepts
```

- **`url` в событии** — permalink поста (валидация источника). **`linked_urls`** — внешние
  HTTP(S)-ссылки из текста и Telethon-entities; `t.me`, `twitter.com`, `x.com` отбрасываются,
  потолок `MAX_LINKED_URLS = 10` (`shared/linked_urls.py`). **Только они** уходят в crawl4ai.
- Альбомы сворачиваются на стороне ingest в один `PostParsedEvent` с `grouped_id` и несколькими
  `media_urls` (`ingest/sources/telegram_source.py`).
- Классов `AlbumAssemblerTask` и `GraphTask` в коде нет вовсе — не «не подключены», а
  отсутствуют (`grep -ril "AlbumAssemblerTask\|GraphTask" . --include=*.py` → пусто).
  В `worker/tasks/` лежат ровно три файла (`enrichment_task.py`, `vision_task.py`,
  `reindex_task.py`), граф пишется inline в `EnrichmentTask` и патчится `ReindexTask`.

> **Vision-событие публикует `EnrichmentTask`, а не ingest.** Прежняя редакция (и раздел
> «As-Built Runtime» ниже по документу) утверждала «ingest uploads media to S3 and emits
> `stream:posts:vision`». Ingest грузит медиа в S3 и кладёт s3-ключи в `media_urls`, а `xadd`
> в `stream:posts:vision` делает только `worker/tasks/enrichment_task.py` (константа
> `STREAM_VISION`, публикация внутри `process_event`). В `ingest/` имени этого стрима нет
> ни разу — проверяется одним grep:
>
> ```bash
> grep -r  "stream:posts:vision" ingest/ --include=*.py   # пусто
> grep -rl "STREAM_VISION"       worker/ --include=*.py   # только tasks/enrichment_task.py
> ```

### Redis Streams

Канонический список стрим/группа — `shared/redis_streams.py::DEFAULT_STREAM_GROUPS`;
именно он используется и мониторингом (`GET /api/pipeline/streams`).

| Stream | Пишет | Consumer group | Читает |
|---|---|---|---|
| `stream:posts:parsed` | ingest, `/api/pipeline/reprocess`, backfill-скрипты | `enrichment_workers` | `EnrichmentTask` |
| `stream:posts:vision` | `EnrichmentTask` | `vision_workers` | `VisionTask` |
| `stream:posts:crawl` | `EnrichmentTask`, MCP `ingest_url` | `crawl4ai_workers` | `Crawl4AIService` |
| `stream:posts:reindex` | `VisionTask`, `Crawl4AIService`, `scripts/enqueue_reindex_enriched_posts.py` | `reindex_workers` | `ReindexTask` |
| `stream:posts:enriched` | `EnrichmentTask` | **нет ни одной** | никто |

> **`stream:posts:enriched` — не «observer stream для downstream-интеграций», а тупик.**
> Проверено 04.08.2026 на живом стеке: `XINFO GROUPS stream:posts:enriched` возвращает
> пустой список — consumer-групп ноль, а `XLEN` при этом стоит у потолка `STREAM_MAXLEN`
> (в замере 10013 при потолке 10 000: тримминг приблизительный, длина слегка гуляет).
> То есть события пишутся и вытесняются, ни разу никем не прочитанные. Любой код,
> рассчитывающий «подписаться и получить историю», получит только последние ~10k записей
> и ничего из прошлого.
>
> ```bash
> docker exec frontier-intelligence-redis-1 redis-cli XINFO GROUPS stream:posts:enriched
> ```

Потолок длины у всех стримов — `STREAM_MAXLEN = 10_000` (`shared/redis_client.py`, там же
разбор OOM 31.07.2026). DLQ-стримы `stream:posts:parsed:dlq` и `stream:posts:vision:dlq`
объявлены в `shared/config.py`, но на 04.08.2026 обоих ключей в Redis нет — ни одно сообщение
туда ещё не уезжало.

Проверка состояния (`docker exec` — контейнер называется `frontier-intelligence-redis-1`,
не `redis`; профили COMPOSE не нужны, если ходить по имени контейнера):

```bash
docker exec frontier-intelligence-redis-1 redis-cli --scan --pattern 'stream:*'
docker exec frontier-intelligence-redis-1 redis-cli XLEN stream:posts:parsed
docker exec frontier-intelligence-redis-1 redis-cli XINFO GROUPS stream:posts:parsed
```

### EnrichmentTask

Порядок: валидация источника → `_save_post` → (при медиа) публикация vision-события →
**relevance** → при прохождении порога **concepts** → **valence** → эмбеддинг первых 2000
символов текста поста → Qdrant upsert → Neo4j → `post_enrichments` (`concepts`, `tags`,
`valence`) → `indexing_status=done`.

- Модели не зашиты в код: все четыре задачи (`relevance`, `concepts`, `valence`,
  `relevance_concepts`) идут через `LLMRouterClient` и живую таблицу
  `admin_runtime_settings.llm_routing_v1`. Смотреть провайдера по факту, а не по документу:
  в логах worker строка `llm_task task=... provider=... actual_model=...`.
- Совмещённая цепочка `relevance_concepts` (один вызов вместо двух) существует, но включается
  флагом `GIGACHAT_RC_JOINT_ENABLED`; на 04.08.2026 он `false`, работают две отдельные цепочки.
- `relevant=False` из-за сбоя LLM не считается вердиктом: `status=="failed"` поднимает
  исключение и уводит событие в общий retry, чтобы провал провайдера не выглядел как
  честный «нерелевантно».
- Нерелевантные посты помечаются `dropped`, существующая точка в Qdrant удаляется.
- Параллелизм батча — `INDEXING_MAX_CONCURRENCY` (на сервере `4`).
- Poison-сообщения (превышен `delivery_count`) уезжают в `INDEXING_DLQ_STREAM`.

> **Релевантность считает не GigaChat.** По живой таблице маршрутов на 04.08.2026 у всех
> четырёх задач основной маршрут — `wormsoft/agent/medium`. Цепочки фолбэков разные:
> у `relevance` и `relevance_concepts` это `polza` (`google/gemma-4-31b-it`) → `gigachat`
> (`GigaChat-2`), у `concepts` и `valence` — сразу `gigachat` (`GigaChat-2`), без polza.
> За GigaChat остались эмбеддинги (`EmbeddingsGigaR`) и `mcp_synthesis` (`GigaChat-2-Pro`).
> Прежняя формулировка «Релевантность (GigaChat)» описывает контур до июльской
> перекладки маршрутов. Смотреть живую таблицу целиком:
>
> ```bash
> docker exec frontier-intelligence-postgres-1 psql -U frontier -d frontier \
>   -tAc "SELECT value FROM admin_runtime_settings WHERE key='llm_routing_v1';"
> ```

### VisionTask

`stream:posts:vision` → скачивание из S3 → vision-модель → при заданном `PADDLEOCR_URL`
дополнительно **PaddleOCR** `POST /v1/ocr/upload` → в enrichment попадают `paddle_ocr_text`
и объединённый `ocr_text` → `post_enrichments(kind='vision')`, `indexing_status.vision_status`,
для альбомов — gzip-сводка в S3 и `media_groups`. При `vision_status='done'` публикуется
`stream:posts:reindex`.

Политика на источник — `sources.extra.vision`: `mode ∈ {full, ocr_only, skip}`,
`max_media_bytes` (дефолт 9 МБ). PaddleOCR запускается при `mode != skip` и mime `image/*`.

Сервис: `docker compose --profile paddleocr up -d paddleocr`, в окружении worker
`PADDLEOCR_URL=http://paddleocr:8008`.

> **Провайдер vision выбирается роутером, и «GigaChat Vision» сейчас не вызывается вообще.**
> Порядок кандидатов берёт `worker/llm_router_client.py::_generate_vision` из семейства
> `vision_generation` живой политики `admin_runtime_settings.llm_control_plane_policy_v2`.
> Если среди кандидатов есть wormsoft — он идёт первым для **всех** источников, дальше
> openrouter → polza → gigachat, а ветка по `quality_tier`
> (`_vision_provider_for_quality_tier`: `trusted`/`primary` → GigaChat, иначе openrouter
> при `VISION_ROUTING_ENABLED`) не выполняется вовсе — она достижима только когда wormsoft
> из политики убран. На 04.08.2026 `wormsoft/vision/medium` в политике включён, и по
> последним 300 vision-обогащениям вызовов GigaChat ноль: `trusted`, `primary` и `standard`
> одинаково уходят в wormsoft, с фолбэком на polza (`qwen/qwen3-vl-30b-a3b-instruct`).
> Записи «GigaChat Vision» в старой редакции не верны ни для одного `quality_tier`.
> Кто на самом деле обрабатывал картинки — видно в самих обогащениях:
>
> ```bash
> docker exec frontier-intelligence-postgres-1 psql -U frontier -d frontier -tAc \
>   "SELECT it->>'quality_tier' AS tier, it->'_llm'->>'provider' AS provider, count(*)
>      FROM (SELECT data FROM post_enrichments WHERE kind='vision'
>             ORDER BY created_at DESC LIMIT 300) t,
>           jsonb_array_elements(t.data->'items') it
>     GROUP BY 1,2 ORDER BY 3 DESC;"
> ```

### Расписания (планировщик admin)

Задачи регистрируются в `admin/backend/scheduler.py::_build_scheduler`, дефолтные краны —
поля `admin_*_cron` в `shared/config.py`, переопределяются через `.env`/`environment` в compose.
Полный список здесь намеренно не дублируется — он растёт (на 04.08.2026 шестнадцать задач)
и устаревает быстрее документа.

```bash
# какие задачи вообще есть — из кода
grep -E 'id="[a-z_]+"' admin/backend/scheduler.py
# живое состояние с next_run_time (нужна авторизация admin)
curl -sS -u admin:"$ADMIN_PASSWORD" http://127.0.0.1:8101/api/pipeline/scheduler
```

> **`docker exec … env | grep CRON` даёт неполную картину.** В окружении контейнера admin
> на 04.08.2026 лежат только шесть переменных `*_CRON` (openrouter × 4, trend alert, xray);
> остальные десять задач работают на дефолтах из `shared/config.py` и в `env` не видны вообще.
> Отсутствие переменной означает «дефолт», а не «задачи нет».

Три опорных расписания, на которые ссылаются остальные разделы:

| Задача | Переменная | Дефолт | Что делает |
|---|---|---|---|
| `run_semantic_clusters` | `ADMIN_SEMANTIC_CLUSTER_CRON` | `35 3 * * *` | семантические кластеры + сигналы, раз в сутки |
| `run_signal_analysis` | `ADMIN_SIGNAL_CLUSTER_CRON` | `20 */8 * * *` | сигналы поверх готовых кластеров + missing signals, трижды в сутки |
| `refresh_source_scores` | `ADMIN_SOURCE_SCORE_REFRESH_CRON` | `17 */6 * * *` | пересчёт `source_score` / `source_authority` |

Ручной запуск есть **не у всех** задач. `admin/backend/manual_jobs.py` — отдельный процесс,
чтобы CPU-bound работа не блокировала event-loop admin; список поддерживаемых имён — функция
`_dispatch` в этом же файле (на 04.08.2026 их десять из шестнадцати; `refresh_openrouter_*`,
`refresh_gigachat_balance`, `xray_health_check`, `urgent_trend_alerts` через него не гоняются).
Наружу по HTTP выведено ещё меньше — пять ручек:

```bash
grep -E '@router.post\("/(run|refresh)-' admin/backend/routers/pipeline.py
```

> **`run_missing_signals` не имеет расписания вообще.** В `_build_scheduler` нет ни одного
> `add_job` с этим id и нет переменной `ADMIN_MISSING_SIGNALS_CRON` — прежняя строка
> «Missing Signals (cron сутки)» описывает несуществующую задачу. Отдельно gap-анализ
> запускается только руками: `POST /api/pipeline/run-missing-signals` или
> `python -m admin.backend.manual_jobs run_missing_signals <workspace|__all__>`.
> При этом сами `missing_signals` **обновляются** — внутри `run_signal_analysis`
> (`worker/services/semantic_clustering.py` → `run_missing_signals_analysis`), то есть
> по крану `20 */8 * * *`. Сверять так: `max(updated_at)` в `missing_signals` должен
> попадать в окно последней серии `cluster_runs.stage='signal-analysis'`. Точного
> совпадения с самым последним запуском ждать не надо — задача идёт по воркспейсам
> по очереди, а при пустом наборе тем строки воркспейса намеренно не переписываются
> (`_replace_missing_signals` → лог `missing_signals_replace_skipped`). Замер 04.08.2026:
> `max(updated_at) = 08:25:37` при трёх запусках в 08:24:52 / 08:25:37 / 08:26:36.
>
> ```bash
> docker exec frontier-intelligence-postgres-1 psql -U frontier -d frontier -tAc \
>   "SELECT (SELECT max(updated_at) FROM missing_signals) AS ms_updated,
>           (SELECT max(started_at) FROM cluster_runs WHERE stage='signal-analysis') AS last_run;"
> ```

### Trend Detection

Пайплайн **один**. Стадии две, и они разнесены по разным задачам планировщика.

```
Стадия 1 — семантические кластеры (run_semantic_clusters, раз в сутки)
  посты за окно → векторы из Qdrant
  → связные компоненты по косинусной близости с ограничением разрыва по времени
  → semantic_clusters + signal_time_series

Стадия 2 — сигналы (внутри обеих задач; отдельно — run_signal_analysis, 3×/сут)
  группировка семантических кластеров → на группу считаются
    burst, velocity, acceleration, coherence, novelty,
    source_diversity, freshness, evidence_strength, change_point
  → взвешенная сумма signal_score (веса и пороги — в настройках кластеризации)
  → stage:
       stable   → trend_clusters        (+ LLM-бриф title_ru / insight / opportunity)
       emerging → emerging_signals
       weak     → weak_signal_snapshots (материал для ретро-петли RSI)
       fading   → пометка жизненного цикла
  → зеркалирование stable-кластеров в Qdrant для search_trend_clusters

Стадия 3 — missing signals (только внутри run_signal_analysis)
  генерация тем по воркспейсу → SearXNG → сравнение с внутренним корпусом
  → gap_score → missing_signals
```

- Кластеризация — **связные компоненты по косинусу**, а не HDBSCAN: пакета `hdbscan` нет
  ни в одном requirements и ни в одном импорте. В Python-коде слово встречается ровно один
  раз — как строка-фикстура в `tests/test_neo4j_concepts.py`, к алгоритму отношения не имеет
  (`grep -ril hdbscan . --include=*.py`). В документации оно ещё живо — в том числе выше
  в этом же файле, в ASCII-схеме архитектуры, и в `docs/saas/`, `docs/trend-detection-future-roadmap.md`:
  это остаток замысла, а не описание кода.
- Пороги и веса (`trend_cluster_stable_threshold`, `trend_cluster_emerging_threshold`,
  `signal_velocity_weight`, …) читаются `_cluster_settings` из `admin_runtime_settings`
  поверх дефолтов и меняются без пересборки — числа здесь не фиксируются.
- LLM трогает только stable-кластеры и только для текста брифа (`_enrich_stable_briefs`).
  Дефолты проставляются **до** обращения к модели, поэтому при выключенном `TREND_BRIEF_ENABLED`
  или сбое генерации строка всё равно пишется: `insight` получает заготовку вида
  «N материалов из M источников», `opportunity` — пустую строку, `title_ru` остаётся `NULL`.
  Сбой самого обогащения ловится в `_persist_signal_outputs` и не роняет запись кластеров.
- Счётчики gap-анализа (`topics_generated`, `topics_dropped_by_*`, `searxng_errors`,
  `topics_kept`) уезжают в `cluster_runs.summary`: по ним видно, кто виноват в нуле,
  без чтения логов дочернего процесса.

> **Реактивного пайплайна не существует.** Прежняя редакция описывала «Reactive (cron 30 мин)»
> с `burst_score` и записью `TrendCluster {pipeline: "reactive"}`. Ни продюсера, ни крана
> в коде нет: слово `reactive` встречается только в трёх объявлениях типа
> (`mcp/tools/search_trend_clusters.py`, `mcp/server.py`, `shared/models/trend_cluster.py`),
> а `_upsert_signal` пишет литерал `'stable'`. В БД на 04.08.2026 — 393 строки `trend_clusters`,
> все с `pipeline='stable'`, ни одной `'reactive'` за всё время с 28.03.2026.
> Параметр `pipeline="reactive"` у MCP-инструмента `search_trend_clusters` синтаксически
> валиден и всегда вернёт пусто.

```bash
docker exec frontier-intelligence-postgres-1 psql -U frontier -d frontier \
  -tAc "SELECT pipeline, count(*) FROM trend_clusters GROUP BY 1;"
```

### Проверка e2e после деплоя

1. **Деплой.** Код запечён в образ через `COPY`, поэтому `rsync + restart` его не обновляет —
   нужен rebuild и `up -d --force-recreate`. Пересобирать надо **все** сервисы со своим
   `build:`, а их девять, а не «worker и admin»; список не переписывай руками, а вытащи
   из compose:

   ```bash
   awk '/^  [a-z0-9_-]+:[[:space:]]*$/{s=$1; sub(":","",s)} /^    build:/{print s}' docker-compose.yml
   ```

   На 04.08.2026 это `gpt2giga-proxy, ingest, xray, worker, paddleocr, mcp, mcp-gateway,
   admin, crawl4ai` — прежняя редакция перечисляла семь и теряла `xray` с `mcp-gateway`.

   С рабочей машины: `.\scripts\push-and-remote-deploy.ps1 -Services worker,admin`
   (параметр `-Services` — массив, дефолт `worker, admin`) или задача Cursor
   «Sync → Server + Rebuild». Только миграции — `.\scripts\push-and-remote-migrate.ps1`,
   на сервере `bash scripts/server-apply-sql-migrations.sh`. Существующая БД
   от одного `init.sql` не обновляется.
2. **Логи.** Маркеры, по которым видно живой конвейер (проверены на живых логах 04.08.2026):

   ```bash
   docker logs --tail 400 frontier-intelligence-worker-1   2>&1 | grep "Enriched "
   docker logs --tail 400 frontier-intelligence-worker-1   2>&1 | grep "llm_task task="
   docker logs --tail 400 frontier-intelligence-worker-1   2>&1 | grep "Vision done"
   docker logs --tail 400 frontier-intelligence-worker-1   2>&1 | grep "Reindex done"
   docker logs --tail 400 frontier-intelligence-crawl4ai-1 2>&1 | grep "Crawl enrichment saved"
   ```

3. **PostgreSQL.** Контейнер `frontier-intelligence-postgres-1`, роль и БД — `frontier`:

   ```bash
   docker exec frontier-intelligence-postgres-1 psql -U frontier -d frontier -tAc \
     "SELECT kind, count(*) FROM post_enrichments GROUP BY 1 ORDER BY 2 DESC;"
   docker exec frontier-intelligence-postgres-1 psql -U frontier -d frontier -tAc \
     "SELECT stage, status, started_at FROM cluster_runs ORDER BY started_at DESC LIMIT 5;"
   ```

   Ожидаемые `kind`: `concepts`, `tags`, `valence`, `crawl`, `vision`.
4. **Qdrant.** Точки появляются только для релевантных постов
   (`indexing_status.embedding_status='done'`), не для `dropped`. Crawl/vision-обогащение
   патчит уже существующую точку через `stream:posts:reindex`. Оба добивочных скрипта требуют
   переменные окружения и зависимости стека, поэтому запускаются **внутри контейнера**,
   а не с хоста (в контейнере Python 3.11, в `/opt/frontier-intelligence/.venv` на сервере —
   3.10, и `.env` там не подхватывается):

   ```bash
   docker exec frontier-intelligence-worker-1 sh -c \
     'cd /app && python scripts/enqueue_reindex_enriched_posts.py --kind crawl --kind vision --limit 1000'
   docker exec frontier-intelligence-worker-1 sh -c \
     'cd /app && python scripts/sync_trend_clusters_to_qdrant.py --dry-run'
   ```

   Первый догоняет накопленные crawl/vision-обогащения, второй зеркалирует stable-кластеры
   в Qdrant (в штатном режиме это делается внутри анализа). У обоих есть `--dry-run` —
   первым делом стоит посмотреть, сколько попадёт под раздачу: `enqueue_*` отдаёт
   `{'matched': N, 'queued': 0}`, `sync_*` — JSON с `rows` / `points_prepared`.
5. **Redis.** Команды — в разделе про стримы выше.
6. **Admin API.** Все эндпоинты живут под префиксом **`/api`** и, кроме `/api/health`,
   `/api/auth/login` и вебхука Alertmanager, закрыты cookie/Basic-авторизацией:

   ```bash
   curl -sS -u admin:"$ADMIN_PASSWORD" http://127.0.0.1:8101/api/pipeline/stats
   curl -sS -u admin:"$ADMIN_PASSWORD" http://127.0.0.1:8101/api/pipeline/streams
   curl -sS -u admin:"$ADMIN_PASSWORD" -X POST http://127.0.0.1:8101/api/pipeline/reprocess/<post_id>
   ```

   `POST /api/pipeline/reprocess/{post_id}` сбрасывает `indexing_status` и заново публикует
   событие, вытаскивая `media_urls` из строки поста и `linked_urls` — из `content`.

> **Без префикса `/api` API не отвечает — но и ошибку не показывает.**
> Роутер подключён как `app.include_router(pipeline_router, prefix="/api/pipeline")`
> (`admin/backend/main.py`), поэтому `GET /pipeline/stats` не 404, а **200 с HTML
> админки** (SPA-фолбэк на любой неизвестный GET), а `POST /pipeline/reprocess/{id}` —
> `405 Method Not Allowed`. Проверено 04.08.2026. Ловушка в том, что `200` легко принять
> за работающий эндпоинт: если в ответе `<!doctype html>` вместо JSON — потерян `/api`.
> Голый `curl` по правильному пути отвечает `401 {"detail":"unauthorized"}` — это
> ожидаемый ответ авторизации, а не поломка сервиса.

---

## Telegram: обход блокировок

```python
# Два слоя, автоматическая ротация при сбое
PROXY_CONFIGS = [
    # MTProxy — встроен в протокол Telegram, сложнее заблокировать
    {"type": ProxyType.MTPROTO,
     "host": MTPROXY_HOST, "port": 443, "secret": MTPROXY_SECRET},
    # SOCKS5 — WireGuard LXC на Proxmox (fallback)
    {"type": ProxyType.SOCKS5,
     "host": "10.0.0.1", "port": 1080},
]

# 2 аккаунта: ротация при FloodWaitError / SessionRevokedError
# iter_messages() — эффективнее get_messages() для Telegram API
# Redis negative cache: album_seen:{channel_id}:{grouped_id}, TTL 6ч

# Управление через Admin UI /sources:
# - добавить/сменить MTProxy (host, port, secret)
# - добавить/сменить SOCKS5
# - назначить источник на аккаунт [0|1]
# - статус аккаунтов, последняя активность
```

---

## Структура репозитория

Полного дерева здесь больше нет намеренно. В git 37 записей верхнего уровня и 307 на двух
уровнях вложенности — такое дерево не читают, а сверять его руками при каждом новом модуле
никто не будет. Ниже карта «каталог → за что отвечает»; фактический состав берётся командой
из конца раздела.

> **Прежнее ASCII-дерево врало по всем каталогам — если помните его, перепроверьте.** Проверено 04.08.2026:
> - `worker/gigachat_client.py` был подписан «LangChain GigaChat + EmbeddingsGigaR». Первая строка
>   файла: `"""GigaChat client using openai SDK directly (no langchain-openai proxies issues)."""`.
>   LangChain не объявлен ни в одном Dockerfile и ни в одном `requirements*.txt`. В рабочем коде
>   стека слово встречается ровно один раз — в этой самой строке; ещё дважды в тестах
>   (`tests/stub_policy.py` глушит `langchain_core` и `langchain_openai`, `tests/test_neo4j_concepts.py`
>   берёт «LangChain» просто как имя концепта в фикстуре графа). Сверка: `git grep -i langchain`.
> - `mcp/mcp_gateway.py` был подписан «SSE gateway». Транспорт — Streamable HTTP (`FastMCP`,
>   эндпоинт `/mcp`), это сказано в докстринге самого файла.
> - `admin/frontend/` был показан как единственный `index.html`. Это 16-строчный загрузчик
>   Vue 3 SPA; код лежит в `js/`, `css/`, `vendor/`. Старая однофайловая админка на 3527 строк
>   лежит рядом как `admin/frontend-legacy.html`: никуда не смонтирована и в git не добавлена —
>   файл untracked, поэтому в выводе `git ls-files` его не будет.
> - Отсутствовали целиком: `crawl4ai/`, `gpt2giga-proxy/`, `services/`, `tests/`, `docs/`.
> - `.cursor/rules/` показан как часть репозитория. Его нет ни в git, ни на сервере — каталог
>   исключён в `.rsync-exclude` (секция «Local AI/editor tooling»).
> - Списки файлов внутри `mcp/tools`, `worker/services`, `worker/chains` и `scripts/` отставали
>   в разы: `scripts/` был показан семью строками при 64 файлах в git.

### Сервисы

Каждый — отдельный образ в `docker-compose.yml`. Код запечён в образ: правка Python требует
пересборки, `rsync` + `restart` её не подхватит.

| каталог | ответственность |
|---|---|
| `ingest/` | Сбор из источников. `main.py` — APScheduler, расписания и конфиг читает из БД, не из файлов. `sources/` — по коннектору на тип (telegram, rss, web, api, email, habr). `account_rotator.py` — прокси и ротация Telegram-аккаунтов, `source_runtime.py` — рантайм-состояние источника. |
| `worker/` | Обогащение. `main.py` — asyncio-супервизор трёх потребителей из `tasks/` (enrichment, vision, reindex). `chains/` — LLM-цепочки по шагу на файл, `prompts/` — их промпты в `.txt`. `services/` — аналитика поверх обогащённых данных (кластеризация, missing signals, ретро-петля, entity resolution, graph maintenance). `integrations/` — Qdrant и Neo4j. Клиенты LLM-провайдеров и guard'ы бюджета/квот/circuit breaker лежат плоско в корне `worker/`. |
| `mcp/` | Два процесса из одного каталога. `server.py` — REST-API инструментов на :8100 (FastAPI, по роутеру на файл в `tools/`). `mcp_gateway.py` — MCP-транспорт Streamable HTTP на :8102 поверх этого REST. `guards.py` — два guard'а для инструментов: SSRF-фильтр для приходящих на вход URL и allowlist `workspace_id`, чьи валидные слаги читаются из `config/workspaces.yml` (с зашитым fallback-списком на случай отсутствия файла в образе). |
| `admin/` | Панель управления. `backend/` — FastAPI: `routers/` по разделу UI, `services/` — фоновые задачи и внешние интеграции (алерты, балансы провайдеров, bootstrap конфигов), плюс `scheduler.py` и `manual_jobs.py`. `frontend/` — Vue 3 SPA без сборщика: `js/views`, `js/components`, `vendor/` с локальными копиями vue и cytoscape. Фронт смонтирован в контейнер как `./admin/frontend:/app/admin/frontend:ro` — правки видны без пересборки, в отличие от backend. |
| `crawl4ai/` | Веб-краулинг и обогащение ссылок. |
| `gpt2giga-proxy/` | Прокси OpenAI-совместимого API к GigaChat, порт 8090. |
| `services/` | Вспомогательные сервисы со своими образами: `paddleocr/` (OCR по HTTP; единственный сервис в репозитории с собственным `requirements.txt`) и `xray/` (egress-прокси). Шаблона конфига xray в репозитории нет: `render_config.py` — обёртка на 18 строк, печатающая в stdout JSON, который собирает из переменных окружения `build_xray_config` из `shared/xray_profile_registry.py`. Больше из `shared/` в образ xray ничего не копируется. |

### Общий код, схемы, конфиги

| каталог | ответственность |
|---|---|
| `shared/` | Общий слой ingest, worker, mcp, admin и crawl4ai. **Не всеми**: paddleocr, gpt2giga-proxy и процесс mcp-gateway не импортируют его вовсе, а из `services/` на него ссылается единственный файл — `xray/render_config.py`. Сверка: `git grep -l 'from shared\.' -- '*.py' \| cut -d/ -f1 \| sort -u`. Внутри: `config.py` (pydantic-settings — единственный легальный вход для секретов), `db.py`, `redis_streams.py`, `metrics.py`, `llm_routing.py` и `llm_control_plane.py` (маршрутизация провайдеров), `provenance.py`, `s3.py`. `events/` — схемы событий Redis Stream. **`models/` ничего не обслуживает**: файлы внутри импортируют только друг друга, ни один сервис на них не ссылается — слой описательный. |
| `storage/` | Схема хранилищ, а не данные. `postgres/init.sql` и `postgres/migrations/` (16 файлов, применяются скриптами из `scripts/`), `qdrant/collections.py`, `neo4j/constraints.cypher`. Данные Docker-томов исключены и из git, и из rsync — правила в `.gitignore` и `.rsync-exclude` не вырезают каталог целиком именно потому, что здесь живут эти файлы. |
| `config/` | YAML-дефолты бутстрапа: `workspaces.yml`, `sources.yml`, `enrichment_policy.yml`. Не bind-mount — копируются в образ (`COPY config/ /app/config/`), правка требует пересборки. Читает `admin/backend/services/bootstrap_configs.py` через UPSERT, поэтому изменение YAML переписывает уже существующие строки в БД. |

### Инфраструктура и обвязка

| каталог | ответственность |
|---|---|
| `prometheus/` | `prometheus.yml`, `alerts.yml`, `alertmanager.yml`. Монтируются **пофайлово**, а не каталогом — новый файл в этой папке контейнер не увидит. `prometheus/textfile/` в репозитории отсутствует: каталог создаёт cron на сервере, в `.rsync-exclude` он защищён отдельным правилом. |
| `grafana/` | Provisioning целиком: `dashboards/` (`frontier-runtime.json`, `frontier-rsi.json`) и `datasources/`. Монтируется каталогом, добавление дашборда пересборки не требует. |
| `searxng/` | `settings.example.yml` — шаблон в git, `limiter.toml` — настройки лимитера. Рабочий `settings.yml` с `secret_key` живёт только на сервере. |
| `scripts/` | 64 файла в git. Основные группы: сборка и развёртывание на сервере (`server-*.sh`), синхронизация с Windows (`sync-push.ps1`, `sync-pull.ps1`), разовые операции с данными (бэкфиллы, cutover алиасов Qdrant, обслуживание S3, миграции), бэкап и восстановление стека (`backup-stack.sh`, `restore-stack.sh`), экспорт метрик в textfile-коллектор node_exporter (`export-*.sh` — способ добавить метрику без пересборки образа), ежедневный разбор алертов (`alert-triage-*.sh`, `alert-watchdog.sh`). |
| `tests/` | pytest, 97 файлов в git: 93 штуки `test_<модуль>.py` плоским списком плюс `__init__.py`, `conftest.py`, `stub_policy.py` и `fixtures/`. Зарегистрированы только маркеры `unit` и `integration`. Прогонять внутри Docker-образа: host-окружение на Python 3.10 несовместимо. |
| `docs/` | 93 markdown-файла. Точка входа — `AUDIT-2026-08-04.md`. Датированная пометка статуса стоит в шапке только у документов, разошедшихся с реальностью; **отсутствие пометки означает «сверен и актуален»**, а не «забыли проставить». Подкаталоги: `harness/` и `chatgpt/` — самые крупные (27 и 23 файла), дальше `saas/` (13), `runbooks/`, `archive/`, `audit/`, `sre/`. |
| `tmp/` | Рабочий каталог для выгрузок и черновиков. В git попадает только `.gitkeep`, содержимое исключено и из git, и из rsync. |

В корне, помимо этого: **три** compose-файла (`docker-compose.yml`, `docker-compose.host-fixes.yml`
для проблемных хостов, `docker-compose.build-host-fix.yml` для BuildKit c `security.insecure`),
**пять** `.env*.example` (базовый, `balanced` и три `.env.mode.*`), `Makefile`, `pyproject.toml`,
`requirements-dev.txt` и `requirements-crawl4ai.txt`. Единого способа объявлять зависимости нет:
worker, ingest, admin, mcp и gpt2giga-proxy перечисляют пакеты инлайн в `pip install` внутри
своего Dockerfile, crawl4ai ставит корневой `requirements-crawl4ai.txt`, а у paddleocr есть
собственный `services/paddleocr/requirements.txt`. Сверка — `git grep -l 'pip install' -- '*Dockerfile*'`.

### Как получить актуальный состав

Источник правды — git, а он только на сервере (локальной копии `.git` нет):

```bash
# два уровня вложенности, только то, что реально в репозитории
ssh frontier-intelligence 'cd /opt/frontier-intelligence && git ls-files | cut -d/ -f1-2 | sort -u'
```

Локально, без git:

```powershell
Get-ChildItem D:\Workspace\frontier-intelligence -Depth 1 -Directory |
  Where-Object { $_.FullName -notmatch '__pycache__|\.git|\.ruff|\.pytest' } |
  ForEach-Object { $_.FullName.Replace("D:\Workspace\frontier-intelligence\", "") }
```

Рабочая копия **шире** репозитория с обеих сторон. `.claude/`, `.cursor/`, `.vscode/`, `.agents/`,
`CLAUDE.md`, `AGENTS.md` перечислены и в `.gitignore`, и в `.rsync-exclude`: в git их нет и через
rsync они не ездят.

> **«Исключено из синхронизации» ≠ «на сервере отсутствует».** Проверено 04.08.2026: в
> `/opt/frontier-intelligence` лежат `.claude/`, `.agents/`, `CLAUDE.md` и `AGENTS.md` — попали
> туда до появления правил исключения и с тех пор просто не обновляются. Реально отсутствуют
> на сервере только `.cursor/` и `.vscode/`. Проверка:
> `ssh frontier-intelligence 'cd /opt/frontier-intelligence && ls -d .claude .agents .cursor .vscode CLAUDE.md AGENTS.md 2>/dev/null'`

Обратное тоже верно: `searxng/settings.yml`, `sessions/`, `prometheus/textfile/` и `.bak`-снапшоты
существуют только на сервере. Полный перечень расхождений — в `.rsync-exclude`, у него
свой синтаксис (первое совпавшее правило побеждает, отмена задаётся `+ ` с пробелом,
а не `!`), и это описано в шапке самого файла.

---

## Переменные окружения

Полная форма — в [`../.env.example`](../.env.example). В git хранить только example-файлы; реальные `.env`, Telethon sessions и SearXNG `secret_key` остаются на сервере.

```bash
# PostgreSQL
POSTGRES_DB=frontier
POSTGRES_USER=frontier
POSTGRES_PASSWORD=<server-only-secret>

# GigaChat
GIGACHAT_CREDENTIALS=<server-only-secret>
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_BASE_URL=https://gigachat.devices.sberbank.ru/api/v1
# False — как в telegram-assistant при MITM; True — прод с доверенным CA в контейнере
GIGACHAT_VERIFY_SSL_CERTS=False
GIGACHAT_EMBEDDINGS_MODEL=EmbeddingsGigaR
EMBED_DIM=2560
GIGACHAT_MODEL=GigaChat-2
GIGACHAT_MODEL_PRO=GigaChat-2-Pro
GIGACHAT_VISION_MODEL=GigaChat-2-Pro

# S3 / Cloud.ru
S3_ENDPOINT_URL=https://s3.cloud.ru
S3_BUCKET_NAME=<server-bucket>
S3_REGION=ru-central-1
S3_ACCESS_KEY_ID=<server-only-secret>
S3_SECRET_ACCESS_KEY=<server-only-secret>

# Telegram аккаунт 0
TG_API_ID_0=<server-only-id>
TG_API_HASH_0=<server-only-secret>
# Telegram аккаунт 1
TG_API_ID_1=
TG_API_HASH_1=

# ── Telethon → Telegram DC (SOCKS5 или MTProxy) ─────────────────────────────
# gpt2giga-proxy (порт 8090) — только HTTP-доступ к GigaChat для worker; в Telethon НЕ подставлять.
# Удобная одна строка: TG_SOCKS5=HOST:PORT:USER:PASS (или TG_PROXY_DSN=…)
# Либо поля: TG_PROXY_HOST / TG_PROXY_PORT / TG_PROXY_USER / TG_PROXY_PASS
# Либо WG_SOCKS_HOST / WG_SOCKS_PORT (+ USER/PASS при необходимости) — см. .env.example
# MTProxy (имеет приоритет над SOCKS5 в env, если заданы host+secret):
MTPROXY_HOST=
MTPROXY_PORT=443
MTPROXY_SECRET=
# Жёстко: не стартовать ingest без прокси в environment (proxy_config только в БД не считается):
# TG_REQUIRE_PROXY=1
# После смены прокси в .env пересоздать контейнер:
#   docker compose --profile core --profile ingest up -d --force-recreate ingest
# В логах ingest ожидать «Telegram proxy: SOCKS5 …» или «MTProxy …», а не прямой DC без прокси.

# Neo4j
NEO4J_PASSWORD=<server-only-secret>

# Сервисы
MCP_PORT=8100
ADMIN_PORT=8101
GRAFANA_PASSWORD=<server-only-secret>

# SearXNG
SEARXNG_URL=http://searxng:8080
# searxng/settings.yml не коммитится; secret_key генерируется на сервере.
```

---

## Docker Compose профили

Профилей **десять**, и все 18 сервисов объявлены хотя бы под одним. `COMPOSE_PROFILES`
в серверном `.env` не задан, поэтому голый `docker compose up -d` поднимает **ноль**
сервисов, а `docker compose logs mcp` падает с обманчивым `no such service: gpt2giga-proxy`.
Без профилей работают только `docker compose ps` и `docker compose exec`.

| Профиль | Сервисы |
|---|---|
| `core` | postgres, redis, qdrant, neo4j, gpt2giga-proxy |
| `ingest` | ingest (telegram, rss, habr, web, api) |
| `xray` | xray (единственный egress наружу) |
| `worker` | worker (enrichment, vision, тренды, сигналы) |
| `crawl` | crawl4ai |
| `paddleocr` | paddleocr |
| `mcp` | mcp (8100, loopback), mcp-gateway (8102) |
| `admin` | admin (8101) |
| `searxng` | searxng (стартует и с профилем `worker`) |
| `monitor` | prometheus, alertmanager, grafana, node-exporter |

> **Профилей `rag` и `analytics` не существует.** Они фигурировали в `CLAUDE.md`
> и `AGENTS.md` до 04.08.2026; обе команды поднимали ноль сервисов.

**Набор нельзя урезать произвольно.** `ingest`, `admin` и `crawl4ai` объявляют
`depends_on: xray`, поэтому без профиля `xray` compose падает ещё до запуска:
`service "ingest" depends on undefined service "xray": invalid compose project`.
Именно на этом на 04.08.2026 ломались три из четырёх точек входа деплоя.

Рабочий набор:

```bash
export COMPOSE_PROFILES="core,ingest,xray,worker,crawl,paddleocr,mcp,admin,searxng,monitor"
docker compose up -d
```

Эталон для сборки — `scripts/server-build-stack.sh`. Проверять валидность набора
до деплоя: `COMPOSE_PROFILES=... docker compose config --services`.

## As-Built Runtime

Здесь была третья по счёту сводка фактической цепочки — после «Архитектуры» и
«Processing Pipeline». Она разошлась с обеими и с кодом: обещала `GigaChat Vision`
(vision давно ушёл на wormsoft), прямое имя коллекции `frontier_docs` вместо алиаса,
`SSE gateway on 8102` (транспорт — Streamable HTTP) и неполный список MCP-инструментов.

Копию убрали намеренно: три описания одного конвейера расходятся быстрее, чем
их успевают править. Источники правды теперь по одному на вопрос:

| Что нужно | Где смотреть |
|---|---|
| Как устроен конвейер, стадии и стримы | [«Processing Pipeline»](#processing-pipeline) выше |
| Общая схема сервисов | [«Архитектура»](#архитектура) выше |
| Что работает, а что частично | [«Статус реализации»](#статус-реализации) выше |
| Что запущено прямо сейчас | `ssh frontier-intelligence "cd /opt/frontier-intelligence && docker compose ps"` |
| Что не доделано, с доказательствами | [TODO-UNFINISHED.md](./TODO-UNFINISHED.md) |
| Расхождения документации со стеком | [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md) |

## Референс (telegram-assistant)

Старый проект используется только как справка. Переносить код напрямую нельзя без адаптации под `workspace_id`, текущие Redis Streams, SQLAlchemy async и отсутствие Supabase/Caddy/JWT/QR-auth.

| Что смотреть | Статус в Frontier Intelligence |
|---|---|
| Telethon iter_messages, account rotation, proxy handling | Adapted in `ingest/` |
| Vision/OCR ideas | Adapted in `worker/tasks/vision_task.py` and `services/paddleocr/` |
| gpt2giga-proxy contour | Kept as local `gpt2giga-proxy/` service |
| crawl4ai pattern | Kept as `crawl4ai/` service |
| Album assembler phases | Reference only; current ingest collapses albums into post/media events |
| Grafana album dashboard | Reference only; current dashboard is `grafana/dashboards/frontier-runtime.json` |
| EmbeddingsGigaR dim=2560 | Current env is `EMBED_DIM=2560` |
