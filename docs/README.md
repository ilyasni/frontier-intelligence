# Frontier Intelligence

## Recent Docs

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

## Current Runtime

- Runtime LLM integration uses `openai==1.54.4` against `gpt2giga-proxy`.
- Current production-safe routing:
  - `GIGACHAT_MODEL=GigaChat-2`
  - `GIGACHAT_MODEL_LITE=GigaChat-2`
  - `GIGACHAT_MODEL_RELEVANCE=GigaChat-2`
  - `GIGACHAT_MODEL_CONCEPTS=GigaChat-2`
  - `GIGACHAT_MODEL_MCP_SYNTHESIS=GigaChat-2`
  - `GIGACHAT_MODEL_PRO=GigaChat-2-Pro`
  - `GIGACHAT_VISION_MODEL=GigaChat-2-Pro`
- `GigaChat-2-Lite` must not be used in runtime config until it appears in `GET /v1/models` on the active proxy/upstream.
- `X-Session-ID` and `/tokens/count` are best-effort optimizations. If the current SDK/proxy pair rejects them, the pipeline should continue working without cache or token-count support.
- For Balanced rollout keep `INDEXING_MAX_CONCURRENCY=1` on this contour unless live metrics show enough headroom.
- Server snapshot on 2026-04-18: the live server tree under
  `/opt/frontier-intelligence` is the first git baseline/source of truth.
  Runtime-only files stay untracked (`.env`, `sessions/`,
  `searxng/settings.yml`, Docker volume data). Qdrant `frontier_docs`
  is green with dense+sparse config and live points; `trend_clusters` is
  used as a secondary vector index for PostgreSQL trend clusters.
- A historical crawl/vision reindex backfill was queued on 2026-04-18.
  Monitor it with `docker compose exec -T redis redis-cli XINFO GROUPS stream:posts:reindex`.
  During this backfill, worker-side GigaChat calls are intentionally
  throttled with `GIGACHAT_MIN_REQUEST_INTERVAL_MS=5000` to keep MCP
  searches from hitting upstream embedding rate limits.

Персональная система мониторинга и синтеза трендов.  
Собирает сигналы из множества источников, обрабатывает через LangChain/GigaChat,
хранит в векторной базе и графе знаний, отдаёт через MCP в Codex/Claude-проекты.

> **Один сервис — несколько рабочих пространств (workspace).**  
> Каждый Codex/Claude-проект подключается к одному MCP, но работает со своей изолированной базой данных.  
> Все настройки — источники, категории, веса, расписания — управляются через Admin UI.

---

## Концепция Workspace

```
Один стек frontier-intelligence
    │
    ├── workspace: disruption    → automotive, hmi, future mobility, ev
    │   └── Codex Project: visionary-designer
    │
    ├── workspace: ai_trends     → LLM, agents, AI tools, research, inference
    │   └── Codex Project: ai-researcher (будущий)
    │
    └── workspace: design        → design systems, UX patterns, visual culture
        └── Codex Project: design-director (будущий)
```

**Изоляция на уровне данных, не инфраструктуры:**
- Каждый документ, тренд, концепт помечен `workspace_id`
- Qdrant payload фильтрует по workspace при каждом запросе
- Neo4j: `(:Workspace)` — отдельный узел-корень для каждого пространства
- PostgreSQL: `workspace_id TEXT NOT NULL` во всех основных таблицах

**Cross-workspace сигналы:**
- `get_frontier_brief` может тянуть из нескольких workspace одновременно
- Тренд из `ai_trends` может попасть в `disruption` если релевантен обоим
- Управляется через Admin UI: настройка cross-workspace bridges

---

## Статус реализации

| Компонент | Статус | Заметки |
|---|---|---|
| Core infra (postgres, redis, qdrant, neo4j, gpt2giga-proxy) | ✅ Работает | |
| Ingest: telegram, rss, habr, web | ✅ Работает | Album dedup, account rotation, DB proxy |
| Worker: relevance → concepts → valence → embeddings → Qdrant | ✅ Работает | Hybrid dense + BM25 sparse via fastembed |
| Worker: Neo4j concept graph | ✅ Работает | Inline write from enrichment; separate `graph_task` is not used |
| Admin UI: workspaces, sources, pipeline, search | ✅ Работает | |
| MCP: search, observability, clusters, source health, ingest_url | ✅ Работает | REST tools on `mcp:8100`, SSE gateway on `8102` |
| Vision pipeline (S3, GigaChat Vision, PaddleOCR) | ✅ Работает | `stream:posts:vision`; media albums are collapsed in ingest, no separate album assembler |
| Semantic/trend/emerging clusters | ✅ Работает | PostgreSQL is canonical; stable trend clusters are mirrored to Qdrant `trend_clusters` as a secondary vector index |
| Missing signals | ✅ Работает | SearXNG gap analysis persists `missing_signals` |
| Prometheus + Grafana dashboards | ✅ Работает | Runtime dashboard provisioned |

---

## Архитектура

```
┌──────────────────────────────────────────────────────────────────────┐
│                          SOURCES LAYER                               │
│                                                                      │
│  ┌──────────────────┐  ┌─────────────┐  ┌────────────────────────┐  │
│  │    Telegram      │  │  RSS / Atom │  │   Web (crawl4ai)       │  │
│  │    Telethon      │  │  feedparser │  │   Habr, Yanko, кастом  │  │
│  │    2 аккаунта    │  │             │  │                        │  │
│  │    MTProxy/SOCKS5│  │             │  │                        │  │
│  └────────┬─────────┘  └──────┬──────┘  └──────────┬────────────┘  │
└───────────┼──────────────────-┼──────────────────── ┼──────────────┘
            └──────────────────-▼─────────────────────┘
                       Redis Stream: stream:posts:parsed
                       payload: {content, source_id, workspace_id, ...}
                                  │
┌─────────────────────────────────▼────────────────────────────────────┐
│                        PROCESSING LAYER                              │
│                        LangChain + GigaChat via gpt2giga-proxy       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  EnrichmentTask (LangChain RunnableBranch)                     │  │
│  │  → Relevance filter: score + category (GigaChat)               │  │
│  │  → Tagging & workspace routing                                 │  │
│  │  → Concept extraction → Neo4j (NER/NEL)                        │  │
│  │  → Embeddings (EmbeddingsGigaR, 2560d) → Qdrant               │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Vision Pipeline (посты с медиа и альбомы)                     │  │
│  │  → GigaChat Vision: labels, OCR, scene, design signals         │  │
│  │  → S3/Cloud.ru: медиа + vision_summary_v1.json (gzip)          │  │
│  │  → Album Assembler: event-driven агрегация (4 фазы)            │  │
│  │  → Vision vectors → Qdrant, visual concepts → Neo4j            │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Trend Detection (два параллельных пайплайна)                  │  │
│  │  Reactive (cron 30 мин):  burst_score — всплески частоты       │  │
│  │  Stable   (cron сутки):   HDBSCAN семантическая кластеризация  │  │
│  │  → TrendCluster {title, insight, opportunity,                  │  │
│  │      burst_score, coherence, novelty}                          │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Missing Signals (cron сутки)                                  │  │
│  │  → SearXNG: "нормальное" распределение тем по workspace        │  │
│  │  → Gap analysis: что должно быть, но отсутствует в базе        │  │
│  └────────────────────────────────────────────────────────────────┘  │
└─────────────────────────┬────────────────────────────────────────────┘
                          │
          ┌───────────────┼────────────┬──────────────┐
          ▼               ▼            ▼              ▼
       Qdrant           Neo4j     PostgreSQL       S3/Cloud.ru
    hybrid RAG        GraphRAG    raw + meta      media + vision
    dense+BM25      community     posts,          images,
    dim=2560        detection     clusters,       album summaries,
    per-workspace   NER/NEL       indexing,       OCR results
                    workspaces    workspaces
          │               │            │
┌─────────▼───────────────▼────────────▼────────────────────────────────┐
│                            MCP SERVER                                  │
│                                                                        │
│  search_frontier       search_balanced      list_clusters              │
│  list_sources_health   get_pipeline_stats   get_signal_timeline        │
│  list_missing_signals  get_source_details   ingest_url                 │
│                                                                        │
│  все инструменты: workspace-aware, GigaChat pre-synthesis внутри      │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                    ┌───────────────┼──────────────────┐
                    ▼               ▼                  ▼
           Codex Project     Codex Project      Codex Project
           visionary-        ai-researcher      design-director
           designer          (будущий)          (будущий)
```

---

## Технологический стек

| Компонент | Образ / Библиотека | Роль |
|---|---|---|
| **gpt2giga-proxy** | локальная сборка, порт 8090 | GigaChat через OpenAI API интерфейс |
| **LangChain / gigachain** | pip | Оркестрация цепочек обработки |
| **Redis** | redis:7-alpine | Event bus (Streams), кэш эмбеддингов TTL 7д |
| **PostgreSQL** | postgres:16-alpine | Посты, TrendCluster, IndexingStatus |
| **Qdrant** | qdrant/qdrant:latest | Hybrid search dense 2560d + sparse BM25 |
| **Neo4j** | neo4j:5.15-community | GraphRAG, NER/NEL, community detection |
| **S3 / Cloud.ru** | boto3, path-style | Медиафайлы, vision summaries gzip |
| **crawl4ai** | локальная сборка, 2GB mem | Web-краулинг источников |
| **SearXNG** | searxng/searxng | Self-hosted поиск для missing signals |
| **Prometheus + Grafana** | standard | Метрики пайплайна и дашборды |
| **Admin UI** | FastAPI + Vanilla JS | Управление всем: workspace, источники, темы |

**Не используется из telegram-assistant:**
Supabase стек (kong/postgrest/studio/meta), Caddy, JWT/QR-auth,
Mini App, мультитенантность (RLS), Telegram Bot, SaluteSpeech.

---

## Admin UI — центр управления

Все настройки системы живут в Admin UI, не в конфигурационных файлах.
`config/` содержит только дефолты при первом запуске — после этого источник правды это БД.

**API workspaces:** `PATCH /api/workspaces/{id}` — частичное обновление (Pydantic `exclude_unset`); `relevance_threshold` **сливается** в JSON `relevance_weights` без удаления весов по категориям (`technology`, `design`, …). `POST /api/workspaces` при upsert делает то же слияние.

**Приёмка после смены порога/категорий:** логи worker на `relevance_category_unknown`; `GET /api/pipeline/stats`; кэш workspace в worker обновляется с шагом до ~90 с.

**Кластеры vs category:** семантические `trend_clusters` и поле `posts.category` — разные оси; кластер не должен жёстко подменяться ненадёжной классификацией поста (см. комментарий в модели `TrendCluster`).

### Разделы

```
/                     Dashboard
                      ├── Статистика по workspace (документов, трендов, сигналов)
                      ├── Pipeline health (очереди, latency, ошибки)
                      └── Последние TrendCluster по каждому workspace

/workspaces           Управление рабочими пространствами
                      ├── Создать / редактировать workspace
                      ├── Категории и веса relevance: {automotive: 0.9, hmi: 0.8}
                      ├── Cross-workspace bridges (какие ws делятся сигналами)
                      └── Design lenses для интерпретации (automotive, ai, design)

/sources              Источники данных
                      ├── Список всех источников с workspace и статусом
                      ├── Добавить источник: [TG | RSS | Habr | Web]
                      │   ├── Workspace: [disruption | ai_trends | design]
                      │   ├── Категории: [automotive] [hmi] [design] [tech]
                      │   ├── Расписание: [15 мин | 1 час | 1 день]
                      │   ├── Telegram аккаунт: [0 | 1]
                      │   └── [Сохранить] [Тест-запрос]
                      └── Управление TG аккаунтами и прокси

/clusters             Семантические кластеры, тренды, emerging/missing signals
                      ├── Фильтр: workspace / категория / pipeline / период
                      ├── TrendCluster card: title + insight + opportunity
                      └── Timeline: динамика кластера по времени

/search               Поиск по базе
                      ├── search_frontier
                      └── search_balanced

/pipeline             Управление обработкой
                      ├── Статус IndexingStatus по постам
                      ├── Ручной запуск semantic/signal analysis
                      └── DLQ: проблемные документы
```

### Что управляется через UI (не через конфиги)

| Настройка | Где в UI |
|---|---|
| Workspace: категории и веса | /workspaces |
| Cross-workspace bridges | /workspaces |
| Design lenses per workspace | /workspaces |
| Список источников, расписания | /sources |
| TG аккаунты, MTProxy / SOCKS5 | /sources |
| Релевантность: threshold (0.6) | /workspaces |
| Trend detection: окно, параметры | /pipeline |
| Missing signals: темы для gap analysis | /signals |

---

## База данных

### PostgreSQL

```sql
-- Рабочие пространства
CREATE TABLE workspaces (
    id              TEXT PRIMARY KEY,  -- 'disruption' | 'ai_trends' | 'design'
    name            TEXT NOT NULL,
    categories      TEXT[],
    relevance_weights JSONB,           -- {"automotive": 0.9, "hmi": 0.8}
    design_lenses   TEXT[],            -- интерпретационные углы
    cross_workspace TEXT[],            -- workspace_ids для cross-signal
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Источники
CREATE TABLE sources (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    type         TEXT NOT NULL,   -- 'telegram' | 'rss' | 'habr' | 'web'
    name         TEXT NOT NULL,
    url          TEXT,
    tg_channel   TEXT,
    tg_account   INTEGER DEFAULT 0,
    categories   TEXT[],
    enabled      BOOLEAN DEFAULT true,
    schedule     TEXT DEFAULT '*/30 * * * *',
    last_parsed  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ DEFAULT now()
);

-- Документы
CREATE TABLE posts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     TEXT NOT NULL REFERENCES workspaces(id),
    source_id        UUID REFERENCES sources(id),
    external_id      TEXT,
    content          TEXT NOT NULL,
    url              TEXT,
    grouped_id       BIGINT,       -- Telegram album ID
    has_media        BOOLEAN DEFAULT false,
    published_at     TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ DEFAULT now(),
    relevance_score  FLOAT,
    categories       TEXT[],
    UNIQUE(source_id, external_id)
);

-- Медиа (content-addressed)
CREATE TABLE media_objects (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_sha256 TEXT UNIQUE NOT NULL,
    s3_key      TEXT NOT NULL,       -- media/{sha256[:2]}/{sha256}
    mime        TEXT,
    size_bytes  BIGINT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE post_media_map (
    post_id     UUID REFERENCES posts(id),
    file_sha256 TEXT REFERENCES media_objects(file_sha256),
    PRIMARY KEY (post_id, file_sha256)
);

-- Альбомы (из telegram-assistant Phase 1-4)
CREATE TABLE media_groups (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    source_id       UUID REFERENCES sources(id),
    grouped_id      BIGINT NOT NULL,
    album_kind      TEXT,            -- 'photo' | 'video' | 'mixed'
    items_count     INTEGER,
    caption_text    TEXT,
    cover_media_id  UUID REFERENCES media_objects(id),
    posted_at       TIMESTAMPTZ,
    meta            JSONB DEFAULT '{}'::jsonb,  -- vision_summary, labels, OCR, s3_key
    UNIQUE(source_id, grouped_id)
);

CREATE TABLE media_group_items (
    group_id         UUID REFERENCES media_groups(id),
    post_id          UUID REFERENCES posts(id),
    position         INTEGER,
    media_object_id  UUID REFERENCES media_objects(id),
    media_kind       TEXT,
    meta             JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY (group_id, position)
);

-- Статус индексирования
CREATE TABLE indexing_status (
    post_id                  UUID PRIMARY KEY REFERENCES posts(id),
    embedding_status         TEXT DEFAULT 'pending',
    graph_status             TEXT DEFAULT 'pending',
    vision_status            TEXT DEFAULT 'pending',
    processing_started_at    TIMESTAMPTZ,
    processing_completed_at  TIMESTAMPTZ,
    error_message            TEXT,
    retry_count              INTEGER DEFAULT 0
);

-- Тренд-кластеры
CREATE TABLE trend_clusters (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    cluster_key  TEXT NOT NULL,
    pipeline     TEXT NOT NULL,    -- 'reactive' | 'stable'
    title        TEXT NOT NULL,
    summary      TEXT,
    -- Слой Insight → Opportunity → Concept
    insight      TEXT,             -- "UI перестает быть экраном → становится пространством"
    opportunity  TEXT,             -- "in-car navigation как spatial layer"
    time_horizon TEXT,             -- "2-4 года"
    keywords     JSONB,
    burst_score  FLOAT,
    coherence    FLOAT,
    novelty      FLOAT,
    category     TEXT,
    doc_ids      UUID[],
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now(),
    UNIQUE(workspace_id, cluster_key)
);

-- Missing signals
CREATE TABLE missing_signals (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    topic        TEXT NOT NULL,
    expected_context TEXT,         -- почему тема должна быть
    gap_score    FLOAT,            -- насколько сильно отсутствует
    opportunity  TEXT,             -- что из этого следует
    detected_at  TIMESTAMPTZ DEFAULT now()
);

-- Обогащение постов
CREATE TABLE post_enrichment (
    post_id              UUID REFERENCES posts(id),
    kind                 TEXT NOT NULL,  -- 'tags'|'vision'|'ocr'|'concepts'
    data                 JSONB,
    enrichment_provider  TEXT,
    updated_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (post_id, kind)
);
```

### Qdrant — коллекции

```python
# Основная: hybrid search per workspace
# dense (EmbeddingsGigaR, 2560d) + sparse (BM25)
# Все запросы фильтруются по workspace_id

Collection("frontier_docs",
    vectors={
        "dense":  {"size": 2560, "distance": "Cosine"},
        "sparse": {}   # BM25
    },
    payload_schema={
        "workspace_id":    "keyword",   # фильтр по умолчанию
        "source_type":     "keyword",
        "category":        "keyword",
        "published_at":    "datetime",
        "relevance_score": "float",
        "concepts":        "keyword[]",
        "tags":            "keyword[]",
        "valence":         "keyword",
        "signal_type":     "keyword",
        "source_region":   "keyword",
        "source_score":    "float"
    }
)

# Тренд-кластеры
Collection("trend_clusters",
    # PostgreSQL is source of truth; Qdrant is a searchable secondary index.
    # Dense vector is the centroid of member post vectors.
    vectors={"dense": {"size": 2560, "distance": "Cosine"}},
    payload_schema={
        "workspace_id": "keyword",
        "pipeline":     "keyword",
        "signal_stage": "keyword",
        "keywords":     "keyword[]",
        "burst_score":  "float",
        "signal_score": "float",
        "detected_at":  "datetime",
        "doc_count":    "integer",
        "source_count": "integer"
    }
)
```

### Neo4j — граф концептов

```cypher
// Узлы
(:Workspace    {id, name, categories[]})
(:Concept      {name, category, first_seen, mentions})
(:Document     {id, url, published_at, source_type, has_media})
(:Source       {name, type, workspace_id})
(:TrendCluster {id, title, insight, opportunity, burst_score})
(:Album        {grouped_id, album_kind, items_count})

// Связи
(:Workspace)   -[:CONTAINS]->          (:Concept)
(:Document)    -[:MENTIONS   {weight}]->(:Concept)
(:Concept)     -[:RELATED_TO {strength, co_occ}]->(:Concept)
(:Concept)     -[:EVOLVED_FROM {year}]->(:Concept)
(:Document)    -[:FROM_SOURCE]->        (:Source)
(:TrendCluster)-[:CONTAINS   {rank}]->  (:Concept)
(:Album)       -[:CONTAINS   {position}]->(:Document)
// Cross-workspace связи
(:Concept)     -[:BRIDGES    {strength}]->(:Concept)
```

---

## MCP Tools

Текущая реализация — REST tools в `mcp/server.py` + SSE gateway в `mcp/mcp_gateway.py`.

Общие правила:

- почти все аналитические инструменты принимают `workspace`;
- поиск использует Qdrant hybrid dense + sparse и `workspace_id` filter;
- `synthesize=true` включает GigaChat-синтез там, где он поддержан;
- Admin UI тоже ходит в эти tools через внутренний `mcp:8100`.

Текущие tools:

| Tool | Назначение |
|---|---|
| `search_frontier` | Hybrid search по `frontier_docs` |
| `search_balanced` | Growth/counter-signal/RU verification/competitor-aware search |
| `search_trend_clusters` | Semantic search по Qdrant `trend_clusters` |
| `search_by_vision` | Поиск по Vision labels/OCR/scenes в `post_enrichments(kind='vision')` |
| `get_concept_graph` | Neo4j concept graph / concept-centered subgraph |
| `get_frontier_brief` | Multi-workspace brief из overview, clusters, weak/emerging, missing signals |
| `ingest_url` | Поставить URL в `stream:posts:crawl` для существующего `post_id` |
| `list_workspaces` | Workspace registry |
| `list_sources_health` | Source score, health, last run, quality tier |
| `get_pipeline_stats` | Статусы ingest/enrichment и recent posts |
| `get_workspace_overview` | Сводка workspace: posts, sources, clusters |
| `list_clusters` | Semantic/trend/emerging clusters |
| `list_emerging_signals` | Emerging signals по стадиям |
| `list_missing_signals` | SearXNG gap-analysis findings |
| `get_cluster_details` | Полная карточка semantic/trend/emerging/missing |
| `get_missing_signal_details` | Missing-signal details + external evidence |
| `get_source_details` | Source runs and recent posts |
| `get_cluster_evidence` | Evidence posts for a cluster |
| `get_signal_timeline` | Time-series points for semantic/trend/emerging entities |

---

## Processing Pipeline

### As-built (текущий код)

Цепочка **фактически** такая (см. `worker/main.py`, `ingest`, `crawl4ai`):

```
ingest (Telegram / др.)
  → stream:posts:parsed
       ├→ EnrichmentTask: пост в PostgreSQL, релевантность, концепты, теги,
       │   эмбеддинг по тексту поста → Qdrant, концепты → Neo4j,
       │   post_enrichments (concepts, tags), stream:posts:enriched (observer stream),
       │   при наличии linked_urls → stream:posts:crawl
       ├→ VisionTask (параллельно): GigaChat Vision → post_enrichments (vision) → stream:posts:reindex
       └→ ReindexTask: stream:posts:reindex → enriched text embedding → Qdrant patch + Neo4j concepts

crawl4ai сервис
  → stream:posts:crawl → crawl → post_enrichments (kind='crawl') → stream:posts:reindex
```

- **`url` в событии** — permalink поста в Telegram (валидация источника). **`linked_urls`** — внешние HTTP(S) ссылки из текста и Telethon entities (без `t.me` / Twitter/X), до 10 шт.; **только они** уходят в crawl4ai.
- **Crawl/Vision reindex**: после сохранения `post_enrichments(kind='crawl'|'vision')` сервисы публикуют `stream:posts:reindex`; `ReindexTask` в worker обновляет canonical Qdrant point enriched-текстом и дописывает crawl/vision concepts в Neo4j.
- Ни **`AlbumAssemblerTask`**, ни отдельный legacy **`GraphTask`** на `stream:posts:enriched` в этом репозитории **не подключены**; альбомы на стороне ingest сворачиваются в один `PostParsedEvent` с `grouped_id` и несколькими `media_urls`, а граф обновляется inline + через `ReindexTask`.

Паттерны полного альбомного пайплайна можно смотреть в [telegram-assistant](https://github.com/ilyasni/telegram-assistant) и в локальном архиве `docs/old_docs ilyasni-telegram-assistant.git/` — переносить только совместимое со стеком Frontier Intelligence (`workspace_id`, `stream:posts:*`, без Supabase/Caddy/Bot из legacy).

### Redis Streams (сводка)

| Stream | Consumer |
|--------|----------|
| `stream:posts:parsed` | `EnrichmentTask` |
| `stream:posts:vision` | `VisionTask` |
| `stream:posts:crawl` | `crawl4ai` (`Crawl4AIService`) |
| `stream:posts:reindex` | `ReindexTask` |
| `stream:posts:enriched` | observer stream для downstream-интеграций; отдельного worker-task нет |

### EnrichmentTask (логика)

Релевантность (GigaChat) → при прохождении порога: концепты, эмбеддинг текста поста, upsert в Qdrant, концепты в Neo4j, `post_enrichments`; при `linked_urls` — сообщение в `stream:posts:crawl`. Нерелевантные посты помечаются `dropped`, точка в Qdrant удаляется при наличии.

### Vision (текущий)

`ingest/sources/telegram_source.py` загружает медиа в S3 → `stream:posts:vision` → `worker/tasks/vision_task.py`: скачивание из S3 → **GigaChat Vision** (labels, scene, `ocr_text` в JSON) → при заданном `PADDLEOCR_URL` дополнительно **PaddleOCR** `POST /v1/ocr/upload` (тот же контракт, что в [telegram-assistant](https://github.com/ilyasni/telegram-assistant) `services/paddleocr`) → в enrichment попадают `paddle_ocr_text` и объединённый `ocr_text` → `post_enrichments` (`kind='vision'`), `indexing_status.vision_status`. Сервис: `docker compose --profile paddleocr up -d paddleocr`, в `.env` у worker: `PADDLEOCR_URL=http://paddleocr:8008`.

### Проверка e2e после деплоя

1. **Деплой**: правки в рабочей копии → Sync → Server (`scripts/sync-push.ps1` или Task); на сервере при смене Dockerfile/зависимостей — `docker compose build`, иначе `docker compose restart` для `worker`, `ingest`, `crawl4ai`, `admin`. Новая схема БД: с рабочей машины можно одним шагом `.\scripts\push-and-remote-migrate.ps1` (rsync через WSL + SSH + `scripts/server-apply-sql-migrations.sh` в Postgres-контейнере и restart сервисов). Либо только на сервере: `cd /opt/frontier-intelligence && bash scripts/server-apply-sql-migrations.sh`. Альтернатива с хоста: `bash scripts/apply-postgres-migrations.sh` при доступном `psql` и `DATABASE_URL`. Существующая БД не обновляется от одного `init.sql`.
2. **PostgreSQL**: `SELECT post_id, kind FROM post_enrichments WHERE kind IN ('vision','crawl') ORDER BY updated_at DESC LIMIT 20;`
3. **Логи**: `worker` — строки `Enriched`, `Reindex done`; `crawl4ai` — `Crawl enrichment saved`; worker vision — `Vision done`.
4. **Qdrant**: точки появляются для **релевантных** постов (`indexing_status.embedding_status = 'done'`), не для dropped. Crawl/Vision enrichment патчит существующую точку через `stream:posts:reindex`; existing enriched rows can be queued with `python scripts/enqueue_reindex_enriched_posts.py --kind crawl --kind vision --limit 1000`. Stable trend clusters mirror to `trend_clusters` after cluster/signal analysis; existing rows can be backfilled with `python scripts/sync_trend_clusters_to_qdrant.py`.
5. **Redis**: при недоступности `docker exec`, при открытом порте с хоста — `redis-cli` (длина стримов, необязательно).
6. **Admin API**: `/pipeline/stats`, повторная постановка `POST /pipeline/reprocess/{post_id}` (передаёт `media_urls` и `linked_urls`, извлечённые из `content`).

### Trend Detection

```
Reactive (cron 30 мин):
  Посты за последние N часов
  burst_score = (current_freq - baseline) / baseline
  GigaChat: title + insight + opportunity для топ-кластеров
  → TrendCluster {pipeline: "reactive", burst_score: 3.7}

Stable (cron сутки):
  Embeddings из Qdrant за 7-30 дней
  HDBSCAN кластеризация
  GigaChat: title + insight + opportunity + time_horizon
  coherence = avg cosine similarity внутри кластера
  novelty = 1 - overlap с предыдущим периодом
  GraphRAG: связать кластеры с существующим графом
  → TrendCluster {pipeline: "stable", coherence: 0.91, novelty: 0.88}

Missing Signals (cron сутки):
  SearXNG: поиск по теме workspace → "нормальный" корпус
  Сравнение с frontier_docs по workspace
  Gap: темы с высоким expected, низким actual presence
  GigaChat: gap → opportunity формулировка
  → missing_signals table
```

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

```
frontier-intelligence/
│
├── docker-compose.yml
├── docker-compose.host-fixes.yml
├── .env.example
├── .env.balanced.example
├── .cursor/rules/
├── Makefile
├── pyproject.toml
│
├── ingest/
│   ├── Dockerfile
│   ├── main.py                  # APScheduler, конфиг из БД (не из файлов)
│   ├── account_rotator.py       # MTProxy/SOCKS5/xray, 2 аккаунта
│   ├── scheduler.py             # расписания из sources table
│   ├── sources/
│   │   ├── base.py
│   │   ├── telegram_source.py   # Telethon, iter_messages(), album-aware
│   │   ├── rss_source.py
│   │   ├── web_source.py
│   │   ├── api_source.py
│   │   └── email_source.py
│
├── worker/
│   ├── Dockerfile
│   ├── main.py                  # supervisor enrichment + vision + reindex
│   ├── tasks/
│   │   ├── enrichment_task.py   # relevance/concepts/valence/embed/Qdrant/Neo4j
│   │   ├── reindex_task.py      # crawl/vision enrichment → Qdrant patch + Neo4j
│   │   └── vision_task.py       # S3 media → GigaChat Vision + PaddleOCR
│   ├── chains/
│   │   ├── relevance_chain.py
│   │   ├── concept_chain.py
│   │   ├── relevance_concepts_chain.py
│   │   └── valence_chain.py
│   ├── services/
│   │   ├── semantic_clustering.py
│   │   ├── missing_signals.py
│   │   └── searxng_client.py
│   ├── integrations/
│   │   ├── qdrant_client.py     # hybrid search, workspace filter
│   │   └── neo4j_client.py      # concepts graph
│   ├── gigachat_client.py       # LangChain GigaChat + EmbeddingsGigaR
│   └── prompts/
│       ├── relevance.txt
│       ├── concepts.txt
│       ├── relevance_concepts.txt
│       └── valence.txt
│
├── mcp/
│   ├── Dockerfile
│   ├── Dockerfile.gateway
│   ├── server.py                # REST MCP tools
│   ├── mcp_gateway.py           # SSE gateway
│   └── tools/
│       ├── search_frontier.py
│       ├── search_balanced.py
│       ├── search_trend_clusters.py
│       ├── search_by_vision.py
│       ├── graph.py
│       ├── frontier_brief.py
│       ├── observability.py
│       └── ingest_url.py
│
├── admin/
│   ├── Dockerfile
│   ├── backend/
│   │   ├── main.py
│   │   ├── scheduler.py
│   │   ├── manual_jobs.py
│   │   ├── services/
│   │   └── routers/
│   │       ├── workspaces.py    # CRUD workspace + настройки
│   │       ├── sources.py       # CRUD источников + TG аккаунты
│   │       ├── pipeline.py      # статус, DLQ, ручной запуск
│   │       ├── clusters.py      # semantic/trend/emerging/missing
│   │       └── search.py        # поиск через MCP tools
│   └── frontend/
│       └── index.html
│
├── shared/
│   ├── events/
│   │   ├── posts_parsed_v1.py
│   │   ├── posts_vision_v1.py
│   │   ├── albums_parsed_v1.py
│   │   └── album_assembled_v1.py
│   └── models/
│
├── storage/
│   ├── postgres/init.sql
│   ├── postgres/migrations/
│   ├── qdrant/collections.py
│   └── neo4j/constraints.cypher
│
├── config/                      # Дефолты при первом запуске
│   ├── workspaces.yml           # стартовые workspace
│   ├── sources.yml              # стартовые источники
│   └── enrichment_policy.yml
│
├── searxng/
│   ├── settings.example.yml      # tracked template
│   └── settings.yml              # ignored/server-local secret_key
│
├── prometheus/
│   ├── prometheus.yml
│   ├── alerts.yml
│   └── alertmanager.yml
├── grafana/dashboards/
│   └── frontier-runtime.json
│
└── scripts/
    ├── sync-push.ps1 / sync-pull.ps1
    ├── server-build-stack.sh
    ├── server-deploy-rebuild.sh
    ├── server-ensure-searxng-secret.sh
    ├── run_semantic_clustering.py
    ├── sync_trend_clusters_to_qdrant.py
    └── init_storage.py
```

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

```
core      — postgres, redis, qdrant, neo4j, gpt2giga-proxy
ingest    — ingest сервис (TG + RSS + Habr)
worker    — LangChain worker (enrichment + vision + trends + signals)
mcp       — MCP сервер (8100)
admin     — Admin UI (8101)
crawl     — crawl4ai (web источники)
searxng   — self-hosted поиск (missing signals, стартует вместе с `worker` profile)
monitor   — prometheus + grafana

Запуск:
docker compose \
  --profile core \
  --profile ingest \
  --profile worker \
  --profile mcp \
  --profile admin \
  up -d
```

Если на хосте **`docker compose build`** падает с **AppArmor / runc** или BuildKit (**`security.insecure` / entitlement**), см. **[docs/server-docker-apparmor.md](server-docker-apparmor.md)**; быстрый путь — **`scripts/server-build-ingest-fix.sh`** (`DOCKER_BUILDKIT=0`), опционально override **`docker-compose.build-host-fix.yml`**.

---

## As-Built Runtime

Фактическая цепочка на сервере:

```
1. Core infrastructure
   postgres + redis + qdrant + neo4j + gpt2giga-proxy
   scripts/init_storage.py: Qdrant collections, Neo4j constraints, workspace bootstrap

2. Ingest
   telegram_source.py (2 accounts, MTProxy/SOCKS5/xray, iter_messages, album-aware)
   rss_source.py / web_source.py / api_source.py / email_source.py
   all sources emit PostParsedEvent into stream:posts:parsed

3. Worker enrichment
   EnrichmentTask: relevance → concepts → valence → EmbeddingsGigaR
   writes PostgreSQL post_enrichments/indexing_status, Qdrant frontier_docs, Neo4j concepts

4. Vision
   ingest uploads media to S3 and emits stream:posts:vision
   VisionTask: S3 media → GigaChat Vision → optional PaddleOCR → post_enrichments(kind='vision')
   ReindexTask patches Qdrant/Neo4j after vision and crawl enrichments
   no separate AlbumAssemblerTask in this repository

5. Admin UI and scheduler
   CRUD sources/workspaces, pipeline reprocess, source scoring
   scheduled/manual semantic clustering and signal analysis

6. Clusters and signals
   semantic_clusters, trend_clusters, emerging_signals, missing_signals in PostgreSQL
   missing signals use SearXNG through server-local settings.yml

7. MCP
   REST tools on 8100, SSE gateway on 8102
   search_frontier/search_balanced/search_trend_clusters/search_by_vision
   get_concept_graph/get_frontier_brief plus observability and cluster/source tools

8. Observability
   Prometheus + Alertmanager + Grafana frontier-runtime dashboard
```

---

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
