# Чеклист E2E: stream → enrichment → Qdrant

Когда в Qdrant `points_count=0` при живом worker, пройди шаги по порядку.

## 1. Redis Stream `stream:posts:parsed`

```bash
docker compose exec redis redis-cli XLEN stream:posts:parsed
docker compose exec redis redis-cli XINFO GROUPS stream:posts:parsed
docker compose exec redis redis-cli XPENDING stream:posts:parsed enrichment_workers
```

- **XLEN ≈ 0** — в стрим не попадают события (ingest/источники).
- Группа **`enrichment_workers`** должна существовать (создаётся worker при старте).
- Высокий **XPENDING** — зависшие сообщения; смотри логи worker и перезапуск consumer.

Если `docker exec` недоступен, см. `CLAUDE.md` (раздел Redis без exec) и `docs/ops-server-troubleshooting.md` (AppArmor, `docker-compose.host-fixes.yml`).

## 2. Логи worker

```bash
docker compose logs -f worker
```

Ищи строки:

- **`Dropped`** (`logger.debug`) — пост отфильтрован по релевантности (порог `relevance_weights.threshold` в workspace).
- **`Enriched`** — успешное обогащение; дальше должны быть эмбеддинги и upsert в Qdrant.
- Ошибки GigaChat, БД, Qdrant — цепочка обрывается до upsert.

## 3. PostgreSQL

Проверь последние посты и статусы индексации (имена таблиц могут отличаться в миграциях — сверь со схемой):

- `posts` / обогащённые поля по `workspace_id`
- `indexing_status`: `embedding_status` (`pending`, `done`, `dropped`, `error`), при необходимости — связанные поля обогащения в `post_enrichments`

Пример (адаптируй под свои таблицы):

```sql
SELECT post_id, embedding_status, error_message, updated_at
FROM indexing_status
ORDER BY updated_at DESC
LIMIT 30;
```

For source-ingest troubleshooting, also check runtime tables:

```sql
SELECT source_id, status, started_at, finished_at, fetched_count, emitted_count
FROM source_runs
ORDER BY started_at DESC
LIMIT 30;

SELECT source_id, last_success_at, last_error, last_seen_published_at
FROM source_checkpoints
ORDER BY updated_at DESC
LIMIT 30;
```

- Many recent `error` rows for one source usually mean connectivity, parser drift, or proxy issues.
- Repeated `running` rows should no longer accumulate; if they do, verify the ingest image is up to date.
- If `posts.preview` looks HTML-ish, verify the source is using the current ingest build with RSS HTML normalization.

## 4. Qdrant

Коллекция по умолчанию: `QDRANT_COLLECTION` / `frontier_docs`.

```bash
curl -s "http://localhost:6333/collections/frontier_docs" | jq .
```

Смотри `points_count`, `indexed_vectors_count`, `config.params.sparse_vectors`. Фильтрация по `workspace_id` в payload — нулевой счётчик при «всё dropped» ожидаем.

Production snapshot на 2026-04-17: `frontier_docs` green, dense 2560d + sparse, `points_count` > 17k. `trend_clusters` is a secondary Qdrant index for stable PostgreSQL trend clusters. If it is empty after rollout, run `python scripts/sync_trend_clusters_to_qdrant.py` from the app container or trigger semantic/signal analysis from Admin.

## 4.1. Urgent trend alerts

Urgent Telegram alerts are not a daily digest. They are a rare notification layer over confirmed `trend_clusters`:

```bash
curl -sS -X POST 'http://127.0.0.1:8101/api/pipeline/run-urgent-trend-alerts?dry_run=true' | jq .
```

Expected production defaults: `TREND_ALERT_MIN_SIGNAL_SCORE=0.80`, `TREND_ALERT_MIN_DOC_COUNT=5`, `TREND_ALERT_MIN_SOURCE_COUNT=3`, `TREND_ALERT_MAX_PER_7D=2`. Sent/deduped alerts are stored in PostgreSQL table `trend_alerts`.

## 5. Порог релевантности

В Admin API / БД: `workspaces.relevance_weights` (в т.ч. `threshold`). Временно снизь порог для проверки, что события перестают уходить в ветку dropped.

## 6. Повторная обработка поста

Admin (профиль `admin`, порт `8101`):

```http
POST http://localhost:8101/api/pipeline/reprocess/{post_id}
```

Публикует событие в stream снова (см. `admin/backend/routers/pipeline.py`).

## 7. PaddleOCR (если используется vision/OCR)

- `GET http://<paddleocr-host>:8008/healthz`
- `POST /v1/ocr/upload` с тестовым изображением
- **503** с `error: model_unavailable` — сеть/веса BCE; см. `.env.example` (`LOCAL_OCR_*_MODEL_DIR`, volume `paddleocr_models`).

## Runtime rollout notes

- Массовые `score=0.0` и `category=other` сразу после rollout чаще говорят о сбое интеграции, чем о реальной нерелевантности. Сначала проверь логи на `No such model`, `/tokens/count` и `gigachat_session_cache_disabled`.
- На текущем production contour baseline-модель для `relevance` и `concepts` — `GigaChat-2`, не `GigaChat-2-Lite`.
- Значение вроде `0.5` при `threshold=0.6` — это уже корректный бизнесовый `dropped`, а не авария пайплайна.
- Перед разбором инцидента не вставляй в issue/chat вывод `.env`; достаточно имён переменных и длины/факта установки.
