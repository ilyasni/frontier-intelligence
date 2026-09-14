---
base_sha: 880a7bc (server /opt/frontier-intelligence @ 2026-09-14)
written_by: claude-code
task: закрыть три повторяющиеся находки ежедневного разбора алертов 08–14.09
---

## Что сделано

Разобраны дайджесты `docs/ops/alert-digests/2026-09-08..14.md` и живой Prometheus. Три
находки, которые разбор повторял изо дня в день, исправлены и выкачены.

1. **`web_smartcitiesdive_transportation` → лента.** `source_runs`: 320 error / 1 success
   с 05.08, почти все `403` на `/topic/transportation/` через xray. Единственный успех
   10.09 принёс 17 «постов» — ссылки навигационного меню (видно по
   `source_checkpoints.cursor_json.seen_external_ids`: `/signup/`, `/topic/housing/`,
   `facilitiesdive.com`…), без дат → `FrontierSourceDatelessRatioJumped` с 10.09.
   Проба штатным `build_httpx_client` из контейнера ingest, дважды:
   `/feeds/topic/transportation/` → 200, 20 записей, 20 с датой; листинг → 403; сами
   статьи → 403. Источник переведён на `source_type: rss` с этим URL, id сохранён,
   `full_content: false`. Применено `bootstrap_sources_from_config(workspace_id="disruption")`
   (перед этим сверка YAML↔БД по disruption: расходился только этот источник), ingest
   перезапущен.
2. **Отрицательный `frontier_last_post_age_seconds`.** `admin/backend/main.py`:
   `MAX(COALESCE(published_at, created_at))` → `MAX(LEAST(published_at, created_at))`.
   `LEAST` в Postgres пропускает NULL (docs 16, через Context7), поэтому бездатные посты
   считаются по `created_at`, как раньше. Сверка на живой базе с `now` = 12.09 12:00 UTC:
   ai_products_media старый −129 600 с, новый 3 774 с; на текущий момент все шесть
   воркспейсов совпадают. `GREATEST(0, …)` отвергнут: возраст 0 держится, пока часы не
   догонят дату, порог так же недостижим.
3. **`frontier_cluster_quality` получил метку `job_kind`** (`semantic_clusters` |
   `signal_analysis`) в `shared/metrics.py` и `admin/backend/scheduler.py`. Правило
   `FrontierClusterArtifactSplitRising`: `and on(workspace)` → `and on(workspace, job_kind)`,
   аннотация больше не предупреждает про смешение джобов. Причина — `ops_cluster_quality_two_publishers`.

Проверки:
- `docker run … frontier-intelligence-admin:latest pytest tests/test_alert_rules_contract.py
  tests/test_last_post_age_metric.py tests/test_pipeline_stage_metrics.py
  tests/test_cluster_quality_metrics.py tests/test_sources_config_contract.py
  tests/test_alerts_workspace_coverage.py tests/test_dateless_posts_observability.py`
  → **110 passed, 2 failed**; оба падения существовали до правок (см. ниже).
- `promtool check rules` → `SUCCESS: 91 rules`; `POST /-/reload` = 200 (файл записан
  с сохранением иноды), `/api/v1/rules` отдаёт новое выражение.
- admin-образ пересобран, контейнер пересоздан, `/api/health` 200; внутри:
  `CLUSTER_QUALITY_GAUGE._labelnames` содержит `job_kind`, SQL содержит `LEAST`,
  `/app/config/sources.yml` — rss. `/metrics` отдаёт положительные возрасты.

## Что не доделано

- Первый rss-прогон smartcitiesdive состоялся 08:30 UTC: `success`, fetched 20 /
  emitted 20; к 08:35 сохранено 11 постов, `published_at` у всех 11 (14.08–31.08, ни одной
  даты вперёд). Остальные в пути через worker — полный счёт не сверял.
- Новые серии `frontier_cluster_quality{job_kind=…}` вживую не видел: гейдж обнулился при
  пересоздании admin и заполнится на ближайшем прогоне signal-analysis/кластеризации.
  Метку подтверждает только юнит-тест и `_labelnames` в контейнере.
- **17 мусорных постов от 10.09 остались в `posts`/Qdrant.** Не удалял: это удаление данных,
  решение владельца. Из кластеризации они исключены и так (`published_at IS NULL`).
  Алерт по доле бездатных уйдёт сам ~17.09 15:25 UTC или раньше, если новые датированные
  посты разбавят долю.
- **`FrontierClusterArtifactSplitRising` молчит ~1 сутки после раската:** у новых серий с
  `job_kind` нет базовой линии в окне `[3d] offset 1d`. Полная база — через ~4 суток.
- Не мои, но красные: `test_auto_batch_enabled_flags_match_the_recorded_state` — пять
  `auto_tg_ru_*` включены в серверном YAML и в БД (незакоммиченная правка 23.08, посты
  идут), а реестр в тесте ждёт `False` с пометкой «включать только вместе с фильтрами».
  Решает владелец. `test_emit_to_stream_actually_counts_published_and_failed` — нет `bs4`
  в admin-образе, окружение.
- **Локальное дерево расходится с сервером** не только в правленых файлах:
  `worker/services/semantic_clustering.py` локально другой (локально падают 10 тестов
  `test_cluster_quality_metrics.py`, на сервере зелёные). Общий `sync-push` в таком
  состоянии опасен — сначала `sync-pull`.
- В серверном `config/sources.yml` остаются чужие незакоммиченные правки (LangChain Blog,
  пять TG auto_hmi); в коммит вошёл только хунк smartcitiesdive (`git apply --cached`).

## Следующий шаг

После ближайшего прогона кластерного джоба (15.09 05:00 UTC или signal-analysis раньше):
`count by (workspace, job_kind) (frontier_cluster_quality{metric="same_artifact_groups"})`
— ожидаются серии с непустым `job_kind`, по одной на джоб.

## Файлы, которые я трогал

- `config/sources.yml` — smartcitiesdive: web → rss, комментарий с замерами
- `admin/backend/main.py` — `LEAST(published_at, created_at)` в запросе возраста
- `shared/metrics.py` — метка `job_kind` у `frontier_cluster_quality`
- `admin/backend/scheduler.py` — отдельный републикатор на каждый кластерный джоб
- `prometheus/alerts.yml` — `on(workspace, job_kind)`, аннотация, комментарий про LEAST
- `tests/test_alert_rules_contract.py` — `test_cluster_split_rule_compares_each_publisher_with_itself`
- `tests/test_pipeline_stage_metrics.py` — `test_cluster_quality_of_two_jobs_lands_in_separate_series`
- `tests/test_last_post_age_metric.py` — новый, статический контракт запроса
- бэкап исходных серверных версий: `/tmp/fi-bak-20260914/` на сервере

## Что проверить

- Через сутки: `count by (job_kind) (frontier_cluster_quality{metric="same_artifact_groups"})`
  — две серии на воркспейс, без суточной прямоугольной волны внутри каждой.
- `min(frontier_last_post_age_seconds) >= 0` в любой момент.
- `FrontierSourceDatelessRatioJumped{source_id="web_smartcitiesdive_transportation"}` гаснет
  не позже ~17.09.
