# Раскат источников по батчам (теги)

Операционная таблица: проблемный или целевой сценарий → действие → тег батча → поля в [`config/sources.yml`](../config/sources.yml). Семантические «теги» сигналов задаются через `extra.quality_tier` и `extra.expected_signal_types`.

## Решение по fintech (зафиксировано)

**Вариант B (принят для текущего rollout):** отдельный workspace `fintech` **не** добавляем. Источники `rss_google_news_tcs_group_ru`, `rss_google_news_tinkoff_fintech_en`, `rss_tbank_github` остаются в конфигурации только при уже существующих записях в БД — рекомендуется держать **`is_enabled: false`** и не включать в disruption-тренды до появления отдельного workspace и гипотезы.

Если позже понадобится **вариант A:** добавить блок в [`config/workspaces.yml`](../config/workspaces.yml), `bootstrap_workspaces`, затем перенести строки в PostgreSQL (UPDATE `workspace_id`) и включить по одному.

---

## Таблица: проблемный id → действие → батч

| Проблемный / контекст | Действие | Тег батча | Примечание |
|----------------------|----------|-----------|------------|
| `web_waymo_blog` | Уже в YAML; мониторить ingest/checkpoint | `fix_existing` | Листинг Waymo без смены id |
| `web_nvidia_autonomous_vehicles` | Смена URL на NVIDIA Developer Blog tag `drive` | `fix_existing` | Тот же `source_id` |
| `web_bosch_software_driven_mobility` | Смена URL на Bosch Mobility SDV topic | `fix_existing` | Прокси xray сохранён |
| `web_mobilityhouse_newsroom` | Смена URL на `int_en` newsroom | `fix_existing` | Прокси xray сохранён |
| `web_nngroup_articles` | Добавлен `proxy_config` xray | `fix_existing` | Тот же id |
| `web_ux_journal` | Добавлен `proxy_config` xray (гео/доступность) | `fix_existing` | Остаётся выключенным в БД до ручного включения после смока |
| `rss_ru_auto_ru_journal`, сломанные TG | Замена новыми источниками | `batch:auto_ru` | Новые id с `is_enabled: false` до приёмки |
| `rss_tesla_blog`, EV-дефицит | RSS charging/battery + web Not a Tesla App | `batch:ev_tesla` | Новые id выключены по умолчанию |
| Глобальный mobility / SDV | TechCrunch transportation RSS, IEEE, Automotive World | `batch:global_mobility` | Новые id выключены по умолчанию |
| Hi-News smart city | Smart Cities Dive + UITP | `batch:smart_city` | Новые id выключены по умолчанию |
| Design UX gap | Baymard + Smashing Magazine RSS | `batch:design_ux` | Workspace `design`, выключены по умолчанию |

---

## Новые source_id и семантика (`extra`)

Все **новые** записи ниже в `sources.yml` создаются с **`is_enabled: false`** до успешного смока и ручного включения в Admin.

### batch:auto_ru

| id | source_type | expected_signal_types (кратко) | quality_tier |
|----|-------------|----------------------------------|--------------|
| `tg_ru_autoruonline` | telegram | auto, ru_market, car_news | trusted |
| `tg_ru_autostatis` | telegram | market_stats, ru_market | trusted |
| `rss_ru_drom_export` | rss | auto, ru_market, reviews | trusted |
| `web_ru_autoreview_news` | web | auto, expert_reviews, ru_market | trusted |
| `web_ru_autonews_rbc` | web | auto, law, ru_market | trusted |
| `web_ru_kolesa_news` | web | auto, new_models | exploratory |

### batch:ev_tesla

| id | source_type | expected_signal_types | quality_tier |
|----|-------------|----------------------|--------------|
| `rss_insideevs_charging` | rss | charging, infrastructure, ev | trusted |
| `rss_insideevs_battery_tech` | rss | battery_tech, ev | trusted |
| `web_notateslaapp_updates` | web | tesla, software_updates, fsd | trusted |
| `rss_teslarati` | rss | tesla, ev, robotaxi | exploratory |

### batch:global_mobility

| id | source_type | expected_signal_types | quality_tier |
|----|-------------|----------------------|--------------|
| `rss_techcrunch_transportation` | rss | mobility, startups, ev, autonomous | trusted |
| `web_ieee_spectrum_autonomous` | web | autonomous, robotics, research | trusted |
| `web_automotiveworld_sdv` | web | sdv, adas, cybersecurity | trusted |

### batch:smart_city

| id | source_type | expected_signal_types | quality_tier |
|----|-------------|----------------------|--------------|
| `web_smartcitiesdive_transportation` | web | urban_mobility, smart_city, policy | trusted |
| `web_uitp_news_views` | web | public_transport, policy | trusted |

### batch:design_ux

| id | source_type | expected_signal_types | quality_tier |
|----|-------------|----------------------|--------------|
| `web_baymard_blog` | web | ecommerce_ux, checkout, forms | trusted |
| `rss_smashingmagazine` | rss | frontend, ux, accessibility | exploratory |

---

## Порядок после merge

1. Перенос на сервер: [`scripts/sync-push.ps1`](../scripts/sync-push.ps1).
2. Пересборка образов с `COPY`: как минимум **admin** (читает `config/`); ingest берёт источники из БД — после bootstrap достаточно перезапуска ingest, если код ingest не менялся.
3. `POST /api/sources/bootstrap` (опционально с `workspace_id` для поочерёдности).
4. Включать новые id батчами по 3–5 в Admin; после каждого батча — чеклист в [`docs/pipeline-e2e-checklist.md`](pipeline-e2e-checklist.md) и health/XRAY из плана раската.

### Команды на сервере (после rsync)

Пересборка **admin** (в образ копируется `config/`):

```bash
cd /opt/frontier-intelligence
export PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE:-frontier/python-base:3.11-slim}
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 docker compose --profile core --profile admin build admin
docker compose --profile core --profile admin up -d --force-recreate admin
```

Bootstrap источников из YAML в PostgreSQL:

```bash
curl -sS -X POST http://127.0.0.1:8101/api/sources/bootstrap
# только один workspace:
curl -sS -X POST 'http://127.0.0.1:8101/api/sources/bootstrap?workspace_id=disruption'
curl -sS -X POST 'http://127.0.0.1:8101/api/sources/bootstrap?workspace_id=design'
```

Мини-проверки:

```bash
curl -sS http://127.0.0.1:8101/api/health
curl -sS http://127.0.0.1:8101/api/monitoring/xray/health
```

После изменений только в [`shared/source_definitions.py`](../shared/source_definitions.py) при необходимости пересобери **ingest**, если валидация выполняется при старте ingest (в противном случае достаточно admin bootstrap).
