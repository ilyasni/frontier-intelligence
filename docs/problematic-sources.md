# Проблемные и недоступные источники

Снимок по данным PostgreSQL на сервере (запрос актуален на момент последней выгрузки; для обновления выполни повторный SQL на сервере).

## Легенда

- **Недоступные для ingest** — источник выключен (`is_enabled = false`).
- **Проблемные при включении** — среди включённых на момент снимка отдельной выборкой не попали источники с последним `error`, высоким fail-rate за 14 дней или непустым `last_error` в checkpoint; основная масса ошибок была у выключенных источников workspace `disruption`.
- **Риск по свежести** — включённые RSS/Web с устаревшим или отсутствующим `last_seen_published_at` (≥120 дней или `never`): имеет смысл проверить ленту и осмысленность данных.

---

## 1. Выключенные источники (все — workspace `disruption`)

| ID | Тип | Название | Последний run | Ошибки за 14 дней (fail / ok) | Комментарий |
|----|-----|----------|---------------|-------------------------------|-------------|
| `rss_hi_news_auto` | rss | Hi-News Auto | success | 0 / 0 | Отключён; последний run успешный |
| `rss_hi_news_smart_city` | rss | Hi-News Smart City | success | 0 / 0 | Отключён; последний run успешный |
| `rss_ru_auto_ru_journal` | rss | Auto.ru Journal | error | 0 / 0 | **404** на RSS |
| `rss_tesla_blog` | rss | Tesla Blog | error | 0 / 0 | **403** на RSS (и в checkpoint) |
| `tg_ru_izvestia_auto` | telegram | Izvestia Auto | error | 247 / 330 | **`telegram_username_unresolved`** (@izvestia_auto) |
| `tg_ru_russianev` | telegram | Russian EV | error | 247 / 331 | **`telegram_username_unresolved`** (@russianev) |
| `web_ux_journal` | web | UX Journal | error | 207 / 121 | Высокая доля ошибок до отключения |

**Остальные workspace** (`ai_trends`, `design`, `ai_research`, `ai_products_media`): все источники включены (0 отключённых).

---

## 2. Включённые источники с риском по «свежести» в checkpoint (RSS/Web)

Имеет смысл проверить актуальность ленты/краула; при недавнем `last_success_at` возможны особенности записи дат в checkpoint.

| ID | Workspace | Тип | Название | last_seen_published_at |
|----|-----------|-----|----------|------------------------|
| `rss_google_news_tcs_group_ru` | disruption | rss | Google News TCS Group RU | 2024-03-14 |
| `rss_google_news_tinkoff_fintech_en` | disruption | rss | Google News Tinkoff Fintech EN | 2019-06-14 |
| `rss_insideevs_ux` | disruption | rss | InsideEVs User Experience | 2025-09-24 |
| `design_rss_insideevs_ux` | design | rss | InsideEVs UX | 2025-09-24 |
| `rss_lenta_future` | disruption | rss | Lenta Future | never |
| `rss_lenta_tech` | disruption | rss | Lenta Tech | never |
| `rss_tbank_github` | disruption | rss | TinkoffCreditSystems GitHub | never |
| `web_bosch_software_driven_mobility` | disruption | web | Bosch Software-Driven Mobility | never |
| `web_mobilityhouse_newsroom` | disruption | web | Mobility House Newsroom | never |
| `web_nngroup_articles` | disruption | web | NNGroup Articles | never |
| `web_nvidia_autonomous_vehicles` | disruption | web | NVIDIA Autonomous Vehicles | never |
| `web_waymo_blog` | disruption | web | Waymo Blog | never |

---

## Повторная проверка на сервере

Отключённые источники:

```bash
ssh -o BatchMode=yes frontier-intelligence "cd /opt/frontier-intelligence && docker compose exec -T postgres psql -U frontier -d frontier -c \"SELECT id, workspace_id, source_type, is_enabled FROM sources WHERE NOT is_enabled ORDER BY workspace_id, id;\""
```

Стейл по `last_seen_published_at` (настрой интервал при необходимости):

```sql
SELECT s.id, s.workspace_id, s.source_type, s.name,
       sc.last_seen_published_at, sc.last_success_at
FROM sources s
JOIN workspaces w ON w.id = s.workspace_id
LEFT JOIN source_checkpoints sc ON sc.source_id = s.id
WHERE s.is_enabled AND w.is_active
  AND s.source_type IN ('rss', 'web', 'api', 'habr')
  AND (
    sc.last_seen_published_at IS NULL
    OR sc.last_seen_published_at < NOW() - INTERVAL '120 days'
  )
ORDER BY s.workspace_id, s.id;
```
