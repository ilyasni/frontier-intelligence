# OpenRouter Dynamic Routing — поддержка живого списка `:free` моделей

Этот документ — продолжение `docs/llm-cost-strategy.md`. Решает проблему «free-каталог OR живёт своей жизнью, и статически прописанная модель в env через месяц превращает primary в fallback».

## Проблема

OpenRouter постоянно ротирует free-семейство:

- Модели с `preview`-статусом могут исчезнуть без предупреждения.
- RPD/RPM меняются провайдером без объявления.
- За одной и той же `:free` моделью могут стоять разные апстримы с разной латентностью.
- Появляются новые модели лучше прежних — не хочется пропускать.

При статическом конфиге это означает: либо мы переживаем тихие отказы (модель удалили — все запросы идут в platный fallback и съедают баланс), либо тратим время разработчика на ручной апдейт env раз в неделю.

## Решение: runtime-реестр + scoring picker

Четыре источника истины, агрегируемые в Redis:

| Что | Источник | Период |
|---|---|---|
| Каталог `:free` моделей с capability и `context_length` | `GET https://openrouter.ai/api/v1/models` (фильтр `pricing.prompt == "0"`) | 15 минут |
| Health (живая ли модель, реальная latency) | свой ping: 1-токен запрос на каждую `:free` | 5 минут |
| Local RPD/RPM (опережающий счётчик) | Redis sliding window, инкремент перед запросом | per-call |
| OR-side rate limit | заголовки `X-RateLimit-Limit/Remaining/Reset` в каждом ответе | per-response |

### Picker

Перед каждым вызовом задачи `T` выбираем модель по скору:

```
score = 100 * capability_match           # hard filter (modality, ctx, json_mode)
      +  50 * health_score               # success rate за час, 0..1
      +  30 * rpd_headroom               # (limit - used) / limit
      -  20 * latency_norm               # p95 нормировано к 0..1 при cap 30s
      - 1000 * quarantine                # если в карантине после 5xx/429
      - 500 * near_rpd_cap               # рядом с safety buffer 10%
```

`capability_match` — жёсткий фильтр по `TASK_REQUIREMENTS`:

| Task | capability | min_context |
|---|---|---:|
| `vision_mass` | `image` в `input_modalities` | 16k |
| `vision_premium` | то же | 32k |
| `relevance_concepts` | `structured_outputs` или `json_mode` или `tools` | 8k |
| `concepts` | то же | 8k |
| `valence` | — (любая текстовая) | 4k |
| `mcp_synthesis` | `tools` | 32k |
| `missing_signals` | structured | 8k |

**Sticky-выбор**: модель используется минимум 10 минут или до падения health-score < 0.7. Решение пишется в `or:picker:decision:<task>` с TTL = 600s. Без этого picker будет мигать между двумя близкими по score моделями и инвалидировать их прогретые соединения.

### Карантин

| Триггер | Действие |
|---|---|
| 429 от OR | модель в карантин до `X-RateLimit-Reset` (но не меньше 60с) |
| 5xx > 3 раз за 5 мин | карантин 30 минут |
| Модель пропала из каталога 2 опроса подряд | удаление из реестра |
| Все free-модели в карантине | reconciler пишет в `RUNTIME_LLM_ROUTING_REDIS_KEY` override на платного провайдера + alert в Telegram |

### Хранение

```
Redis:
  or:catalog:snapshot           JSON со списком моделей и метаданными (TTL 1h)
  or:catalog:fetched_at         timestamp последнего fetch
  or:health:<model_id>          {success, fail, p95_ms, last_check, in_quarantine_until}
  or:rpd:<model_id>:YYYYMMDD    INT, TTL 25h
  or:rpm:<model_id>:HHMM        INT, TTL 2min
  or:picker:decision:<task>     {model_id, score, sticky_until, candidates[]}
  frontier:runtime:llm_routing  override (уже существует — пишем сюда)
```

## Файлы

| Файл | Что делает | Статус |
|---|---|---|
| `admin/backend/services/openrouter_catalog.py` | Тянет `/api/v1/models`, фильтрует free, нормализует, кладёт в Redis | scaffold готов |
| `admin/backend/services/openrouter_picker.py` | Скоринг + sticky-выбор + запись в runtime override | scaffold готов |
| `admin/backend/services/openrouter_health.py` | Cron-пробинг каждой free-модели 1-токен запросом | TODO |
| `worker/openrouter_client.py` | OpenAI-совместимый клиент с парсингом `X-RateLimit-*` и колбэком в picker | TODO |
| `shared/metrics.py` | + `set_openrouter_catalog_snapshot()`, `note_openrouter_call()` | TODO |
| `admin/backend/scheduler.py` | + три cron-задачи: refresh_catalog (15m), probe_health (5m), reconcile_routing (1m) | TODO |
| `shared/config.py` | + `openrouter_api_key`, `openrouter_rpd_limit`, `openrouter_referrer` | TODO |

## Cron-задачи в `admin/backend/scheduler.py`

```python
# Каталог — раз в 15 минут
scheduler.add_job(
    scheduled_refresh_openrouter_catalog,
    CronTrigger.from_crontab("*/15 * * * *"),
    id="refresh_openrouter_catalog",
    coalesce=True,
    max_instances=1,
)

# Health-probe — раз в 5 минут
scheduler.add_job(
    scheduled_probe_openrouter_health,
    CronTrigger.from_crontab("*/5 * * * *"),
    id="probe_openrouter_health",
    coalesce=True,
    max_instances=1,
)

# Reconciler — раз в минуту
scheduler.add_job(
    scheduled_reconcile_openrouter_routing,
    CronTrigger.from_crontab("* * * * *"),
    id="reconcile_openrouter_routing",
    coalesce=True,
    max_instances=1,
)
```

Все три обёрнуты в asyncio.Lock как `refresh_wormsoft_limits` (см. `_wormsoft_limits_lock`), чтобы skipped-runs не наслаивались.

## Метрики Prometheus

```
openrouter_catalog_models_total{service}                # сколько free-моделей в каталоге
openrouter_catalog_refresh_timestamp{service}           # last successful refresh
openrouter_health_success_rate{model_id}                # последний 1ч
openrouter_health_p95_ms{model_id}
openrouter_rpd_used{model_id}                           # текущие за день
openrouter_rpd_limit{service}                           # 50 / 1000 в зависимости от deposit
openrouter_in_quarantine{model_id}                      # 0/1
openrouter_picker_decision{task,model_id}               # incremented at each pick
openrouter_picker_fallback_total{task,reason}           # когда picker отдал в paid
openrouter_request_total{model_id,status}               # 200/429/5xx counters
openrouter_x_ratelimit_remaining{model_id}              # из заголовков
```

## Алерты

```yaml
# prometheus/rules/openrouter.yml
groups:
  - name: openrouter
    rules:
      - alert: OpenRouterCatalogStale
        expr: time() - openrouter_catalog_refresh_timestamp > 1800
        for: 5m
        annotations:
          summary: "Каталог OR не обновлялся 30+ минут"
      - alert: OpenRouterAllModelsQuarantined
        expr: sum(openrouter_in_quarantine) == count(openrouter_in_quarantine)
        for: 2m
        annotations:
          summary: "Все free-модели OR в карантине — переход на paid fallback"
      - alert: OpenRouterFallbackBurst
        expr: rate(openrouter_picker_fallback_total[10m]) > 5
        for: 5m
        annotations:
          summary: "Picker отдаёт >5 запросов/мин в paid fallback"
      - alert: OpenRouter429Burst
        expr: rate(openrouter_request_total{status="429"}[5m]) > 1
        for: 5m
        annotations:
          summary: "429 от OR — пересмотреть RPM throttle"
```

## Как использовать в worker

В `worker/llm_router_client.py`:

```python
from admin.backend.services.openrouter_picker import pick_model, record_call_result

async def call_via_openrouter(task: str, prompt: str, **kwargs):
    decision = await pick_model(task)
    if not decision.get("model_id"):
        return await fallback_to_paid(decision["fallback_to"], prompt, **kwargs)

    model_id = decision["model_id"]
    t0 = time.monotonic()
    try:
        resp = await openrouter_client.chat(model=model_id, prompt=prompt, **kwargs)
        await record_call_result(
            model_id,
            success=True,
            latency_ms=(time.monotonic() - t0) * 1000,
            or_remaining=resp.headers_x_ratelimit_remaining,
            or_reset_at=resp.headers_x_ratelimit_reset / 1000.0,
        )
        return resp
    except OpenRouterRateLimit as exc:
        await record_call_result(
            model_id,
            success=False,
            latency_ms=(time.monotonic() - t0) * 1000,
            or_reset_at=exc.reset_at,
        )
        # На уровне router — повторить с pick_model(force_refresh=True)
        raise
```

## Конфиг

Добавить в `.env.example` и `shared/config.py`:

```bash
OPENROUTER_API_KEY=                       # обязателен
OPENROUTER_REFERRER=https://frontier-intelligence.local
OPENROUTER_RPD_LIMIT=1000                 # 50 без депозита, 1000 при $10+
OPENROUTER_RPM_THROTTLE=18                # держим на 2 ниже честных 20
OPENROUTER_QUARANTINE_BASE_SEC=60
OPENROUTER_QUARANTINE_5XX_SEC=1800
OPENROUTER_HEALTH_PROBE_TOKENS=1          # дёшево, 1 токен на check
OPENROUTER_PICKER_STICKY_SEC=600          # 10 минут
OPENROUTER_FALLBACK_PROVIDER=gigachat
OPENROUTER_FALLBACK_MODEL=GigaChat-2-Pro
```

## Почему именно так

**Не статический whitelist в env.** При статике появляются два провала: (a) исчезла модель → primary всегда падает в fallback, (b) появилась новая, лучше — мы её не используем неделями.

**Не один раз в день, а каждые 15 минут.** OR публикует preview-модели и снимает их в течение часов. 15 минут — компромисс между нагрузкой и свежестью.

**Sticky-выбор обязателен.** Без него на каждом запросе будет переключение между моделями с близким score → потеря TLS-кешей, перегрев конкретного провайдера, нестабильная latency. 10 минут — достаточно, чтобы прогреть пул и при этом отреагировать на деградацию.

**Карантин по `X-RateLimit-Reset`, а не на фиксированный таймаут.** OR честно говорит, когда лимит вернётся — нет смысла угадывать.

**Health-probe раз в 5 минут — обязательно.** В каталоге модель есть, но реально может отдавать 5xx. Без активной проверки узнаешь только в продакшене.

**Reconciler раз в минуту, picker — per-call.** Reconciler пишет дефолтный override (на случай если worker не вызвал picker сам); picker уточняет per-task. Двухуровневая защита.

## Чего не делать

**Не задавать `OPENROUTER_RPD_LIMIT=1000` без депозита.** Реальный лимит без депозита — 50/день, picker будет переоценивать headroom и каждый день в первый же час уйдёт в карантин на сутки.

**Не использовать `:floor`, `:nitro` или провайдерные суффиксы.** Они платные и/или меняют поведение routing'а — picker логика рассчитана только на чистый `:free`.

**Не игнорировать `Failed attempts` в RPD.** OR засчитывает их в дневной счётчик. `record_call_result` инкрементит счётчик и при success=False — это правильно.

**Не делать picker полностью stateless.** Без sticky-кеша и health-state на каждый запрос будет отдельный pick → дёргание разных моделей и невозможность прогреть соединение.
