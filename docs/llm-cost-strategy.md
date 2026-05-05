# LLM Cost Strategy — Frontier Intelligence

Документ покрывает: оценку фактических трат на LLM, сравнение провайдеров с подписочными лимитами кредитов, и рекомендуемую стратегию маршрутизации. Все цифры — на май 2026.

Связанные документы:
- `docs/llm-cost-scenarios.xlsx` — расчётная модель и сценарии
- `docs/llm-providers-paid-comparison.md` — детальное сравнение Polza/VseLLM/OpenRouter по платным моделям
- `docs/openrouter-dynamic-routing.md` — дизайн runtime-реестра OR free-моделей (catalog + picker + health-probe)

## TL;DR

Текущая конфигурация (Wormsoft Simple + GigaChat-2-Pro Vision) обходится ориентировочно в **8 200 ₽/мес ≈ 275 ₽/день**. Гибридный сценарий с OpenRouter `:free` для массового vision и оставшимся текстом на Wormsoft Simple снижает счёт до **3 600 ₽/мес ≈ 120 ₽/день — минус 56% без потери качества для trusted-источников**. Использование чистых pay-as-you-go без подписок (Polza.ai / VseLLM) даёт сопоставимую с текущей цену, но без предсказуемого потолка.

| Сценарий | ₽/мес | ₽/день | vs S0 |
|---|---:|---:|---:|
| **S0** As-is: Wormsoft Simple + GigaChat-2-Pro Vision | **8 250** | 275 | 100% |
| **S1** Только GigaChat (без подписок) | 22 995 | 766 | 279% |
| **S2** Wormsoft Simple + OpenRouter `:free` vision | 1 500 | 50 | 18% |
| **S3** Pay-as-you-go: Polza.ai / VseLLM | 7 758 | 259 | 94% |
| **S4 Рекомендуется** Гибрид: Wormsoft+Giga Pro для trusted, OR `:free` для массы | **3 625** | **121** | **44%** |

S2 теоретически самый дешёвый, но vision полностью зависит от стабильности OpenRouter free-tier — рискованно для прода. S4 даёт баланс: trusted-источники остаются на Pro, массовый vision уходит на free, embed и текст — в подписке Wormsoft.

## Оценка текущего объёма

Базируется на `config/sources.yml`, `shared/config.py` и `.env.balanced.example`:

- **Источники:** 47 (31 RSS, 7 Telegram, 6 web, 3 api).
- **Постов в день:** ~500 (RSS ≈ 250, TG ≈ 140, web/api ≈ 110).
- **Постов в месяц:** ~15 000.
- **Постов с картинками (vision):** ~30% = ~4 500/мес.

Бюджеты токенов на вызов (фиксированы в env):

| Задача | Input | Output |
|---|---:|---:|
| `relevance_concepts` (joint) | 1 800 | 300 |
| `valence` | 1 200 | 200 |
| `embed` (EmbeddingsGigaR 2560d) | 1 200 | 0 |
| `vision_prompt + image` | ~2 600 | 400 |
| `mcp_synthesis` (frontier_brief, search_balanced) | 4 000 | 1 500 |
| `missing_signals` probe | 5 000 | 1 000 |

Итоговая нагрузка: **≈ 84M input + 12M output токенов / месяц**, из них vision — 13.5M.

Точное измерение требует поднять счётчики в Prometheus на сервере (`gigachat_tokens_total`, `wormsoft_credits_used_total`); шаблон под Grafana уже лежит в `grafana/`. Из VM-песочницы SSH к серверу недоступен, поэтому здесь — оценка.

## Карта провайдеров

### Текущие

**Wormsoft Simple — 1 500 ₽/мес.** 500 000 кредитов каждые 5 часов = ≈ 72M кредитов/мес при 100% использовании. Кредиты ≠ токены: цена в кредитах за 1M токенов прописана в дашборде Wormsoft (`/api/money/token-pricing`). Для агентских моделей `wormsoft/agent/*` эффективная цена внутри Simple перекрывает основную текстовую обработку проекта (~50M input + 7M output) с большим запасом. Embedding-эндпоинт `/api/gpt/embedding` (`qwen/qwen3-embedding:8b`) тоже в подписке.

Минусы: нет публичного account-level remaining-credits endpoint — мониторим 429-всплески через `admin/backend/services/wormsoft_limits.py` и метрики `wormsoft_limits_snapshot`.

**GigaChat-2-Pro — 1 500 ₽ за 3M токенов** (≈ 500 ₽/1M, после февральского снижения цен Sber). Используется для vision (`GIGACHAT_VISION_MODEL=GigaChat-2-Pro`), потому что он надёжно понимает русский, не имеет ToS-проблем для коммерческой обработки публичных постов, и хорошо работает с длинным prompt (4096 ctx у EmbeddingsGigaR, ~128k у Pro). Vision примерно 13.5M токенов/мес ≈ 4-5 пакетов = 6 000-7 500 ₽/мес.

### Кандидаты на замену / fallback

**Polza.ai.** Pay-as-you-go без подписок. 250+ моделей, оплата картами РФ через ЮKassa/СБП. Маркап над оригиналом провайдера ~5-10%. GPT-4o Mini — 13/53 ₽ за 1M (in/out), Claude 4.5 Sonnet — 264/1318 ₽, GigaChat-2 / Qwen — ~50/100 ₽. Это **не** «фиксированный пакет кредитов», а классический счётчик в рублях. Подходит как:

- **Backup-провайдер** для ситуаций 429 у Wormsoft (один лишний адаптер в `worker/llm_router_client.py`).
- **Недорогой текстовый fallback** для редких задач, где Wormsoft уже исчерпан.

**VseLLM.** Тоже pay-as-you-go в рублях, ~10% ниже рынка. От 38 ₽/1M input на минимальных моделях, GigaChat-2-Max ~650 ₽/1M, Qwen3 VL 30B ~80/200 ₽/1M. Premium-тариф для юрлиц с прямой маршрутизацией и SLA. Идентичные с Polza преимущества; выбор между ними — по UX биллинга и латентности.

**OpenRouter.** Подписочной модели «фикс кредитов раз в N часов» нет, но есть **бесплатные модели `:free`** — это и есть способ экономии:

- Без депозита: 50 запросов/сутки на ВСЕ free-модели суммарно (бесполезно для прода).
- С депозитом $10 (≈ 900 ₽ единоразово): **1 000 RPD на free-модели**, 20 RPM. Депозит не списывается за запросы к `:free` — он только разблокирует лимит.
- Ведущие free-модели (май 2026): **Qwen3 VL 235B Thinking** и **Qwen3.6 Plus** (vision, 1M context), **DeepSeek R1**, **Llama 3.3 70B**, **Gemma 3 12B/27B**, **Nemotron Nano 12B VL**.
- Free embeddings — **нет** (важный нюанс, embed-задачу не закроем).
- Failed attempts засчитываются в RPD; нужен throttle на нашей стороне.
- Free-статус — preview, может закончиться без warning, нужен health-check `/api/v1/models`.

При нашем объёме vision (~150 запросов/день, ~5 000/мес) лимит 1 000 RPD покрывает с шестикратным запасом — это и делает OpenRouter `:free` основным донором экономии.

## Стратегия S4 (рекомендуется)

Идея: **подписка Wormsoft Simple покрывает всю текстовую обработку и embed**, **vision разделяется** на trusted-источники (остаются на GigaChat-2-Pro для качества) и mass-источники (уходят на OpenRouter `:free`).

| Задача | Primary | Fallback 1 | Fallback 2 | Триггер fallback |
|---|---|---|---|---|
| `relevance_concepts` | Wormsoft `wormsoft/agent/medium` | GigaChat-2-Pro | OpenRouter DeepSeek R1 `:free` | 429 / latency >5s |
| `valence` | Wormsoft `wormsoft/agent/small` | GigaChat-2 | OpenRouter Llama 3.3 70B `:free` | 429 |
| `mcp_synthesis` | Wormsoft `wormsoft/agent/large` | GigaChat-2-Pro | Polza.ai Claude Haiku | качество < порог |
| `missing_signals` | Wormsoft `wormsoft/agent/medium` | OpenRouter DeepSeek R1 `:free` | — | 429 |
| `embed` | Wormsoft `qwen/qwen3-embedding:8b` | GigaChat `EmbeddingsGigaR` | — | dim mismatch |
| `vision` (trusted 30%) | GigaChat-2-Pro Vision | Polza.ai GigaChat-2-Pro | OpenRouter Qwen3 VL `:free` | quality_tier=trusted в `enrichment_policy` |
| `vision` (mass 70%) | OpenRouter Qwen3 VL 235B `:free` | OpenRouter Qwen3.6 Plus `:free` | GigaChat-2-Pro | 429 / 5xx |

Где «trusted» = `quality_tier: trusted` в `config/sources.yml` (TechCrunch, WIRED AI, AvtoVAZ Official и т.д.); «mass» = `exploratory` или ниже.

### Что нужно докрутить в коде

Что уже есть и переиспользуется:

- `shared/llm_routing.py` — `LLMRoutingSettings` с per-task `provider+model+fallback`, runtime override через `RUNTIME_LLM_ROUTING_REDIS_KEY` (ключ `frontier:runtime:llm_routing`).
- `worker/llm_router_client.py` — фасад с маршрутизацией и метриками `note_llm_fallback`.
- `admin/backend/services/wormsoft_limits.py` — снапшот тарифов и плани кредитов.

Что добавить (порядок ~1-2 дня работы):

1. **Расширить `LLMRoute`** добавлением `fallback2_provider` / `fallback2_model` (или, лучше, заменить на список `fallbacks: list[LLMRoute]`). Изменения локальные, миграция через дефолты.
2. **OpenRouter-адаптер** в `worker/openrouter_client.py` поверх OpenAI-совместимого API. Размер ~50-80 строк, по аналогии с `worker/wormsoft_client.py`. Обязательно поддержать заголовки `HTTP-Referer` и `X-Title` для корректной идентификации.
3. **Локальный счётчик RPD/RPM** в `admin/backend/services/openrouter_limits.py`. У OR нет remaining-endpoint, поэтому держим Redis-счётчик с TTL 24ч и 1м, отказываем 429 локально до отправки. Метрики — `openrouter_rpd_used_total`, `openrouter_rpm_used`.
4. **Vision routing по quality_tier** в `config/enrichment_policy.yml`:
   ```yaml
   vision_routing:
     quality_tier_map:
       trusted: gigachat_pro
       exploratory: openrouter_qwen_vl_free
       default: openrouter_qwen_vl_free
   ```
   Чтение в `worker/tasks/vision_task.py` перед вызовом клиента.
5. **Динамический реестр OR free-моделей** — отдельный документ `docs/openrouter-dynamic-routing.md`. Кратко: каталог раз в 15 минут, health-probe раз в 5 минут, sticky-picker на 10 минут, карантин по `X-RateLimit-Reset`. Никаких хардкодов конкретных моделей в env — только семейство задач. Стартовые scaffold-файлы лежат в `admin/backend/services/openrouter_catalog.py` и `openrouter_picker.py`.
6. **Метрики Prometheus**: фасет `provider` в существующих `llm_request_total`, `llm_request_duration_seconds`, `llm_fallback_total` для значения `openrouter`.
7. **Алерт**: если `rate(openrouter_429_total[1h]) > 50`, runtime override на 6 часов переводит mass vision на GigaChat-2-Pro.

### Депозит OpenRouter $10

Окупается за первый же месяц. Не списывается за `:free`-запросы — это просто условие для подъёма RPD с 50 до 1000. Хранить можно как технический холд; если понадобится платная модель в fallback, депозит начинает расходоваться по обычным ценам.

## Чего делать не стоит

**Polza.ai как единственный провайдер.** Дешевле, чем GigaChat-only, но дороже текущего S0 — модель «без подписок» не окупает Wormsoft Simple. Polza удобна как backup и точка доступа к Claude / GPT-4o Mini для редких задач.

**OpenRouter `:free` для embeddings.** Эндпоинта нет; имитировать через text→embedding через любую LLM — плохая идея, размерности нестабильны. Embed остаётся на Wormsoft `qwen/qwen3-embedding:8b` (внутри Simple) с GigaChat `EmbeddingsGigaR` как fallback.

**OpenRouter `:free` для всего текста.** При 500 постов/день × (relevance+concepts+valence) ≈ 1 500 запросов/день — выходим за 1 000 RPD. И качество DeepSeek R1 на русских промптах для concepts ниже, чем у `wormsoft/agent/medium`. Использовать только как fallback.

**GigaChat-2-Max в проде.** Тарифицируется ~3× дороже Pro, выгоды на наших задачах не видно. Оставить только для ручных quality-экспериментов.

## Метрики, по которым валидировать стратегию

После раскатки S4 проверять:

- `wormsoft_credits_remaining{plan="simple"}` — не должен опускаться ниже 20% за окно 5ч (запас на пики).
- `gigachat_balance_remaining{type="pro"}` — расход ≤ 1 пакета (3M токенов) в месяц.
- `openrouter_rpd_used / 1000` — стабильно < 0.5 (полупустой бюджет = свободный fallback на текст).
- `llm_fallback_total{from="wormsoft",to="gigachat"}` — < 5% всех вызовов; больше — пересмотреть лимиты Wormsoft Simple → Payed.
- `llm_request_duration_seconds{provider="openrouter"}` p95 < 15s — иначе OR даёт холодные старты, нужен retry с jitter.

## Источники

- [Polza AI — каталог моделей](https://polza.ai/models)
- [Polza.ai о ценообразовании (vc.ru)](https://vc.ru/ai/2789769-reiting-top-20-agregatorov-neyrosetey)
- [VseLLM — тарифы провайдеров](https://vsellm.ru/provider/VSELLM)
- [VseLLM — калькулятор токенов](https://vsellm.ru/calc)
- [OpenRouter — Pricing](https://openrouter.ai/pricing)
- [OpenRouter — API Rate Limits](https://openrouter.ai/docs/api/reference/limits)
- [OpenRouter — Free Models Collection](https://openrouter.ai/collections/free-models)
- [OpenRouter — Vision Models Collection](https://openrouter.ai/collections/vision-models)
- [costgoat.com — All 33 Free Models on OpenRouter, May 2026](https://costgoat.com/pricing/openrouter-free-models)
- [GigaChat 2 Pro — спецификация и цены](https://cloudprice.net/models/gigachat-2-pro)
- [Sber снизил цены GigaChat API втрое (Feb 2026)](https://ixbt.pro/en/news/2026/02/02/sber-obieiavil-o-trexkratnom-snizenii-cen-na-gigachat-api.html)
- [Тарифы GigaChat API для физлиц](https://developers.sber.ru/docs/ru/gigachat/tariffs/individual-tariffs)
- [Wormsoft AI — корневая страница](https://ai.wormsoft.ru/)
