# Frontier Intelligence — Technical Roadmap v2

> **Ревизия:** v2.0, июнь 2026
> **Ключевое изменение:** Добавлена Фаза 0.5 «Harness Delivery Infrastructure». Скорректирован фрейм Фазы 1. Часть задач из Фазы 2 повышена до критических и перемещена в Фазу 0.5.

---

## Контекст продукта

Frontier Intelligence — это не SaaS-дашборд с доступом к данным по подписке. Это **role-specific intelligence harness**: клиент получает готовый к использованию пакет, включающий Claude Project Template + Skill + настроенный MCP endpoint + автономная доставка брифов. Веб-интерфейс — вспомогательный инструмент, а не core product.

Этот сдвиг в понимании определяет порядок фаз.

---

## Обзор фаз

| Фаза | Название | Фокус | Оценка |
|------|----------|-------|--------|
| 0 | Foundation | Backend инфраструктура, мульти-тенантность, Frontier data pipeline | 4–5 чел-нед |
| **0.5** | **Harness Delivery Infrastructure** | **Каталог харнесов, Claude Project Template, Autonomous delivery MVP** | **6 чел-нед** |
| 1 | Role-first UI | Role selector, harness installer, delivery settings — не дашборд | 4 чел-нед |
| 2 | Intelligence Layer | Персонализация по роли, расширение каталога, аналитика | 5–6 чел-нед |
| 3 | Scale & Ecosystem | Мультиканальность, self-serve onboarding, partner API | 6–8 чел-нед |

---

## Фаза 0 — Foundation
**Статус:** в работе / завершена
**Оценка:** 4–5 чел-нед

### Задачи
- Мульти-тенантная архитектура: таблица `tenants`, изоляция данных по `tenant_id`
- Подключение Frontier data pipeline: источники, парсинг, дедупликация
- Кластеризация сигналов и базовая таксономия
- API-скелет: аутентификация (JWT + API key), rate limiting, базовый CRUD
- Схема БД: `tenants`, `signals`, `clusters`, `cluster_memberships`, `sources`
- CI/CD: деплой окружений (dev / staging / prod), логирование, алерты

### Выход фазы
- Работающий pipeline: сигналы собираются, кластеризуются, доступны через API
- Мульти-тенантность: данные изолированы, API-ключи работают

---

## Фаза 0.5 — Harness Delivery Infrastructure ⬅ НОВАЯ
**Приоритет:** Критический — до любого UI
**Оценка:** 6 чел-нед

Это ключевая фаза, которую пропускал roadmap v1. Без неё продукт — просто API с данными, а не intelligence harness.

---

### 0.5.1 — Harness Catalog
**Оценка:** 1.5 чел-нед

**Схема БД:**
```sql
CREATE TABLE harnesses (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  role_name     TEXT NOT NULL,          -- 'design_lead', 'ai_pm', 'vc_analyst'
  display_name  TEXT NOT NULL,
  description   TEXT,
  system_prompt TEXT NOT NULL,          -- system prompt для Claude Project
  skill_config  JSONB NOT NULL,         -- конфигурация скилла (modes, tools, triggers)
  workspace_defaults JSONB,             -- дефолтные настройки воркспейса
  mcp_tools     TEXT[],                 -- список MCP инструментов для роли
  version       TEXT NOT NULL DEFAULT '1.0.0',
  is_active     BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);
```

**API:**
- `GET /harnesses` — список доступных харнесов (публичный, без auth)
- `GET /harnesses/{id}` — детали харнеса
- `GET /harnesses/{id}/install-package` — скачать zip-пакет (system prompt + skill YAML + README)

**Seed data — первые 3 харнеса:**

| role_name | display_name | Ключевые MCP tools |
|-----------|-------------|-------------------|
| `design_lead` | Design Lead | frontier_brief, cluster_details, signal_timeline, concept_graph |
| `ai_pm` | AI Product Manager | frontier_brief, search_frontier, trend_clusters, emerging_signals |
| `vc_analyst` | VC Analyst | frontier_brief, search_balanced, source_details, cluster_evidence |

Каждый харнес имеет свой system prompt, оптимизированный под роль: режимы (Stakeholder Move, Vision Scan и т.д.), инструкции по использованию MCP, формат выхода.

---

### 0.5.2 — Claude Project Template Installer
**Оценка:** 2 чел-нед

**Endpoint экспорта:**
- `GET /harnesses/{id}/claude-project.json` — экспорт конфига в формат, совместимый с Claude Projects
  - Включает: system prompt, список MCP endpoints, workspace settings, skill configuration
  - Формат: JSON согласно Claude Project import spec

**MCP endpoint per tenant:**
```
https://api.frontier-intelligence.com/mcp/{tenant_id}
Authorization: Bearer {tenant_mcp_token}
```
- Уникальный endpoint для каждого тенанта
- `tenant_mcp_token` — отдельный токен, не совпадает с API key
- Ротация токена: POST /tenants/me/mcp-token/rotate

**Onboarding wizard (backend logic, UI в Фазе 1):**

Шаг 1 — Выбор роли: `POST /onboarding/start` → `{ harness_id, tenant_id }`
Шаг 2 — Генерация пакета: `GET /harnesses/{id}/claude-project.json` + install-package
Шаг 3 — Активация MCP: `POST /onboarding/activate-mcp` → возвращает mcp_endpoint + token

Таблица для отслеживания онбординга:
```sql
CREATE TABLE onboarding_sessions (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   UUID REFERENCES tenants(id),
  harness_id  UUID REFERENCES harnesses(id),
  step        INTEGER DEFAULT 1,   -- 1, 2, 3
  completed   BOOLEAN DEFAULT FALSE,
  metadata    JSONB,
  created_at  TIMESTAMPTZ DEFAULT now()
);
```

---

### 0.5.3 — Autonomous Delivery MVP ⬅ ПЕРЕМЕЩЕНО ИЗ ФАЗЫ 2
**Приоритет:** Критический (core value proposition, не nice-to-have)
**Оценка:** 1.5 чел-нед

Это то, что отличает харнес от просто API: сигналы **сами приходят к пользователю**, а не ждут запроса.

**Схема БД:**
```sql
CREATE TABLE delivery_settings (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     UUID REFERENCES tenants(id),
  harness_id    UUID REFERENCES harnesses(id),
  channel_type  TEXT NOT NULL CHECK (channel_type IN ('telegram', 'email', 'slack')),
  channel_config JSONB NOT NULL,      -- { "chat_id": "...", "bot_token": "..." }
  schedule      TEXT NOT NULL,        -- cron expression, e.g. "0 9 * * 1-5"
  workspace_ids TEXT[],              -- фильтр по источникам Frontier
  timezone      TEXT DEFAULT 'UTC',
  enabled       BOOLEAN DEFAULT TRUE,
  last_sent_at  TIMESTAMPTZ,
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE delivery_log (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  delivery_id     UUID REFERENCES delivery_settings(id),
  tenant_id       UUID REFERENCES tenants(id),
  status          TEXT CHECK (status IN ('sent', 'failed', 'skipped')),
  brief_summary   TEXT,
  signal_count    INTEGER,
  error_message   TEXT,
  sent_at         TIMESTAMPTZ DEFAULT now()
);
```

**Daily Brief Pipeline:**
```
cron trigger (по расписанию тенанта)
  → get_frontier_brief(workspace_ids, role_context)
  → format_for_channel(harness.role_name, channel_type)
  → deliver(channel_config)
  → log(delivery_log)
```

**MVP — только Telegram:**
- Telegram Bot: один бот на платформу, `chat_id` per tenant
- Формат: Markdown-сообщение с топ-3 сигналами + ссылка на подробности
- Команды бота: `/brief` — немедленный бриф, `/pause` — приостановить доставку

**Следующие каналы (Фаза 3):** Email, Slack webhook.

---

### 0.5.4 — Role Intelligence Foundation
**Оценка:** 1 чел-нед

Закладывает данные для персонализации, которая придёт в Фазе 2. Логировать с первого дня — данных должно быть достаточно к моменту когда персонализация понадобится.

**Схема БД:**
```sql
CREATE TABLE harness_interactions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id       UUID REFERENCES tenants(id),
  harness_id      UUID REFERENCES harnesses(id),
  mode            TEXT,              -- 'stakeholder_move', 'vision_scan', 'frontier_brief', etc.
  query_summary   TEXT,              -- краткое описание запроса (без PII)
  signal_ids_used UUID[],            -- какие сигналы были использованы в ответе
  cluster_ids     UUID[],            -- какие кластеры были задействованы
  feedback        SMALLINT,          -- -1 / 0 / 1 (thumbs down / neutral / thumbs up)
  response_ms     INTEGER,           -- время ответа
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- Индексы для будущей аналитики
CREATE INDEX ON harness_interactions (tenant_id, harness_id);
CREATE INDEX ON harness_interactions (created_at);
CREATE INDEX ON harness_interactions USING GIN (signal_ids_used);
```

**Что логируем (и что не логируем):**
- ✅ Логируем: harness_id, mode, signal/cluster IDs, feedback, timing
- ❌ Не логируем: полный текст запроса, контент ответа, персональные данные

API: `POST /interactions` — принимает события из Skill/MCP. Вызывается автоматически skill-ом после каждого ответа.

---

### Итого Фаза 0.5 — 6 чел-нед

| Подзадача | Оценка | Кто |
|-----------|--------|-----|
| 0.5.1 Harness Catalog | 1.5 нед | Backend |
| 0.5.2 Claude Project Template Installer | 2 нед | Backend + DevOps |
| 0.5.3 Autonomous Delivery MVP | 1.5 нед | Backend |
| 0.5.4 Role Intelligence Foundation | 1 нед | Backend |

**Критический путь:** 0.5.1 → 0.5.2 → 0.5.3 (параллельно с 0.5.4)

---

## Фаза 1 — Role-first UI ⬅ СКОРРЕКТИРОВАН ФРЕЙМ
**Оценка:** 4 чел-нед
**Зависит от:** Фаза 0.5 полностью

> **Важно:** Первый UI — это **не** дашборд с кластерами и сигналами. Это интерфейс онбординга и управления харнесом. Просмотр кластеров — power user feature, приходит позже.

---

### 1.1 — Role Selector & Harness Installer
**Оценка:** 1.5 чел-нед

Это первый экран, который видит новый пользователь.

**Экраны:**
- `/onboarding` — грид из 3 карточек харнесов (Design Lead / AI PM / VC Analyst)
  - Карточка: роль, описание, список из 3 use case'ов, кнопка «Начать»
- `/onboarding/{harness_id}/step-1` — подтверждение выбора
- `/onboarding/{harness_id}/step-2` — скачать Claude Project Template (кнопка + инструкция)
- `/onboarding/{harness_id}/step-3` — MCP endpoint + токен + QR/ссылка для Claude

**UX-принцип:** пользователь уходит с онбординга с **работающим харнесом в Claude**, а не с аккаунтом на сайте.

---

### 1.2 — Delivery Settings UI
**Оценка:** 1 чел-нед

Управление автономной доставкой — второй по важности экран.

**Экраны:**
- `/settings/delivery` — список настроенных доставок
- `/settings/delivery/new` — форма: выбор канала (Telegram), время, дни недели, таймзона
- Telegram connect flow: кнопка «Подключить Telegram» → инструкция → поле для `chat_id` → тест-сообщение
- Кнопка «Отправить тестовый бриф» — мгновенная проверка

---

### 1.3 — Harness Dashboard (минимальный)
**Оценка:** 1 чел-нед

Не главная страница, а дополнительный экран для тех, кто хочет больше контроля.

**Что входит:**
- Статус активного харнеса + последняя доставка
- История delivery_log (последние 7 дней)
- Кнопка «Сменить харнес»
- Счётчик взаимодействий и uptime

**Что НЕ входит в Фазу 1:** детальный просмотр кластеров, поиск по сигналам, аналитика — это Фаза 2.

---

### 1.4 — Settings & Account
**Оценка:** 0.5 чел-нед

- API Key management (просмотр, ротация)
- MCP token rotation
- Базовый billing placeholder (plan, usage)

---

## Фаза 2 — Intelligence Layer
**Оценка:** 5–6 чел-нед
**Зависит от:** Фаза 1 + накопленные данные из harness_interactions

### 2.1 — Персонализация по роли
- Анализ harness_interactions: какие сигналы и кластеры чаще всего полезны для данного тенанта
- Персонализированный ранжинг сигналов в брифах
- Настройка тематических фильтров: `POST /tenants/me/signal-preferences`

### 2.2 — Power User UI: Signal Explorer
- Полноценный просмотр кластеров и сигналов
- Поиск по семантике (search_frontier API)
- Сохранённые фильтры, закладки, история

### 2.3 — Расширение каталога харнесов
- Добавление новых ролей (минимум 3–5 новых)
- A/B тестирование system prompt'ов разных версий
- Пользовательские модификации харнеса (кастомный system prompt поверх базового)

### 2.4 — Аналитика для тенанта
- Dashboard: сколько брифов получено, какие сигналы, топ-кластеры по роли
- Экспорт истории взаимодействий в CSV

---

## Фаза 3 — Scale & Ecosystem
**Оценка:** 6–8 чел-нед

### 3.1 — Мультиканальная доставка
- Email delivery: интеграция с Resend/SendGrid, HTML-шаблон под роль
- Slack webhook: форматирование под блоки Slack, канал на воркспейс

### 3.2 — Self-serve Onboarding
- Регистрация без участия команды
- Free trial: 14 дней, 1 харнес, 1 канал доставки
- Stripe billing: subscription + usage-based для heavy API users

### 3.3 — Partner / White-label API
- `POST /harnesses` — создание кастомного харнеса партнёром
- Branded onboarding page per partner
- Revenue share модель (конфигурируемый split)

### 3.4 — Harness Marketplace
- Публичный каталог с рейтингами
- Community-submitted харнесы (с модерацией)
- Версионирование и changelog харнесов

---

## Пересмотренные приоритеты: что изменилось

| Задача | Было | Стало | Причина |
|--------|------|-------|---------|
| Harness Catalog | Не было | Фаза 0.5 — Критический | Core delivery mechanism |
| Claude Project Template | Не было | Фаза 0.5 — Критический | Без него клиент не получает продукт |
| Autonomous Delivery | Фаза 2 — Nice-to-have | Фаза 0.5 — Критический | Core value proposition |
| Role Intelligence logging | Фаза 3 — Future | Фаза 0.5 — Важный | Данные нужны с первого дня |
| UI: кластер-дашборд | Фаза 1 — Главный экран | Фаза 2 — Power user | Не то, что нужно при онбординге |
| UI: Role Selector | Не было | Фаза 1 — Главный экран | Первое, что видит клиент |
| UI: Delivery Settings | Фаза 2 | Фаза 1 | Нужно сразу после онбординга |

---

## Риски и зависимости

**Технические риски:**
- Claude Project Template format может измениться — нужен версионированный экспорт + changelog
- Telegram Bot: rate limits при масштабировании (решение: queue-based delivery)
- MCP endpoint latency: нужен мониторинг p95 < 2s

**Продуктовые риски:**
- Онбординг в 3 шага может быть сложным — нужно юзер-тестирование до запуска Фазы 1
- Харнесы требуют тонкой настройки system prompt под каждую роль — выделить время на QA с реальными пользователями

**Зависимости:**
- Фаза 0.5.2 зависит от стабильного Claude Projects API (экспорт формата)
- Фаза 0.5.3 зависит от Telegram Bot token (нужен аккаунт + верификация)
- Фаза 2 (персонализация) требует минимум 4 недели данных из harness_interactions

---

## Критерии готовности к запуску (Definition of Done per Phase)

**Фаза 0.5 готова, когда:**
- [ ] Тенант может пройти onboarding и скачать claude-project.json за < 5 минут
- [ ] Импортированный в Claude Project MCP endpoint отвечает на get_frontier_brief
- [ ] Telegram бриф приходит по расписанию минимум 5 рабочих дней подряд без ошибок
- [ ] harness_interactions логируются при каждом MCP вызове

**Фаза 1 готова, когда:**
- [ ] Новый пользователь проходит онбординг без посторонней помощи (тест на 3 незнакомых людях)
- [ ] Delivery settings можно настроить и протестировать из UI
- [ ] История доставок отображается корректно

---

*Следующий шаг: синхронизировать этот roadmap с командой и зафиксировать владельца каждого блока Фазы 0.5.*
