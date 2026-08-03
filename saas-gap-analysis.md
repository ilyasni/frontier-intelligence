# SaaS Gap Analysis — Frontier Intelligence

**Дата:** 2026-06-28  
**Объект анализа:** `D:\Workspace\frontier-intelligence`  
**Текущее состояние:** один тенант, один оператор, один сервер  
**Целевое состояние:** полноценный multi-tenant SaaS с самообслуживанием

---

## Резюме

Frontier Intelligence — зрелая одиночная система мониторинга трендов с хорошей инфраструктурой: event-driven pipeline, векторный поиск, GraphRAG, LLM-оркестрация, Admin UI, Prometheus/Grafana. Понятие «workspace» уже есть в коде, но реализовано как **namespace для одного пользователя**, а не как изоляция между клиентами. Аутентификации нет вообще. Биллинга нет. До SaaS — 3–4 месяца плотной работы.

---

## 1. Multi-tenancy: что есть, чего нет

### Что сделано

`workspace_id` пронизывает весь стек как логический namespace:

- **PostgreSQL**: `workspace_id TEXT NOT NULL` стоит во всех 15+ таблицах (`posts`, `sources`, `trend_clusters`, `semantic_clusters`, `emerging_signals`, `media_objects` и т.д.). Индексы по `workspace_id` расставлены.
- **Qdrant**: фильтр по `workspace_id` в payload при каждом поиске. Коллекции `frontier_docs` и `trend_clusters` — единые, разделение логическое.
- **Neo4j**: узел `(:Workspace)` как корень для каждого пространства.
- **MCP API**: параметр `workspace` принимается во всех инструментах (`search_frontier`, `search_balanced`, `get_frontier_brief` и т.д.).
- **Admin API**: все роутеры фильтруют по workspace.

### Чего нет (критические пробелы)

**Нет изоляции между разными клиентами.** Текущие workspaces (`disruption`, `ai_trends`, `design` и др.) — это тематические разделы **одного** оператора, а не разные клиенты. Любой, кто знает URL Admin UI (`:8101`), видит все данные всех пространств без ограничений.

- В схеме нет таблицы `users`, `tenants` или `accounts`.
- В `workspaces` нет поля `owner_id` или `tenant_id`.
- В `sources` нет владельца — источники глобальны для инстанса.
- Qdrant не разбит на коллекции per-tenant — один индекс на всех.
- Redis Streams общий, без namespace по tenancy.

**Вердикт:** multi-tenancy на уровне данных — 40% сделано (workspace_id есть). На уровне изоляции клиентов — 0%.

---

## 2. Аутентификация и авторизация

### Что есть

Ничего. Ни одной строки auth-кода.

- `admin/backend/main.py` — FastAPI без middleware для аутентификации, CORS открытый (`allow_origins=["*"]`).
- `mcp/server.py` — аналогично, нет ни одного `Depends(get_current_user)`.
- Нет JWT, нет API-ключей, нет OAuth, нет сессий.
- Защита только сетевая: Admin UI и MCP слушают на локальных портах сервера (`:8101`, `:8100`), доступ только через SSH-туннель или firewall.

**Вердикт:** auth — 0%. Для SaaS нужно построить с нуля.

---

## 3. Биллинг и подписки

### Что есть

Элементы LLM FinOps внутри системы: `admin/backend/services/llm_finops.py`, `gigachat_balance.py`, `wormsoft_limits.py` — мониторинг расхода токенов и балансов **операторских** LLM-ключей. Это внутренний cost control, не billing для клиентов.

**Биллинга нет совсем.** Нет:
- Таблиц `subscriptions`, `invoices`, `usage_records`.
- Интеграции со Stripe / YooKassa / CloudPayments.
- Счётчиков потребления на уровне tenant (кол-во источников, запросов MCP, документов).
- Тарифных планов, лимитов, grace period.

**Вердикт:** billing — 0%. Построить с нуля.

---

## 4. Добавление источников пользователем

### Что есть (хорошо)

Admin UI (`:8101`) имеет полноценный CRUD для источников:
- Форма добавления источника (тип: Telegram / RSS / Web / API / Email / Habr).
- Поддержка пресетов для RSS (`RSS_PRESETS` в `shared/source_definitions.py`).
- Управление proxy-конфигом, расписанием cron, авторитетом источника.
- Валидация на бэкенде через `validate_source_payload`.
- Диагностика Telegram: статус разрешения канала, drift, peer.
- Bootstrap из `config/sources.yml` через Admin API.

### Чего не хватает для SaaS

- **Нет self-service для конечного пользователя** — текущий Admin UI требует знания системы (cron-синтаксис, proxy, tg_account_idx). Нужен упрощённый wizard: «вставь ссылку на Telegram-канал → добавить».
- **Telegram-аутентификация per-tenant** — сейчас `TG_API_ID_0/1` глобальные env-переменные. Для SaaS каждый клиент должен использовать свой Telegram-аккаунт (или пул оператора с балансировкой).
- **Нет квот** — пользователь может добавить 1000 источников, не ограничен ничем.
- **Нет UI для workspace** — пространства создаются через `config/workspaces.yml` и `init.sql`, не через UI.

**Вердикт:** backend для источников — 60% готов (логика есть). UX для самообслуживания — 20% (есть CRUD, нет wizard'а и квот).

---

## 5. Детальный gap-анализ по блокам

### Блок A. Идентификация пользователей и tenancy (0% → 100%)

| Компонент | Статус | Что делать |
|---|---|---|
| Таблица `users` (email, hashed_password, created_at) | ❌ нет | создать |
| Таблица `tenants` / `organizations` | ❌ нет | создать |
| Поле `tenant_id` в `workspaces`, `sources` | ❌ нет | добавить FK |
| JWT / OAuth 2.0 (Google, GitHub) | ❌ нет | FastAPI-Users или Auth0 |
| API-ключи для MCP (per-tenant) | ❌ нет | таблица `api_keys` + middleware |
| RBAC (owner / admin / viewer) | ❌ нет | роли в DB |
| Row-Level Security в Postgres | ❌ нет | включить RLS по tenant_id |

**Оценка:** 3–4 недели (один разработчик, fullstack).

---

### Блок B. Регистрация и онбординг (0% → 100%)

| Компонент | Статус | Что делать |
|---|---|---|
| Страница Sign Up / Login | ❌ нет | React SPA или Next.js |
| Email-верификация | ❌ нет | SMTP + токены |
| Онбординг wizard (выбор тем, первый источник) | ❌ нет | 3–5 шагов |
| Создание workspace при регистрации | ❌ нет | auto-provision |
| Телеграм-бот как альтернативный вход | 🔧 частично (token есть) | доделать |

**Оценка:** 2–3 недели.

---

### Блок C. Изоляция данных между клиентами (40% → 100%)

| Компонент | Статус | Что делать |
|---|---|---|
| `workspace_id` в PostgreSQL | ✅ есть | + добавить `tenant_id` |
| Qdrant: per-tenant коллекции или namespace | 🔶 workspace-фильтр | для SaaS — per-tenant коллекции |
| Neo4j: per-tenant subgraph | 🔶 workspace-корень | добавить `tenant_id` метку |
| Redis Streams: namespace `{tenant_id}:stream:posts:*` | ❌ нет | prefixing |
| Row-Level Security (Postgres) | ❌ нет | критично для SaaS |
| S3: per-tenant prefix или bucket | ❌ нет | `{tenant_id}/` prefix |

**Оценка:** 2 недели (database migrations + код).

---

### Блок D. Биллинг и тарифные планы (0% → 100%)

| Компонент | Статус | Что делать |
|---|---|---|
| Таблицы `plans`, `subscriptions`, `usage` | ❌ нет | schema |
| Интеграция со Stripe / YooKassa | ❌ нет | webhooks + SDK |
| Счётчики использования (sources, docs, MCP calls) | ❌ нет | Redis counters + cron sync |
| Лимиты по плану (enforce) | ❌ нет | middleware / guard |
| Invoices, receipt emails | ❌ нет | шаблоны |
| Trial период (14 дней, потом paywall) | ❌ нет | `trial_ends_at` в tenants |

**Оценка:** 3–4 недели (+ интеграция с платёжной системой занимает непредсказуемо).

---

### Блок E. Self-service UI для пользователей (20% → 100%)

| Компонент | Статус | Что делать |
|---|---|---|
| Dashboard (тренды, сигналы) | 🔶 в Admin UI | сделать user-facing версию |
| Добавление источника (wizard) | 🔶 admin CRUD | упрощённый UX без cron-синтаксиса |
| Управление workspace | ❌ только через YAML | UI create/edit/delete |
| Поиск по базе (user-facing) | 🔶 в Admin UI | перенести / выделить |
| Уведомления (trend alerts) | 🔶 Telegram-бот | + email, web push |
| Настройки профиля, API-ключи | ❌ нет | страница Account Settings |

**Оценка:** 4–6 недель (самый большой блок, UX с нуля).

---

### Блок F. Инфраструктура для масштабирования (частично готово)

| Компонент | Статус | Что делать |
|---|---|---|
| Stateless workers | ✅ есть | — |
| Redis event bus | ✅ есть | namespacing по tenant |
| Горизонтальное масштабирование worker | ✅ возможно | — |
| Один Docker stack → per-tenant провизионинг | ❌ один инстанс | решить: один инстанс / tenant или shared |
| Qdrant: per-tenant коллекции vs namespace | 🔶 один индекс | нужен выбор архитектуры |
| CI/CD + IaC (Terraform / Pulumi) | ❌ нет | для managed deploy |
| Rate limiting per-tenant | ❌ нет | Redis leaky bucket (шаблон уже есть в коде) |

**Оценка:** 2–3 недели.

---

### Блок G. Операционные возможности SaaS (0% → 100%)

| Компонент | Статус | Что делать |
|---|---|---|
| Super-admin панель (все тенанты) | ❌ нет | отдельный дашборд |
| Метрики per-tenant (Prometheus labels) | ❌ нет | добавить tenant_id в labels |
| Impersonation (support tool) | ❌ нет | временный токен |
| Audit log (кто что делал) | ❌ нет | таблица `audit_log` |
| Система уведомлений об инцидентах | 🔶 Telegram | + статусная страница |

**Оценка:** 2 недели.

---

## 6. Что уже готово и можно переиспользовать

**Сильные стороны текущей системы:**

- **Pipeline архитектура** — event-driven (Redis Streams), stateless workers, dead-letter queue — идеально для SaaS без рефакторинга.
- **LLM-оркестрация** — mature, multi-provider с circuit breakers, rate limiting, failover. Переносится 1:1, только добавить per-tenant billing counters.
- **workspace_id** — фундамент multi-tenancy уже заложен. Нужно добавить tenant_id поверх него и RLS.
- **Admin API** — хорошо структурированный FastAPI с роутерами по доменам. Можно переиспользовать большинство эндпоинтов, добавив auth middleware.
- **Мониторинг** — Prometheus + Grafana + Alertmanager — production-ready.
- **Telegram-интеграция** — проксирование, account rotator, тип источника — редко встречается в open-source системах такого уровня.
- **Схема PostgreSQL** — нормализованная, с индексами, idempotent init.sql. Миграции — добавить Alembic.
- **Документация** — обширная (docs/, .cursor/rules/) — ускорит онбординг новых разработчиков.

---

## 7. Суммарная оценка трудозатрат

| Блок | Трудозатраты | Приоритет |
|---|---|---|
| A. Auth + tenancy | 3–4 недели | 🔴 критично, первым |
| B. Регистрация + онбординг | 2–3 недели | 🔴 критично |
| C. Изоляция данных (RLS, namespacing) | 2 недели | 🔴 критично |
| D. Биллинг | 3–4 недели | 🟡 важно (без него нет SaaS) |
| E. Self-service UI | 4–6 недель | 🟡 важно (пользователь не видит систему) |
| F. Инфраструктура масштабирования | 2–3 недели | 🟢 можно отложить до 100+ клиентов |
| G. Операционные возможности | 2 недели | 🟢 можно отложить |
| **Итого** | **~18–24 недели (4–6 мес.)** | |

> *Оценка для команды 2–3 человека (fullstack + backend + devops). Один разработчик — умножить на 1.5–2x.*

---

## 8. Рекомендуемая последовательность

**Фаза 1 (месяц 1–2): Фундамент безопасности**
1. Добавить `tenants` + `users` таблицы, Alembic-миграции.
2. JWT auth middleware для Admin API и MCP.
3. Row-Level Security в PostgreSQL.
4. API-ключи для MCP per-tenant.

**Фаза 2 (месяц 2–3): Пользовательский опыт**
5. Sign Up / Login страница, email-верификация.
6. Онбординг wizard (создание workspace + первый источник).
7. User-facing dashboard (тренды, сигналы, поиск).

**Фаза 3 (месяц 3–4): Монетизация**
8. Таблицы биллинга, интеграция со Stripe / YooKassa.
9. Тарифные планы с лимитами (источники, документы, MCP-вызовы).
10. Trial период и paywall.

**Фаза 4 (месяц 4–6): Масштабирование**
11. Per-tenant namespacing в Redis, Qdrant, S3.
12. Rate limiting per-tenant.
13. Super-admin панель, audit log.
14. CI/CD, IaC.

---

## 9. Ключевые архитектурные решения (нужен выбор)

**Shared vs Siloed инфраструктура:**  
Сейчас — один Qdrant индекс, одна PostgreSQL база, один Redis. Для первых 50–100 клиентов shared-подход с RLS достаточен. При росте до 1000+ клиентов потребуется sharding или per-tenant БД. Рекомендация: начать shared + RLS, спроектировать migration path.

**Telegram-доступ per-tenant:**  
Критичная проблема для SaaS. Варианты: (a) оператор предоставляет пул Telethon-аккаунтов, (b) клиент привязывает свой номер (сложно + Terms of Service Telegram), (c) ограничить Telegram только для enterprise-планов. Нужно решить до фазы 1.

**LLM-расходы:**  
Сейчас все GigaChat/Wormsoft/OpenRouter ключи — оператора. Для SaaS нужно: (a) включать LLM в стоимость тарифа с жёсткими лимитами, или (b) дать клиенту ввести свои ключи (BYOK). Рекомендация: BYOK для enterprise + managed для basic.
