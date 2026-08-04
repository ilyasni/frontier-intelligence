# Frontier Intelligence → SaaS: Технический Roadmap

<!-- audit-status:2026-08-04 -->
> **📌 ИСТОРИЧЕСКИЙ СНИМОК · сверено 2026-08-04.**
> Датированный снимок/решение своего момента. Ценен как история — описанием сегодняшнего состояния не является.
> Перекрыт: **technical-roadmap-v2.md и пакет docs/saas/**. Предыдущие версии оставлены намеренно — расхождение между ними и есть содержание (коммит `f8d7bbf`).
> Конкретных расхождений найдено: **3** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

**Дата:** июнь 2026  
**Горизонт:** 24 недели  
**Команда:** 2–3 инженера  
**Текущий статус:** работающий pet-project с полным pipeline (ingest → worker → MCP), Admin API и схемой БД с `workspace_id`

---

## Оглавление

1. [Архитектурный контекст](#архитектурный-контекст)
2. [Must-have vs Nice-to-have](#must-have-vs-nice-to-have)
3. [Фаза 0 — Закрытая Beta (0–8 нед)](#фаза-0--закрытая-beta-08-нед)
4. [Фаза 1 — Public Beta (8–16 нед)](#фаза-1--public-beta-816-нед)
5. [Фаза 2 — Production SaaS (16–24 нед)](#фаза-2--production-saas-1624-нед)
6. [Зависимости между фазами](#зависимости-между-фазами)
7. [Риски и митигация](#риски-и-митигация)

---

## Архитектурный контекст

### Что уже работает

| Компонент | Статус | Примечания |
|---|---|---|
| Ingest pipeline | ✅ готов | URL → parsing → chunking |
| Worker (async tasks) | ✅ готов | очереди через Redis |
| LLM-оркестрация | ✅ готов | chain-of-thought, кластеризация |
| MCP-сервер | ✅ готов | API для агентов |
| Admin API | ✅ готов | внутреннее управление |
| PostgreSQL schema | ✅ + `workspace_id` | миграции через Alembic |
| Qdrant | ✅ готов | векторный поиск |
| Neo4j | ✅ готов | граф концептов |
| Redis | ✅ готов | кэш + очереди |
| S3 | ✅ готов | хранение raw контента |

### Что нужно построить

```
Auth layer (JWT + OAuth) → Tenant model → RLS → Billing → UI → Ops
```

---

## Must-have vs Nice-to-have

### 🔴 MUST-HAVE (нельзя пропустить — безопасность и работоспособность)

- **Row-Level Security (RLS)** в PostgreSQL — без этого возможна утечка данных между тенантами
- **Tenant isolation в Qdrant и Neo4j** — namespace/filter на каждый запрос
- **JWT-аутентификация** с коротким TTL + refresh tokens
- **Blacklist токенов** в Redis (при logout/revoke)
- **HTTPS** с автопродлением сертификатов (Let's Encrypt / Traefik)
- **Secret management** — никаких credentials в коде, только env/Vault
- **Rate limiting** per API key — защита от abuse ещё до биллинга
- **Input validation** на всех публичных endpoint-ах (Pydantic уже есть, но нужен audit)
- **Quota enforcement** — нельзя давать неограниченный доступ даже на beta

### 🟡 NICE-TO-HAVE (можно отложить)

- SSO / SAML (нужен только для enterprise)
- Полноценный UI (в фазе 0 достаточно Swagger)
- SDK-генерация (Python/JS клиент)
- Мультирегиональное развёртывание
- Audit log UI
- PDF-инвойсы
- Продвинутая аналитика (cohort, churn)
- Read replicas PostgreSQL

---

## Фаза 0 — Закрытая Beta (0–8 нед)

**Цель:** дать доступ 10 доверенным пользователям через API. UI — Swagger/Postman. Биллинга нет, онбординг — вручную.

**Состав команды:** 2 инженера full-time

---

### 0.1 Мультиарендность — схема данных (нед 1–2)

**Оценка:** 1.5 чел.-нед

#### Миграции Alembic

```sql
-- Таблица тенантов
CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,          -- используется в subdomain/URL
    plan        TEXT NOT NULL DEFAULT 'beta',
    status      TEXT NOT NULL DEFAULT 'active', -- active | suspended | deleted
    limits      JSONB NOT NULL DEFAULT '{}',    -- {"requests_per_day": 1000, "sources_max": 50}
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Пользователи
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email           TEXT NOT NULL UNIQUE,
    hashed_password TEXT,                       -- NULL если OAuth-only
    role            TEXT NOT NULL DEFAULT 'member', -- owner | admin | member | viewer
    status          TEXT NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_users_tenant ON users(tenant_id);
CREATE INDEX idx_users_email  ON users(email);

-- API-ключи
CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id     UUID REFERENCES users(id) ON DELETE SET NULL,
    key_hash    TEXT NOT NULL UNIQUE,           -- bcrypt/sha256 от реального ключа
    name        TEXT NOT NULL,
    scopes      TEXT[] NOT NULL DEFAULT '{}',   -- ['read', 'write', 'ingest']
    last_used   TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_api_keys_tenant ON api_keys(tenant_id);

-- Refresh-токены
CREATE TABLE refresh_tokens (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked     BOOL NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### Привязка workspace_id → tenant_id

Существующие таблицы уже имеют `workspace_id`. Нужна миграция-алиас:

```sql
-- workspace_id теперь фактически tenant_id
ALTER TABLE sources  ADD COLUMN tenant_id UUID REFERENCES tenants(id);
ALTER TABLE clusters ADD COLUMN tenant_id UUID REFERENCES tenants(id);
-- ... аналогично для всех таблиц с workspace_id
-- После бэкфилла старых данных workspace_id можно оставить как alias
```

**Definition of Done:** миграции применены, тесты на FK-constraints проходят, старые данные не сломаны.

---

### 0.2 Row-Level Security в PostgreSQL (нед 2)

**Оценка:** 0.5 чел.-нед

```sql
-- Включить RLS на всех tenant-scoped таблицах
ALTER TABLE sources  ENABLE ROW LEVEL SECURITY;
ALTER TABLE clusters ENABLE ROW LEVEL SECURITY;
ALTER TABLE signals  ENABLE ROW LEVEL SECURITY;
-- ... и все остальные

-- Политика: видим только свой tenant
CREATE POLICY tenant_isolation ON sources
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- FastAPI dependency устанавливает контекст перед каждым запросом:
-- SET LOCAL app.current_tenant_id = '<uuid>';
```

Приложение через `asyncpg`/`SQLAlchemy` выполняет `SET LOCAL app.current_tenant_id` в начале каждой транзакции через dependency injection.

```python
# app/dependencies/tenant.py
async def get_db_with_tenant(
    db: AsyncSession = Depends(get_db),
    current_tenant: Tenant = Depends(get_current_tenant)
) -> AsyncSession:
    await db.execute(
        text("SET LOCAL app.current_tenant_id = :tid"),
        {"tid": str(current_tenant.id)}
    )
    return db
```

**Definition of Done:** интеграционный тест — пользователь тенанта A не видит данные тенанта B даже при прямом SQL-запросе через app-пользователя БД.

---

### 0.3 Tenant isolation в Qdrant и Neo4j (нед 2–3)

**Оценка:** 1 чел.-нед

#### Qdrant

Два подхода — выбираем коллекцию-per-tenant для максимальной изоляции:

```python
# app/services/vector_store.py
class TenantVectorStore:
    def collection_name(self, tenant_id: str) -> str:
        return f"tenant_{tenant_id}"

    async def ensure_collection(self, tenant_id: str):
        name = self.collection_name(tenant_id)
        if not await self.client.collection_exists(name):
            await self.client.create_collection(name, vectors_config=...)

    async def search(self, tenant_id: str, query_vector, limit=10):
        return await self.client.search(
            collection_name=self.collection_name(tenant_id),
            query_vector=query_vector,
            limit=limit
        )
```

Альтернатива (если коллекций будет много): единая коллекция + обязательный фильтр `{"must": [{"key": "tenant_id", "match": {"value": tenant_id}}]}` — менее изолировано, но дешевле по памяти.

#### Neo4j

```cypher
-- Все ноды получают label :Tenant_{tenant_id} или property tenant_id
-- Все запросы принудительно содержат WHERE n.tenant_id = $tenant_id
```

```python
# app/services/graph_store.py
async def get_concept(self, tenant_id: str, concept_id: str):
    result = await session.run(
        "MATCH (n:Concept {id: $id, tenant_id: $tid}) RETURN n",
        {"id": concept_id, "tid": tenant_id}
    )
```

**Definition of Done:** тест — поиск с tenant_id=A возвращает только векторы/ноды тенанта A.

---

### 0.4 Auth — JWT + API Keys (нед 3–4)

**Оценка:** 2 чел.-нед

#### Endpoints

```
POST /api/v1/auth/register          # только для приглашённых (invite code)
POST /api/v1/auth/login             # email + password → {access_token, refresh_token}
POST /api/v1/auth/refresh           # refresh_token → новый access_token
POST /api/v1/auth/logout            # revoke refresh token + blacklist access token
POST /api/v1/auth/password-reset/request
POST /api/v1/auth/password-reset/confirm

GET  /api/v1/me                     # текущий пользователь + tenant info
PATCH /api/v1/me                    # обновить профиль

POST /api/v1/api-keys               # создать API key (возвращает raw key ОДИН РАЗ)
GET  /api/v1/api-keys               # список ключей (без raw значений)
DELETE /api/v1/api-keys/{id}        # revoke
```

#### JWT-конфигурация

```python
# app/core/security.py
ACCESS_TOKEN_EXPIRE_MINUTES = 15      # короткий TTL
REFRESH_TOKEN_EXPIRE_DAYS   = 30
ALGORITHM = "RS256"                   # asymmetric — публичный ключ можно шарить
```

#### Blacklist в Redis

```python
# При logout или revoke:
await redis.setex(
    f"blacklist:token:{jti}",
    ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    "1"
)
# В AuthMiddleware перед каждым запросом:
is_blacklisted = await redis.exists(f"blacklist:token:{jti}")
```

#### FastAPI dependency-цепочка

```python
get_token_from_header
    → decode_jwt / lookup_api_key
        → get_user_from_db
            → get_current_tenant
                → check_tenant_active
                    → inject into request
```

**Definition of Done:** E2E тест — login → вызов API → logout → повторный вызов возвращает 401.

---

### 0.5 Rate Limiting (нед 4)

**Оценка:** 0.5 чел.-нед

```python
# app/middleware/rate_limit.py
# Sliding window counter в Redis
# Ключ: rate:{tenant_id}:{window_minute}
# Лимиты берутся из tenants.limits JSONB

DEFAULTS = {
    "beta": {"requests_per_minute": 60, "requests_per_day": 5000}
}
```

**Ответ при превышении:** `HTTP 429` с заголовком `Retry-After`.

**Definition of Done:** тест — 61-й запрос в минуту возвращает 429.

---

### 0.6 Quota Enforcement (нед 4–5)

**Оценка:** 1 чел.-нед

```sql
CREATE TABLE usage_counters (
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    metric      TEXT NOT NULL,   -- 'sources_ingested', 'api_calls_today', 'tokens_used'
    period      DATE NOT NULL,   -- для дневных/месячных счётчиков
    value       BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, metric, period)
);
```

```python
# app/services/quota.py
class QuotaService:
    async def check_and_increment(self, tenant_id, metric, amount=1):
        limit = tenant.limits.get(metric)
        current = await self.get_current(tenant_id, metric)
        if limit and current + amount > limit:
            raise QuotaExceededError(metric, limit)
        await self.increment(tenant_id, metric, amount)
```

Квоты применяются в: ingest endpoint, search endpoint, LLM-вызовах.

**Definition of Done:** при достижении лимита источников endpoint возвращает 402/429 с читаемым сообщением.

---

### 0.7 HTTPS + Infrastructure (нед 5)

**Оценка:** 0.5 чел.-нед

- Traefik как reverse proxy с Let's Encrypt (ACME)
- `docker-compose.prod.yml` с network isolation
- CORS: только разрешённые origins
- Security headers: `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`
- `.env.example` с документированными переменными

---

### 0.8 Observability (нед 5–6)

**Оценка:** 1 чел.-нед

```python
# Структурированные логи
import structlog
log = structlog.get_logger()
log.info("request.start", tenant_id=..., endpoint=..., method=...)
```

- **Sentry** — error tracking с tenant_id в контексте
- `GET /health` — liveness check
- `GET /health/ready` — readiness (проверяет PostgreSQL, Redis, Qdrant)
- `GET /metrics` — Prometheus endpoint (базовые HTTP метрики + очередь)

**Definition of Done:** ошибка в продакшне появляется в Sentry с tenant_id и stack trace.

---

### 0.9 Внутренний Admin CLI (нед 6–7)

**Оценка:** 0.5 чел.-нед

```bash
# cli/admin.py — используется командой для ручного онбординга
python -m cli.admin create-tenant --name "Acme Corp" --slug acme --plan beta
python -m cli.admin invite-user --tenant acme --email user@acme.com
python -m cli.admin set-limits --tenant acme --requests-per-day 2000
python -m cli.admin list-tenants
```

**Definition of Done:** можно онбордить нового beta-пользователя за 2 минуты без прямого SQL.

---

### ✅ Итог Фазы 0

| Метрика | Значение |
|---|---|
| Общая оценка | **7.5–9 чел.-нед** |
| Критический путь | RLS → Auth → Tenant isolation → Rate limit |
| Deliverable | 10 beta-тенантов с API-доступом |
| Риск | Утечка данных между тенантами — закрывается RLS + тестами |

**Definition of Done Фазы 0:**
- [ ] 10 тестовых тенантов онбордингованы без прямого SQL
- [ ] Тест на изоляцию данных между тенантами: PASS
- [ ] JWT-аутентификация работает (login/refresh/logout/revoke)
- [ ] Rate limiting срабатывает корректно
- [ ] HTTPS на продакшн-домене
- [ ] Ошибки попадают в Sentry с контекстом тенанта
- [ ] Health checks возвращают статус всех сервисов

---

## Фаза 1 — Public Beta (8–16 нед)

**Цель:** первые платящие клиенты. Самостоятельная регистрация, биллинг через Stripe, минимальный UI, командная работа.

**Состав команды:** 2–3 инженера (добавляем frontend)

---

### 1.1 Email-верификация и самостоятельная регистрация (нед 8–9)

**Оценка:** 1 чел.-нед

```sql
CREATE TABLE email_verifications (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    used        BOOL NOT NULL DEFAULT FALSE
);
```

```
POST /api/v1/auth/register          # открытая регистрация (снимаем invite-only)
POST /api/v1/auth/email/verify      # подтверждение по токену из письма
POST /api/v1/auth/email/resend      # повторная отправка
```

Email-провайдер: **SendGrid** (или AWS SES) с transactional templates.

**Важно:** до верификации email пользователь получает `403` на все endpoint-ы кроме верификации и resend.

---

### 1.2 Биллинг — Stripe (нед 9–12)

**Оценка:** 4 чел.-нед (биллинг всегда сложнее, чем кажется)

#### Схема данных

```sql
CREATE TABLE plans (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,               -- 'starter', 'pro', 'team'
    stripe_price_id TEXT NOT NULL UNIQUE,
    price_monthly   NUMERIC(10,2),
    limits          JSONB NOT NULL,
    -- {"sources_max": 50, "requests_per_day": 5000, "seats_max": 3, "tokens_per_month": 1000000}
    is_active       BOOL NOT NULL DEFAULT TRUE
);

CREATE TABLE subscriptions (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               UUID NOT NULL UNIQUE REFERENCES tenants(id),
    stripe_subscription_id  TEXT UNIQUE,
    stripe_customer_id      TEXT NOT NULL,
    plan_id                 UUID NOT NULL REFERENCES plans(id),
    status                  TEXT NOT NULL,   -- active | past_due | canceled | trialing
    trial_end               TIMESTAMPTZ,
    current_period_start    TIMESTAMPTZ,
    current_period_end      TIMESTAMPTZ,
    cancel_at_period_end    BOOL NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE invoices (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    stripe_invoice_id   TEXT NOT NULL UNIQUE,
    amount_due          NUMERIC(10,2) NOT NULL,
    amount_paid         NUMERIC(10,2) NOT NULL DEFAULT 0,
    status              TEXT NOT NULL,   -- draft | open | paid | void | uncollectible
    period_start        TIMESTAMPTZ,
    period_end          TIMESTAMPTZ,
    pdf_url             TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Детализированные события использования (для metered billing)
CREATE TABLE usage_events (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    metric      TEXT NOT NULL,
    value       NUMERIC NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_usage_events_tenant_metric ON usage_events(tenant_id, metric, recorded_at DESC);
```

#### Stripe Webhook (идемпотентный обработчик)

```
POST /api/v1/webhooks/stripe
```

Обрабатываемые события:
- `customer.subscription.created` → активировать тенант
- `customer.subscription.updated` → обновить план/лимиты
- `customer.subscription.deleted` → downgrade → free/suspended
- `invoice.payment_succeeded` → сохранить invoice, разблокировать если был past_due
- `invoice.payment_failed` → перевести в `past_due`, уведомить email
- `checkout.session.completed` → финализировать подписку

**Паттерн:** webhook-обработчик проверяет Stripe-подпись, обрабатывает события идемпотентно (проверка по `stripe_event_id`).

#### Endpoints

```
GET  /api/v1/billing/plans              # список планов
GET  /api/v1/billing/subscription       # текущая подписка тенанта
POST /api/v1/billing/checkout           # создать Stripe Checkout Session
POST /api/v1/billing/portal             # ссылка на Stripe Customer Portal
GET  /api/v1/billing/invoices           # список инвойсов
GET  /api/v1/billing/usage              # текущее использование vs лимиты
```

**Definition of Done:** полный flow — регистрация → выбор плана → Stripe Checkout → успешный webhook → тенант на платном плане.

---

### 1.3 Управление командой (нед 11–12)

**Оценка:** 1.5 чел.-нед

```sql
CREATE TABLE invitations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email           TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'member',
    token_hash      TEXT NOT NULL UNIQUE,
    invited_by      UUID REFERENCES users(id),
    expires_at      TIMESTAMPTZ NOT NULL,
    accepted_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```
POST /api/v1/team/invite                # пригласить по email
GET  /api/v1/team/members               # список участников
PATCH /api/v1/team/members/{user_id}    # изменить роль
DELETE /api/v1/team/members/{user_id}   # удалить из команды
GET  /api/v1/auth/invitation/{token}    # принять приглашение
```

**RBAC на endpoint-ах:**

| Роль | Действия |
|---|---|
| `owner` | всё, включая billing и удаление тенанта |
| `admin` | управление командой, настройки, источники |
| `member` | чтение и запись данных |
| `viewer` | только чтение |

---

### 1.4 Frontend UI — Next.js (нед 9–16)

**Оценка:** 6–8 чел.-нед (параллельно с бэкендом)

**Стек:** Next.js 14 (App Router), TypeScript, Tailwind CSS, shadcn/ui

#### Роуты и страницы

```
/login                      # форма логина
/register                   # регистрация + email verify
/onboarding                 # wizard: первый источник → ждём индексации

/dashboard                  # обзор: кластеры, сигналы, статистика
/sources                    # список источников
/sources/new                # добавить URL / RSS / интеграцию
/search                     # поиск по индексу
/clusters                   # граф кластеров (list view)
/clusters/{id}              # детальная страница кластера

/settings/general           # название, slug тенанта
/settings/api-keys          # управление ключами
/settings/team              # участники, приглашения
/settings/billing           # план, способ оплаты, инвойсы
/settings/usage             # текущее использование + прогресс-бары
```

**Definition of Done:** новый пользователь может зарегистрироваться, добавить источник и выполнить поиск без помощи команды.

---

### 1.5 Улучшение observability (нед 13–14)

**Оценка:** 1 чел.-нед

- **Prometheus + Grafana** дашборд: HTTP latency, queue depth, LLM-calls/min, ошибки по тенантам
- **Alerting:** queue backlog > 1000 → Slack/email; p99 latency > 5s → alert
- Retention policy для `usage_events` (партиционирование по месяцу)

---

### ✅ Итог Фазы 1

| Метрика | Значение |
|---|---|
| Общая оценка | **14–16 чел.-нед** |
| Критический путь | Email-verify → Stripe → Webhook → Plan enforcement → UI |
| Deliverable | Первые платящие клиенты через self-service |
| Риск | Stripe webhook reliability — решается идемпотентностью + retry |

**Definition of Done Фазы 1:**
- [ ] Полный flow самостоятельного онбординга без участия команды
- [ ] Stripe Checkout → активная подписка (e2e тест)
- [ ] Payment failure → уведомление + деградация доступа
- [ ] Team invite flow работает
- [ ] UI: все 10+ страниц в рабочем состоянии
- [ ] Квоты синхронизируются с купленным планом
- [ ] Dashboard показывает реальное использование

---

## Фаза 2 — Production SaaS (16–24 нед)

**Цель:** надёжный продукт с полным самообслуживанием, enterprise-ready, GDPR, масштабируемость.

---

### 2.1 OAuth2 / SSO (нед 16–17)

**Оценка:** 1.5 чел.-нед

```
GET  /api/v1/auth/oauth/{provider}/authorize   # Google, GitHub
GET  /api/v1/auth/oauth/{provider}/callback
```

```sql
CREATE TABLE oauth_accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider        TEXT NOT NULL,          -- 'google', 'github'
    provider_user_id TEXT NOT NULL,
    access_token    TEXT,                   -- зашифрован в БД
    refresh_token   TEXT,
    UNIQUE (provider, provider_user_id)
);
```

**Библиотека:** `authlib` (FastAPI-совместима).

---

### 2.2 Audit Log (нед 17–18)

**Оценка:** 1 чел.-нед

```sql
CREATE TABLE audit_logs (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    user_id     UUID REFERENCES users(id),
    action      TEXT NOT NULL,  -- 'source.created', 'api_key.revoked', 'user.invited', ...
    resource    TEXT,
    resource_id TEXT,
    metadata    JSONB,
    ip_address  INET,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_audit_logs_tenant ON audit_logs(tenant_id, created_at DESC);
```

```
GET /api/v1/audit-logs?resource=api_key&limit=50
```

Middleware `AuditMiddleware` автоматически логирует мутирующие операции (POST/PUT/PATCH/DELETE).

---

### 2.3 GDPR — Data Portability & Right to Erasure (нед 18–19)

**Оценка:** 1.5 чел.-нед

#### Data Export

```
POST /api/v1/data/export-request
GET  /api/v1/data/export/{job_id}
GET  /api/v1/data/export/{job_id}/download  # ссылка на S3
```

Async job генерирует ZIP: sources.json, clusters.json, signals.json, audit_logs.json.

#### Right to Erasure

```
DELETE /api/v1/tenants/{id}  # только owner
```

Cascade-удаление:
1. PostgreSQL: CASCADE FK уже настроен
2. Qdrant: удалить коллекцию `tenant_{id}`
3. Neo4j: `MATCH (n {tenant_id: $tid}) DETACH DELETE n`
4. S3: удалить prefix `tenants/{id}/`
5. Redis: удалить все ключи `tenant:{id}:*`
6. Stripe: cancel subscription

Реализуется как background job с `status` таблицей.

---

### 2.4 Infrastructure — Production Hardening (нед 19–21)

**Оценка:** 3 чел.-нед

#### Kubernetes (Helm)

```yaml
# Helm chart структура
charts/frontier/
  templates/
    api-deployment.yaml        # FastAPI, HPA min=2 max=10
    worker-deployment.yaml     # celery/arq workers, HPA по queue depth
    postgres-exporter.yaml
    qdrant-statefulset.yaml
    neo4j-statefulset.yaml
    redis-statefulset.yaml
  values.yaml
  values.prod.yaml
```

#### Database

- **PgBouncer** в режиме transaction pooling (FastAPI + asyncpg не держат persistent connections)
- Партиционирование `usage_events` и `audit_logs` по месяцу
- Автоматические бэкапы через `pg_dump` в S3 с retention 30 дней
- Runbook для restore

#### Worker Scaling

```yaml
# Приоритеты очередей arq/celery
QUEUE_HIGH: ingest_requested, webhook_process
QUEUE_MEDIUM: llm_cluster, vector_index
QUEUE_LOW: analytics_compute, export_job
```

#### Secrets

Миграция на **HashiCorp Vault** или **AWS Secrets Manager** — убрать все secrets из env-файлов в CI/CD.

---

### 2.5 Advanced Billing (нед 20–21)

**Оценка:** 1.5 чел.-нед

- Годовые планы со скидкой (stripe `billing_cycle_anchor`)
- Metered billing для LLM-токенов через `stripe.UsageRecord`
- Overage handling: soft cap (предупреждение) → hard cap (блокировка)
- Upgrade/downgrade в середине цикла — prorated billing через Stripe
- Enterprise план: ручное выставление счётов (Stripe Invoice manual)

---

### 2.6 Status Page & SLA (нед 21–22)

**Оценка:** 0.5 чел.-нед

- **Statuspage.io** или self-hosted (**Upptime** на GitHub Actions) — публичная страница `status.yourdomain.com`
- Synthetic monitoring: каждые 5 минут — POST /api/v1/search с test-тенантом
- Automated incident creation при 3 consecutive failures
- Runbook для каждого критического компонента (PostgreSQL down, Qdrant OOM, Queue backlog)

---

### 2.7 Developer Experience (нед 22–23)

**Оценка:** 1.5 чел.-нед

- Версионирование API: `/api/v1/` → `/api/v2/` с deprecation notices
- Python SDK auto-generated из OpenAPI spec (`openapi-generator`)
- Webhooks для pipeline events:
  ```
  source.indexed       # индексация завершена
  cluster.updated      # кластер изменился
  signal.new           # обнаружен новый сигнал
  quota.warning        # 80% квоты использовано
  ```
- `POST /api/v1/webhooks` — регистрация endpoint, HMAC-подпись, retry с backoff

---

### 2.8 Business Analytics (нед 23–24)

**Оценка:** 1 чел.-нед

**Internal admin dashboard** (не public):

```sql
-- Метрики для команды
SELECT 
    COUNT(DISTINCT t.id) as tenants_total,
    COUNT(DISTINCT t.id) FILTER (WHERE s.status = 'active') as paying,
    SUM(p.price_monthly) as mrr
FROM tenants t
LEFT JOIN subscriptions s ON s.tenant_id = t.id
LEFT JOIN plans p ON p.id = s.plan_id;
```

- MRR / ARR
- Churn rate (отменённые подписки за период)
- Trial-to-paid conversion
- Top tiers по использованию
- Источники регистраций (UTM)

---

### ✅ Итог Фазы 2

| Метрика | Значение |
|---|---|
| Общая оценка | **12–14 чел.-нед** |
| Критический путь | K8s hardening → GDPR → Status page |
| Deliverable | Enterprise-ready SaaS с SLA |

**Definition of Done Фазы 2:**
- [ ] Kubernetes deployment с автоскейлингом
- [ ] Erasure request выполняется полностью (все 5 хранилищ очищены)
- [ ] Data export работает для реального тенанта
- [ ] OAuth login через Google/GitHub
- [ ] Status page публичный и обновляется автоматически
- [ ] Python SDK установить через pip и использовать без документации
- [ ] Годовые планы в Stripe работают
- [ ] Audit log доступен через API

---

## Зависимости между фазами

```
Фаза 0                          Фаза 1                          Фаза 2
────────────────────────────────────────────────────────────────────────────
tenants table ──────────────►  subscriptions table ──────────► annual plans
RLS ─────────────────────────► (остаётся, расширяется) ──────► audit log
JWT auth ────────────────────► email verify + OAuth ──────────► SSO/SAML
api_keys ────────────────────► (остаётся) ────────────────────► SDK
rate limiting ───────────────► quota enforcement ─────────────► metered billing
docker-compose.prod ─────────► (остаётся) ────────────────────► Kubernetes
structlog + Sentry ──────────► Prometheus + Grafana ──────────► business analytics
```

**Критическое правило:** нельзя начинать Фазу 1 без полного прохождения DoD Фазы 0. Утечка данных между тенантами в публичном продукте — это PR-катастрофа.

---

## Риски и митигация

| Риск | Вероятность | Impact | Митигация |
|---|---|---|---|
| Data leak между тенантами | Средняя | Критический | RLS + интеграционные тесты + security review перед Фазой 1 |
| Stripe webhook потеря | Высокая | Высокий | Идемпотентные обработчики, reconciliation job раз в час |
| Neo4j multi-tenancy complexity | Средняя | Средний | Тесты изоляции, рассмотреть переход на PostgreSQL для метаданных |
| JWT secret rotation | Низкая | Высокий | RS256 с ротируемой парой ключей, `kid` в header |
| Worker queue backlog | Средняя | Средний | HPA по queue depth, circuit breaker на LLM-вызовах |
| Qdrant OOM при росте | Средняя | Средний | Мониторинг memory per collection, retention policy для старых векторов |
| Недооценка объёма UI | Высокая | Средний | Выделить отдельного frontend-инженера в Фазе 1 |
| GDPR compliance | Низкая | Высокий | Erasure job в Фазе 2, DPA с sub-processors (Stripe, AWS, Sentry) |

---

## Итоговая сводка по времени

| Фаза | Длительность | Оценка чел.-нед | Команда | Цель |
|---|---|---|---|---|
| **Фаза 0** | 0–8 нед | 7.5–9 | 2 инженера | 10 beta-пользователей |
| **Фаза 1** | 8–16 нед | 14–16 | 2–3 инженера | первые платящие |
| **Фаза 2** | 16–24 нед | 12–14 | 2–3 инженера | полноценный SaaS |
| **ИТОГО** | **24 нед** | **34–39 чел.-нед** | — | — |

> Бюджет 18–24 недели при команде 2–3 человека реалистичен при условии, что pipeline и Admin API не требуют значительного рефакторинга, и что команда не занимается параллельно другими проектами.

---

*Roadmap составлен: июнь 2026. Пересмотр рекомендуется после завершения каждой фазы.*
