# Autonomous Delivery System — Frontier Intelligence
<!-- audit-status:2026-08-04 -->
> **📐 ЗАМЫСЕЛ, НЕ РЕАЛИЗОВАНО · сверено 2026-08-04.**
> Замысел, а не описание системы: на дату сверки не реализован. Не читать как отчёт о готовом.
> Конкретных расхождений найдено: **3** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> Архитектурный дизайн-документ. Версия 1.0 · Июнь 2026

---

## Общая идея

Клиент подписывается → система сама доставляет инсайты без ручных запросов.

Ядро системы — `delivery` сервис (отдельный Docker-контейнер), который работает рядом с существующим `worker`. Он читает из PostgreSQL расписания доставки, триггерится от Redis Streams по событиям новизны, вызывает MCP-инструменты Frontier и отправляет результат в Telegram / Email.

```
┌─────────────────────────────────────────────────────────────────┐
│                        FRONTIER BACKEND                         │
│                                                                 │
│  FastAPI  ──►  PostgreSQL  ◄──  Worker (LLM tasks)             │
│     │              │                │                           │
│     │         Redis Streams         │                           │
│     │              │                │                           │
│     └──────► DELIVERY SERVICE ◄─────┘                          │
│                    │                                            │
│             ┌──────┴──────┐                                     │
│         Telegram Bot    Email (SMTP/SES)                        │
└─────────────────────────────────────────────────────────────────┘
```

**Почему отдельный сервис, а не расширение worker?**
Worker занимается тяжёлыми LLM-задачами (векторизация, кластеризация, суммаризация). Delivery — лёгкий планировщик с сетевыми вызовами. Разные профили нагрузки, разные причины падения, разный масштаб. Смешивать — плохая идея.

---

## База данных: новые таблицы

### `delivery_settings` — настройки доставки на тенанта

```sql
CREATE TABLE delivery_settings (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    channel_type    VARCHAR(20) NOT NULL CHECK (channel_type IN ('telegram', 'email')),
    schedule_type   VARCHAR(20) NOT NULL CHECK (schedule_type IN ('daily', 'weekly', 'alert')),

    -- Telegram
    telegram_chat_id    BIGINT,

    -- Email
    email_address       VARCHAR(255),
    email_name          VARCHAR(255),

    -- Расписание
    cron_expression     VARCHAR(100),           -- '0 8 * * *' для daily
    timezone            VARCHAR(64) NOT NULL DEFAULT 'UTC',
    day_of_week         SMALLINT,               -- 0-6, для weekly (0 = воскресенье)
    send_hour           SMALLINT,               -- час отправки в timezone клиента

    -- Контент
    workspace_ids       UUID[] NOT NULL DEFAULT '{}',  -- воркспейсы для этого тенанта
    max_signals         SMALLINT DEFAULT 10,
    novelty_threshold   NUMERIC(4,3) DEFAULT 0.75,     -- для alert-триггера
    language            VARCHAR(10) DEFAULT 'ru',

    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_tenant_channel_schedule UNIQUE (tenant_id, channel_type, schedule_type)
);

CREATE INDEX idx_delivery_settings_tenant ON delivery_settings(tenant_id);
CREATE INDEX idx_delivery_settings_enabled ON delivery_settings(enabled) WHERE enabled = TRUE;
```

### `delivery_log` — история отправок

```sql
CREATE TABLE delivery_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    settings_id     UUID NOT NULL REFERENCES delivery_settings(id) ON DELETE CASCADE,
    tenant_id       UUID NOT NULL,
    schedule_type   VARCHAR(20) NOT NULL,
    status          VARCHAR(20) NOT NULL CHECK (status IN ('sent', 'failed', 'skipped')),
    channel_type    VARCHAR(20) NOT NULL,

    -- Метрики
    signals_count   SMALLINT,
    clusters_count  SMALLINT,
    message_chars   INT,

    -- Отладка
    error_message   TEXT,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms     INT
);

CREATE INDEX idx_delivery_log_tenant ON delivery_log(tenant_id, sent_at DESC);
CREATE INDEX idx_delivery_log_settings ON delivery_log(settings_id, sent_at DESC);
```

### `novelty_events` — события для alert-триггера

```sql
CREATE TABLE novelty_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id      UUID NOT NULL,
    workspace_id    UUID NOT NULL,
    novelty_score   NUMERIC(4,3) NOT NULL,
    signal_count    SMALLINT NOT NULL,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered       BOOLEAN NOT NULL DEFAULT FALSE,

    -- Дедупликация: один алерт на кластер за период
    CONSTRAINT uq_cluster_window UNIQUE (cluster_id, date_trunc('hour', fired_at))
);

CREATE INDEX idx_novelty_events_undelivered ON novelty_events(fired_at DESC)
    WHERE delivered = FALSE;
```

---

## Сценарий 1: Ежедневный Telegram-бриф

### Архитектура

```
CRON (APScheduler)
    │  08:00 timezone клиента
    ▼
DailyBriefJob.run(tenant_id)
    │
    ├── get_frontier_brief(workspace_ids)   ← вызов MCP-инструмента
    │
    ├── BriefFormatter.format_telegram()    ← чистый текст, без markdown-помойки
    │
    └── TelegramSender.send(chat_id, text)
         │
         └── delivery_log INSERT
```

**Где живёт логика:** `delivery` сервис, модуль `jobs/daily_brief.py`

### Псевдокод

```python
# jobs/daily_brief.py

class DailyBriefJob:
    async def run(self, settings: DeliverySettings):
        start = time.monotonic()
        try:
            # 1. Получаем бриф по всем воркспейсам тенанта
            brief = await self.mcp_client.call(
                "get_frontier_brief",
                workspace_ids=settings.workspace_ids
            )

            # 2. Форматируем под Telegram
            text = self.formatter.daily_brief(brief, lang=settings.language)

            # 3. Отправляем
            await self.telegram.send(settings.telegram_chat_id, text)

            # 4. Логируем успех
            await self.log_delivery(settings, status="sent",
                signals_count=len(brief.signals),
                duration_ms=int((time.monotonic() - start) * 1000))

        except Exception as e:
            await self.log_delivery(settings, status="failed", error=str(e))
            logger.error(f"DailyBrief failed for tenant {settings.tenant_id}: {e}")


# formatting/telegram.py

class TelegramFormatter:
    def daily_brief(self, brief: FrontierBrief, lang: str = "ru") -> str:
        today = datetime.now().strftime("%d %B")
        lines = [
            f"📡 Frontier Brief — {today}",
            "",
        ]

        if brief.top_signals:
            lines.append("Главное за сутки:")
            for i, signal in enumerate(brief.top_signals[:5], 1):
                lines.append(f"{i}. {signal.title} ({signal.source_count} источника)")
                if signal.summary:
                    lines.append(f"   {signal.summary[:120]}...")
            lines.append("")

        if brief.emerging_clusters:
            lines.append("Растущие кластеры:")
            for cluster in brief.emerging_clusters[:3]:
                lines.append(f"• {cluster.name} — {cluster.signal_delta:+d} сигналов")
            lines.append("")

        lines.append(f"Всего сигналов за 24ч: {brief.total_signals_24h}")
        lines.append(f"🔗 frontier.app/brief/{brief.workspace_id}")

        return "\n".join(lines)
```

**Ключевые решения:**
- **Без markdown** — только чистый текст + Unicode-эмодзи. Telegram не требует форматирования для читаемости.
- **Не более 5 сигналов** — больше не читают в утреннем брифе.
- **Ссылка в конце** — один deep-link на полный дашборд, не на каждый сигнал.

### Планировщик с timezone

```python
# scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

class DeliveryScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()

    async def load_schedules(self):
        """Загружаем все активные расписания из БД при старте"""
        settings_list = await db.fetch_all(
            "SELECT * FROM delivery_settings WHERE enabled = TRUE"
        )
        for settings in settings_list:
            self._add_job(settings)

    def _add_job(self, settings: DeliverySettings):
        tz = pytz.timezone(settings.timezone)

        if settings.schedule_type == "daily":
            trigger = CronTrigger(
                hour=settings.send_hour,
                minute=0,
                timezone=tz
            )
            self.scheduler.add_job(
                daily_brief_job.run,
                trigger=trigger,
                args=[settings],
                id=f"daily_{settings.id}",
                replace_existing=True
            )

        elif settings.schedule_type == "weekly":
            trigger = CronTrigger(
                day_of_week=settings.day_of_week,  # 0 = воскресенье
                hour=settings.send_hour,
                minute=0,
                timezone=tz
            )
            self.scheduler.add_job(
                weekly_digest_job.run,
                trigger=trigger,
                args=[settings],
                id=f"weekly_{settings.id}",
                replace_existing=True
            )
```

**Новые таблицы:** `delivery_settings`, `delivery_log`
**Зависимости:** MCP-клиент (`get_frontier_brief`), Telegram Bot API, APScheduler, pytz

**Оценка реализации:** 3 человеко-дня
- День 1: `delivery` сервис, структура, Telegram-клиент, `delivery_settings` миграция
- День 2: `DailyBriefJob`, форматтер, планировщик с timezone
- День 3: `delivery_log`, тесты, Docker-интеграция, деплой

---

## Сценарий 2: Novelty Alert

### Архитектура

**Выбор:** Redis Streams, не polling-cron.

Cron проверяет базу каждые N минут — это O(кластеры × тенанты) запросов, которые никогда не находят ничего нового в 95% случаев. Redis Streams решают проблему инверсией: `worker` пишет событие в стрим когда находит новизну → `delivery` читает и реагирует.

```
Worker (кластеризация)
    │  обнаружил cluster с novelty_score > 0.8
    │
    ▼
Redis XADD frontier:novelty:events
    {
      cluster_id: "...",
      workspace_id: "...",
      novelty_score: 0.87,
      signal_count: 12,
      top_signals: [...]
    }
    │
    ▼
delivery сервис — consumer group "delivery"
    │  XREADGROUP
    ▼
NoveltyAlertJob.handle(event)
    │
    ├── Найти тенантов, подписанных на этот workspace
    │   (delivery_settings WHERE 'workspace_id' = ANY(workspace_ids)
    │    AND schedule_type = 'alert' AND enabled = TRUE)
    │
    ├── Проверить дедупликацию (novelty_events таблица)
    │
    ├── get_cluster_details(cluster_id)   ← MCP для полного контекста
    │
    ├── AlertFormatter.format_telegram()
    │
    └── TelegramSender.send() × N тенантов
```

### Псевдокод

```python
# jobs/novelty_alert.py

class NoveltyAlertConsumer:
    STREAM_KEY = "frontier:novelty:events"
    GROUP_NAME = "delivery"
    CONSUMER_NAME = "delivery-worker-1"

    async def start(self):
        # Создаём consumer group если не существует
        try:
            await redis.xgroup_create(
                self.STREAM_KEY, self.GROUP_NAME,
                id="$", mkstream=True
            )
        except ResponseError:
            pass  # группа уже есть

        while True:
            # Читаем батч событий
            messages = await redis.xreadgroup(
                groupname=self.GROUP_NAME,
                consumername=self.CONSUMER_NAME,
                streams={self.STREAM_KEY: ">"},
                count=10,
                block=5000  # ждём 5 сек если нет событий
            )

            for stream, events in (messages or []):
                for msg_id, data in events:
                    await self.handle_event(msg_id, data)

    async def handle_event(self, msg_id: str, data: dict):
        try:
            event = NoveltyEvent(**data)

            # Дедупликация: не слать алерт по одному кластеру дважды за час
            already_fired = await self.check_dedup(event.cluster_id)
            if already_fired:
                await redis.xack(self.STREAM_KEY, self.GROUP_NAME, msg_id)
                return

            # Найти подписчиков на этот workspace
            subscribers = await db.fetch_all("""
                SELECT ds.* FROM delivery_settings ds
                WHERE ds.schedule_type = 'alert'
                  AND ds.enabled = TRUE
                  AND ds.novelty_threshold <= :novelty_score
                  AND :workspace_id = ANY(ds.workspace_ids)
            """, workspace_id=event.workspace_id,
                novelty_score=event.novelty_score)

            if not subscribers:
                await redis.xack(self.STREAM_KEY, self.GROUP_NAME, msg_id)
                return

            # Получаем полный контекст кластера
            cluster = await self.mcp_client.call(
                "get_cluster_details",
                cluster_id=event.cluster_id
            )

            # Рассылаем всем подписчикам
            for settings in subscribers:
                text = self.formatter.novelty_alert(cluster, event)
                await self.telegram.send(settings.telegram_chat_id, text)
                await self.log_delivery(settings, status="sent", clusters_count=1)

            # Записываем в novelty_events для дедупликации
            await db.execute("""
                INSERT INTO novelty_events
                    (cluster_id, workspace_id, novelty_score, signal_count, delivered)
                VALUES (:cluster_id, :workspace_id, :novelty_score, :signal_count, TRUE)
                ON CONFLICT (cluster_id, date_trunc('hour', fired_at)) DO NOTHING
            """, **event.__dict__)

            await redis.xack(self.STREAM_KEY, self.GROUP_NAME, msg_id)

        except Exception as e:
            logger.error(f"NoveltyAlert failed for msg {msg_id}: {e}")
            # НЕ делаем xack — сообщение вернётся в PEL для retry


# Формат алерта в Telegram
class AlertFormatter:
    def novelty_alert(self, cluster: ClusterDetails, event: NoveltyEvent) -> str:
        score_bar = "🔥" * min(int(event.novelty_score * 5), 5)
        return "\n".join([
            f"⚡ Новый сигнал · {score_bar}",
            "",
            f"{cluster.name}",
            "",
            f"Что происходит: {cluster.summary[:200]}",
            "",
            f"Источники ({event.signal_count}): "
            + ", ".join(s.domain for s in cluster.top_sources[:3]),
            "",
            f"Почему важно: {cluster.significance[:150]}",
            "",
            f"🔗 frontier.app/cluster/{cluster.id}",
        ])
```

**Почему не cron?**
- Cron с 5-минутным интервалом = 288 запросов в день на каждый workspace. При 100 тенантах = 28 800 запросов/день в холостую.
- Redis Streams: событие пришло → доставили. 0 холостых запросов.
- Latency: cron даёт до 5 мин задержки, Streams — секунды.

**Что нужно добавить в worker:** При завершении кластеризации добавить один `XADD`:

```python
# В существующем worker, после расчёта novelty_score
if cluster.novelty_score > NOVELTY_THRESHOLD:
    await redis.xadd("frontier:novelty:events", {
        "cluster_id": str(cluster.id),
        "workspace_id": str(cluster.workspace_id),
        "novelty_score": str(cluster.novelty_score),
        "signal_count": str(cluster.signal_count),
    })
```

**Новые таблицы:** `novelty_events`
**Зависимости:** Redis Streams (уже есть), MCP-клиент (`get_cluster_details`), Telegram Bot API, изменение в `worker`

**Оценка реализации:** 2 человеко-дня
- День 1: Redis consumer, дедупликация, `novelty_events` миграция, изменение в worker
- День 2: AlertFormatter, multi-tenant рассылка, тесты, observability

---

## Сценарий 3: Еженедельный дайджест

### Архитектура

```
CRON (воскресенье 20:00 timezone клиента)
    │
    ▼
WeeklyDigestJob.run(settings)
    │
    ├── list_emerging_signals(7d, workspace_ids)   ← MCP
    ├── get_concept_graph(workspace_ids)            ← MCP (топ-3 кластера)
    ├── [сравнение с прошлой неделей из delivery_log + snapshot]
    │
    ├── LLM-суммаризация (через worker queue) ←── ВАЖНЫЙ ВЫБОР
    │
    ├── TelegramFormatter.weekly_digest()
    ├── EmailRenderer.weekly_digest()          ← Jinja2 HTML шаблон
    │
    └── TelegramSender.send() + EmailSender.send()
```

### LLM vs шаблон: что выбрать?

**Рекомендация: гибрид.** Структура и данные — из шаблона. Одна фраза "Главный вывод недели" — от LLM.

| Часть дайджеста | Источник |
|---|---|
| Топ-10 сигналов (список) | Шаблон (данные из MCP) |
| Топ-3 кластера (название + число) | Шаблон |
| "Что нового vs прошлая неделя" (дельта) | Шаблон (расчёт в БД) |
| **Редакционный вывод (2–3 предложения)** | **LLM через worker** |

Чистый LLM без шаблона: непредсказуемое форматирование, галлюцинации в числах, зависимость от качества промпта. Чистый шаблон: скучно, нет инсайта. Гибрид даёт точность данных + человекочитаемый вывод.

### Псевдокод

```python
# jobs/weekly_digest.py

class WeeklyDigestJob:
    async def run(self, settings: DeliverySettings):
        # 1. Собираем данные параллельно
        signals, graph, prev_snapshot = await asyncio.gather(
            self.mcp_client.call("list_emerging_signals",
                workspace_ids=settings.workspace_ids,
                days=7, limit=20),
            self.mcp_client.call("get_concept_graph",
                workspace_ids=settings.workspace_ids),
            self.get_previous_snapshot(settings.tenant_id)
        )

        # 2. Считаем дельту vs прошлая неделя
        delta = self.compute_delta(signals, prev_snapshot)

        # 3. Запрашиваем LLM-вывод через worker queue
        editorial = await self.request_editorial_summary(
            signals[:10], graph.top_clusters[:3], delta
        )

        # 4. Формируем контент
        context = WeeklyContext(
            signals=signals[:10],
            top_clusters=graph.top_clusters[:3],
            delta=delta,
            editorial=editorial,
            week_label=self.get_week_label()
        )

        # 5. Сохраняем snapshot для следующей недели
        await self.save_snapshot(settings.tenant_id, signals)

        # 6. Доставляем в оба канала
        tasks = []
        if settings.telegram_chat_id:
            tg_text = TelegramFormatter().weekly_digest(context)
            tasks.append(self.telegram.send(settings.telegram_chat_id, tg_text))

        if settings.email_address:
            html = EmailRenderer().weekly_digest(context)
            tasks.append(self.email.send(
                to=settings.email_address,
                subject=f"Frontier Weekly — {context.week_label}",
                html_body=html
            ))

        await asyncio.gather(*tasks)

    async def request_editorial_summary(self, signals, clusters, delta) -> str:
        """Ставим задачу в очередь worker и ждём результат"""
        job_id = await self.worker_queue.enqueue(
            task_type="summarize_weekly",
            payload={
                "signals": [s.dict() for s in signals],
                "clusters": [c.dict() for c in clusters],
                "delta": delta.dict()
            },
            timeout_sec=30
        )
        result = await self.worker_queue.await_result(job_id, timeout=35)
        return result.text if result else "Данные за неделю собраны."
```

### Таблица недельных снапшотов

```sql
CREATE TABLE weekly_snapshots (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    workspace_ids   UUID[] NOT NULL,
    week_start      DATE NOT NULL,
    signal_ids      UUID[] NOT NULL DEFAULT '{}',
    cluster_ids     UUID[] NOT NULL DEFAULT '{}',
    total_signals   INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_tenant_week UNIQUE (tenant_id, week_start)
);
```

### Email HTML шаблон (Jinja2)

```html
<!-- templates/weekly_digest.html -->
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body { font-family: -apple-system, sans-serif; max-width: 600px;
           margin: 0 auto; color: #1a1a1a; }
    .header { background: #0f0f0f; color: #fff; padding: 24px;
              border-radius: 8px 8px 0 0; }
    .week-label { color: #888; font-size: 13px; margin: 0; }
    .editorial { background: #f5f5f5; border-left: 3px solid #0066ff;
                 padding: 16px; margin: 24px 0; border-radius: 0 8px 8px 0; }
    .signal-item { border-bottom: 1px solid #eee; padding: 12px 0; }
    .cluster-chip { display: inline-block; background: #e8f0fe;
                    color: #0066ff; padding: 4px 10px; border-radius: 20px;
                    font-size: 12px; margin: 4px; }
    .delta-up { color: #00a86b; } .delta-down { color: #e53e3e; }
  </style>
</head>
<body>
  <div class="header">
    <p class="week-label">Frontier Intelligence · {{ week_label }}</p>
    <h1 style="margin:8px 0 0">Еженедельный дайджест</h1>
  </div>

  <div style="padding: 24px">
    <div class="editorial">
      <strong>Вывод недели</strong><br>
      {{ editorial }}
    </div>

    <h2>Топ-10 сигналов</h2>
    {% for signal in signals %}
    <div class="signal-item">
      <strong>{{ loop.index }}. {{ signal.title }}</strong>
      <span style="color:#888; font-size:13px"> · {{ signal.source_count }} источников</span>
      <p style="margin:4px 0; color:#555; font-size:14px">{{ signal.summary }}</p>
    </div>
    {% endfor %}

    <h2>Топ-3 кластера</h2>
    {% for cluster in top_clusters %}
    <span class="cluster-chip">{{ cluster.name }}</span>
    {% endfor %}

    <h2>Изменения vs прошлая неделя</h2>
    <p>
      Сигналов:
      <span class="{{ 'delta-up' if delta.signals_diff > 0 else 'delta-down' }}">
        {{ '%+d' % delta.signals_diff }}
      </span>
      · Новых кластеров: <strong>{{ delta.new_clusters_count }}</strong>
      · Исчезло тем: {{ delta.vanished_count }}
    </p>

    <p style="margin-top:32px; text-align:center">
      <a href="https://frontier.app/workspace/{{ workspace_id }}"
         style="background:#0f0f0f; color:#fff; padding:12px 24px;
                border-radius:6px; text-decoration:none; font-weight:500">
        Открыть Frontier →
      </a>
    </p>
  </div>
</body>
</html>
```

**Новые таблицы:** `weekly_snapshots`
**Зависимости:** MCP-клиент (`list_emerging_signals`, `get_concept_graph`), Worker queue (LLM), Jinja2, SMTP/SES, Telegram Bot API

**Оценка реализации:** 4 человеко-дня
- День 1: `WeeklyDigestJob`, сбор данных, дельта-расчёт, `weekly_snapshots` миграция
- День 2: LLM-интеграция через worker queue, EmailRenderer, Jinja2 шаблон
- День 3: TelegramFormatter для еженедельного формата, тестовые прогоны
- День 4: Email-доставка (SMTP/SES), тесты, rollout

---

## Multi-tenant: как это работает вместе

### Модель данных

```
tenants (1) ──► delivery_settings (N)
                    │
                    ├── schedule_type = 'daily'   → DailyBriefJob
                    ├── schedule_type = 'weekly'  → WeeklyDigestJob
                    └── schedule_type = 'alert'   → NoveltyAlertConsumer
```

Один тенант может иметь до 6 записей: daily/weekly/alert × telegram/email. Каждая запись имеет свой timezone, workspace_ids, threshold.

### Загрузка расписаний

```python
# При старте delivery-сервиса
async def bootstrap():
    all_settings = await db.fetch_all(
        "SELECT * FROM delivery_settings WHERE enabled = TRUE"
    )
    for s in all_settings:
        if s.schedule_type in ("daily", "weekly"):
            scheduler.add_job(s)
        # alert-тип слушается через Redis Streams, не через cron

    # Подписываемся на изменения настроек (pg NOTIFY)
    await db.listen("delivery_settings_changed", on_settings_change)

async def on_settings_change(payload: str):
    """Хот-релоад расписания без рестарта сервиса"""
    settings_id = UUID(payload)
    settings = await db.fetch_one(
        "SELECT * FROM delivery_settings WHERE id = :id", id=settings_id
    )
    if settings and settings.enabled:
        scheduler.upsert_job(settings)
    else:
        scheduler.remove_job(settings_id)
```

### PostgreSQL NOTIFY триггер

```sql
-- Автоматически уведомляем delivery-сервис при изменении настроек
CREATE OR REPLACE FUNCTION notify_delivery_settings_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('delivery_settings_changed', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delivery_settings_change_trigger
    AFTER INSERT OR UPDATE ON delivery_settings
    FOR EACH ROW EXECUTE FUNCTION notify_delivery_settings_change();
```

---

## UI: настройка доставки в веб-дашборде

**Где живёт:** отдельная страница `/settings/delivery` в существующем веб-дашборде.

```
┌─────────────────────────────────────────────┐
│  Delivery Settings                          │
├─────────────────────────────────────────────┤
│  Timezone: [Europe/Moscow        ▼]         │
│  Workspaces: [✓ AI Trends] [✓ Robotics]    │
│                                             │
│  ┌─ Daily Brief ──────────────────────┐    │
│  │  Telegram chat ID: [__________]    │    │
│  │  Send at: [08:00 ▼]               │    │
│  │  [Enable] [Send test]              │    │
│  └────────────────────────────────────┘    │
│                                             │
│  ┌─ Weekly Digest ────────────────────┐    │
│  │  Telegram: [__________]            │    │
│  │  Email: [user@company.com]         │    │
│  │  Day: [Sunday ▼] Time: [20:00 ▼]  │    │
│  │  [Enable] [Send test]              │    │
│  └────────────────────────────────────┘    │
│                                             │
│  ┌─ Novelty Alerts ───────────────────┐    │
│  │  Telegram: [__________]            │    │
│  │  Threshold: [━━━━●──] 0.75         │    │
│  │  [Enable]                          │    │
│  └────────────────────────────────────┘    │
│                                             │
│  Recent Deliveries                          │
│  • Daily Brief · сегодня 08:00 · ✓ sent    │
│  • Weekly · вс 20:00 · ✓ sent              │
│  • Alert · вчера 14:23 · ✓ sent            │
└─────────────────────────────────────────────┘
```

**API endpoints (новые):**

```
GET    /api/delivery/settings          → список настроек тенанта
PUT    /api/delivery/settings/:id      → обновить настройки
POST   /api/delivery/settings          → создать новую
DELETE /api/delivery/settings/:id      → отключить
POST   /api/delivery/settings/:id/test → отправить тестовое сообщение
GET    /api/delivery/log               → история доставок (пагинация)
```

---

## Docker Compose: добавление delivery-сервиса

```yaml
# docker-compose.yml (добавить к существующим сервисам)

delivery:
  build:
    context: ./delivery
    dockerfile: Dockerfile
  environment:
    - DATABASE_URL=${DATABASE_URL}
    - REDIS_URL=${REDIS_URL}
    - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    - SMTP_HOST=${SMTP_HOST}
    - SMTP_PORT=${SMTP_PORT}
    - SMTP_USER=${SMTP_USER}
    - SMTP_PASSWORD=${SMTP_PASSWORD}
    - MCP_SERVER_URL=http://mcp:8000
  depends_on:
    - postgres
    - redis
    - mcp
  restart: unless-stopped
  healthcheck:
    test: ["CMD", "python", "-c", "import httpx; httpx.get('http://localhost:8001/health')"]
    interval: 30s
    timeout: 10s
    retries: 3
```

---

## Итоговая оценка реализации

| Компонент | Человеко-дни |
|---|---|
| Сценарий 1: Daily Brief | 3 дня |
| Сценарий 2: Novelty Alert | 2 дня |
| Сценарий 3: Weekly Digest | 4 дня |
| Multi-tenant (БД, хот-релоад, pg_notify) | 2 дня |
| UI в дашборде (settings + log) | 3 дня |
| Ops (мониторинг, алерты на падения delivery) | 1 день |
| **Итого** | **~15 человеко-дней** |

**Критический путь:** сначала multi-tenant + Daily Brief (даёт MVP), потом Alert, потом Weekly.

---

## Риски и решения

**Telegram заблокировал бота** → exponential retry с jitter, fallback-уведомление на email если доступен

**LLM-worker недоступен (сценарий 3)** → таймаут 35 сек → отправляем дайджест без editorial-вывода с плашкой "AI-анализ временно недоступен"

**Тенант изменил timezone** → pg_notify → delivery-сервис перезагружает job с новым CronTrigger, следующая отправка уже в правильном времени

**Redis Streams переполнен (novelty events)** → MAXLEN на стриме + PEL monitoring. Необработанные сообщения старше 24ч → discard + alert в Sentry

**Дублированная отправка при рестарте** → `delivery_log` + `ON CONFLICT` дедупликация: проверяем, что за сегодня/эту неделю отправка уже была

---

*Документ готов к review. Следующий шаг: утвердить стек (APScheduler vs Celery Beat), выбрать email-провайдера (SMTP self-hosted vs SES), и оценить нужен ли отдельный Telegram-бот или используем существующий.*
