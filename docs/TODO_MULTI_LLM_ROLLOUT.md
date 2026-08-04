# TODO: Multi-LLM Rollout Follow-Ups

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **6** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).

Статус на **2026-05-05** после Phase 4 и ретюна alerting.

## Closed

- `relevance` chain escalation больше не уходит массово в GigaChat из-за симметричной gray-zone логики.
- Для `OpenRouter` и `Polza` добавлены provider-specific alerts.
- Vision и text fallback chain работают через `Wormsoft -> OpenRouter -> Polza -> GigaChat` там, где это предусмотрено текущей стратегией.

## TODO

### 1. Wormsoft 429 mitigation

Базовый mitigation уже внедрён:

- у `WormsoftTextClient` появились отдельные `WORMSOFT_MAX_SIMULTANEOUS_REQUESTS`,
  `WORMSOFT_MIN_REQUEST_INTERVAL_MS`, `WORMSOFT_MAX_RETRIES`;
- production pacing отвязан от `gigachat_*` defaults;
- SDK retries для Wormsoft отключены, чтобы не умножать `429`.

Сейчас нужно дотюнить и донаблюдать:

Что сделать:

- проверить фактический live RPM / burst profile у `wormsoft/agent/medium` после нескольких reset окон;
- при необходимости ещё поднять `WORMSOFT_MIN_REQUEST_INTERVAL_MS`;
- если 429 сохраняются даже после pacing-tune, вынести часть text fallback трафика раньше в `OpenRouter/Polza`, а не дожидаться provider failure;
- добавить короткий runbook: какие Prometheus queries смотреть при всплеске `wormsoft` rate-limit.

### 2. OpenRouter paid credit semantics

Сейчас alert `FrontierOpenRouterCreditLow` firing, потому что `frontier_openrouter_key_limit_remaining=0`.

Что сделать:

- перепроверить, как именно `GET /api/v1/key` трактует `limit_remaining` для текущего paid/free mix;
- если это нормальный ноль для вашего режима free-routing, ретюнить alert или сделать его conditional только для paid-routing mode;
- если это реальный нулевой paid balance, пополнить OR paid budget или отключить alert до ввода paid traffic.

### 3. Redis stream backlog

Сейчас firing:

- `FrontierRedisStreamLagHigh`
- `FrontierRedisStreamOldestPendingTooOld`

Что сделать:

- проверить `stream:posts:parsed` consumer groups и зависшие pending messages;
- понять, это временный backlog после rollout или устойчивый throughput bottleneck;
- при необходимости увеличить worker throughput после стабилизации wormsoft limits.

### 4. Clean rebuild / deploy path

Runtime hotfix уже работает, но clean image rebuild всё ещё зависит от нестабильного registry access на сервере.

Что сделать:

- добить стабильный base-image pull через mirror/internal registry;
- вернуть rollout к `sync -> build -> up --force-recreate` без `docker cp`;
- после этого сделать один чистый rebuild `worker/admin/mcp/ingest`.

### 5. Dashboards and runbooks

Алерты уже есть, но operational слой ещё не завершён.

Что сделать:

- добавить Grafana panels для `openrouter picker skips`, `openrouter vision/text fallbacks`, `polza -> gigachat spillover`;
- оформить короткие runbooks для `WormsoftRateLimitBurst`, `OpenRouterPickerSkipBurst`, `PolzaFallbackBurst`.

### 6. Embeddings switch (deferred)

Статус: **deferred (отложено отдельно от текущего выравнивания оркестратора)**.

Что запланировано:

- Ввести multi-provider routing для `embeddings` по схеме `wormsoft -> openrouter -> polza -> gigachat` только после подтверждения реальной поддержки embedding API у провайдеров.
- Добавить capability registry для embeddings (dimension, max context, input-prefix profile, billing unit) и строгую валидацию совместимости с `EMBED_DIM`.
- Реализовать policy-driven switch по embedding options моделей (dim/profile/cost tier), а не только по provider availability.

Acceptance criteria:

- Для каждого кандидата embeddings есть подтверждённые capability snapshots и стабильные runtime probes.
- FinOps корректно считает `estimated_cost_total` и `actual_cost_total` по embeddings для всех включённых провайдеров.
- Есть runbook аварийного отката в single-provider режим без потери индексационной совместимости.

Dependencies:

- Стабильный provider catalog + health snapshots для OpenRouter/Polza/Wormsoft embedding endpoints.
- Подтверждённая стратегия миграции индексов при смене embedding profile (dimension/prefix family).
