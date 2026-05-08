# LLM Orchestrator Alerts Runbook

Дата: 2026-05-08
Область: `text_generation`, `vision_generation`, `embeddings`.

## Цель

Единый operational runbook для классов алертов оркестратора:

- `provider_outage`
- `local_throttle`
- `quota_exhausted`
- `cost_drift`
- `catalog_stale`

Каждый класс описан как: Trigger -> Diagnosis -> Immediate Action -> Validation -> Rollback.

## Базовые проверки (для любого инцидента)

1. `GET /api/settings/provider-state`
2. `GET /api/settings/circuits`
3. `GET /api/settings/budget-state`
4. `GET /api/settings/routing-events`
5. `GET /metrics`

Ключевые метрики:

- `frontier_llm_requests_total`
- `frontier_llm_fallbacks_total`
- `frontier_llm_throttle_events_total`
- `frontier_llm_finops_runtime_drift_total`
- provider-specific health/quota metrics

## Class: provider_outage

Trigger:
- Всплеск fallback c `reason=provider_unavailable|upstream_5xx|timeout` и/или open circuits.

Diagnosis:
- Проверить `provider-state` и `circuits`, коррелировать с `frontier_llm_fallbacks_total`.
- Подтвердить, что это upstream, а не локальный guard/throttle.

Immediate Action:
- Открыть/сохранить quarantine для деградировавшего provider/model.
- Временно усилить fallback order на следующий устойчивый кандидат.

Validation:
- Падение доли `provider_unavailable` fallback в 15-30 минутном окне.
- Отсутствие новых критичных 5xx/timeouts по деградировавшему провайдеру.

Rollback:
- Снять quarantine после стабилизации.
- Вернуть исходный candidate order.

## Class: local_throttle

Trigger:
- Рост `frontier_llm_throttle_events_total` при отсутствии подтвержденного upstream outage.

Diagnosis:
- Проверить причины throttle (`guard_interval`, `guard_quarantine`, budget soft/hard caps).
- Убедиться, что circuit не открывается из-за `throttled_local`.

Immediate Action:
- Временно увеличить pacing headroom (interval/concurrency) или перевести guardrail в monitor-only.
- Проверить актуальные credit-window cap ratios.

Validation:
- Снижение throttle burst и стабилизация fallback mix.
- Отсутствие ложных provider circuit opens.

Rollback:
- Вернуть прежние pacing/cap параметры, если растут ошибки или стоимость.

## Class: quota_exhausted

Trigger:
- Alerts на исчерпание/низкий остаток лимитов (OpenRouter/GigaChat/Wormsoft published quotas).

Diagnosis:
- Проверить свежесть snapshot и расхождения между runtime и published quota state.

Immediate Action:
- Ограничить fallback в контуры с высоким cost impact.
- Переключить часть нагрузки на безопасный кандидат с доступной квотой.

Validation:
- Уменьшение quota-related fallbacks/errors.
- Стабилизация `provider-state` по остаткам.

Rollback:
- Вернуть стандартный routing order после восстановления квот.

## Class: cost_drift

Trigger:
- Рост `frontier_llm_finops_runtime_drift_total` и/или reconciliation gap alerts.

Diagnosis:
- Проверить доли fallback и provider mix за тот же период.
- Проверить stale snapshots и актуальность pricing assumptions.

Immediate Action:
- Ужесточить soft/hard budget caps.
- Ограничить дорогие fallback ветки для не-критичных task families.

Validation:
- Замедление роста drift.
- Возврат fallback mix в целевой коридор.

Rollback:
- Вернуть предыдущие cap ratios, если качество/latency деградируют выше SLO.

## Class: catalog_stale

Trigger:
- Alerts `*CatalogRefreshStale` / `*CatalogUnavailable`.

Diagnosis:
- Проверить источник snapshot, refresh timestamps, last success.

Immediate Action:
- Переключить dynamic picker в trusted static route.
- Зафиксировать allow-list безопасных моделей.

Validation:
- Стабильные ответы и предсказуемый fallback без зависимости от stale catalog.

Rollback:
- Вернуть dynamic picker после подтвержденного восстановления snapshot freshness.

## Post-Change Verification (обязательный)

После любого изменения routing/policy:

1. Проверить API-state эндпоинты (см. базовые проверки).
2. Снять срез метрик до/после в окне минимум 30 минут.
3. Подтвердить:
   - нет ложных circuit opens от local throttle,
   - fallback причины объяснимы,
   - cost drift в контролируемом коридоре.

## Server Delivery Notes

- Доставка только через `Sync -> Server` или `scripts/sync-push.ps1`.
- Для `worker/admin/mcp/ingest/crawl4ai/paddleocr/gpt2giga-proxy` после source changes нужен rebuild + `up -d --force-recreate`.
- Перед rollout фиксировать контрольную точку: commit hash и runtime параметры.
