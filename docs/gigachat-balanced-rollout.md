# GigaChat Balanced Rollout

Практический runbook для мягкого включения token optimization в staging, а затем в production.

## Recommended env

Рекомендуемые значения для `Balanced`-режима:

```env
GIGACHAT_MODEL=GigaChat-2
GIGACHAT_MODEL_LITE=GigaChat-2
GIGACHAT_MODEL_PRO=GigaChat-2-Pro
GIGACHAT_MODEL_MAX=GigaChat-2-Max

GIGACHAT_MODEL_RELEVANCE=GigaChat-2
GIGACHAT_MODEL_CONCEPTS=GigaChat-2
GIGACHAT_MODEL_VALENCE=GigaChat-2
GIGACHAT_MODEL_MCP_SYNTHESIS=GigaChat-2
GIGACHAT_VISION_MODEL=GigaChat-2-Pro

GIGACHAT_SESSION_CACHE_ENABLED=true
GIGACHAT_ESCALATION_ENABLED=true

GIGACHAT_TOKEN_BUDGET_RELEVANCE=1500
GIGACHAT_TOKEN_BUDGET_CONCEPTS=1500
GIGACHAT_TOKEN_BUDGET_VALENCE=1200
GIGACHAT_TOKEN_BUDGET_EMBED=1200
GIGACHAT_TOKEN_BUDGET_VISION_PROMPT=600
GIGACHAT_RELEVANCE_GRAY_ZONE=0.1
INDEXING_MAX_CONCURRENCY=1
```

Runtime mode overlays are documented in `docs/runtime-modes.md`.

## Production notes

- On the current server contour the supported baseline models are `GigaChat-2`, `GigaChat-2-Pro`, `GigaChat-2-Max` and `EmbeddingsGigaR`.
- Do not route runtime traffic to `GigaChat-2-Lite` until it appears in `GET /v1/models` on the active proxy/upstream.
- `X-Session-ID` and `/tokens/count` should be treated as `best-effort`. If SDK or proxy rejects them, disable those optimizations without breaking the enrichment pipeline.
- For this production contour prefer `INDEXING_MAX_CONCURRENCY=1` to keep `429` under control; raise only after live metrics show headroom.

## Rollout order

1. Включить новые env в staging без изменения остальных параметров worker.
2. Перезапустить `gpt2giga-proxy`, `worker` и `mcp`, чтобы новые настройки применились одновременно.
3. Дождаться появления новых метрик `frontier_gigachat_*` в Prometheus.
4. Прогнать контрольную выборку реальных документов через enrichment и 3-5 search-запросов с `synthesize=true`.
5. Сравнить качество relevance/concepts с недавними результатами до rollout.
6. Если качество стабильно, оставить `Lite-first` в staging на 12-24 часа.
7. После этого повторить те же env в production.

## Commands

Проверить итоговую compose-конфигурацию с профилями:

```powershell
docker compose --profile core --profile worker --profile mcp --env-file .env.balanced.example config --services
```

Сухой прогон rollout-скрипта:

```powershell
.\scripts\rollout-gigachat-balanced.ps1 -DryRun
```

Фактический rollout при наличии рабочего `.env` с секретами:

```powershell
.\scripts\rollout-gigachat-balanced.ps1
```

## Staging checklist

- `frontier_gigachat_requests_total` растёт для `task=relevance` и `task=concepts`.
- Для `task=relevance` и `task=concepts` появляется заметная доля `model=GigaChat-2`.
- `frontier_gigachat_precached_prompt_tokens_total` растёт, если session cache реально поддерживается текущей связкой SDK/proxy.
- `frontier_gigachat_billable_tokens_total` на документ ниже, чем до rollout.
- `frontier_gigachat_escalations_total` не взрывается; умеренный рост допустим.
- `frontier_rate_limit_events_total` не растёт из-за перехода на новую схему.
- В логах enrichment встречаются структурированные строки `gigachat_task task=relevance ...` и `gigachat_task task=concepts ...`.
- В логах нет `No such model` после rollout.

## Quick checks

Проверка метрик в Prometheus:

```promql
sum by (task, model, status) (increase(frontier_gigachat_requests_total[1h]))
```

```promql
sum by (task, model) (increase(frontier_gigachat_billable_tokens_total[1h]))
```

```promql
sum by (task, model) (increase(frontier_gigachat_precached_prompt_tokens_total[1h]))
```

```promql
sum by (task, from_model, to_model) (increase(frontier_gigachat_escalations_total[1h]))
```

## What to tune first

- Если качество relevance просело: держать `GIGACHAT_RELEVANCE_GRAY_ZONE` около `0.10` и перепроверять долю пограничных `done`.
- Если escalation слишком много: сначала уменьшить серую зону, а не возвращать `Pro` по умолчанию.
- Если concepts стали беднее: поднять `GIGACHAT_TOKEN_BUDGET_CONCEPTS` до `1800`.
- Если precached tokens почти не растут: проверить, что инстансы действительно используют одинаковые system prompts и новый код клиента.
- Если в логах есть `gigachat_model_unavailable` или `No such model`: проверить `/v1/models` на proxy и убрать неподдерживаемый алиас из env.
- Если MCP synthesis слишком поверхностный: оставить `Lite` по умолчанию, но поднимать в `Pro` для длинных multi-document запросов.
