# LLM Orchestrator SLI/SLO

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **5** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

Дата: 2026-05-08
Область: `text_generation`, `vision_generation`, `embeddings`.

## Цель

Формализовать SLI/SLO и правила эскалации для runtime-оркестрации.

## SLI

### 1) text_generation

- Success rate:
  - `sum(rate(frontier_llm_requests_total{task=~"relevance|relevance_concepts|concepts|valence|mcp_synthesis",status="ok"}[5m]))`
  - /
  - `sum(rate(frontier_llm_requests_total{task=~"relevance|relevance_concepts|concepts|valence|mcp_synthesis"}[5m]))`
- Fallback rate:
  - `sum(rate(frontier_llm_fallbacks_total{task=~"relevance|relevance_concepts|concepts|valence|mcp_synthesis"}[5m]))`
  - /
  - `sum(rate(frontier_llm_requests_total{task=~"relevance|relevance_concepts|concepts|valence|mcp_synthesis"}[5m]))`
- Throttle rate:
  - `sum(rate(frontier_llm_throttle_events_total{provider="wormsoft"}[5m]))`

### 2) vision_generation

- Success rate:
  - `sum(rate(frontier_llm_requests_total{task=~"vision_generation|vision",status="ok"}[5m]))`
  - /
  - `sum(rate(frontier_llm_requests_total{task=~"vision_generation|vision"}[5m]))`
- Fallback rate:
  - `sum(rate(frontier_openrouter_vision_fallbacks_total[5m]))`
- Error burst:
  - `sum(increase(frontier_polza_vision_requests_total{status="error"}[15m]))`

### 3) embeddings

- Success rate:
  - `sum(rate(frontier_llm_requests_total{task=~"embed|embeddings",status="ok"}[5m]))`
  - /
  - `sum(rate(frontier_llm_requests_total{task=~"embed|embeddings"}[5m]))`
- Strict profile compliance:
  - отсутствие незапланированных fallback при `embeddings` task family.

### 4) FinOps drift

- Drift absolute:
  - `abs(frontier_llm_finops_runtime_drift_total)`
- Reconciliation gap:
  - `abs(frontier_llm_finops_reconciliation_gap)`

## SLO (стартовые целевые значения)

- `text_generation`:
  - success rate >= 99.0% (30d)
  - fallback rate <= 8% (rolling 24h)
- `vision_generation`:
  - success rate >= 97.5% (30d)
  - fallback rate <= 20% (rolling 24h)
- `embeddings`:
  - success rate >= 99.5% (30d)
  - незапланированный fallback = 0 в steady-state
- FinOps:
  - drift within approved monthly envelope

## Error Budget и эскалация

- Warning:
  - SLI вышел за цель в rolling 1h.
- Critical:
  - SLI нарушен > 4h или burn-rate ускоряется.
- Действия:
  1. перейти в runbook-класс инцидента (`provider_outage` / `local_throttle` / `quota_exhausted` / `cost_drift` / `catalog_stale`);
  2. зафиксировать причину и action в incident notes;
  3. проверить rollback критерии.

## Связь с alerting

- `prometheus/alerts.yml` покрывает trigger-level сигналы.
- Этот документ задает SLO-target и интерпретацию для операционных решений.

## Acceptance (post-deploy)

Изменение считается принятым, если:

1. Зафиксирован baseline до rollout.
2. В окне наблюдения после rollout:
   - нет ложных circuit opens от local throttle,
   - fallback причины прозрачны и соответствуют policy,
   - нет неконтролируемого роста cost drift.
3. Результат проверки задокументирован в weekly snapshot.
