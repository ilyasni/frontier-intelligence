# LLM Orchestrator Weekly Snapshot

Использование: еженедельный audit snapshot для `text_generation`, `vision_generation`, `embeddings`.

## Метаданные

- Week:
- Author:
- Reviewed by:
- Production window:
- Related commits:

## 1. Baseline vs Current

- Baseline timestamp:
- Current timestamp:
- Delta summary:

Проверяемые источники:

- `/api/settings/provider-state`
- `/api/settings/circuits`
- `/api/settings/budget-state`
- `/api/settings/routing-events`
- `/metrics`

## 2. Routing и fallback

- Primary providers by family:
- Fallback shares by reason:
- Есть ли смешение `throttled_local` и `provider_unavailable`:

## 3. Circuit / Quota / Budget

- Open circuits (provider/model):
- Quota pressure:
- Budget caps events:
- Local throttle events:

## 4. SLI/SLO status

См. `docs/sre/llm-orchestrator-sli-slo.md`.

- text_generation:
- vision_generation:
- embeddings:
- error budget burn-rate:

## 5. FinOps

- Drift summary:
- Reconciliation gap:
- Cost envelope status:

## 6. Alerts review

- Triggered alerts this week:
- False positives:
- Alert tuning actions:

## 7. Context7 Gate (обязательный)

- Libraries checked:
- Recommendations applied:
- Recommendations rejected:
- Reasons for rejection:

Без этого блока изменения policy/routing не считаются готовыми к merge.

## 8. Server delivery and verification

- Delivery method: `Sync -> Server` / `scripts/sync-push.ps1`
- Services rebuilt/restarted:
- Post-deploy verification status:
- Rollback readiness validated:

## 9. Decisions and next actions

- Approved changes:
- Deferred changes:
- Risks for next week:
