# LLM Orchestrator Context7 Gate

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **2** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).

Дата: 2026-05-08
Статус: обязательный quality gate для всех изменений в orchestration policy.

## Правило

Перед реализацией любого изменения в routing/fallback/retry/circuit/budget:

1. Выполнить Context7 сверку по релевантной документации.
2. Зафиксировать вывод в PR/отчете:
   - что применяем,
   - что не применяем,
   - почему.
3. Только после этого делать rollout.

## Минимальные домены сверки

- Retry vs fallback vs circuit semantics.
- Provider routing preferences и failover order.
- Quota/budget guardrails и cost envelope.
- Observability/alerting patterns.

## Шаблон записи Context7-сверки

- Scope:
- Library docs checked:
- Date/time:
- Recommended pattern:
- Implementation decision:
- Risk/rollback notes:

## Gate критерий

Изменение не готово к merge, если:

- нет Context7-сверки по релевантному домену, или
- нет объяснения, почему рекомендация отклонена.
