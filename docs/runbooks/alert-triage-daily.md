# Daily Alert Triage Loop

<!-- audit-status:2026-08-04 -->
> **🟡 ЧАСТИЧНО УСТАРЕЛО · сверено 2026-08-04.**
> Основа верна, но часть утверждений разошлась с рабочим стеком. Сверяйтесь с разбором, прежде чем опираться на числа и команды.
> Конкретных расхождений найдено: **4** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

Дата: 2026-07-19
Область: ежедневный автоматический разбор алертов Frontier.

## Что это

Раз в сутки локальный headless-прогон Claude Code собирает возникшие/висящие алерты
с сервера, ставит по каждому вероятный диагноз (по runbook'ам + памяти повторяющихся
инцидентов) и отдаёт дайджест в Telegram + в лог на сервере.

Почему локально, а не в облаке: Prometheus (`:9090`) и Alertmanager (`:9093`) слушают
только на `127.0.0.1` сервера. Дотянуться до них можно лишь с рабочей машины через
`ssh frontier-intelligence`, поэтому прогон крутится на Windows по расписанию.

## Поток

```
Task Scheduler (09:15 local, если пользователь залогинен)
  → .claude\run-alert-triage.ps1
     → claude -p "/alert-triage"  (allowedTools: Bash Read Grep Glob Write; --max-budget-usd 2)
        → ssh: bash scripts/alert-triage-collect.sh      # read-only бандл алертов+метрик
        → точечная диагностика (PromQL, docker logs, docker inspect) по горящим алертам
        → сверка с памятью повторяющихся паттернов
        → Markdown-дайджест (TL;DR первым)
        → ssh: bash scripts/alert-triage-deliver.sh        # сохранить + Telegram
```

## Компоненты

| Файл | Где живёт | Роль |
|---|---|---|
| `.claude/commands/alert-triage.md` | локально (не синкается) | «Мозг»: процедура разбора + карта «алерт → диагностика» |
| `.claude/run-alert-triage.ps1` | локально | Обёртка для Task Scheduler (headless, лимит бюджета, лог) |
| `scripts/alert-triage-collect.sh` | сервер (синкается) | Read-only бандл: firing/24h алерты, Alertmanager, контейнеры, host, key metrics |
| `scripts/alert-triage-deliver.sh` | сервер (синкается) | Сохраняет дайджест в лог + шлёт в Telegram (creds из `.env`) |
| `docs/ops/alert-digests/<UTC-date>.md` | **только сервер** (в `.rsync-exclude`) | История дайджестов |
| `.claude/alert-triage.log` | локально | Лог прогонов (stdout claude) |

## Развёртывание / изменение

Скрипты сервера правятся локально и уезжают обычным `Sync → Server` (rsync).
Важно: `scripts/alert-triage-*.sh` должны оставаться с **LF**-переводами строк —
на сервере они вызываются через `tr -d '\r' | bash`, так что CRLF не фатален, но лучше LF.

После правки:
```
.\scripts\sync-push.ps1
```
Лог `docs/ops/alert-digests/` исключён из синка (`.rsync-exclude`), поэтому push с
`--delete` его не сотрёт.

## Ручной запуск

```powershell
# полный прогон (с доставкой в Telegram)
powershell -ExecutionPolicy Bypass -File D:\Workspace\frontier-intelligence\.claude\run-alert-triage.ps1

# сухой прогон — собрать и показать дайджест, без Telegram
powershell -ExecutionPolicy Bypass -File D:\Workspace\frontier-intelligence\.claude\run-alert-triage.ps1 -Dry
```
Или в интерактивной сессии Claude Code в этом проекте: `/alert-triage` (или `/alert-triage dry`).

Только собрать бандл (без разбора):
```
ssh frontier-intelligence "cd /opt/frontier-intelligence && tr -d '\r' < scripts/alert-triage-collect.sh | bash"
```

## Расписание (Windows Task Scheduler)

Задача: **FrontierAlertTriage**, ежедневно 09:15 local, запускается когда пользователь залогинен.

```powershell
# посмотреть
Get-ScheduledTask -TaskName FrontierAlertTriage | Get-ScheduledTaskInfo
# сменить время (например на 08:00)
$t = New-ScheduledTaskTrigger -Daily -At 08:00
Set-ScheduledTask -TaskName FrontierAlertTriage -Trigger $t
# запустить сейчас
Start-ScheduledTask -TaskName FrontierAlertTriage
# удалить
Unregister-ScheduledTask -TaskName FrontierAlertTriage -Confirm:$false
```

## Настройка / тюнинг

- **Доставка**: лог `docs/ops/alert-digests/` пишется КАЖДЫЙ день; в Telegram (тот же
  чат, что и сырые алерты) уходит **только если есть firing critical/warning** — гейтинг
  через 2-й аргумент `deliver.sh` (`send`|`skip`). «Всё зелено», только `info`, только
  `pending` или только отгремевшее за 24ч → лог есть, пуша нет. Fail-safe: дефолт `deliver.sh`
  = `send`, поэтому забытый аргумент шлёт (лишний пинг), а не глушит алерт.
  Telegram отправляется штатным `send_telegram_alert_message` внутри контейнера `admin`
  (host-`curl` не резолвит socks5-прокси `xray`); creds/proxy из `.env` контейнера admin.
- **Стоимость**: `--max-budget-usd 2` в обёртке — предохранитель от разгона. Модель —
  по умолчанию сессии; при желании удешевить добавь `--model` в `run-alert-triage.ps1`.
- **Права**: headless идёт с узким `--allowedTools`, без полного bypass. Диагностика —
  строго read-only (curl к Prometheus, `docker compose logs`, `docker inspect`).

## Связанные материалы

- `prometheus/alerts.yml` — каталог правил (~40 алертов).
- `prometheus/alertmanager.yml` — маршрут (всё → Telegram, дедуп 30м, repeat 6h).
- `docs/runbooks/llm-orchestrator-alerts.md` — классы LLM-алертов (provider_outage,
  local_throttle, quota_exhausted, cost_drift, catalog_stale).
- Память повторяющихся инцидентов: `MEMORY.md` + файлы `memory/` (AdminDown, enrichment
  throughput, proxy egress, OpenRouter credit metric, S3 bucket quota и т.д.).
