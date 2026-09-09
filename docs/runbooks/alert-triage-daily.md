# Daily Alert Triage Loop

<!-- audit-status:2026-09-09 -->
> **Сверено 2026-09-09.** Операционные алерты и daily triage доставляются в ntfy.
> Единственное исключение — urgent trend alerts: они по-прежнему отправляются в Telegram.

Дата: 2026-07-19; расписание сверено 2026-09-09
Область: ежедневный автоматический разбор алертов Frontier.

## Что это

Раз в сутки локальный headless-прогон Claude Code собирает возникшие/висящие алерты
с сервера, ставит по каждому вероятный диагноз (по runbook'ам + памяти повторяющихся
инцидентов) и отдаёт дайджест в ntfy + в лог на сервере.

Почему локально, а не в облаке: Prometheus (`:9090`) и Alertmanager (`:9093`) слушают
только на `127.0.0.1` сервера. Дотянуться до них можно лишь с рабочей машины через
`ssh frontier-intelligence`, поэтому прогон крутится на Windows по расписанию.

## Поток

```
Claude Code scheduled task (ежедневно 09:36 local)
  → ~/.claude/scheduled-tasks/frontier-alert-triage/SKILL.md
        → ssh: bash scripts/alert-triage-collect.sh      # read-only бандл алертов+метрик
        → точечная диагностика (PromQL, docker logs, docker inspect) по горящим алертам
        → сверка с памятью повторяющихся паттернов
        → Markdown-дайджест (TL;DR первым)
        → ssh: bash scripts/alert-triage-deliver.sh        # сохранить + ntfy
```

## Компоненты

| Файл | Где живёт | Роль |
|---|---|---|
| `~/.claude/scheduled-tasks/frontier-alert-triage/SKILL.md` | user scope Claude Code | Живой источник процедуры и расписания, ежедневно 09:36 |
| `.claude/commands/alert-triage.md` | локально (не синкается) | Историческая копия для ручного `/alert-triage`; к расписанию не подключена |
| `scripts/alert-triage-collect.sh` | сервер (синкается) | Read-only бандл: firing/24h алерты, Alertmanager, контейнеры, host, key metrics |
| `scripts/alert-triage-deliver.sh` | сервер (синкается) | Сохраняет дайджест в лог + шлёт в ntfy через sender внутри `admin` |
| `docs/ops/alert-digests/<UTC-date>.md` | **только сервер** (в `.rsync-exclude`) | История дайджестов |

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

Процедуру или время scheduled task меняют в
`~/.claude/scheduled-tasks/frontier-alert-triage/SKILL.md`, а не в историческом
`.claude/commands/alert-triage.md`.

## Ручной запуск

В интерактивной сессии Claude Code в этом проекте: `/alert-triage` для доставки или
`/alert-triage dry` без ntfy. Команда использует историческую локальную копию процедуры;
scheduled task исполняет отдельный user-scope skill.

Только собрать бандл (без разбора):
```
ssh frontier-intelligence "cd /opt/frontier-intelligence && tr -d '\r' < scripts/alert-triage-collect.sh | bash"
```

## Расписание (Claude Code)

Живёт только Claude Code scheduled task
`~/.claude/scheduled-tasks/frontier-alert-triage/SKILL.md`: ежедневно в **09:36 local**.
Windows-задача `FrontierAlertTriage` и `.claude/run-alert-triage.ps1` удалены
18.08.2026. Это явно зафиксировано в шапке `.claude/commands/alert-triage.md`; тот файл
оставлен для истории и ручного вызова и не получает scheduler-specific шаги 0.5 и 7.

## Настройка / тюнинг

- **Credential files**: `NTFY_URL` общий и не секретный. В server `.env` лежат только пути:
  `NTFY_APP_CREDENTIAL_FILE`, `NTFY_ALERTMANAGER_CREDENTIAL_FILE` и
  `NTFY_WATCHDOG_CREDENTIAL_FILE`. В контейнер `admin` файл приложения монтируется как
  `/run/secrets/ntfy-app`, поэтому внутри него задано
  `NTFY_CREDENTIAL_FILE=/run/secrets/ntfy-app`. Значения credentials в `.env` не хранятся.

  Каноническая матрица прав на сервере соответствует фактическим consumers: каталог
  `/etc/frontier-intelligence/credentials` — `root:ilyasni 0710`, `ntfy-app` —
  `root:root 0600`, `ntfy-alertmanager` — `65534:65534 0400` (официальный image работает
  как `nobody`), `ntfy-watchdog` — `root:ilyasni 0640` для user-cron. Root-cron для
  watchdog не используется.
- **URL и direct webhook**: production compose допускает только `https://host/topic`
  без явного порта; topic — 1–64 символа `A–Z`, `a–z`, `0–9`, `_`, `-`.
  Служебные topics `account`, `admin`, `app`, `docs`, `file`, `health`, `metrics`,
  `settings`, `static`, `v1`, query и fragment запрещены. Direct-маршрут
  Alertmanager использует native HTTP webhook semantics: успешный HTTP-ответ
  сам по себе не означает проверку JSON-квитанции ntfy. Квитанцию проверяют
  Python publishers приложения и host watchdog; watchdog сохраняет state после
  неудачной доставки recovery и повторяет её на следующем прогоне.
- **Доставка**: лог `docs/ops/alert-digests/` пишется КАЖДЫЙ день; в ntfy уходит
  **только если есть firing critical/warning** — гейтинг
  через 2-й аргумент `deliver.sh` (`send`|`skip`). «Всё зелено», только `info`, только
  `pending` или только отгремевшее за 24ч → лог есть, пуша нет. Fail-safe: дефолт `deliver.sh`
  = `send`, поэтому забытый аргумент шлёт (лишний пинг), а не глушит алерт.
  Сообщение отправляется штатным `send_ntfy_alert_message` внутри контейнера `admin`.
  `truncate_ntfy_message` ограничивает сообщение 4096 UTF-8 байт, не разрезая символ;
  полный Markdown остаётся в `docs/ops/alert-digests/` даже при сбое отправки.
- **Права диагностики**: серверные команды должны оставаться read-only (curl к Prometheus,
  `docker compose logs`, `docker inspect`); доставка пишет только digest и notification.

## Связанные материалы

- `prometheus/alerts.yml` — каталог правил (~40 алертов).
- `prometheus/alertmanager.yml` — маршруты операционных алертов в ntfy.
- `scripts/alert-watchdog.sh` + `scripts/notify_ntfy.py` — внешний host-direct watchdog;
  читает из серверного `.env` только `NTFY_URL` и путь `NTFY_WATCHDOG_CREDENTIAL_FILE`.
- Urgent trend alerts не относятся к этому контуру и остаются в Telegram.
- `docs/runbooks/llm-orchestrator-alerts.md` — классы LLM-алертов (provider_outage,
  local_throttle, quota_exhausted, cost_drift, catalog_stale).
- Память повторяющихся инцидентов: `MEMORY.md` + файлы `memory/` (AdminDown, enrichment
  throughput, proxy egress, OpenRouter credit metric, S3 bucket quota и т.д.).
