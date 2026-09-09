# Handoff: перевод operational alerts на ntfy

Date: 2026-09-09
State: `complete` — cutover выполнен и проверен в live
base_sha: `97194eb85228c9006834cd9aaf967da15440f6f6`
written_by: `codex`

## Понимание задачи

Перевести operational notifications из Telegram в общий ntfy, topic `home-network`.
Срочные тренды оставить в Telegram. Сохранить два независимых пути доставки critical alerts
и host-direct dead-man watchdog.

## Изменённые файлы

- Application publisher и settings: `admin/backend/services/ntfy_alerts.py`,
  `shared/config.py`.
- Producers: `admin/backend/routers/monitoring.py`,
  `admin/backend/services/gigachat_balance.py`,
  `admin/backend/services/xray_health.py`.
- Alertmanager и rules: `prometheus/alertmanager.yml`, `prometheus/alerts.yml`,
  `prometheus/alerts.test.yml`, `docker-compose.yml`, `.env.example`.
- Host publishers: `scripts/notify_ntfy.py`, `scripts/alert-watchdog.sh`,
  `scripts/alert-triage-deliver.sh`.
- Документация: `docs/README.md`, `docs/runbooks/alert-triage-daily.md`,
  `docs/TODO-UNFINISHED.md`.
- Contract tests: `tests/test_ntfy_alerts.py`, `tests/test_notify_ntfy_script.py`,
  `tests/test_ntfy_alert_scripts.py`, `tests/test_ntfy_alertmanager_config.py`,
  `tests/test_admin_alert_notifications.py`, `tests/test_admin_monitoring_webhook.py`,
  `tests/test_alert_rules_contract.py`.

## Что сделано

- Добавлен HTTPS-only ntfy publisher. Credential читается из отдельного файла, redirects
  запрещены, размер сообщения ограничен по UTF-8 bytes, успех подтверждается ntfy JSON receipt.
- GigaChat balance, Xray health и Alertmanager admin webhook переведены на ntfy. Ошибки
  доставки остаются retryable и не продвигают cooldown/dedup state.
- У 73 active Prometheus rules `notify: telegram` заменён на `notify: ntfy`;
  `notify: never`, watchdog blackhole и 15 rules без `notify` сохранены.
- Срочные trend alerts по-прежнему отправляются через Telegram; их env и код не менялись.
- Critical alerts идут двумя независимыми путями: native Alertmanager ntfy webhook и admin
  webhook. Host watchdog отправляет напрямую в ntfy, без Docker и Alertmanager.
- Для app, Alertmanager и watchdog созданы отдельные write-only ntfy credentials к одному
  topic. Значения не читались и нигде не записаны в git.
- Credentials установлены в `/etc/frontier-intelligence/credentials/`: каталог
  `root:ilyasni 0710`, app `root:root 0600`, Alertmanager `65534:65534 0400`, watchdog
  `root:ilyasni 0640`. User-cron читает только watchdog credential; root-cron не используется.
- В Alertmanager entrypoint добавлен fail-fast на нечитаемый или пустой credential. Это
  защищает от тихой деградации при смене UID container image.
- Исторический закрытый пункт TODO дополнен актуальным ntfy-состоянием; исходный срез
  аудита 2026-08-04 сохранён и явно обозначен как исторический.
- Первый implementation commit на server-first checkout: `a8622d7 feat: route operational
  alerts through ntfy`. Старые несвязанные изменения в working tree сохранены.

## Live cutover и найденная проблема

- Три credential независимо получили валидный ntfy receipt; app publisher также проверен
  через собранный image.
- Обычный Dockerfile build дважды остановился на timeout Docker Hub при получении metadata
  `python:3.11-slim`. Для cutover из прежнего локального admin image собран clean-HEAD overlay
  `frontier-intelligence-admin:ntfy-a8622d7`; предыдущий image сохранён как
  `frontier-intelligence-admin:pre-ntfy-a8622d7`.
- Пересозданы только `admin`, `alertmanager` и `prometheus`. Все три health endpoints дают
  HTTP 200; Alertmanager config и Prometheus rules загружены.
- После rsync Prometheus продолжал видеть старый inode индивидуально bind-mounted файла.
  Recreate синхронизировал host/container inode и реально загрузил новые labels.
- Первый live smoke обнаружил `permission denied`: image Alertmanager работает как UID/GID
  65534 и не мог прочитать файл `root:root 0600`. Ownership исправлен, а regression test
  сначала воспроизвёл отсутствие startup guard, затем подтверждён fail-fast.
- Повторный `FrontierNtfySmoke2` прошёл целиком. На FIRING и RESOLVED nginx зарегистрировал
  по два HTTP 200 (`python-httpx/0.27.0` и `Alertmanager/0.32.0`); admin принял оба webhook
  с HTTP 200. Метрики Alertmanager после smoke: `webhook total=4`, все failure reasons `0`.
- После выхода старой ошибки из 15-minute window production watchdog подтвердил здоровый
  контур, очистил state и отправил recovery напрямую в ntfy с HTTP 200.

## Проверка

- Финальный targeted pytest: `219 passed, 38 subtests passed`.
- Targeted Ruff (`E9,F63,F7,F82,F811`), Python compilation и Bash syntax: passed.
- Compose render: passed.
- Server `promtool check rules`: 91 rules, success.
- Live Prometheus: 91 rules; `notify=ntfy` 73, `never` 2, `watchdog` 1, без `notify` 15;
  `notify=telegram` отсутствует.
- Rendered Alertmanager config: critical -> `ntfy-direct,ntfy-admin`; warning ->
  `ntfy-admin`; watchdog и `notify=never` -> `blackhole`.
- Полный локальный suite ранее дал документированный Windows baseline: 16 failures вне
  ntfy-изменений. Broad Ruff не является clean из-за существующего formatting/UP debt;
  релевантный CI subset проходит.

## Откат и остаточные риски

- Backup ntfy auth DB на VPS: `/root/frontier-ntfy-cutover-20260909-a8622d7/user.db.before`.
- Backup server env и прежний admin image tag: каталог
  `/root/frontier-ntfy-cutover-20260909-a8622d7/` и tag
  `frontier-intelligence-admin:pre-ntfy-a8622d7`.
- `prom/alertmanager:latest` mutable и сейчас запускается как UID 65534. При смене UID
  fail-fast остановит Alertmanager вместо скрытой потери direct delivery; permissions придётся
  сверить с фактическим UID нового image.
- Overlay image функционально содержит clean HEAD, но при восстановлении Docker Hub следует
  повторить стандартный Dockerfile build и заменить overlay обычным image.
- В server working tree остаются старые несвязанные изменения, включая PostgreSQL
  `shm_size` в `docker-compose.yml`; их нельзя включать в ntfy commit.

## Что дальше

Обязательной работы по cutover нет. При доступном Docker Hub пересобрать `admin` стандартным
Dockerfile и повторить его health check; это не меняет routing или credentials.
