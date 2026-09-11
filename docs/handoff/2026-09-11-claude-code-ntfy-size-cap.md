# Handoff: кэп сообщения ntfy строго ниже 4096 байт

Date: 2026-09-11
State: `complete` — задеплоено, дайджест 5595 байт доставлен живым sender'ом
base_sha: `cb8bea121bd6685ada7342052ebc55b7c34e0eb3`
written_by: `claude-code`

## Понимание задачи

11.09 утренний триаж упал на `NtfyDeliveryError: ntfy rejected publication: HTTP 400`
(`admin/backend/services/ntfy_alerts.py:155`): дайджест 5595 байт усекался до
`_MAX_MESSAGE_BYTES = 4096` и отправлялся ровно 4096-байтным телом. Нужно было найти
точную границу ntfy, поставить кэп ниже неё, закрепить тестом и дать sender'у
диагностику вместо голого кода.

## Что выяснено

- **Граница.** ntfy 2.28.0 (`/etc/ntfy/server.yml` на VPS 109 без `message-size-limit`
  → дефолт 4096, `attachment-cache-dir` нет). `util.Peek(body, 4096)` ставит
  `LimitReached: read == limit`, и `handlePublishBody` уводит тело **ровно** 4096 байт в
  `handleBodyAsAttachment` → `errHTTPBadRequestAttachmentsDisallowed`
  (`{"code":40014,"http":400,"error":"invalid request: attachments not allowed"}`).
  Замер живым sender'ом из контейнера admin: **4096 → 400, 4095 → 200** с квитанцией.
  Две пробы ушли на телефон владельца с заголовком «ntfy size probe», priority min.
- **Масштаб потери — проверен по nginx на VPS 109** (`/var/log/nginx/access.log*`,
  все ротации). Гипотеза «каждый send-дайджест с 09.09 потерян» **не подтвердилась**:
  за всю историю ровно один 400 от `python-httpx` — 11.09 07:35:53 UTC. 10.09 в 06:39
  POST от httpx нет вовсе (дайджест ушёл в `skip`-режиме: горели только `info`);
  09.09 триаж (06:46) был до деплоя ntfy (a8622d7, 19:06). Потерян один дайджест —
  сегодняшний, и он дослан после фикса.

## Изменённые файлы

- `admin/backend/services/ntfy_alerts.py` — `_NTFY_SERVER_MESSAGE_SIZE_LIMIT = 4096`,
  `_MAX_MESSAGE_BYTES = 3800`; `_describe_error_body()` вытаскивает из JSON-ответа
  только `code`/`error` (≤200 символов, без `link`, без заголовков); при не-200
  `logger.warning` + те же поля в тексте `NtfyDeliveryError`.
- `scripts/notify_ntfy.py` — та же граница (`MAX_MESSAGE_BYTES = 3800`) у host-watchdog
  publisher'а: у него была та же ошибка на 4096.
- `scripts/alert-triage-deliver.sh`, `docs/runbooks/alert-triage-daily.md` — комментарии/
  документация про 3800 и причину.
- Тесты: `tests/test_ntfy_alerts.py` (+7: кэп < 4096, приём на кэпе, усечение реального
  кириллического дайджеста < 4096 без разрезанного code point, JSON-ошибка в тексте и логе
  без credential/тела, устойчивость `_describe_error_body`), `tests/test_notify_ntfy_script.py`,
  `tests/test_ntfy_alert_scripts.py` (`< 4096` и `<= 3800`, фейковый admin на 3800),
  `tests/test_admin_alert_notifications.py` (три `<= 4096` → `< 4096`).

## Валидация

- `python -m pytest tests -k "ntfy or alert_notifications" -q` (Windows, Python 3.11.15):
  **139 passed, 41 subtests passed**. Ruff по CI-подмножеству `E9,F63,F7,F82,F811` — чисто;
  `bash -n scripts/alert-triage-deliver.sh` — ок.
- Деплой: файлы скопированы scp (дифф на сервере — только эти 8 файлов),
  `bash scripts/server-build-stack.sh admin` — стандартная сборка прошла (Docker Hub жив),
  overlay не понадобился. Откат: тег `frontier-intelligence-admin:pre-ntfycap-20260911`.
  Recreate через `rm -sf` + `up -d` (не `--force-recreate`, см. память о конфликте имён).
  `/api/health` 200; внутри контейнера `_MAX_MESSAGE_BYTES == 3800`.
- Критерий готовности: `tr -d '\r' < scripts/alert-triage-deliver.sh | bash -s --
  /tmp/frontier-alert-digest.md send` с дайджестом 5595 байт →
  `deliver: ntfy sent (via admin sender)`; в nginx VPS 109 — POST 200.

## Что не сделано / неопределённости

- Alertmanager-путь (`prometheus/alertmanager.yml`, `max_alerts=3`, шаблон) кэп не трогал:
  его сообщения — сотни байт. Если шаблон когда-нибудь вырастет — та же граница 4096.
- В `pve_orchestrator/docs/NTFY-NOTIFICATIONS.md` написано «лимит 4 КиБ» — формально
  верно, но граница *строгая*; там не правил (чужой репозиторий).
- `notify_ntfy.py` в `pve_orchestrator/scripts/netwatch/` — отдельная копия с той же
  проверкой `> 4096`; не трогал, стоит поправить там же.

## Риски отката

Единственный поведенческий сдвиг — сообщения 3801–4095 байт теперь режутся (раньше
уходили). Откат — вернуть тег `pre-ntfycap-20260911` и файлы из `cb8bea1`.
