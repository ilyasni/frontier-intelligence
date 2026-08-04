#!/usr/bin/env bash
# Dead man's switch для контура алертинга.
#
# Зачем. До 04.08.2026 доставка алертов шла единственным путём: Prometheus ->
# Alertmanager -> вебхук в admin -> Telegram. Цепочка завязана на тот самый
# сервис, о падении которого надо сообщать: за 7 дней 135 провалов доставки из
# 296, и все они пришлись на окно, когда admin лежал. Молчание было неотличимо
# от отсутствия проблем.
#
# Этот скрипт наблюдает за контуром СНАРУЖИ него: крутится на хосте по cron,
# опрашивает Prometheus и Alertmanager по loopback и, если что-то не так, шлёт
# сообщение в Telegram, не задействуя ни admin, ни маршрутизацию Alertmanager.
#
# Ограничение, которое надо знать. Прямого egress к api.telegram.org с хоста
# нет, а socks5-прокси живёт по адресу xray:10808 внутри docker-сети и с хоста
# не резолвится. Поэтому отправка идёт через docker exec в первый живой
# контейнер, у которого есть httpx и переменная с прокси. admin пробуется
# последним: именно он чаще всего и оказывается причиной.
#
# Дальше цепочка кончается: если умрёт сам cron или docker, сообщить будет
# некому. Это неустранимый конец на одном хосте, и он тут назван честно.
#
# Cron (через bash, а НЕ прямым вызовом): sync-push с Windows передаёт файлы с
# --chmod=Fu=rw и снимает бит исполнения при каждой синхронизации. Прямой вызов
# сломался бы на первом же пуше, причём молча.
#   */10 * * * * bash /opt/frontier-intelligence/scripts/alert-watchdog.sh >> /opt/frontier-intelligence/backups/watchdog.log 2>&1
set -uo pipefail

ROOT_DIR="${ROOT_DIR:-/opt/frontier-intelligence}"
ENV_FILE="$ROOT_DIR/.env"
STATE_FILE="${STATE_FILE:-$ROOT_DIR/runtime/watchdog-state}"
TEXTFILE_DIR="${TEXTFILE_DIR:-$ROOT_DIR/prometheus/textfile}"
TEXTFILE_OUT="$TEXTFILE_DIR/frontier_watchdog.prom"

PROM="${PROM:-http://127.0.0.1:9090}"
ALERTMANAGER="${ALERTMANAGER:-http://127.0.0.1:9093}"

# Не повторять одно и то же сообщение чаще, чем раз в N секунд.
COOLDOWN="${COOLDOWN:-10800}"   # 3 часа
# Контейнеры-отправители, по порядку. admin последним — он подозреваемый.
SENDERS="${SENDERS:-worker mcp ingest admin}"

NOW="$(date +%s)"
PROBLEMS=()

log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }

# ─────────────────────────────────────────────────────────── проверки

http_code() {
  timeout 10 curl -sS -o /dev/null -w '%{http_code}' "$1" 2>/dev/null || echo "000"
}

PROM_CODE="$(http_code "$PROM/-/healthy")"
[ "$PROM_CODE" = "200" ] || PROBLEMS+=("Prometheus не отвечает на /-/healthy (код $PROM_CODE)")

AM_CODE="$(http_code "$ALERTMANAGER/-/healthy")"
[ "$AM_CODE" = "200" ] || PROBLEMS+=("Alertmanager не отвечает на /-/healthy (код $AM_CODE)")

# Watchdog-алерт обязан присутствовать: правило firing всегда, а из Alertmanager
# он исчезает через resolve_timeout, если Prometheus перестал его слать.
if [ "$AM_CODE" = "200" ]; then
  WD_PRESENT="$(timeout 10 curl -sS "$ALERTMANAGER/api/v2/alerts?active=true" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    alerts = json.load(sys.stdin)
except Exception:
    print('parse_error'); raise SystemExit
print('yes' if any(a.get('labels', {}).get('alertname') == 'FrontierWatchdog' for a in alerts) else 'no')
" 2>/dev/null || echo "error")"
  case "$WD_PRESENT" in
    yes) : ;;
    no)  PROBLEMS+=("FrontierWatchdog отсутствует в Alertmanager — связка Prometheus -> правила -> Alertmanager разорвана") ;;
    *)   PROBLEMS+=("не удалось разобрать ответ Alertmanager /api/v2/alerts ($WD_PRESENT)") ;;
  esac
fi

# Провалы доставки. Считаем ростом за окно, а не абсолютом: счётчик копится с
# запуска процесса, и ненулевое значение само по себе ничего не значит.
if [ "$PROM_CODE" = "200" ]; then
  FAILED="$(timeout 10 curl -sS --get "$PROM/api/v1/query" \
      --data-urlencode 'query=sum(increase(alertmanager_notifications_failed_total[15m]))' 2>/dev/null \
    | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    r = d['data']['result']
    print(f\"{float(r[0]['value'][1]):.0f}\" if r else '0')
except Exception:
    print('error')
" 2>/dev/null || echo "error")"
  case "$FAILED" in
    error) PROBLEMS+=("не удалось получить alertmanager_notifications_failed_total из Prometheus") ;;
    0|"")  : ;;
    *)     PROBLEMS+=("Alertmanager не смог доставить $FAILED уведомлений за 15 минут") ;;
  esac
fi

# ─────────────────────────────────────────────────────────── отправка

send_telegram() {
  local text="$1" sent=0 c cname
  local token chat_id
  token="$(grep -E '^TELEGRAM_BOT_TOKEN=' "$ENV_FILE" | cut -d= -f2- | tr -d '\r')"
  chat_id="$(grep -E '^ALERT_TELEGRAM_CHAT_ID=' "$ENV_FILE" | cut -d= -f2- | tr -d '\r')"
  if [ -z "$token" ] || [ -z "$chat_id" ]; then
    log "отправка невозможна: TELEGRAM_BOT_TOKEN или ALERT_TELEGRAM_CHAT_ID пуст"
    return 1
  fi

  for c in $SENDERS; do
    cname="frontier-intelligence-${c}-1"
    docker inspect "$cname" --format '{{.State.Running}}' 2>/dev/null | grep -q true || continue
    # Секреты уходят в контейнер через stdin, а не аргументами: argv виден в ps.
    if printf '%s' "$(python3 -c "
import json, sys
print(json.dumps({'token': sys.argv[1], 'chat_id': sys.argv[2], 'text': sys.argv[3]}))
" "$token" "$chat_id" "$text")" \
      | timeout 40 docker exec -i "$cname" python -c "
import json, os, sys
import httpx

p = json.load(sys.stdin)
proxy = os.environ.get('TELEGRAM_ALERT_PROXY_URL') or None
r = httpx.post(
    f\"https://api.telegram.org/bot{p['token']}/sendMessage\",
    json={'chat_id': p['chat_id'], 'text': p['text'], 'disable_web_page_preview': True},
    proxy=proxy,
    timeout=30,
)
r.raise_for_status()
" >/dev/null 2>&1; then
      log "отправлено через $c"
      sent=1
      break
    else
      log "через $c не удалось, пробую следующий"
    fi
  done

  [ "$sent" = 1 ] || { log "ОТПРАВИТЬ НЕ УДАЛОСЬ НИ ЧЕРЕЗ ОДИН КОНТЕЙНЕР"; return 1; }
  return 0
}

# ─────────────────────────────────────────────────────────── состояние

mkdir -p "$(dirname "$STATE_FILE")" "$TEXTFILE_DIR"
PREV_SIG=""
PREV_TS=0
if [ -f "$STATE_FILE" ]; then
  PREV_SIG="$(sed -n '1p' "$STATE_FILE" 2>/dev/null || true)"
  PREV_TS="$(sed -n '2p' "$STATE_FILE" 2>/dev/null || echo 0)"
fi
case "$PREV_TS" in ''|*[!0-9]*) PREV_TS=0 ;; esac

if [ "${#PROBLEMS[@]}" -eq 0 ]; then
  STATUS=0
  log "контур алертинга в порядке"
  # О восстановлении сообщаем, только если до этого сообщали о поломке.
  if [ -n "$PREV_SIG" ]; then
    send_telegram "Frontier watchdog: контур алертинга восстановлен.

Prometheus и Alertmanager отвечают, FrontierWatchdog на месте, провалов доставки за 15 минут нет." || true
  fi
  : > "$STATE_FILE"
else
  STATUS=1
  SIG="$(printf '%s\n' "${PROBLEMS[@]}" | sort | md5sum | cut -c1-32)"
  BODY="Frontier watchdog: контур алертинга сломан.

$(printf '  - %s\n' "${PROBLEMS[@]}")
Это сообщение отправлено в обход admin и маршрутизации Alertmanager.
Хост: $(hostname). Проверено: $(date -Is)."
  log "проблемы: ${PROBLEMS[*]}"
  if [ "$SIG" = "$PREV_SIG" ] && [ $((NOW - PREV_TS)) -lt "$COOLDOWN" ]; then
    log "то же самое уже отправляли $((NOW - PREV_TS))с назад, молчу до истечения $COOLDOWN с"
  else
    if send_telegram "$BODY"; then
      printf '%s\n%s\n' "$SIG" "$NOW" > "$STATE_FILE"
    else
      # Не записываем состояние: на следующем запуске попробуем снова.
      log "состояние не сохранено, повтор на следующем запуске"
    fi
  fi
fi

# ─────────────────────────────────────────────── метрика о самом себе
# Пишется всегда, в том числе при поломке: по ней видно, что скрипт вообще жив.
TMP="$(mktemp "${TEXTFILE_OUT}.XXXXXX")"
{
  echo "# HELP frontier_watchdog_last_run_timestamp_seconds Время последнего прогона alert-watchdog.sh."
  echo "# TYPE frontier_watchdog_last_run_timestamp_seconds gauge"
  echo "frontier_watchdog_last_run_timestamp_seconds $NOW"
  echo "# HELP frontier_watchdog_problems Количество найденных проблем в контуре алертинга."
  echo "# TYPE frontier_watchdog_problems gauge"
  echo "frontier_watchdog_problems ${#PROBLEMS[@]}"
} > "$TMP"
chmod 644 "$TMP"
mv "$TMP" "$TEXTFILE_OUT"

exit "$STATUS"
