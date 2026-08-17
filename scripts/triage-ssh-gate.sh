#!/usr/bin/env bash
# triage-ssh-gate.sh — forced command behind the alert-triage routine's restricted SSH key.
#
# The daily triage loop moved off the Windows Task Scheduler (it died silently on
# 2026-08-07 when the local CLI session logged out, and nobody noticed for eleven days)
# onto a cloud routine running in the hub-orchestrator bridge environment, 192.168.31.116.
# That environment sits on the same /24 as this server, so it can reach us — but it is
# unattended, so it gets a key that can do three named things instead of a shell.
#
# authorized_keys entry (single line, on this host, for the routine's key):
#   from="192.168.31.116",command="/opt/frontier-intelligence/scripts/triage-ssh-gate.sh",\
#   no-agent-forwarding,no-port-forwarding,no-X11-forwarding,no-pty ssh-ed25519 AAAA... comment
#
# The client asks for a verb; sshd puts it in SSH_ORIGINAL_COMMAND and runs this instead:
#   ssh frontier-intelligence collect              -> read-only diagnostic bundle on stdout
#   ssh frontier-intelligence logs <service> [n]   -> last n lines of one service's log
#   ssh frontier-intelligence deliver <send|skip>  -> digest read from stdin, then delivered
#
# Anything else is refused and logged. The whitelist is the security boundary: a compromised
# routine gets these three verbs, not the machine.

set -uo pipefail

REPO="/opt/frontier-intelligence"
LOG="${REPO}/backups/triage-gate.log"
MAX_LINES=400
MAX_DIGEST_BYTES=$((256 * 1024))

# Служебные имена сервисов, чьи логи петля вправе читать. Список закрытый: неизвестное
# имя отвергается, а не подставляется в docker — иначе аргумент становится дырой.
ALLOWED_SERVICES="admin alertmanager crawl4ai gpt2giga-proxy grafana ingest mcp mcp-gateway neo4j node-exporter paddleocr postgres prometheus qdrant redis searxng worker xray"

audit() {
  printf '%s gate: %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOG" 2>/dev/null
}

refuse() {
  audit "REFUSED: $*"
  printf 'triage-gate: отказано — %s\n' "$*" >&2
  exit 1
}

REQUEST="${SSH_ORIGINAL_COMMAND:-}"
[ -n "$REQUEST" ] || refuse "пустая команда (ключ не даёт интерактивный шелл)"

# Разбираем сами, без eval: аргументы приходят снаружи и доверенными не являются.
read -r VERB ARG1 ARG2 EXTRA <<< "$REQUEST"
[ -z "${EXTRA:-}" ] || refuse "слишком много аргументов: '$REQUEST'"

case "$VERB" in
  collect)
    [ -z "${ARG1:-}" ] || refuse "collect не принимает аргументов"
    audit "collect"
    # tr -d '\r': часть scripts/*.sh приезжает с Windows с CRLF и без этого падает на `set:`.
    tr -d '\r' < "${REPO}/scripts/alert-triage-collect.sh" | bash
    ;;

  logs)
    SERVICE="${ARG1:-}"
    [ -n "$SERVICE" ] || refuse "logs требует имя сервиса"
    case " $ALLOWED_SERVICES " in
      *" $SERVICE "*) : ;;
      *) refuse "сервис '$SERVICE' не в списке разрешённых" ;;
    esac
    LINES="${ARG2:-200}"
    case "$LINES" in
      ''|*[!0-9]*) refuse "число строк должно быть целым, получено '$LINES'" ;;
    esac
    [ "$LINES" -le "$MAX_LINES" ] || LINES="$MAX_LINES"
    audit "logs $SERVICE $LINES"
    # docker logs по имени контейнера, а не compose: у compose все сервисы под profiles,
    # и без COMPOSE_PROFILES он отвечает «no such service» вместо логов.
    docker logs --tail "$LINES" "frontier-intelligence-${SERVICE}-1" 2>&1
    ;;

  deliver)
    MODE="${ARG1:-}"
    case "$MODE" in
      send|skip) : ;;
      *) refuse "режим доставки должен быть send или skip, получено '$MODE'" ;;
    esac
    DIGEST="$(mktemp /tmp/frontier-triage-digest.XXXXXX.md)" || refuse "не удалось создать временный файл"
    trap 'rm -f "$DIGEST"' EXIT
    head -c "$MAX_DIGEST_BYTES" > "$DIGEST"
    [ -s "$DIGEST" ] || refuse "дайджест пуст (ожидается на stdin)"
    audit "deliver $MODE ($(wc -c < "$DIGEST") байт)"
    tr -d '\r' < "${REPO}/scripts/alert-triage-deliver.sh" | bash -s -- "$DIGEST" "$MODE"
    ;;

  *)
    refuse "неизвестная команда '$VERB' (разрешены: collect, logs, deliver)"
    ;;
esac
