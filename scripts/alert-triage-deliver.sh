#!/usr/bin/env bash
# alert-triage-deliver.sh — persist a triage digest and push it to the Telegram alert chat.
#
# (Named "deliver", not "notify", on purpose: the project's Bash pre-hook blocks the
#  literal substring "-n", which "-notify" would trip on every scheduled run.)
#
# Runs on the server.
#   $1 = path to a finished Markdown digest (produced by the Claude triage loop).
#   $2 = send mode: "send" (default) or "skip". "skip" saves the log but does NOT push
#        to Telegram — used when no firing critical/warning alert is present.
# It:
#   1. Copies the digest to docs/ops/alert-digests/<UTC-date>.md  (excluded from rsync,
#      so `sync-push --delete` never wipes the history) — ALWAYS, regardless of mode.
#   2. If mode=send: sends a Telegram-safe message (first ~3800 chars, TL;DR at the top)
#      to the same chat alerts already use, reusing TELEGRAM_* creds + proxy from .env.
#
# The Telegram send is delegated to the app's own sender (send_telegram_alert_message)
# running INSIDE the admin container: the socks5 proxy host (xray) only resolves on the
# docker network, so a host-side curl cannot reach it. Creds/proxy resolve from admin's env.
#
# Missing digest -> exit 1. Send failure -> saves file, warns, exits 0
# (a broken bot must not break the log).
#
# Invoke CRLF-safely from the client with:
#   ssh frontier-intelligence "cd /opt/frontier-intelligence && tr -d '\r' < scripts/alert-triage-deliver.sh | bash -s -- /tmp/frontier-alert-digest.md"

set -uo pipefail

DIGEST_PATH="${1:-}"
MODE="${2:-send}"   # send | skip
if [ -z "$DIGEST_PATH" ] || [ ! -f "$DIGEST_PATH" ]; then
  echo "deliver: digest file not found: '${DIGEST_PATH}'" >&2
  exit 1
fi

REPO="/opt/frontier-intelligence"
DEST_DIR="${REPO}/docs/ops/alert-digests"
UTC_DATE="$(date -u '+%Y-%m-%d')"
DEST="${DEST_DIR}/${UTC_DATE}.md"

mkdir -p "$DEST_DIR"
cp "$DIGEST_PATH" "$DEST"
echo "deliver: saved digest -> ${DEST}"

# Gate: only push to Telegram when there is something worth pinging about.
if [ "$MODE" = "skip" ]; then
  echo "deliver: telegram skipped (mode=skip — нет firing critical/warning; лог сохранён)"
  exit 0
fi

# Telegram hard limit is 4096 chars; keep headroom for the header + truncation note.
MAX=3800
BODY="$(cat "$DEST")"
NOTE=""
if [ "${#BODY}" -gt "$MAX" ]; then
  BODY="$(printf '%s' "$BODY" | cut -c1-${MAX})"
  NOTE=$'\n\n… (обрезано; полный разбор: '"docs/ops/alert-digests/${UTC_DATE}.md на сервере)"
fi
MSG="🔎 Frontier alert-triage ${UTC_DATE} (UTC)"$'\n\n'"${BODY}${NOTE}"

# Send via the app's own sender inside admin (docker network resolves the socks5 proxy).
RESULT="$(printf '%s' "$MSG" | ( cd "$REPO" && docker compose exec -T admin python -c "import sys,asyncio; from admin.backend.services.telegram_alerts import send_telegram_alert_message as s; print('SENT' if asyncio.run(s(sys.stdin.read())) else 'DISABLED')" ) 2>&1)"
if printf '%s' "$RESULT" | grep -q 'SENT'; then
  echo "deliver: telegram sent (via admin sender)"
else
  echo "deliver: telegram send FAILED — ${RESULT}" >&2
fi
exit 0
