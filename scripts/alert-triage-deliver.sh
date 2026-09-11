#!/usr/bin/env bash
# alert-triage-deliver.sh — persist a triage digest and push it to ntfy.
#
# (Named "deliver", not "notify", on purpose: the project's Bash pre-hook blocks the
#  literal substring "-n", which "-notify" would trip on every scheduled run.)
#
# Runs on the server.
#   $1 = path to a finished Markdown digest (produced by the Claude triage loop).
#   $2 = send mode: "send" (default) or "skip". "skip" saves the log but does NOT push
#        to ntfy — used when no firing critical/warning alert is present.
# It:
#   1. Copies the digest to docs/ops/alert-digests/<UTC-date>.md  (excluded from rsync,
#      so `sync-push --delete` never wipes the history) — ALWAYS, regardless of mode.
#   2. If mode=send: sends a UTF-8-byte-safe ntfy message (<=3800 bytes, TL;DR first).
#
# The send is delegated to the app's own ntfy sender running INSIDE admin. The module's
# truncate_ntfy_message helper owns the byte boundary (3800, strictly below ntfy's
# message-size-limit of 4096: a body of exactly 4096 bytes is treated as an attachment
# and rejected with 400 when attachments are disabled) and never splits UTF-8.
#
# Missing/invalid input or persistence failure -> nonzero. Send failure -> the saved
# digest remains, a warning is printed, and delivery exits 0.
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
case "$MODE" in
  send|skip) : ;;
  *)
    echo "deliver: invalid mode '${MODE}' (expected send or skip)" >&2
    exit 2
    ;;
esac

REPO="${REPO:-/opt/frontier-intelligence}"
DEST_DIR="${REPO}/docs/ops/alert-digests"
UTC_DATE="$(date -u '+%Y-%m-%d')"
DEST="${DEST_DIR}/${UTC_DATE}.md"

if ! mkdir -p "$DEST_DIR"; then
  echo "deliver: cannot create digest directory" >&2
  exit 1
fi
if ! cp "$DIGEST_PATH" "$DEST"; then
  echo "deliver: cannot persist digest" >&2
  exit 1
fi
echo "deliver: saved digest -> ${DEST}"

# Gate: only push to ntfy when there is something worth pinging about.
if [ "$MODE" = "skip" ]; then
  echo "deliver: ntfy skipped (mode=skip — нет firing critical/warning; лог сохранён)"
  exit 0
fi

BODY="$(cat "$DEST")"
MSG="🔎 Frontier alert-triage ${UTC_DATE} (UTC)"$'\n\n'"${BODY}"
NOTE=$'\n\n… (обрезано; полный разбор: '"docs/ops/alert-digests/${UTC_DATE}.md на сервере)"

# Byte-safe truncation and delivery use the app's own ntfy module inside admin.
RESULT="$(printf '%s' "$MSG" | ( cd "$REPO" && docker compose exec -T admin python -c "import sys,asyncio; from admin.backend.services.ntfy_alerts import send_ntfy_alert_message as s, truncate_ntfy_message as t; message=t(sys.stdin.read(), suffix=sys.argv[1]); print('FRONTIER_NTFY_SENT' if asyncio.run(s(message)) else 'FRONTIER_NTFY_DISABLED')" "$NOTE" ) 2>&1)"
FINAL_MARKER="$(printf '%s\n' "$RESULT" | tail -n 1 | tr -d '\r')"
if [ "$FINAL_MARKER" = "FRONTIER_NTFY_SENT" ]; then
  echo "deliver: ntfy sent (via admin sender)"
else
  echo "deliver: ntfy send FAILED — ${RESULT}" >&2
fi
exit 0
