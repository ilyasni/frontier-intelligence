#!/bin/sh
set -eu

XRAY_CONFIG_PATH="${XRAY_CONFIG_PATH:-/etc/xray/config.json}"
XRAY_RELOAD_TRIGGER_PATH="${XRAY_RELOAD_TRIGGER_PATH:-/runtime/xray-reload.trigger}"
XRAY_ACTIVE_PROFILE_PATH="${XRAY_ACTIVE_PROFILE_PATH:-/runtime/xray-active-profile.txt}"
XRAY_PROFILE_REGISTRY_PATH="${XRAY_PROFILE_REGISTRY_PATH:-/runtime/xray-profiles.json}"
XRAY_RELOAD_POLL_SECONDS="${XRAY_RELOAD_POLL_SECONDS:-2}"

render_config() {
  python /opt/xray/render_config.py > "${XRAY_CONFIG_PATH}"
}

reload_marker() {
  python - <<'PY'
from __future__ import annotations

import hashlib
import os
from pathlib import Path

paths = [
    os.environ.get("XRAY_PROFILE_REGISTRY_PATH", "/runtime/xray-profiles.json"),
    os.environ.get("XRAY_ACTIVE_PROFILE_PATH", "/runtime/xray-active-profile.txt"),
    os.environ.get("XRAY_RELOAD_TRIGGER_PATH", "/runtime/xray-reload.trigger"),
]
h = hashlib.sha256()
for raw_path in paths:
    path = Path(raw_path)
    h.update(str(path).encode("utf-8"))
    if path.exists():
        stat = path.stat()
        h.update(str(int(stat.st_mtime_ns)).encode("utf-8"))
        h.update(str(stat.st_size).encode("utf-8"))
    else:
        h.update(b"missing")
print(h.hexdigest())
PY
}

start_xray() {
  render_config
  /usr/local/bin/xray -config "${XRAY_CONFIG_PATH}" &
  XRAY_PID=$!
}

stop_xray() {
  if kill -0 "${XRAY_PID}" 2>/dev/null; then
    kill "${XRAY_PID}" 2>/dev/null || true
    wait "${XRAY_PID}" 2>/dev/null || true
  fi
}

start_xray
LAST_MARKER="$(reload_marker)"

trap 'stop_xray; exit 0' INT TERM

while true; do
  sleep "${XRAY_RELOAD_POLL_SECONDS}"
  if ! kill -0 "${XRAY_PID}" 2>/dev/null; then
    wait "${XRAY_PID}" || true
    exit 1
  fi
  CURRENT_MARKER="$(reload_marker)"
  if [ "${CURRENT_MARKER}" != "${LAST_MARKER}" ]; then
    LAST_MARKER="${CURRENT_MARKER}"
    stop_xray
    start_xray
  fi
done
