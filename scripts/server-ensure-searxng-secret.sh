#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-/opt/frontier-intelligence}"
CONFIG_FILE="${ROOT_DIR}/searxng/settings.yml"
TEMPLATE_FILE="${ROOT_DIR}/searxng/settings.example.yml"

if [ ! -f "${CONFIG_FILE}" ]; then
  if [ ! -f "${TEMPLATE_FILE}" ]; then
    echo "Missing template: ${TEMPLATE_FILE}" >&2
    exit 1
  fi
  cp "${TEMPLATE_FILE}" "${CONFIG_FILE}"
fi

if grep -Eq 'secret_key: "(CHANGE_ME_GENERATE_ON_SERVER|ultrasecretkey)"' "${CONFIG_FILE}"; then
  if command -v openssl >/dev/null 2>&1; then
    secret="$(openssl rand -hex 32)"
  else
    secret="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  fi
  sed -i -E "s#secret_key: \".*\"#secret_key: \"${secret}\"#" "${CONFIG_FILE}"
  echo "Generated SearXNG secret_key in ${CONFIG_FILE} (value not printed)."
else
  echo "SearXNG secret_key already set in ${CONFIG_FILE} (value not printed)."
fi
