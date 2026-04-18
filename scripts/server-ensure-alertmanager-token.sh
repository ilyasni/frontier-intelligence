#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-/opt/frontier-intelligence}"
ENV_FILE="$ROOT_DIR/.env"
KEY="ALERTMANAGER_WEBHOOK_TOKEN"

mkdir -p "$ROOT_DIR"
touch "$ENV_FILE"

generate_token() {
  head -c 32 /dev/urandom | base64 | tr -d '/+=\n' | cut -c1-43
}

if grep -q "^${KEY}=" "$ENV_FILE"; then
  current="$(grep "^${KEY}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2-)"
  if [[ -n "$current" ]]; then
    echo "present"
    exit 0
  fi
  token="$(generate_token)"
  sed -i "s#^${KEY}=.*#${KEY}=${token}#" "$ENV_FILE"
  echo "updated"
  exit 0
fi

token="$(generate_token)"
printf '\n%s=%s\n' "$KEY" "$token" >> "$ENV_FILE"
echo "created"
