#!/usr/bin/env bash
# Сборка сервиса с обходом типичных сбоев BuildKit+AppArmor на Linux.
# По умолчанию: DOCKER_BUILDKIT=0 (classic builder), только docker-compose.yml.
# Если задать USE_COMPOSE_BUILD_PRIVILEGED=1 — добавится docker-compose.build-host-fix.yml
# (нужен демон BuildKit с разрешённым security.insecure; иначе будет ошибка entitlement).
#
# Примеры:
#   bash scripts/server-compose-build-with-fix.sh ingest
#   bash scripts/server-compose-build-with-fix.sh ingest --up
set -euo pipefail
cd /opt/frontier-intelligence

svc="${1:?service name, e.g. ingest}"
shift || true
DO_UP=false
if [[ "${1:-}" == "--up" ]]; then
  DO_UP=true
  shift || true
fi

export DOCKER_BUILDKIT=0

compose_args=(-f docker-compose.yml)
if [[ "${USE_COMPOSE_BUILD_PRIVILEGED:-}" == "1" ]]; then
  compose_args+=(-f docker-compose.build-host-fix.yml)
fi

docker compose "${compose_args[@]}" --profile core --profile ingest build "$svc"

if $DO_UP; then
  docker compose "${compose_args[@]}" --profile core --profile ingest up -d --force-recreate "$svc"
fi
