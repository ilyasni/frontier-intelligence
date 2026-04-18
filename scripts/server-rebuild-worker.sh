#!/usr/bin/env bash
# РџСЂРё РѕС€РёР±РєРµ "no such service: worke" вЂ” РІ С„Р°Р№Р»Рµ CRLF; sed -i 's/\r$//' вЂ¦/server-rebuild-worker.sh
set -euo pipefail
cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
export COMPOSE_PROFILES=core,worker
svc=worker
docker compose -f docker-compose.yml build "$svc"
docker compose -f docker-compose.yml up -d --no-deps --force-recreate "$svc"
echo OK
