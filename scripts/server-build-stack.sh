#!/usr/bin/env bash
# Р РЋР В±Р С•РЎР‚Р С”Р В° Р С•Р В±РЎР‚Р В°Р В·Р С•Р Р† Р Р…Р В° РЎРѓР ВµРЎР‚Р Р†Р ВµРЎР‚Р Вµ Р С—РЎР‚Р С‘ РЎРѓР В±Р С•РЎРЏРЎвЂ¦ BuildKit/AppArmor (РЎРѓР С. docs/ops-server-troubleshooting.md).
set -euo pipefail
cd /opt/frontier-intelligence

export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
export COMPOSE_PROFILES="${COMPOSE_PROFILES:-core,ingest,xray,worker,crawl,paddleocr,mcp,admin}"

DEFAULT_SERVICES=(gpt2giga-proxy worker crawl4ai ingest admin mcp paddleocr)

if [[ $# -gt 0 ]]; then
  SERVICES=("$@")
else
  SERVICES=("${DEFAULT_SERVICES[@]}")
fi

echo "COMPOSE_PROFILES=$COMPOSE_PROFILES"
echo "build: ${SERVICES[*]}"
docker compose -f docker-compose.yml \
  --profile core \
  --profile ingest \
  --profile xray \
  --profile worker \
  --profile crawl \
  --profile paddleocr \
  --profile mcp \
  --profile admin \
  build "${SERVICES[@]}"
echo "OK build. Р вЂќР В°Р В»РЎРЉРЎв‚¬Р Вµ: docker compose -f docker-compose.yml up -d --force-recreate <РЎРѓР ВµРЎР‚Р Р†Р С‘РЎРѓРЎвЂ№>"
