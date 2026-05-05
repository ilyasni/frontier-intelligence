#!/usr/bin/env bash
# Р В Р Р‹Р В Р’В±Р В РЎвЂўР РЋР вЂљР В РЎвЂќР В Р’В° Р В РЎвЂўР В Р’В±Р РЋР вЂљР В Р’В°Р В Р’В·Р В РЎвЂўР В Р вЂ  Р В Р вЂ¦Р В Р’В° Р РЋР С“Р В Р’ВµР РЋР вЂљР В Р вЂ Р В Р’ВµР РЋР вЂљР В Р’Вµ Р В РЎвЂ”Р РЋР вЂљР В РЎвЂ Р РЋР С“Р В Р’В±Р В РЎвЂўР РЋР РЏР РЋРІР‚В¦ BuildKit/AppArmor (Р РЋР С“Р В РЎВ. docs/ops-server-troubleshooting.md).
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
bash scripts/server-prepare-base-images.sh "${SERVICES[@]}"
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
echo "OK build. Р В РІР‚СњР В Р’В°Р В Р’В»Р РЋР Р‰Р РЋРІвЂљВ¬Р В Р’Вµ: docker compose -f docker-compose.yml up -d --force-recreate <Р РЋР С“Р В Р’ВµР РЋР вЂљР В Р вЂ Р В РЎвЂР РЋР С“Р РЋРІР‚в„–>"
