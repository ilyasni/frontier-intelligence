#!/usr/bin/env bash
# РџРѕСЃР»Рµ rsync: РїРµСЂРµСЃР±РѕСЂРєР° РѕР±СЂР°Р·РѕРІ Рё РїРµСЂРµР·Р°РїСѓСЃРє worker, mcp, crawl4ai, paddleocr.
# Р”РІРµ С„Р°Р·С‹: СЃРЅР°С‡Р°Р»Р° worker+crawl4ai (Р±С‹СЃС‚СЂС‹Р№ РїР°Р№РїР»Р°Р№РЅ), РїРѕС‚РѕРј mcp+paddleocr (С‚СЏР¶С‘Р»С‹Р№ pip).
# РРЅР°С‡Рµ РѕРґРёРЅ РѕР±С‰РёР№ `docker compose build` Р¶РґС‘С‚ СЃР°РјС‹Р№ РјРµРґР»РµРЅРЅС‹Р№ СЃРµСЂРІРёСЃ (PyPI / Paddle).
# РЎРј. docs/ops-server-troubleshooting.md.
set -euo pipefail
cd /opt/frontier-intelligence

export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-0}"
export COMPOSE_DOCKER_CLI_BUILD="${COMPOSE_DOCKER_CLI_BUILD:-0}"
export COMPOSE_PROFILES="${COMPOSE_PROFILES:-core,worker,mcp,crawl,paddleocr}"

echo "COMPOSE_PROFILES=$COMPOSE_PROFILES"

echo "=== Phase 1: worker crawl4ai (build + up) ==="
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml \
  --profile core --profile worker --profile crawl \
  build --pull worker crawl4ai
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml \
  --profile core --profile worker --profile crawl \
  up -d --force-recreate worker crawl4ai

echo "=== Phase 2: mcp paddleocr (build + up) ==="
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml \
  --profile core --profile mcp --profile paddleocr \
  build --pull mcp paddleocr
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml \
  --profile core --profile mcp --profile paddleocr \
  up -d --force-recreate mcp paddleocr

echo "OK. РЎРјРѕРє: bash scripts/server_checks.sh"
