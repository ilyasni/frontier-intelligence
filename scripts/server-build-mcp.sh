#!/usr/bin/env bash
# РЎР±РѕСЂРєР° РѕР±СЂР°Р·Р° mcp РЅР° СЃРµСЂРІРµСЂРµ С‡РµСЂРµР· classic builder Рё РѕРїС†РёРѕРЅР°Р»СЊРЅС‹Р№ mirror PyPI.
set -euo pipefail
cd /opt/frontier-intelligence

: "${PIP_INDEX_URL:=https://mirrors.aliyun.com/pypi/simple/}"
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

exec docker compose \
  --profile core \
  --profile mcp \
  build \
  --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL}" \
  mcp
