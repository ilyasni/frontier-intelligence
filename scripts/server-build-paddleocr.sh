#!/usr/bin/env bash
# РЎР±РѕСЂРєР° РѕР±СЂР°Р·Р° paddleocr РЅР° СЃРµСЂРІРµСЂРµ (СѓСЃС‚РѕР№С‡РёРІРѕ Рє РѕР±СЂС‹РІСѓ SSH: Р·Р°РїСѓСЃРєР°Р№ РЅР° СЃРµСЂРІРµСЂРµ РёР»Рё С‡РµСЂРµР· nohup)
set -euo pipefail
cd /opt/frontier-intelligence
: "${PIP_INDEX_URL:=https://mirrors.aliyun.com/pypi/simple/}"
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
exec docker compose --profile core --profile paddleocr build \
  --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL}" \
  paddleocr
