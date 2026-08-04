#!/usr/bin/env bash
# Сборка образа paddleocr на сервере (устойчиво к обрыву SSH: запускай на сервере или через nohup)
set -euo pipefail
cd /opt/frontier-intelligence
: "${PIP_INDEX_URL:=https://mirrors.aliyun.com/pypi/simple/}"
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
exec docker compose --profile core --profile paddleocr build \
  --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL}" \
  paddleocr
