#!/usr/bin/env bash
set -euo pipefail

cd /opt/frontier-intelligence

export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
# Набор профилей — из единственного места (scripts/compose-profiles.sh), а не копией здесь.
# Этот скрипт был эталоном, но эталон, существующий как одна из пяти копий, эталоном
# быть перестаёт: остальные четыре от него разъехались, три оказались невалидны.
. "$(dirname "$0")/compose-profiles.sh"
export COMPOSE_PROFILES="${COMPOSE_PROFILES:-$FRONTIER_PROFILES_BUILD}"
export FRONTIER_ENABLE_OVERLAY_FALLBACK="${FRONTIER_ENABLE_OVERLAY_FALLBACK:-1}"
frontier_assert_profiles || exit 1
eval "$(bash scripts/server-ensure-python-base-image.sh)"

DEFAULT_SERVICES=(gpt2giga-proxy worker crawl4ai ingest admin mcp paddleocr)

if [[ $# -gt 0 ]]; then
  SERVICES=("$@")
else
  SERVICES=("${DEFAULT_SERVICES[@]}")
fi

overlay_supported() {
  case "$1" in
    worker|admin|mcp) return 0 ;;
    *) return 1 ;;
  esac
}

standard_build() {
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
}

can_overlay_all=1
for service in "${SERVICES[@]}"; do
  if ! overlay_supported "$service"; then
    can_overlay_all=0
    break
  fi
done

echo "COMPOSE_PROFILES=$COMPOSE_PROFILES"
echo "PYTHON_BASE_IMAGE=$PYTHON_BASE_IMAGE"
echo "build: ${SERVICES[*]}"

if ! standard_build; then
  if [[ "$FRONTIER_ENABLE_OVERLAY_FALLBACK" == "1" && "$can_overlay_all" == "1" ]]; then
    echo "standard build failed; falling back to overlay rebuild for: ${SERVICES[*]}"
    bash scripts/server-build-overlay.sh "${SERVICES[@]}"
  else
    echo "standard build failed and overlay fallback is unavailable for: ${SERVICES[*]}" >&2
    exit 1
  fi
fi

echo "OK build. Next step: docker compose -f docker-compose.yml up -d --force-recreate <services>"
