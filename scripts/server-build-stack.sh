#!/usr/bin/env bash
set -euo pipefail

cd /opt/frontier-intelligence

export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
# Набор профилей — из единственного места (scripts/compose-profiles.sh), а не копией здесь.
# Этот скрипт был эталоном, но эталон, существующий как одна из пяти копий, эталоном
# быть перестаёт: остальные четыре от него разъехались, три оказались невалидны.
#
# Путь ОТНОСИТЕЛЬНО корня (мы уже сделали cd выше), а не через dirname "$0".
# При запуске конвейером `tr -d '\r' < файл | ssh 'bash -s'` — документированный
# обход CRLF — $0 равен "bash", dirname даёт ".", файла рядом нет, и под
# set -euo pipefail скрипт молча обрывался бы ещё до сборки.
. ./scripts/compose-profiles.sh
export COMPOSE_PROFILES="${COMPOSE_PROFILES:-$FRONTIER_PROFILES_BUILD}"
export FRONTIER_ENABLE_OVERLAY_FALLBACK="${FRONTIER_ENABLE_OVERLAY_FALLBACK:-1}"
frontier_assert_profiles || exit 1
eval "$(bash scripts/server-ensure-python-base-image.sh)"

# mcp-gateway добавлен 04.08.2026. Его здесь не было, хотя у него отдельный
# образ из mcp/Dockerfile.gateway и в нём тот же запечённый код shared/ и mcp/.
# Из-за пропуска штатная сборка его никогда не обновляла: аудит нашёл у шлюза
# устаревший shared/config.py (не было own_stake_*, searxng_*, а polza-модели
# остались прежними), и первая же попытка вывести туда контур RSI собрала образ
# mcp, а шлюз остался с 22 инструментами из 32.
DEFAULT_SERVICES=(gpt2giga-proxy worker crawl4ai ingest admin mcp mcp-gateway paddleocr)

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
