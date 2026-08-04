#!/usr/bin/env bash
# После rsync: пересборка образов и перезапуск worker, mcp, crawl4ai, paddleocr.
# Две фазы: сначала worker+crawl4ai (быстрый пайплайн), потом mcp+paddleocr (тяжёлый pip).
# Иначе один общий `docker compose build` ждёт самый медленный сервис (PyPI / Paddle).
# См. docs/ops-server-troubleshooting.md.
set -euo pipefail
cd /opt/frontier-intelligence

# Набор профилей берётся из единственного места, а не переписывается здесь заново.
# Прежний локальный набор "core,worker,mcp,crawl,paddleocr" был НЕВАЛИДЕН и ронял
# скрипт ещё до сборки: `service "crawl4ai" depends on undefined service "xray"`.
# Ошибка возникает на разборе проекта, поэтому падала вся команда целиком.
# Путь относительно корня (cd сделан выше), а не через dirname "$0": при запуске
# конвейером `tr -d '\r' < файл | ssh 'bash -s'` $0 равен "bash", dirname даёт ".",
# и под set -euo pipefail скрипт обрывался бы молча.
. ./scripts/compose-profiles.sh
export COMPOSE_PROFILES="${COMPOSE_PROFILES:-$FRONTIER_PROFILES_BUILD}"

export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-0}"
export COMPOSE_DOCKER_CLI_BUILD="${COMPOSE_DOCKER_CLI_BUILD:-0}"

# Проверяем набор ДО первой сборки: иначе невалидность обнаружится через несколько
# минут ожидания pip, когда откатываться дороже.
frontier_assert_profiles || exit 1

eval "$(bash scripts/server-ensure-python-base-image.sh)"

echo "COMPOSE_PROFILES=$COMPOSE_PROFILES"
echo "PYTHON_BASE_IMAGE=$PYTHON_BASE_IMAGE"

# Инлайновые --profile сняты намеренно: они дублировали набор третьим местом и
# точно так же не включали xray. COMPOSE_PROFILES действует на всю команду.
echo "=== Phase 1: worker crawl4ai (build + up) ==="
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml build worker crawl4ai
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml up -d --force-recreate worker crawl4ai

echo "=== Phase 2: mcp paddleocr (build + up) ==="
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml build mcp paddleocr
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
docker compose -f docker-compose.yml up -d --force-recreate mcp paddleocr

echo "OK. Смок: bash scripts/server_checks.sh"
