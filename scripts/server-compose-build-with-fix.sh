#!/usr/bin/env bash
# РЎР±РѕСЂРєР° СЃРµСЂРІРёСЃР° СЃ РѕР±С…РѕРґРѕРј С‚РёРїРёС‡РЅС‹С… СЃР±РѕРµРІ BuildKit+AppArmor РЅР° Linux.
# РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ: DOCKER_BUILDKIT=0 (classic builder), С‚РѕР»СЊРєРѕ docker-compose.yml.
# Р•СЃР»Рё Р·Р°РґР°С‚СЊ USE_COMPOSE_BUILD_PRIVILEGED=1 вЂ” РґРѕР±Р°РІРёС‚СЃСЏ docker-compose.build-host-fix.yml
# (РЅСѓР¶РµРЅ РґРµРјРѕРЅ BuildKit СЃ СЂР°Р·СЂРµС€С‘РЅРЅС‹Рј security.insecure; РёРЅР°С‡Рµ Р±СѓРґРµС‚ РѕС€РёР±РєР° entitlement).
#
# РџСЂРёРјРµСЂС‹:
#   bash scripts/server-compose-build-with-fix.sh ingest
#   bash scripts/server-compose-build-with-fix.sh ingest --up
set -euo pipefail
cd /opt/frontier-intelligence

svc="${1:?service name, e.g. ingest}"
shift || true
DO_UP=false
if [[ "${1:-}" == "--up" ]]; then
  DO_UP=true
  shift || true
fi

export DOCKER_BUILDKIT=0

compose_args=(-f docker-compose.yml)
if [[ "${USE_COMPOSE_BUILD_PRIVILEGED:-}" == "1" ]]; then
  compose_args+=(-f docker-compose.build-host-fix.yml)
fi

docker compose "${compose_args[@]}" --profile core --profile ingest build "$svc"

if $DO_UP; then
  docker compose "${compose_args[@]}" --profile core --profile ingest up -d --force-recreate "$svc"
fi
