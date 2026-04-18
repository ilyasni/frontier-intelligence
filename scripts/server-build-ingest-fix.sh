#!/usr/bin/env bash
# РЎР±РѕСЂРєР° ingest РЅР° С…РѕСЃС‚Р°С… СЃ AppArmor/runc РїСЂРё BuildKit (РѕС€РёР±РєР° profile РЅР° RUN pip).
# РќР° РјРЅРѕРіРёС… СЃРµСЂРІРµСЂР°С… РґРѕСЃС‚Р°С‚РѕС‡РЅРѕ legacy builder вЂ” Р±РµР· privileged Рё Р±РµР· entitlement.
set -euo pipefail
cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
docker compose --profile core --profile xray --profile ingest build ingest
docker compose --profile core --profile xray --profile ingest up -d --force-recreate xray ingest
