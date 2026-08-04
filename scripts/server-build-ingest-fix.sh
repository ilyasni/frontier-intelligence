#!/usr/bin/env bash
# Сборка ingest на хостах с AppArmor/runc при BuildKit (ошибка profile на RUN pip).
# На многих серверах достаточно legacy builder — без privileged и без entitlement.
set -euo pipefail
cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
docker compose --profile core --profile xray --profile ingest build ingest
docker compose --profile core --profile xray --profile ingest up -d --force-recreate xray ingest
