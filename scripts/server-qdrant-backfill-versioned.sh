#!/usr/bin/env bash
set -euo pipefail

cd /opt/frontier-intelligence

docker compose \
  --profile core --profile worker --profile xray \
  exec -T worker \
  python /app/scripts/qdrant_backfill_versioned.py "$@"
