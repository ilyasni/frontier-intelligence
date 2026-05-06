#!/usr/bin/env bash
set -euo pipefail

cd /opt/frontier-intelligence

docker compose \
  --profile core --profile worker --profile xray \
  exec -T worker \
  python scripts/qdrant_alias_cutover.py "$@"
