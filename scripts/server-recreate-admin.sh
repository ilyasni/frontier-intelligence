#!/usr/bin/env bash
set -euo pipefail
cd /opt/frontier-intelligence
docker compose --profile core up -d --no-deps --force-recreate admin
echo OK
