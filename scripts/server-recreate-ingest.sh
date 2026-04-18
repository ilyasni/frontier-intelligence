#!/usr/bin/env bash
set -euo pipefail
cd /opt/frontier-intelligence
exec docker compose --profile core --profile xray --profile ingest up -d --force-recreate xray ingest
