#!/usr/bin/env bash
# Проверки paddleocr + worker + admin (запуск с сервера: bash scripts/server-pipeline-checks.sh)
set -euo pipefail
cd /opt/frontier-intelligence

echo "=== docker compose up paddleocr (recreate) ==="
docker compose --profile core --profile paddleocr up -d --force-recreate paddleocr

echo "=== wait + paddleocr healthz (при LOCAL_OCR_PRELOAD=true — дольше, качает модели с BCE) ==="
sleep 8
ok=0
for i in $(seq 1 48); do
  if out=$(curl -sS --connect-timeout 5 --max-time 15 http://127.0.0.1:8008/healthz 2>/dev/null) && echo "$out" | grep -q '"status"'; then
    echo "$out"
    ok=1
    break
  fi
  echo "  ... healthz попытка $i/48"
  sleep 5
done
if [ "$ok" != 1 ]; then
  echo "FAIL curl 8008 после ожидания"
  docker logs frontier-intelligence-paddleocr-1 2>&1 | tail -40 || true
  exit 1
fi

echo "=== docker compose up worker (recreate) ==="
docker compose --profile core --profile worker up -d --force-recreate worker

echo "=== admin health ==="
curl -sS --connect-timeout 10 http://127.0.0.1:8101/api/health && echo ""

echo "=== paddleocr container status ==="
docker inspect -f '{{.Name}} {{.State.Status}}' frontier-intelligence-paddleocr-1 2>/dev/null || true
docker inspect -f '{{.Name}} {{.State.Status}}' frontier-intelligence-worker-1 2>/dev/null || true

echo "=== OK ==="
