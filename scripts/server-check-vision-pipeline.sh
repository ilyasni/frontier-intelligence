#!/usr/bin/env bash
# Диагностика vision-пайплайна на сервере (Redis stream + логи worker)
set -euo pipefail
WORKER="${WORKER_CONTAINER:-frontier-intelligence-worker-1}"

echo "=== worker: строки с vision (последние 50) ==="
docker logs "$WORKER" 2>&1 | grep -i vision | tail -50 || true

echo ""
echo "=== Redis stream:posts:vision (sync redis из образа worker) ==="
if docker exec "$WORKER" true 2>/dev/null; then
  docker exec "$WORKER" python3 <<'PY'
import os
import redis

url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
r = redis.Redis.from_url(url, decode_responses=True)
key = "stream:posts:vision"
if not r.exists(key):
    print("stream отсутствует (XLEN=0)")
else:
    print("XLEN", r.xlen(key))
    try:
        groups = r.xinfo_groups(key)
        for g in groups:
            name = g.get("name", g)
            pending = g.get("pending", "?")
            lag = g.get("lag", "?")
            last = g.get("last-delivered-id", "?")
            print(f"  group={name} pending={pending} lag={lag} last-delivered-id={last}")
    except Exception as e:
        print("xinfo_groups error:", e)
PY
else
  echo "(docker exec недоступен — см. XLEN/XINFO вручную: redis-cli на хосте)"
fi

echo ""
echo "=== PADDLEOCR_URL в worker (пусто = OCR шаг пропускается молча?) ==="
docker exec "$WORKER" sh -c 'echo "PADDLEOCR_URL=${PADDLEOCR_URL:-}"'

echo "=== OK (скрипт завершён) ==="
