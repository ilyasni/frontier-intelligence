#!/usr/bin/env bash
# Р”РёР°РіРЅРѕСЃС‚РёРєР° vision-РїР°Р№РїР»Р°Р№РЅР° РЅР° СЃРµСЂРІРµСЂРµ (Redis stream + Р»РѕРіРё worker)
set -euo pipefail
WORKER="${WORKER_CONTAINER:-frontier-intelligence-worker-1}"

echo "=== worker: СЃС‚СЂРѕРєРё СЃ vision (РїРѕСЃР»РµРґРЅРёРµ 50) ==="
docker logs "$WORKER" 2>&1 | grep -i vision | tail -50 || true

echo ""
echo "=== Redis stream:posts:vision (sync redis РёР· РѕР±СЂР°Р·Р° worker) ==="
if docker exec "$WORKER" true 2>/dev/null; then
  docker exec "$WORKER" python3 <<'PY'
import os
import redis

url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
r = redis.Redis.from_url(url, decode_responses=True)
key = "stream:posts:vision"
if not r.exists(key):
    print("stream РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ (XLEN=0)")
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
  echo "(docker exec РЅРµРґРѕСЃС‚СѓРїРµРЅ вЂ” СЃРј. XLEN/XINFO РІСЂСѓС‡РЅСѓСЋ: redis-cli РЅР° С…РѕСЃС‚Рµ)"
fi

echo ""
echo "=== PADDLEOCR_URL РІ worker (РїСѓСЃС‚Рѕ = OCR С€Р°Рі РїСЂРѕРїСѓСЃРєР°РµС‚СЃСЏ РјРѕР»С‡Р°?) ==="
docker exec "$WORKER" sh -c 'echo "PADDLEOCR_URL=${PADDLEOCR_URL:-}"'

echo "=== OK (СЃРєСЂРёРїС‚ Р·Р°РІРµСЂС€С‘РЅ) ==="
