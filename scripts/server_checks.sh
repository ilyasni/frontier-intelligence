#!/usr/bin/env bash
# РЎРјРѕРє-РїСЂРѕРІРµСЂРєРё РЅР° С…РѕСЃС‚Рµ СЃ Docker (Р·Р°РїСѓСЃРє: bash scripts/server_checks.sh).
set -euo pipefail

echo "=== PaddleOCR /healthz ==="
curl -sfS --max-time 10 "http://127.0.0.1:${PADDLEOCR_PORT:-8008}/healthz"
echo ""

echo "=== PaddleOCR /readyz (503 РґРѕ Р·Р°РіСЂСѓР·РєРё РІРµСЃРѕРІ вЂ” РЅРѕСЂРјР°; С‚Р°Р№РјР°СѓС‚ вЂ” РЅРµС‚) ==="
curl -sS --max-time 15 -w "\nHTTP %{http_code}\n" "http://127.0.0.1:${PADDLEOCR_PORT:-8008}/readyz" || true
echo ""

echo "=== PaddleOCR POST /v1/ocr/upload (1x1 PNG) ==="
PNG_B64="iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
echo "$PNG_B64" | base64 -d > /tmp/fi_ocr_probe.png
rm -f /tmp/fi_ocr_resp.json
code=$(curl -sS --max-time 120 -o /tmp/fi_ocr_resp.json -w "%{http_code}" \
  -F "file=@/tmp/fi_ocr_probe.png" \
  "http://127.0.0.1:${PADDLEOCR_PORT:-8008}/v1/ocr/upload" || true)
echo "HTTP ${code:-000}"
if [[ -s /tmp/fi_ocr_resp.json ]]; then
  head -c 500 /tmp/fi_ocr_resp.json
  echo ""
elif [[ "${code:-000}" == "000" ]]; then
  echo "(РїСѓСЃС‚РѕР№ РѕС‚РІРµС‚: С‚Р°Р№РјР°СѓС‚ РёР»Рё СЃРµС‚СЊ РґРѕ paddleocr)"
fi
echo ""

echo "=== Qdrant collection (frontier_docs) ==="
curl -sfS "http://127.0.0.1:${QDRANT_PORT:-6333}/collections/frontier_docs" | head -c 600
echo ""

echo "=== MCP /healthz ==="
curl -sfS "http://127.0.0.1:${MCP_PORT:-8100}/healthz" || curl -sfS "http://127.0.0.1:${MCP_PORT:-8100}/readyz"
echo ""

echo "=== Admin /api/health ==="
curl -sfS "http://127.0.0.1:${ADMIN_PORT:-8101}/api/health"
echo ""

echo "=== Redis stream:posts:parsed XLEN ==="
if ! cd /opt/frontier-intelligence 2>/dev/null; then
  echo "SKIP: /opt/frontier-intelligence not found"
else
  NET=$(docker network ls --format '{{.Name}}' | grep -E 'frontier.*frontier-net$' | head -1)
  if out=$(docker compose exec -T redis redis-cli XLEN stream:posts:parsed 2>&1) && [[ "$out" =~ ^[0-9]+$ ]]; then
    echo "$out"
  elif [[ -n "${NET:-}" ]]; then
    out=$(docker run --rm --network "$NET" redis:7-alpine redis-cli -h redis XLEN stream:posts:parsed 2>&1) || true
    echo "$out (ephemeral redis-cli вЂ” РѕР±С…РѕРґ AppArmor РЅР° docker exec)"
  else
    echo "РќРµ РЅР°Р№РґРµРЅР° СЃРµС‚СЊ frontier-net Рё exec redis РЅРµ СЃСЂР°Р±РѕС‚Р°Р»"
  fi
fi

echo "=== Recent InterfaceError (worker + crawl4ai, last 500 lines) ==="
if cd /opt/frontier-intelligence 2>/dev/null; then
  docker compose logs worker --tail 500 2>&1 | grep -i InterfaceError || echo "worker: none"
  docker compose logs crawl4ai --tail 500 2>&1 | grep -i InterfaceError || echo "crawl4ai: none"
fi

echo "=== done ==="
