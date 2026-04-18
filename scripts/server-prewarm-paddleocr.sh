#!/usr/bin/env bash
# РџСЂРѕРІРµСЂРєР°/РїСЂРѕРіСЂРµРІ PaddleOCR С‡РµСЂРµР· /readyz Рё СЏРІРЅС‹Р№ preload РІ РєРѕРЅС‚РµР№РЅРµСЂРµ.
set -euo pipefail

cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

echo "=== Recreate paddleocr ==="
docker compose --profile core --profile paddleocr up -d --force-recreate paddleocr

echo "=== Probe /readyz (expect 200 when local model dirs or cached models are available) ==="
curl -sS --max-time 15 -w "\nHTTP %{http_code}\n" "http://127.0.0.1:${PADDLEOCR_PORT:-8008}/readyz" || true

echo "=== If not ready, try in-container preload once ==="
docker compose --profile core --profile paddleocr exec -T paddleocr \
  python -c "from app.service import get_default_service; s = get_default_service(); s._ensure_loaded(); print({'ready': s.is_models_ready(), 'error': s.get_models_error()})" || true

echo "=== Probe /readyz again ==="
curl -sS --max-time 15 -w "\nHTTP %{http_code}\n" "http://127.0.0.1:${PADDLEOCR_PORT:-8008}/readyz" || true

echo "=== Note ==="
echo "If /readyz stays 503 or times out, preload local weights into paddleocr_models and set LOCAL_OCR_*_MODEL_DIR in .env."
