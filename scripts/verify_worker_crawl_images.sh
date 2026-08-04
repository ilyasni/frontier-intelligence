#!/usr/bin/env bash
# Сверка: образы worker/crawl4ai vs дата файлов на диске (после rsync без rebuild образ «старый»).
set -euo pipefail
echo "=== docker images (worker, crawl4ai) ==="
docker images frontier-intelligence-worker --no-trunc --format '{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.CreatedSince}}' || true
docker images frontier-intelligence-crawl4ai --no-trunc --format '{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.CreatedSince}}' || true
echo ""
echo "=== containers (Image ID + Created) ==="
for c in frontier-intelligence-worker-1 frontier-intelligence-crawl4ai-1; do
  if docker inspect "$c" &>/dev/null; then
    docker inspect "$c" --format '{{.Name}}  image_id={{.Image}}  started={{.State.StartedAt}}'
  else
    echo "$c: not found"
  fi
done
echo ""
echo "=== image full ID + Created ==="
docker image inspect frontier-intelligence-worker --format 'worker  id={{.Id}}  created={{.Created}}' 2>/dev/null || echo "worker image: missing"
docker image inspect frontier-intelligence-crawl4ai --format 'crawl4ai  id={{.Id}}  created={{.Created}}' 2>/dev/null || echo "crawl4ai image: missing"
echo ""
echo "=== host files (mtime) — должны быть новее образа, если только rsync без rebuild ==="
for f in shared/sqlalchemy_pool.py worker/tasks/enrichment_task.py crawl4ai/crawl4ai_service.py; do
  p="/opt/frontier-intelligence/$f"
  if [[ -f "$p" ]]; then
    stat -c '%y  %n' "$p" 2>/dev/null || ls -la "$p"
  else
    echo "missing: $p"
  fi
done
echo ""
echo "=== вывод ==="
echo "Если mtime файлов на диске новее docker image Created — контейнер крутит старый слой (код в образе не обновлён до docker compose build + up)."
