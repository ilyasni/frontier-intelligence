#!/usr/bin/env bash
# РџСЂРѕРіСЂРµРІ fastembed/BM25 cache РґР»СЏ worker Рё mcp РІ shared volume.
set -euo pipefail

cd /opt/frontier-intelligence
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

SERVICES=("$@")
if [[ ${#SERVICES[@]} -eq 0 ]]; then
  SERVICES=(worker mcp)
fi

for service in "${SERVICES[@]}"; do
  case "$service" in
    worker)
      profiles=(--profile core --profile worker)
      ;;
    mcp)
      profiles=(--profile core --profile mcp)
      ;;
    *)
      echo "Unsupported service: $service" >&2
      exit 2
      ;;
  esac

  echo "=== Prewarm fastembed for $service ==="
  docker compose "${profiles[@]}" run --rm "$service" \
    python -c "from shared.qdrant_sparse import sparse_encode; import json; vec = sparse_encode('frontier intelligence warmup'); print(json.dumps({'ok': vec is not None, 'service': '$service'}))"
done

echo "=== done ==="
