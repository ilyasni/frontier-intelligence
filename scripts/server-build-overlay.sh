#!/usr/bin/env bash
set -euo pipefail

cd /opt/frontier-intelligence

DEFAULT_SERVICES=(worker admin mcp)

if [[ $# -gt 0 ]]; then
  SERVICES=("$@")
else
  SERVICES=("${DEFAULT_SERVICES[@]}")
fi

image_name_for_service() {
  case "$1" in
    worker) echo "frontier-intelligence-worker:latest" ;;
    admin) echo "frontier-intelligence-admin:latest" ;;
    mcp) echo "frontier-intelligence-mcp:latest" ;;
    *)
      echo "overlay rebuild is not supported for service: $1" >&2
      return 1
      ;;
  esac
}

write_overlay_dockerfile() {
  local service="$1"
  local base_tag="$2"
  local dockerfile="$3"

  case "$service" in
    worker)
      cat > "$dockerfile" <<EOF
FROM ${base_tag}
COPY shared/ /app/shared/
COPY worker/ /app/worker/
COPY admin/ /app/admin/
RUN mkdir -p /app/scripts
COPY scripts/enqueue_reindex_enriched_posts.py /app/scripts/enqueue_reindex_enriched_posts.py
ENV PYTHONPATH=/app
EOF
      ;;
    admin)
      cat > "$dockerfile" <<EOF
FROM ${base_tag}
COPY shared/ /app/shared/
COPY admin/ /app/admin/
COPY worker/ /app/worker/
COPY config/ /app/config/
COPY scripts/ /app/scripts/
ENV PYTHONPATH=/app
EOF
      ;;
    mcp)
      cat > "$dockerfile" <<EOF
FROM ${base_tag}
COPY shared/ /app/shared/
COPY worker/ /app/worker/
COPY admin/ /app/admin/
COPY mcp/ /app/mcp/
ENV PYTHONPATH=/app
EOF
      ;;
    *)
      echo "overlay dockerfile is not supported for service: $service" >&2
      return 1
      ;;
  esac
}

timestamp="$(date +%Y%m%d%H%M%S)"

for service in "${SERVICES[@]}"; do
  image_name="$(image_name_for_service "$service")"
  if ! docker image inspect "$image_name" >/dev/null 2>&1; then
    echo "overlay rebuild requires an existing local image: $image_name" >&2
    exit 1
  fi

  backup_tag="${image_name%:latest}:overlay-base-${timestamp}"
  docker tag "$image_name" "$backup_tag"

  dockerfile="/tmp/frontier-overlay-${service}.Dockerfile"
  write_overlay_dockerfile "$service" "$backup_tag" "$dockerfile"

  echo "overlay rebuild: $service from $backup_tag"
  docker build -f "$dockerfile" -t "$image_name" .
done

echo "overlay rebuild OK for: ${SERVICES[*]}"
