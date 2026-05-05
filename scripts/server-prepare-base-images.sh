#!/usr/bin/env bash
set -euo pipefail

cd /opt/frontier-intelligence

DEFAULT_SERVICES=(gpt2giga-proxy worker crawl4ai ingest admin mcp paddleocr)

if [[ $# -gt 0 ]]; then
  SERVICES=("$@")
else
  SERVICES=("${DEFAULT_SERVICES[@]}")
fi

dockerfile_for_service() {
  case "$1" in
    admin) echo "admin/Dockerfile" ;;
    crawl4ai) echo "crawl4ai/Dockerfile" ;;
    gpt2giga-proxy) echo "gpt2giga-proxy/Dockerfile" ;;
    ingest) echo "ingest/Dockerfile" ;;
    mcp) echo "mcp/Dockerfile" ;;
    mcp-gateway) echo "mcp/Dockerfile.gateway" ;;
    paddleocr) echo "services/paddleocr/Dockerfile" ;;
    worker) echo "worker/Dockerfile" ;;
    xray) echo "services/xray/Dockerfile" ;;
    *)
      echo "unknown service for base image preparation: $1" >&2
      return 1
      ;;
  esac
}

collect_base_images() {
  local dockerfile="$1"
  awk '
    toupper($1) == "FROM" {
      print $2
    }
  ' "$dockerfile"
}

resolve_image_ref() {
  local image_ref="$1"

  if [[ "$image_ref" =~ ^\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]+))?\}$ ]]; then
    local var_name="${BASH_REMATCH[1]}"
    local default_value="${BASH_REMATCH[3]:-}"
    local resolved="${!var_name:-$default_value}"
    if [[ -z "$resolved" ]]; then
      echo "unable to resolve image ref: $image_ref" >&2
      return 1
    fi
    printf '%s\n' "$resolved"
    return 0
  fi

  printf '%s\n' "$image_ref"
}

pull_with_retry() {
  local image="$1"
  local max_attempts="${BASE_IMAGE_PULL_RETRIES:-3}"
  local attempt=1

  if docker image inspect "$image" >/dev/null 2>&1; then
    echo "base image present: $image"
    return 0
  fi

  while (( attempt <= max_attempts )); do
    echo "pulling base image: $image (attempt $attempt/$max_attempts)"
    if docker pull "$image"; then
      return 0
    fi

    if (( attempt == max_attempts )); then
      echo "failed to pull base image after $max_attempts attempts: $image" >&2
      return 1
    fi

    sleep $(( attempt * 5 ))
    attempt=$(( attempt + 1 ))
  done
}

declare -A seen_images=()

for service in "${SERVICES[@]}"; do
  dockerfile="$(dockerfile_for_service "$service")"
  while IFS= read -r image; do
    [[ -z "$image" ]] && continue
    image="$(resolve_image_ref "$image")"
    if [[ -z "${seen_images[$image]+x}" ]]; then
      seen_images["$image"]=1
      pull_with_retry "$image"
    fi
  done < <(collect_base_images "$dockerfile")
done

echo "base image preparation OK"
