#!/usr/bin/env bash
set -euo pipefail

SOURCE_IMAGE="${FRONTIER_PYTHON_BASE_SOURCE:-python:3.11-slim}"
MIRROR_IMAGE="${FRONTIER_PYTHON_BASE_MIRROR:-frontier/python-base:3.11-slim}"
MAX_ATTEMPTS="${BASE_IMAGE_PULL_RETRIES:-3}"
SEED_IMAGES=(
  ${FRONTIER_PYTHON_BASE_SEED_IMAGES:-frontier-intelligence-admin:latest frontier-intelligence-mcp:latest frontier-intelligence-worker:latest}
)

image_exists() {
  docker image inspect "$1" >/dev/null 2>&1
}

tag_source_to_mirror() {
  local source_image="$1"
  local mirror_image="$2"

  docker tag "$source_image" "$mirror_image"
}

pull_source_with_retry() {
  local source_image="$1"
  local attempt=1

  while (( attempt <= MAX_ATTEMPTS )); do
    echo "pulling python base image: $source_image (attempt $attempt/$MAX_ATTEMPTS)" >&2
    if docker pull "$source_image" >/dev/null; then
      return 0
    fi

    if (( attempt == MAX_ATTEMPTS )); then
      return 1
    fi

    sleep $(( attempt * 5 ))
    attempt=$(( attempt + 1 ))
  done
}

tag_first_seed_image() {
  local mirror_image="$1"
  local seed_image

  for seed_image in "${SEED_IMAGES[@]}"; do
    if image_exists "$seed_image"; then
      echo "tagging local python base mirror from project image: $seed_image -> $mirror_image" >&2
      docker tag "$seed_image" "$mirror_image"
      return 0
    fi
  done

  return 1
}

if image_exists "$MIRROR_IMAGE"; then
  :
elif image_exists "$SOURCE_IMAGE"; then
  echo "tagging local python base mirror from cached image: $SOURCE_IMAGE -> $MIRROR_IMAGE" >&2
  tag_source_to_mirror "$SOURCE_IMAGE" "$MIRROR_IMAGE"
elif tag_first_seed_image "$MIRROR_IMAGE"; then
  :
elif pull_source_with_retry "$SOURCE_IMAGE"; then
  echo "tagging local python base mirror from freshly pulled image: $SOURCE_IMAGE -> $MIRROR_IMAGE" >&2
  tag_source_to_mirror "$SOURCE_IMAGE" "$MIRROR_IMAGE"
else
  echo "python base pull failed; falling back to direct source reference: $SOURCE_IMAGE" >&2
  printf 'export PYTHON_BASE_IMAGE=%q\n' "$SOURCE_IMAGE"
  exit 0
fi

printf 'export PYTHON_BASE_IMAGE=%q\n' "$MIRROR_IMAGE"
