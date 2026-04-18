#!/usr/bin/env bash
# Apply a non-secret runtime overlay on the server without modifying .env.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/server-apply-runtime-mode.sh <mode> [--base-env .env] [--dry-run]

Modes:
  full-vision      GigaChat Vision + optional PaddleOCR, Pro/Max fallbacks enabled
  no-vision        Text pipeline only, Pro/Max fallbacks enabled
  gigachat-2-only  Text pipeline only, all chat tasks on GigaChat-2, no Pro/Max escalation

Aliases:
  full, vision, text-only, economy, giga-only
USAGE
}

MODE=""
BASE_ENV=".env"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-env)
      shift
      BASE_ENV="${1:-}"
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$MODE" ]]; then
        MODE="$1"
      else
        echo "Unexpected argument: $1" >&2
        usage
        exit 2
      fi
      ;;
  esac
  shift
done

case "$MODE" in
  full|vision|full-vision)
    MODE="full-vision"
    OVERLAY_ENV=".env.mode.full-vision.example"
    ;;
  no-vision|text-only)
    MODE="no-vision"
    OVERLAY_ENV=".env.mode.no-vision.example"
    ;;
  gigachat-2-only|economy|giga-only)
    MODE="gigachat-2-only"
    OVERLAY_ENV=".env.mode.gigachat-2-only.example"
    ;;
  "")
    echo "Mode is required." >&2
    usage
    exit 2
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    usage
    exit 2
    ;;
esac

load_env_file() {
  local file="$1"
  local line key value
  if [[ ! -f "$file" ]]; then
    echo "Env file not found: $file" >&2
    exit 1
  fi

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" == *"="* ]] || continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key//[[:space:]]/}"
    if [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      export "$key=$value"
    fi
  done < "$file"
}

require_env() {
  local missing=()
  local name
  for name in "$@"; do
    if [[ -z "${!name:-}" ]]; then
      missing+=("$name")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing required env values: ${missing[*]}" >&2
    exit 1
  fi
}

load_env_file "$BASE_ENV"
load_env_file "$OVERLAY_ENV"

require_env POSTGRES_PASSWORD NEO4J_PASSWORD GIGACHAT_CREDENTIALS

profiles=(--profile core --profile xray --profile worker --profile mcp --profile admin)
services=(gpt2giga-proxy worker mcp admin)

if [[ "$MODE" == "full-vision" ]]; then
  profiles+=(--profile paddleocr)
  services+=(paddleocr)
fi

echo "runtime_mode=${FRONTIER_RUNTIME_MODE:-custom}"
echo "vision_enabled=${VISION_ENABLED:-}"
echo "gpt2giga_enable_images=${GPT2GIGA_ENABLE_IMAGES:-}"
echo "relevance_model=${GIGACHAT_MODEL_RELEVANCE:-${GIGACHAT_MODEL_LITE:-}}"
echo "concepts_model=${GIGACHAT_MODEL_CONCEPTS:-${GIGACHAT_MODEL_LITE:-}}"
echo "valence_model=${GIGACHAT_MODEL_VALENCE:-${GIGACHAT_MODEL_LITE:-}}"
echo "mcp_synthesis_model=${GIGACHAT_MODEL_MCP_SYNTHESIS:-${GIGACHAT_MODEL_LITE:-}}"
echo "pro_route=${GIGACHAT_MODEL_PRO:-}"
echo "max_route=${GIGACHAT_MODEL_MAX:-}"
echo "escalation_enabled=${GIGACHAT_ESCALATION_ENABLED:-}"

cmd=(docker compose "${profiles[@]}" up -d --force-recreate "${services[@]}")
echo "${cmd[*]}"

if [[ "$DRY_RUN" == "1" ]]; then
  exit 0
fi

"${cmd[@]}"
