#!/usr/bin/env bash
# Р—Р°РїСѓСЃРєР°С‚СЊ РќРђ РЎР•Р Р’Р•Р Р• РёР· РєРѕСЂРЅСЏ СЂРµРїРѕР·РёС‚РѕСЂРёСЏ: bash scripts/server-apply-sql-migrations.sh
# РџСЂРёРјРµРЅСЏРµС‚ storage/postgres/migrations/*.sql. РЎРЅР°С‡Р°Р»Р° docker compose exec; РїСЂРё СЃР±РѕРµ (AppArmor Рё С‚.Рї.)
# вЂ” psql РёР· РѕС‚РґРµР»СЊРЅРѕРіРѕ РєРѕРЅС‚РµР№РЅРµСЂР° СЃ --network container:<postgres>.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose --profile core)

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

PG_USER="${POSTGRES_USER:-frontier}"
PG_DB="${POSTGRES_DB:-frontier}"
PG_PASS="${POSTGRES_PASSWORD:-}"

apply_sql_file() {
  local f="$1"
  echo "==> $f"
  set +e
  cat "$f" | "${COMPOSE[@]}" exec -T postgres \
    sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -f -'
  local st=$?
  set -e
  if [ "$st" -eq 0 ]; then
    return 0
  fi

  echo "WARN: compose exec failed (exit $st), trying docker run + network container:postgres"
  local pg_cid
  pg_cid="$("${COMPOSE[@]}" ps -q postgres 2>/dev/null | head -1)"
  if [ -z "$pg_cid" ]; then
    echo "ERROR: postgres container not found"
    return 1
  fi
  if [ -z "$PG_PASS" ]; then
    echo "ERROR: POSTGRES_PASSWORD not set (need .env for fallback)"
    return 1
  fi
  cat "$f" | docker run --rm -i \
    --network "container:${pg_cid}" \
    -e "PGPASSWORD=${PG_PASS}" \
    postgres:16-alpine \
    psql -h 127.0.0.1 -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -f -
}

shopt -s nullglob
for f in "$ROOT/storage/postgres/migrations"/*.sql; do
  apply_sql_file "$f"
done

echo "==> restart services"
"${COMPOSE[@]}" restart worker ingest crawl4ai admin 2>/dev/null || true

echo "OK"
