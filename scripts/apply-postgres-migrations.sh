#!/usr/bin/env bash
# РџСЂРёРјРµРЅРёС‚СЊ РІСЃРµ .sql РёР· storage/postgres/migrations РїРѕ РїРѕСЂСЏРґРєСѓ (СЃРµСЂРІРµСЂ / Linux).
# РўСЂРµР±СѓРµС‚СЃСЏ psql Рё РїРµСЂРµРјРµРЅРЅР°СЏ DATABASE_URL (postgresql://..., Р±РµР· +asyncpg).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS="$ROOT/storage/postgres/migrations"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "ERROR: Р·Р°РґР°Р№С‚Рµ DATABASE_URL (postgresql://user:pass@host:5432/db)" >&2
  exit 1
fi

# asyncpg DSN РЅРµ РїРѕРґС…РѕРґРёС‚ РґР»СЏ psql
PSQL_URL="${DATABASE_URL//+asyncpg/}"

shopt -s nullglob
for f in "$MIGRATIONS"/*.sql; do
  echo "==> $f"
  psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$f"
done
echo "Migrations OK"
