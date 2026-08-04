#!/usr/bin/env bash
# Применить все .sql из storage/postgres/migrations по порядку (сервер / Linux).
# Требуется psql и переменная DATABASE_URL (postgresql://..., без +asyncpg).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS="$ROOT/storage/postgres/migrations"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "ERROR: задайте DATABASE_URL (postgresql://user:pass@host:5432/db)" >&2
  exit 1
fi

# asyncpg DSN не подходит для psql
PSQL_URL="${DATABASE_URL//+asyncpg/}"

shopt -s nullglob
for f in "$MIGRATIONS"/*.sql; do
  echo "==> $f"
  psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$f"
done
echo "Migrations OK"
