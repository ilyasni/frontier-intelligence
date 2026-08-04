#!/usr/bin/env bash
# Frontier Intelligence — восстановление стека из бэкапа.
#
# Обратная половина к scripts/backup-stack.sh, которой до 04.08.2026 не существовало
# вовсе: бэкапы снимались каждую ночь, но ни скрипта восстановления, ни раннбука,
# ни одной проверки дампа. Бэкап, из которого ни разу не восстанавливались, — это
# не бэкап, а надежда.
#
# Режимы (по возрастанию опасности):
#
#   verify [--date YYYY-MM-DD]
#       Ничего не запускает и не меняет. Проверяет целостность артефактов:
#       pg_restore --list по дампу, tar по qdrant-снапшотам, gzip по tar.gz.
#       Быстро, безопасно, годится для cron.
#
#   drill [--date YYYY-MM-DD]
#       Учение. Поднимает ВРЕМЕННЫЙ postgres на отдельном томе, разворачивает
#       в него дамп, сверяет контрольные числа с продовой базой и всё сносит.
#       Прод не трогается ни на одном шаге. Это единственный способ узнать,
#       что дамп разворачивается, до того как он понадобится всерьёз.
#
#   postgres | qdrant | neo4j | secrets   --confirm-production
#       Настоящее восстановление В ПРОД. Перезаписывает данные. Требует явного
#       флага; без него скрипт откажется работать.
#
#   fetch --date YYYY-MM-DD
#       Скачать бэкап из S3, если локальной копии уже нет (ретеншн 7 суток).
#
# Запуск: bash scripts/restore-stack.sh <режим> [опции]
# Про neo4j: файл tar.gz принадлежит root с правами 600 (его пишет контейнер),
# поэтому режимы, читающие его, требуют root — запускай через sudo.
set -uo pipefail

ROOT="${FRONTIER_ROOT:-/opt/frontier-intelligence}"
BACKUP_ROOT="${FRONTIER_BACKUP_ROOT:-$ROOT/backups}"
DRILL_PREFIX="fi-restore-drill"

MODE="${1:-}"; shift || true
DATE=""
DIR=""
CONFIRM=0
COLLECTION=""
OUT=""

while [ $# -gt 0 ]; do
  case "$1" in
    --date) DATE="${2:-}"; shift 2 ;;
    --dir) DIR="${2:-}"; shift 2 ;;
    --collection) COLLECTION="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --confirm-production) CONFIRM=1; shift ;;
    *) echo "неизвестный аргумент: $1"; exit 2 ;;
  esac
done

if [ -z "$DIR" ]; then
  if [ -z "$DATE" ]; then
    DATE="$(ls -d "$BACKUP_ROOT"/20*-*-* 2>/dev/null | sort | tail -1 | xargs -r basename)"
  fi
  DIR="$BACKUP_ROOT/$DATE"
fi

log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }
die() { log "ОСТАНОВ: $*"; exit 1; }

img_of() { docker inspect "frontier-intelligence-$1-1" --format '{{.Config.Image}}' 2>/dev/null; }

PG_IMG="$(img_of postgres)"; PG_IMG="${PG_IMG:-postgres:16-alpine}"
QD_IMG="$(img_of qdrant)";   QD_IMG="${QD_IMG:-qdrant/qdrant:v1.17.0}"

# ─────────────────────────────────────────────────────────────── verify

do_verify() {
  local rc=0
  [ -d "$DIR" ] || die "каталога бэкапа нет: $DIR"
  log "проверяю $DIR"

  if [ -f "$DIR/postgres.dump" ]; then
    local objs
    objs="$(docker run --rm -v "$DIR:/b:ro" --entrypoint pg_restore "$PG_IMG" \
              --list /b/postgres.dump 2>/dev/null | grep -cE '^[0-9]+;')"
    if [ "${objs:-0}" -gt 0 ]; then
      log "  postgres.dump: валидный custom-format, объектов $objs"
    else
      log "  postgres.dump: ЧИТАЕТСЯ КАК ПОВРЕЖДЁННЫЙ (pg_restore --list не дал объектов)"; rc=1
    fi
  else
    log "  postgres.dump: ОТСУТСТВУЕТ"; rc=1
  fi

  local snap found=0
  for snap in "$DIR"/qdrant_*.snapshot; do
    [ -e "$snap" ] || continue
    found=1
    # Снапшот Qdrant — обычный tar, целостность видно листингом.
    if tar tf "$snap" >/dev/null 2>&1; then
      log "  $(basename "$snap"): tar читается, $(du -h "$snap" | cut -f1)"
    else
      log "  $(basename "$snap"): TAR ПОВРЕЖДЁН"; rc=1
    fi
  done
  [ "$found" = 1 ] || { log "  qdrant-снапшотов НЕТ"; rc=1; }

  local tgz
  for tgz in neo4j_data.tar.gz secrets_config.tar.gz; do
    if [ -f "$DIR/$tgz" ]; then
      if [ -r "$DIR/$tgz" ]; then
        if gzip -t "$DIR/$tgz" 2>/dev/null; then
          log "  $tgz: gzip цел, $(du -h "$DIR/$tgz" | cut -f1)"
        else
          log "  $tgz: GZIP ПОВРЕЖДЁН"; rc=1
        fi
      else
        # Не ошибка бэкапа: neo4j-архив пишет контейнер от root с правами 600.
        log "  $tgz: НЕТ ПРАВ НА ЧТЕНИЕ (файл root:600) — перезапусти через sudo"; rc=1
      fi
    else
      log "  $tgz: ОТСУТСТВУЕТ"; rc=1
    fi
  done

  [ "$rc" = 0 ] && log "verify: всё в порядке" || log "verify: ЕСТЬ ПРОБЛЕМЫ"
  return "$rc"
}

# ─────────────────────────────────────────────────────────────── drill

control_numbers_sql() {
  cat <<'SQL'
select 'tables'  as k, count(*)::text as v from information_schema.tables where table_schema='public'
union all select 'posts',            count(*)::text from posts
union all select 'post_enrichments', count(*)::text from post_enrichments
union all select 'indexing_status',  count(*)::text from indexing_status
union all select 'sources',          count(*)::text from sources
union all select 'semantic_clusters',count(*)::text from semantic_clusters
union all select 'trend_clusters',   count(*)::text from trend_clusters
union all select 'workspaces',       count(*)::text from workspaces
order by 1;
SQL
}

# Имена ГЛОБАЛЬНЫЕ, а не local: trap на EXIT срабатывает уже после выхода из
# функции, когда локальные переменные не существуют, и уборка падала на
# `cname: unbound variable` — то есть временный контейнер и том оставались жить.
DRILL_CNAME="${DRILL_PREFIX}-pg"
DRILL_VNAME="${DRILL_PREFIX}-vol"

cleanup_drill() {
  log "убираю за собой"
  docker rm -f "$DRILL_CNAME" >/dev/null 2>&1
  docker volume rm "$DRILL_VNAME" >/dev/null 2>&1
}

do_drill() {
  [ -f "$DIR/postgres.dump" ] || die "нет $DIR/postgres.dump"
  local cname="$DRILL_CNAME" vname="$DRILL_VNAME"
  local pw="drill$$"

  trap cleanup_drill EXIT

  cleanup_drill
  log "поднимаю временный postgres ($PG_IMG) на отдельном томе"
  docker volume create "$vname" >/dev/null
  docker run -d --name "$cname" \
    -e POSTGRES_PASSWORD="$pw" -e POSTGRES_USER=drill -e POSTGRES_DB=drill \
    -v "$vname:/var/lib/postgresql/data" \
    -v "$DIR:/b:ro" \
    "$PG_IMG" >/dev/null || die "не удалось запустить временный postgres"

  local i
  for i in $(seq 1 60); do
    docker exec "$cname" pg_isready -U drill -d drill >/dev/null 2>&1 && break
    sleep 2
  done
  docker exec "$cname" pg_isready -U drill -d drill >/dev/null 2>&1 \
    || die "временный postgres не поднялся"
  log "поднялся, разворачиваю дамп (это занимает минуты)"

  local t0 t1
  t0="$(date +%s)"
  # --no-owner/--no-acl: роли прода во временной базе не существуют.
  docker exec "$cname" pg_restore -U drill -d drill \
    --no-owner --no-acl --jobs 4 /b/postgres.dump >/tmp/drill-restore.log 2>&1
  local pg_rc=$?
  t1="$(date +%s)"
  log "pg_restore завершился с кодом $pg_rc за $((t1 - t0))с"
  if [ "$pg_rc" != 0 ]; then
    log "  последние строки вывода:"; tail -12 /tmp/drill-restore.log | sed 's/^/    /'
    log "  код НЕ ноль — но pg_restore ругается и на безобидное (отсутствующие роли);"
    log "  решают контрольные числа ниже, а не код возврата."
  fi

  log "сверяю контрольные числа"
  local prod_out drill_out
  prod_out="$(control_numbers_sql | docker exec -i frontier-intelligence-postgres-1 \
      sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -F"|"' 2>/dev/null)"
  drill_out="$(control_numbers_sql | docker exec -i "$cname" \
      psql -U drill -d drill -tA -F'|' 2>/dev/null)"

  # Критерий не «числа равны». Прод продолжает наполняться после снятия дампа,
  # поэтому по растущим таблицам расхождение — норма, и требовать равенства
  # значило бы проваливать каждое учение. Проверяем то, что действительно
  # должно выполняться:
  #   tables      — точное равенство (схема восстановилась целиком);
  #   растущие    — 0 < восстановлено <= прод (данные на месте и не из будущего);
  #   справочные  — точное равенство (workspaces/sources так быстро не меняются).
  local EXACT=" tables workspaces "
  printf '\n  %-20s %14s %14s   %s\n' "таблица" "прод" "восстановлено" "итог"
  local ok=1 key pv dv
  while IFS='|' read -r key pv; do
    [ -n "$key" ] || continue
    dv="$(printf '%s\n' "$drill_out" | awk -F'|' -v k="$key" '$1==k{print $2}')"
    local verdict
    if [ -z "$dv" ]; then
      verdict="НЕТ ТАБЛИЦЫ"; ok=0
    elif [[ "$EXACT" == *" $key "* ]]; then
      if [ "$pv" = "$dv" ]; then verdict="совпало"; else verdict="РАСХОЖДЕНИЕ"; ok=0; fi
    elif [ "$dv" -gt 0 ] 2>/dev/null && [ "$dv" -le "$pv" ] 2>/dev/null; then
      verdict="ок (+$((pv - dv)) после дампа)"
    elif [ "$pv" = "0" ] && [ "$dv" = "0" ]; then
      verdict="обе пусты"
    else
      verdict="НЕПРАВДОПОДОБНО"; ok=0
    fi
    printf '  %-20s %14s %14s   %s\n' "$key" "$pv" "${dv:-—}" "$verdict"
  done <<< "$prod_out"

  echo
  if [ "$ok" = 1 ]; then
    log "УЧЕНИЕ ПРОЙДЕНО: дамп разворачивается, схема полная, данные на месте"
    log "время восстановления postgres: $((t1 - t0))с на $(du -h "$DIR/postgres.dump" | cut -f1)"
    log "это RTO только для БД: qdrant и neo4j восстанавливаются отдельно"
  else
    log "УЧЕНИЕ ПРОВАЛЕНО: см. строки выше"
  fi
  [ "$ok" = 1 ]
}

# ─────────────────────────────────────────────────── восстановление в прод

require_confirm() {
  [ "$CONFIRM" = 1 ] || die "это перезапишет продовые данные. Добавь --confirm-production, если действительно нужно."
}

do_postgres() {
  require_confirm
  [ -f "$DIR/postgres.dump" ] || die "нет $DIR/postgres.dump"
  log "ВОССТАНОВЛЕНИЕ POSTGRES В ПРОД из $DIR"
  log "останови потребителей заранее: worker, ingest, admin, mcp, crawl4ai"
  docker exec -i frontier-intelligence-postgres-1 sh -lc \
    'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists --no-owner --no-acl --jobs 4' \
    < "$DIR/postgres.dump"
  log "готово, код $?"
}

do_qdrant() {
  require_confirm
  [ -n "$COLLECTION" ] || die "укажи --collection <имя>"
  local snap="$DIR/qdrant_${COLLECTION}.snapshot"
  [ -f "$snap" ] || die "нет снапшота $snap"
  log "ВОССТАНОВЛЕНИЕ КОЛЛЕКЦИИ $COLLECTION В ПРОД"
  curl -sS -X POST "http://127.0.0.1:6333/collections/${COLLECTION}/snapshots/upload?priority=snapshot" \
    -H 'Content-Type: multipart/form-data' -F "snapshot=@${snap}"
  echo
}

do_neo4j() {
  require_confirm
  local tgz="$DIR/neo4j_data.tar.gz"
  [ -r "$tgz" ] || die "нет прав на чтение $tgz (файл root:600) — запусти через sudo"
  log "ВОССТАНОВЛЕНИЕ NEO4J В ПРОД: останавливаю neo4j"
  docker stop frontier-intelligence-neo4j-1 >/dev/null
  log "очищаю том и распаковываю (архив crash-consistent, при старте пройдёт recovery)"
  docker run --rm -v frontier-intelligence_neo4j_data:/data -v "$DIR:/b:ro" \
    "$(img_of redis 2>/dev/null || echo redis:7-alpine)" \
    sh -c 'rm -rf /data/* && tar xzf /b/neo4j_data.tar.gz -C /data'
  docker start frontier-intelligence-neo4j-1 >/dev/null
  log "neo4j запущен, дай ему время на recovery"
}

do_secrets() {
  local tgz="$DIR/secrets_config.tar.gz"
  [ -f "$tgz" ] || die "нет $tgz"
  OUT="${OUT:-/tmp/fi-secrets-restore}"
  mkdir -p "$OUT"; chmod 700 "$OUT"
  tar xzf "$tgz" -C "$OUT"
  log "распаковано в $OUT (там .env, sessions/, config/ — не оставляй копию)"
  ls -la "$OUT"
}

do_fetch() {
  [ -n "$DATE" ] || die "укажи --date YYYY-MM-DD"
  local wimg; wimg="$(img_of worker)"
  [ -n "$wimg" ] || die "не нашёл образ worker для boto3"
  mkdir -p "$BACKUP_ROOT/$DATE"
  log "качаю s3://.../backups/$DATE в $BACKUP_ROOT/$DATE"
  docker run --rm --env-file "$ROOT/.env" \
    -v "$BACKUP_ROOT/$DATE:/out" "$wimg" python -c "
import os, boto3
s3 = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT_URL'],
                  aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
                  aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
                  region_name=os.environ.get('S3_REGION') or None)
b = os.environ['S3_BUCKET_NAME']; p = 'backups/${DATE}/'
for o in s3.list_objects_v2(Bucket=b, Prefix=p).get('Contents', []):
    name = o['Key'].split('/')[-1]
    print('качаю', name, o['Size'])
    s3.download_file(b, o['Key'], '/out/' + name)
print('готово')
"
}

case "$MODE" in
  verify)   do_verify ;;
  drill)    do_drill ;;
  postgres) do_postgres ;;
  qdrant)   do_qdrant ;;
  neo4j)    do_neo4j ;;
  secrets)  do_secrets ;;
  fetch)    do_fetch ;;
  *)
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
