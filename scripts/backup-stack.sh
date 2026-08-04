#!/usr/bin/env bash
# Frontier Intelligence — резервное копирование стека (запускать на сервере).
#   Postgres  — pg_dump custom-format (консистентный логический дамп)
#   Qdrant    — snapshot REST API по каждой коллекции (консистентно)
#   Neo4j     — best-effort tar тома (Community не умеет онлайн-дамп; граф восстановим из PG+Qdrant)
#   Секреты   — tar .env, sessions/, config/
#   Выгрузка  — в S3/Cloud.ru через worker-образ (boto3 + S3_* креды из .env)
# Каждый шаг независим: падение одного не отменяет остальные. Значения секретов не печатаются.
#
# Запуск вручную:  bash /opt/frontier-intelligence/scripts/backup-stack.sh
# Cron (пример):   30 3 * * *  /opt/frontier-intelligence/scripts/backup-stack.sh

set -uo pipefail

ROOT="${FRONTIER_ROOT:-/opt/frontier-intelligence}"
# Бэкапы: in-repo (ilyasni владеет; /opt root-only). Исключены из build-context (.dockerignore) и rsync (.rsync-exclude).
BACKUP_ROOT="${FRONTIER_BACKUP_ROOT:-$ROOT/backups}"
TS="$(date +%Y-%m-%d_%H%M%S)"
DAY="$(date +%Y-%m-%d)"
DEST="$BACKUP_ROOT/$DAY"
LOG="$BACKUP_ROOT/backup.log"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-7}"
QDRANT_URL="http://127.0.0.1:6333"
# helper-образ для tar тома Neo4j: берём уже присутствующий локально (Docker Hub с сервера недоступен)
HELPER_IMG="${BACKUP_HELPER_IMG:-redis:7-alpine}"
FAIL=0

mkdir -p "$DEST"
# лог в файл + stdout (для cron-письма при ошибке)
exec > >(tee -a "$LOG") 2>&1
echo "=== backup start $TS -> $DEST ==="

# --- .env: S3-креды + (опционально) qdrant api-key. Значения не печатаем. ---
if [ -f "$ROOT/.env" ]; then
  set -a; . "$ROOT/.env"; set +a
fi
QDRANT_HDR=()
if [ -n "${QDRANT__SERVICE__API_KEY:-}" ]; then
  QDRANT_HDR=(-H "api-key: ${QDRANT__SERVICE__API_KEY}")
fi

step() { echo "--- $1 ---"; }
mark_fail() { echo "!!! FAILED: $1"; FAIL=1; }

# --- 1. PostgreSQL (pg_dump custom-format) ---
step "postgres"
if docker exec frontier-intelligence-postgres-1 sh -lc \
      'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "$DEST/postgres.dump" 2>>"$LOG" \
   && [ -s "$DEST/postgres.dump" ]; then
  echo "postgres.dump: $(du -h "$DEST/postgres.dump" | cut -f1)"
  # Проверка читаемости архива. Непустой файл ничего не доказывает: оборванный
  # pg_dump оставляет ровно такой же непустой файл, и до 04.08.2026 такой дамп
  # уехал бы в S3 как успешный. pg_restore --list разбирает оглавление целиком,
  # то есть ловит обрыв и порчу, и при этом ничего не восстанавливает.
  PG_OBJECTS="$(docker exec -i frontier-intelligence-postgres-1 \
      pg_restore --list /dev/stdin < "$DEST/postgres.dump" 2>>"$LOG" \
      | grep -cE '^[0-9]+;')"
  if [ "${PG_OBJECTS:-0}" -gt 0 ]; then
    echo "postgres.dump: оглавление читается, объектов $PG_OBJECTS"
  else
    mark_fail "postgres dump verify (pg_restore --list не дал объектов)"
  fi
else
  mark_fail "postgres pg_dump"
  rm -f "$DEST/postgres.dump"
fi

# --- 2. Qdrant (snapshot API по каждой коллекции) ---
step "qdrant"
# Бэкапим только «живые» коллекции. Базовая коллекция, вытесненная
# версионированной (frontier_docs -> frontier_docs__embeddingsgigar__dense_2560,
# на которую указывает алиас frontier_docs_active), — исторический остаток: в неё
# не пишут, но её снапшот стоил 641 MB каждую ночь. Вместе с ростом основного
# снапшота это упирало бакет в квоту, и не доезжал как раз самый важный файл.
#
# Правило самоподдерживающееся: пропускаем ТОЛЬКО базу, у которой есть
# алиасированный потомок с префиксом "<база>__". Коллекцию без алиаса и без
# потомка бэкапим — молча терять данные хуже, чем потратить место.
# BACKUP_QDRANT_ALL=1 отключает фильтр вытеснения целиком (на случай полного DR).
#
# Отдельно — список исключений QDRANT_BACKUP_EXCLUDE. Он НЕ про место, а про то,
# что вообще не должно покидать хост. По умолчанию туда попадает личный корпус
# автора (задача B): коллекция создаётся в init_storage.py безусловно, ещё до
# решения включать вторую ось, и правило «нет алиаса и нет потомка → бэкапим»
# аккуратно уносит её в общий S3-бакет. Текста чанков в payload нет, но есть
# path/heading/project по приватным репозиториям и неопубликованным черновикам
# плюс сами векторы. Исключение сильнее BACKUP_QDRANT_ALL: полный DR — это про
# восстановимость стека, а корпус восстанавливается прогоном index_own_corpus.py
# с машины автора. Отключается только явно: QDRANT_BACKUP_EXCLUDE= (пустое значение).
# Совпадение по точному имени и по версионированным потомкам "<имя>__...".
QDRANT_BACKUP_EXCLUDE="${QDRANT_BACKUP_EXCLUDE-${QDRANT_OWN_CORPUS_COLLECTION:-own_corpus}}"
COLLS_JSON="$(curl -sS -m 10 "${QDRANT_HDR[@]}" "$QDRANT_URL/collections" || true)"
ALIASES_JSON="$(curl -sS -m 10 "${QDRANT_HDR[@]}" "$QDRANT_URL/aliases" || true)"
QDRANT_SEL="$(COLLS_JSON="$COLLS_JSON" ALIASES_JSON="$ALIASES_JSON" \
         QDRANT_BACKUP_EXCLUDE="$QDRANT_BACKUP_EXCLUDE" \
         BACKUP_QDRANT_ALL="${BACKUP_QDRANT_ALL:-0}" python3 -c '
import json, os, sys

try:
    colls = [c["name"] for c in json.loads(os.environ["COLLS_JSON"])["result"]["collections"]]
    targets = {a["collection_name"] for a in json.loads(os.environ["ALIASES_JSON"])["result"]["aliases"]}
except Exception:
    sys.exit(1)

patterns = [p for p in os.environ.get("QDRANT_BACKUP_EXCLUDE", "").replace(",", " ").split() if p]


def is_excluded(name):
    return any(name == p or name.startswith(p + "__") for p in patterns)


keep, skipped, excluded = [], [], []
for name in colls:
    if is_excluded(name):
        excluded.append(name)
    elif os.environ.get("BACKUP_QDRANT_ALL") == "1" or name in targets:
        keep.append(name)
    elif any(t.startswith(name + "__") for t in targets):
        skipped.append(name)
    else:
        keep.append(name)

for name in skipped:
    print("qdrant: пропуск вытесненной коллекции " + name, file=sys.stderr)
for name in excluded:
    print("qdrant: исключено из выгрузки (QDRANT_BACKUP_EXCLUDE) " + name, file=sys.stderr)
print(" ".join(keep))
print(" ".join(skipped + excluded))
' || true)"
# Вторая строка — всё, чей локальный снапшот подлежит удалению из каталога выгрузки:
# и вытесненные коллекции, и исключённые.
DEAD_COLLS="$(printf '%s\n' "$QDRANT_SEL" | tail -1)"
COLLS="$(printf '%s\n' "$QDRANT_SEL" | head -1)"

# Убрать снапшоты вытесненных и исключённых коллекций из каталога выгрузки. Без
# этого пропуск на шаге снапшота ничего не даёт: backup_s3_upload.py выгружает
# КАТАЛОГ целиком, поэтому файл, оставшийся от прогона до появления фильтра (или от
# другого прогона в тот же день — каталог именован по дате), всё равно уезжает
# в S3. Ровно так 02.08.2026 обратно уехали 611 MiB, только что удалённые.
for c in $DEAD_COLLS; do
  if [ -f "$DEST/qdrant_${c}.snapshot" ]; then
    echo "qdrant: удаляю локальный снапшот qdrant_${c}.snapshot (коллекция не подлежит выгрузке)"
    rm -f "$DEST/qdrant_${c}.snapshot"
  fi
done

if [ -z "$COLLS" ]; then
  mark_fail "qdrant list collections"
else
  for c in $COLLS; do
    snap="$(curl -sS -m 600 -X POST "${QDRANT_HDR[@]}" "$QDRANT_URL/collections/$c/snapshots" \
            | grep -oE '"name":"[^"]+"' | head -1 | sed 's/"name":"//; s/"$//')"
    if [ -n "$snap" ] \
       && curl -sS -m 900 "${QDRANT_HDR[@]}" "$QDRANT_URL/collections/$c/snapshots/$snap" \
            --output "$DEST/qdrant_${c}.snapshot" \
       && [ -s "$DEST/qdrant_${c}.snapshot" ]; then
      echo "qdrant_${c}.snapshot: $(du -h "$DEST/qdrant_${c}.snapshot" | cut -f1)"
      curl -sS -m 60 -X DELETE "${QDRANT_HDR[@]}" "$QDRANT_URL/collections/$c/snapshots/$snap" >/dev/null 2>&1
    else
      mark_fail "qdrant snapshot $c"
    fi
  done
fi

# --- 3. Neo4j (best-effort tar тома; Community без онлайн-дампа) ---
step "neo4j"
if docker run --rm \
      -v frontier-intelligence_neo4j_data:/data:ro \
      -v "$DEST":/backup \
      "$HELPER_IMG" sh -c 'tar czf /backup/neo4j_data.tar.gz -C /data .' 2>>"$LOG" \
   && [ -s "$DEST/neo4j_data.tar.gz" ]; then
  echo "neo4j_data.tar.gz: $(du -h "$DEST/neo4j_data.tar.gz" | cut -f1) (crash-consistent, restore прогонит recovery)"
else
  mark_fail "neo4j volume tar"
fi

# --- 4. Секреты и конфиг ---
step "secrets/config"
if tar czf "$DEST/secrets_config.tar.gz" -C "$ROOT" \
      $( [ -f "$ROOT/.env" ] && echo .env ) \
      $( [ -d "$ROOT/sessions" ] && echo sessions ) \
      $( [ -d "$ROOT/config" ] && echo config ) 2>>"$LOG" \
   && [ -s "$DEST/secrets_config.tar.gz" ]; then
  chmod 600 "$DEST/secrets_config.tar.gz"
  echo "secrets_config.tar.gz: $(du -h "$DEST/secrets_config.tar.gz" | cut -f1)"
else
  mark_fail "secrets/config tar"
fi

# манифест
{ echo "backup $TS"; ls -la "$DEST"; } > "$DEST/MANIFEST.txt"

# --- 5. Выгрузка в S3/Cloud.ru через worker-образ (boto3) ---
step "s3 upload"
if [ -n "${S3_BUCKET_NAME:-}" ] && [ -n "${S3_ENDPOINT_URL:-}" ]; then
  WORKER_IMG="$(docker inspect --format '{{.Config.Image}}' frontier-intelligence-worker-1 2>/dev/null)"
  if [ -n "$WORKER_IMG" ] && docker run --rm \
        --env-file "$ROOT/.env" \
        -v "$ROOT/scripts:/scripts:ro" \
        -v "$DEST:/backup:ro" \
        "$WORKER_IMG" python /scripts/backup_s3_upload.py /backup "backups/$DAY"; then
    echo "s3 upload ok -> s3://$S3_BUCKET_NAME/backups/$DAY/"
  else
    mark_fail "s3 upload"
  fi
else
  echo "S3_BUCKET_NAME/S3_ENDPOINT_URL не заданы — пропускаю выгрузку (локальная копия сохранена)"
fi

# --- 6. Ретеншн локальных копий ---
step "retention (${RETENTION_DAYS}d)"
find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime +"$RETENTION_DAYS" -exec rm -rf {} \; 2>/dev/null

if [ "$FAIL" -eq 0 ]; then
  echo "=== backup OK $TS ==="
else
  echo "=== backup FINISHED WITH ERRORS $TS ==="
fi
exit "$FAIL"
