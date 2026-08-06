#!/usr/bin/env bash
# Экспортирует состояние бэкапов и занятость S3-бакета в node_exporter textfile collector.
#
# Зачем. До 04.08.2026 у бэкапов не было ни одной метрики и ни одного алерта из 56:
# `grep -iE 'backup|s3|restore' prometheus/alerts.yml` не давал ничего. Если бы
# ночной прогон упал — на переполнении квоты, на недоступности S3, на чём угодно —
# узнать об этом было бы неоткуда: cron пишет только в backups/cron.log, который
# никто не читает. При этом квота была выбрана на 13.1 из ~15 GiB, то есть
# свободного места оставалось меньше одного суточного бэкапа.
#
# Метрики:
#   frontier_backup_last_success_timestamp_seconds  когда последний раз всё прошло
#   frontier_backup_last_run_timestamp_seconds      когда последний раз запускалось
#   frontier_backup_artifact_bytes{artifact=...}    размеры артефактов последнего дня
#   frontier_backup_local_days                      сколько суточных копий лежит локально
#   frontier_s3_bucket_bytes                        занято в бакете
#   frontier_s3_quota_bytes                         квота (из S3_QUOTA_BYTES, иначе 0)
#
# Опрос S3 стоит времени и запросов, поэтому он делается не на каждом прогоне,
# а не чаще раза в S3_POLL_MIN_INTERVAL секунд (по умолчанию 6 часов): предыдущее
# значение переиспользуется из кэша.
#
# Cron (через bash — sync-push снимает бит исполнения):
#   7 * * * * bash /opt/frontier-intelligence/scripts/export-backup-metrics.sh >/dev/null 2>&1

set -uo pipefail

PROJECT_DIR="${FRONTIER_ROOT:-/opt/frontier-intelligence}"
BACKUP_ROOT="${FRONTIER_BACKUP_ROOT:-$PROJECT_DIR/backups}"
OUT_DIR="$PROJECT_DIR/prometheus/textfile"
OUT="$OUT_DIR/frontier_backup.prom"
TMP="$OUT.$$"
CACHE="$PROJECT_DIR/runtime/s3-usage-cache"
S3_POLL_MIN_INTERVAL="${S3_POLL_MIN_INTERVAL:-21600}"

mkdir -p "$OUT_DIR" "$(dirname "$CACHE")"

# S3_* и S3_QUOTA_BYTES живут в .env.
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a; . "$PROJECT_DIR/.env"; set +a
fi

NOW="$(date +%s)"
LOG="$BACKUP_ROOT/backup.log"

# 2026-08-04_033001 -> unix time. Метка ставится локальным `date` в backup-stack.sh,
# поэтому и разбираем её как локальную, без -u: иначе метрика уехала бы на
# смещение часового пояса и «свежий» бэкап выглядел бы протухшим (или наоборот).
to_epoch() {
  local s="${1:-}"
  [ -n "$s" ] || { echo 0; return; }
  local d="${s%_*}" t="${s#*_}"
  t="$(printf '%s' "$t" | sed -E 's/^(..)(..)(..)$/\1:\2:\3/')"
  date -d "$d $t" +%s 2>/dev/null || echo 0
}

# ── когда бэкап последний раз запускался и последний раз прошёл целиком ──
LAST_RUN=0
LAST_OK=0
if [ -r "$LOG" ]; then
  # Строки вида "=== backup start 2026-08-04_033001 -> ..." и "=== backup OK 2026-08-04_033001 ==="
  LAST_RUN="$(to_epoch "$(grep -oE '=== backup start [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{6}' "$LOG" | tail -1 | awk '{print $4}')")"
  LAST_OK="$(to_epoch "$(grep -oE '=== backup OK [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{6}' "$LOG" | tail -1 | awk '{print $4}')")"
fi

# ── размеры артефактов последнего суточного каталога ──
LATEST_DIR="$(ls -d "$BACKUP_ROOT"/20*-*-* 2>/dev/null | sort | tail -1)"
LOCAL_DAYS="$(ls -d "$BACKUP_ROOT"/20*-*-* 2>/dev/null | wc -l | tr -d ' ')"

# ── занятость бакета: дорого, поэтому через кэш ──
S3_BYTES=0
S3_TS=0
if [ -r "$CACHE" ]; then
  # Без awk вообще. Две предыдущие редакции ломались на одном и том же —
  # на попытке пропустить 11-значное число через числовой формат awk:
  #   "$2+0"      → арифметика идёт через double и печатается как %.6g,
  #                 10366531632 превращалось в 1.03665e+10;
  #   printf "%d" → на этом хосте awk = mawk 1.3.4, а он печатает %d через
  #                 int32 и НАСЫЩАЕТ: 10717881069 становилось 2147483647
  #                 (ровно INT32_MAX). Замер 06.08.2026: экспортировалось
  #                 2147483647 при кэше 10717881069, ratio 0.133 вместо 0.665,
  #                 то есть FrontierS3QuotaHigh (>0.85) не могла сработать ни
  #                 при каком наполнении — а бакет за окно доходил до 13.2 ГиБ
  #                 (88% квоты) и об этом никто не узнал.
  # Кэш пишется этим же скриптом строкой "<epoch> <bytes>", поэтому читаем поля
  # как строки и валидируем — никакой конверсии, никакой разрядности.
  read -r S3_TS S3_BYTES _rest < "$CACHE" || true
  # \r отрезаем намеренно: кэш пишет этот же скрипт, но файл может приехать
  # с Windows (перенос каталога, ручная правка) — а с висящим \r проверка
  # «только цифры» ниже честно обнулила бы измеренное значение.
  S3_TS="${S3_TS%$'\r'}"
  S3_BYTES="${S3_BYTES%$'\r'}"
  case "${S3_TS:-}" in ''|*[!0-9]*) S3_TS=0 ;; esac
  case "${S3_BYTES:-}" in ''|*[!0-9]*) S3_BYTES=0 ;; esac
fi
if [ $((NOW - S3_TS)) -ge "$S3_POLL_MIN_INTERVAL" ]; then
  WORKER_IMG="$(docker inspect --format '{{.Config.Image}}' frontier-intelligence-worker-1 2>/dev/null)"
  if [ -n "$WORKER_IMG" ]; then
    fresh="$(docker run --rm --env-file "$PROJECT_DIR/.env" "$WORKER_IMG" python -c "
import os, boto3
from botocore.config import Config
try:
    s3 = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT_URL'],
        aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
        region_name=os.environ.get('S3_REGION') or None,
        config=Config(s3={'addressing_style': os.environ.get('S3_ADDRESSING_STYLE', 'path')}))
    total = 0
    token = None
    while True:
        kw = {'Bucket': os.environ['S3_BUCKET_NAME']}
        if token:
            kw['ContinuationToken'] = token
        r = s3.list_objects_v2(**kw)
        total += sum(o['Size'] for o in r.get('Contents', []))
        if not r.get('IsTruncated'):
            break
        token = r.get('NextContinuationToken')
    print(total)
except Exception:
    print('')
" 2>/dev/null | tr -d '\r\n ')"
    case "$fresh" in
      ''|*[!0-9]*) : ;;   # не удалось — оставляем прежнее значение из кэша
      *) S3_BYTES="$fresh"; S3_TS="$NOW"; printf '%s %s\n' "$NOW" "$S3_BYTES" > "$CACHE" ;;
    esac
  fi
fi

emit() {
  echo '# HELP frontier_backup_last_run_timestamp_seconds Время старта последнего прогона backup-stack.sh.'
  echo '# TYPE frontier_backup_last_run_timestamp_seconds gauge'
  echo "frontier_backup_last_run_timestamp_seconds $LAST_RUN"

  echo '# HELP frontier_backup_last_success_timestamp_seconds Время последнего прогона, завершившегося без ошибок.'
  echo '# TYPE frontier_backup_last_success_timestamp_seconds gauge'
  echo "frontier_backup_last_success_timestamp_seconds $LAST_OK"

  echo '# HELP frontier_backup_local_days Сколько суточных каталогов бэкапа лежит локально.'
  echo '# TYPE frontier_backup_local_days gauge'
  echo "frontier_backup_local_days ${LOCAL_DAYS:-0}"

  echo '# HELP frontier_backup_artifact_bytes Размер артефакта в последнем суточном бэкапе.'
  echo '# TYPE frontier_backup_artifact_bytes gauge'
  if [ -n "$LATEST_DIR" ] && [ -d "$LATEST_DIR" ]; then
    for f in "$LATEST_DIR"/*; do
      [ -f "$f" ] || continue
      name="$(basename "$f")"
      case "$name" in MANIFEST.txt) continue ;; esac
      size="$(stat -c %s "$f" 2>/dev/null || echo 0)"
      printf 'frontier_backup_artifact_bytes{artifact="%s"} %s\n' "$name" "$size"
    done
  fi

  echo '# HELP frontier_s3_bucket_bytes Занято в S3-бакете, байт.'
  echo '# TYPE frontier_s3_bucket_bytes gauge'
  echo "frontier_s3_bucket_bytes ${S3_BYTES:-0}"

  echo '# HELP frontier_s3_bucket_measured_timestamp_seconds Когда занятость бакета измерялась в последний раз.'
  echo '# TYPE frontier_s3_bucket_measured_timestamp_seconds gauge'
  echo "frontier_s3_bucket_measured_timestamp_seconds ${S3_TS:-0}"

  echo '# HELP frontier_s3_quota_bytes Квота бакета из S3_QUOTA_BYTES. 0 = не задана, алерты по заполнению отключены.'
  echo '# TYPE frontier_s3_quota_bytes gauge'
  echo "frontier_s3_quota_bytes ${S3_QUOTA_BYTES:-0}"
}

# Атомарно: textfile collector читает каталог на каждом скрейпе и на
# недописанном файле выдал бы parse error вместо метрик.
emit > "$TMP"
mv "$TMP" "$OUT"
chmod 0644 "$OUT"
