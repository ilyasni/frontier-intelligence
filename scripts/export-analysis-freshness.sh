#!/usr/bin/env bash
# Экспортирует свежесть аналитического слоя в node_exporter textfile collector.
#
# Зачем: провал scheduled clustering / signal-analysis логируется как УСПЕХ —
# error-хендлер пишет UPDATE cluster_runs в уже аборченной транзакции, падает сам,
# прогон навсегда остаётся в status='running', APScheduler рапортует "executed
# successfully". Поэтому алертить по статусу джоба бесполезно: надо смотреть,
# сдвинулись ли данные. 31.07–02.08.2026 аналитика стояла 54 часа незамеченной.
#
# Ставится в cron на хосте, например каждые 10 минут:
#   */10 * * * * /opt/frontier-intelligence/scripts/export-analysis-freshness.sh >/dev/null 2>&1

set -euo pipefail

PROJECT_DIR=/opt/frontier-intelligence
OUT_DIR="$PROJECT_DIR/prometheus/textfile"
OUT="$OUT_DIR/frontier_analysis.prom"
TMP="$OUT.$$"
PG_CONTAINER=frontier-intelligence-postgres-1

mkdir -p "$OUT_DIR"

emit() {
    echo '# HELP frontier_analysis_last_update_timestamp_seconds Unix time of the most recent analysis-layer row write.'
    echo '# TYPE frontier_analysis_last_update_timestamp_seconds gauge'

    docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
        -At -F'|' -c "
            SELECT 'emerging_signals', workspace_id, extract(epoch from max(updated_at))::bigint
              FROM emerging_signals GROUP BY workspace_id
            UNION ALL
            SELECT 'semantic_clusters', workspace_id, extract(epoch from max(updated_at))::bigint
              FROM semantic_clusters GROUP BY workspace_id
        " | while IFS='|' read -r tbl ws ts; do
        if [ -z "${ts:-}" ]; then
            continue
        fi
        printf 'frontier_analysis_last_update_timestamp_seconds{table="%s",workspace="%s"} %s\n' \
            "$tbl" "$ws" "$ts"
    done
}

# Пишем атомарно: textfile collector читает каталог на каждом скрейпе и на
# частично записанном файле выдал бы parse error вместо метрики.
emit > "$TMP"
mv "$TMP" "$OUT"
chmod 0644 "$OUT"
