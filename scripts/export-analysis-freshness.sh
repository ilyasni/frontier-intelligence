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
        " </dev/null | while IFS='|' read -r tbl ws ts; do
        if [ -z "${ts:-}" ]; then
            continue
        fi
        printf 'frontier_analysis_last_update_timestamp_seconds{table="%s",workspace="%s"} %s\n' \
            "$tbl" "$ws" "$ts"
    done

    # Покрытие кластеризации: какая доля ПОДХОДЯЩИХ постов в окне вообще попала
    # в семантический кластер. Свежести мало — она отвечает на вопрос «слой
    # шевелится?», но не на «сколько корпуса он видит». 04.08.2026 у disruption
    # свежесть была в норме (прогон каждую ночь), при этом кластеризация видела
    # 40% окна: выборка берёт N самых свежих постов, а поток вдвое больше N.
    # Окно у каждого воркспейса своё: код берёт max(semantic_window, trend_window).
    echo '# HELP frontier_clustering_coverage_ratio Доля подходящих постов окна, попавших в семантический кластер.'
    echo '# TYPE frontier_clustering_coverage_ratio gauge'
    echo '# HELP frontier_clustering_eligible_posts Подходящих постов в окне кластеризации.'
    echo '# TYPE frontier_clustering_eligible_posts gauge'

    docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
        -At -F'|' -c "
            SELECT w.id,
                   count(p.id),
                   count(p.id) FILTER (WHERE COALESCE(p.semantic_cluster_id,'') <> '')
              FROM workspaces w
              LEFT JOIN posts p
                ON p.workspace_id = w.id
               AND p.published_at IS NOT NULL
               AND COALESCE(p.relevance_score,0) >= 0.6
               AND p.published_at > now() - make_interval(days => GREATEST(
                     COALESCE((w.extra->'cluster_analysis'->>'semantic_cluster_window_days')::int, 7),
                     COALESCE((w.extra->'cluster_analysis'->>'trend_cluster_window_days')::int, 30)))
               AND EXISTS (SELECT 1 FROM indexing_status i
                            WHERE i.post_id = p.id AND i.embedding_status = 'done')
             WHERE w.is_active
             GROUP BY w.id
        " </dev/null | while IFS='|' read -r ws eligible clustered; do
        if [ -z "${ws:-}" ] || [ "${eligible:-0}" = "0" ]; then
            continue
        fi
        printf 'frontier_clustering_eligible_posts{workspace="%s"} %s\n' "$ws" "$eligible"
        printf 'frontier_clustering_coverage_ratio{workspace="%s"} %s\n' \
            "$ws" "$(awk -v c="$clustered" -v e="$eligible" 'BEGIN{printf "%.4f", c/e}')"
    done
}

# Пишем атомарно: textfile collector читает каталог на каждом скрейпе и на
# частично записанном файле выдал бы parse error вместо метрики.
emit > "$TMP"
mv "$TMP" "$OUT"
chmod 0644 "$OUT"
