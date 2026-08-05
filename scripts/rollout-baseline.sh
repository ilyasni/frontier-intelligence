#!/usr/bin/env bash
# Базовая линия перед волной раската источников.
# Ретенция Prometheus 200ч, а серия волн растянута на дни — к последней волне
# базовая линия первой уже вымыта, и сравнение «до/после» по тем же сериям
# построить нельзя. Поэтому снимок кладётся в файл.
set -uo pipefail
cd /opt/frontier-intelligence || exit 1
OUT="docs/ops/rollout-baselines"
mkdir -p "$OUT"
STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
FILE="$OUT/$1-$STAMP.txt"

q() {
    curl -sS "http://127.0.0.1:9090/api/v1/query" --data-urlencode "query=$1" |
        python3 -c "
import sys, json
d = json.load(sys.stdin)
for x in d['data']['result']:
    m = {k: v for k, v in x['metric'].items() if k not in ('__name__', 'instance', 'job')}
    print('   ', m, '=', x['value'][1])
" 2>/dev/null
}

{
    echo "волна: $1"
    echo "снято: $STAMP"
    echo
    echo "== источников включено =="
    docker compose exec -T postgres psql -U frontier -d frontier -At \
        -c "SELECT count(*) FROM sources WHERE is_enabled;" </dev/null
    echo
    echo "== стадии конвейера =="
    q 'sum by (stage,outcome) (frontier_pipeline_stage_total)'
    echo
    echo "== исходы краула =="
    q 'sum by (outcome) (frontier_crawl_outcomes_total)'
    echo
    echo "== длительность кластеризации, сек =="
    docker compose exec -T postgres psql -U frontier -d frontier -At -F'|' -c \
        "SELECT workspace_id, round(extract(epoch from (finished_at-started_at)))
           FROM cluster_runs
          WHERE stage='full' AND status='success' AND finished_at IS NOT NULL
          ORDER BY started_at DESC LIMIT 6;" </dev/null
    echo
    echo "== покрытие кластеризации =="
    q 'frontier_clustering_coverage_ratio'
    echo
    echo "== расход LLM за сутки =="
    q 'sum by (provider,status) (increase(frontier_llm_requests_total[24h]))'
} > "$FILE" 2>&1

echo "записано: $FILE"
tail -40 "$FILE"
