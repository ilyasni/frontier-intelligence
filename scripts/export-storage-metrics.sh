#!/usr/bin/env bash
# Экспортирует состояние хранилищ (Redis, PostgreSQL, Neo4j) в node_exporter textfile collector.
# Пункт 14 docs/TODO-UNFINISHED.md.
#
# Зачем. Три базы держат весь пайплайн и до сих пор не измерялись ничем. 31.07.2026
# Redis упёрся в maxmemory и на 9 часов положил ingest, обогащение и поиск — при этом
# consumer lag и pending были нулевые (писать в стрим было уже нечем), поэтому все
# существовавшие алерты промолчали. Занятость памяти, вытеснение ключей и статус
# bgsave никто не собирал, так что узнать о происходящем было физически неоткуда.
#
# Почему textfile, а не экспортёры. Оба канонических пути на этом хосте закрыты:
#   * контейнеры redis_exporter / postgres_exporter не притащить — `docker pull`
#     падает по I/O timeout, Docker Hub с сервера недоступен;
#   * self-metrics у образов нет — neo4j:5.15-community отдаёт /metrics только
#     в Enterprise, а у redis:7-alpine и postgres:16-alpine эндпоинта нет вовсе.
# Остаётся третий путь, уже обкатанный на freshness и backup: снять значения
# `docker exec`'ом и положить .prom-файл в каталог коллектора.
#
# Метрики (каждый блок со своим _up, см. комментарий у emit):
#   frontier_redis_up / _used_memory_bytes / _maxmemory_bytes /
#                       _evicted_keys_total / _connected_clients / _rdb_last_bgsave_ok
#   frontier_pg_up / _numbackends / _deadlocks_total / _xact_rollback_total /
#                    _database_size_bytes / _longest_query_seconds
#   frontier_neo4j_up / _node_count / _relationship_count / _measured_timestamp_seconds
#
# Cron (через bash — sync-push снимает бит исполнения):
#   */10 * * * * bash /opt/frontier-intelligence/scripts/export-storage-metrics.sh >/dev/null 2>&1
#
# Проверить руками, ничего не записывая в prometheus/textfile:
#   bash /opt/frontier-intelligence/scripts/export-storage-metrics.sh --dry-run

set -uo pipefail

PROJECT_DIR="${FRONTIER_ROOT:-/opt/frontier-intelligence}"
ENV_FILE="$PROJECT_DIR/.env"
OUT_DIR="$PROJECT_DIR/prometheus/textfile"
# Свой файл, а не frontier_analysis.prom: тот пишет export-analysis-freshness.sh,
# и два писателя в один путь через `mv` затирали бы друг друга через раз.
OUT="$OUT_DIR/frontier_storage.prom"
TMP="$OUT.$$"
CACHE="$PROJECT_DIR/runtime/storage-neo4j-cache"

REDIS_CONTAINER="${REDIS_CONTAINER:-frontier-intelligence-redis-1}"
PG_CONTAINER="${PG_CONTAINER:-frontier-intelligence-postgres-1}"
GRAPH_CONTAINER="${GRAPH_CONTAINER:-frontier-intelligence-neo4j-1}"

# Периодичность разведена намеренно, и вот замер (04.08.2026, тот же хост):
#   docker exec redis-cli INFO ......... 0.061s
#   docker exec cypher-shell (счётчики)  1.772s
# Тридцатикратная разница — не в запросе: count-store в Neo4j отвечает за O(1),
# оба числа он берёт из метаданных. Дорог КЛИЕНТ: `cypher-shell` поднимает JVM
# заново на каждый вызов, и почти вся эта секунда с лишним — её старт. На хосте
# всего 3 ядра, их и так делят worker и admin, поэтому Redis и Postgres снимаются
# на каждом прогоне, а граф — не чаще раза в NEO4J_MIN_INTERVAL секунд; между
# опросами переиспользуется значение из кэша.
#
# Значение по умолчанию — 540, а не 600, ровно под действующую конвенцию cron `*/10`:
# порог должен быть чуть МЕНЬШЕ шага крона, иначе джиттер запуска (тик пришёл на
# секунду раньше) отбрасывал бы опрос до следующего тика, и фактическая
# периодичность прыгала бы между 10 и 20 минутами. Порог здесь — не расписание,
# а пол: он защищает от учащённого крона и от ручных прогонов подряд.
NEO4J_MIN_INTERVAL="${NEO4J_MIN_INTERVAL:-540}"

DRY_RUN="${DRY_RUN:-0}"
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        *) echo "usage: $(basename "$0") [--dry-run]" >&2; exit 2 ;;
    esac
done

if [ "$DRY_RUN" != "1" ]; then
    mkdir -p "$OUT_DIR" "$(dirname "$CACHE")"
fi

NOW="$(date +%s)"

is_uint() { case "${1:-}" in '' | *[!0-9]*) return 1 ;; *) return 0 ;; esac; }
is_num() { case "${1:-}" in '' | *[!0-9.]* | *.*.*) return 1 ;; *) return 0 ;; esac; }

# Читаем из .env ТОЛЬКО нужный ключ, а не `. .env` целиком: там же лежат S3-ключи,
# токены четырёх LLM-провайдеров и токен Telegram-бота, и затягивать их в окружение
# ради одного пароля — лишний способ утечь в окружение дочерних процессов и в дампы.
read_env() {
    [ -r "$ENV_FILE" ] || return 0
    local v
    v="$(sed -n "s/^$1=//p" "$ENV_FILE" | head -1 | tr -d '\r')"
    v="${v%\"}"; v="${v#\"}"
    v="${v%\'}"; v="${v#\'}"
    printf '%s' "$v"
}

# ── Redis ────────────────────────────────────────────────────────────────────
# Один `INFO` целиком вместо четырёх посекционных: секции memory/stats/clients/
# persistence приходят в том же ответе, а каждый лишний exec — это ещё один
# fork в контейнер.
REDIS_UP=0
REDIS_USED=0
REDIS_MAXMEMORY=0
REDIS_EVICTED=0
REDIS_CLIENTS=0
REDIS_BGSAVE_OK=0

redis_raw="$(docker exec "$REDIS_CONTAINER" redis-cli INFO </dev/null 2>/dev/null)"

redis_field() {
    printf '%s\n' "$redis_raw" | awk -F: -v key="$1" '
        $1 == key { v = $2; gsub(/\r/, "", v); print v; exit }'
}

# used_memory и есть доказательство живости: если он пришёл числом, опрос удался.
_v="$(redis_field used_memory)"
if is_uint "$_v"; then
    REDIS_UP=1
    REDIS_USED="$_v"

    _v="$(redis_field maxmemory)"
    # 0 здесь — легальное значение Redis, оно означает «лимита нет». Отличать
    # его от «не смогли прочитать» — работа frontier_redis_up, не этой метрики.
    is_uint "$_v" && REDIS_MAXMEMORY="$_v"

    _v="$(redis_field evicted_keys)"
    is_uint "$_v" && REDIS_EVICTED="$_v"

    _v="$(redis_field connected_clients)"
    is_uint "$_v" && REDIS_CLIENTS="$_v"

    # rdb_last_bgsave_status приходит словом ok/err — Prometheus слов не хранит.
    _v="$(redis_field rdb_last_bgsave_status)"
    [ "$_v" = "ok" ] && REDIS_BGSAVE_OK=1
fi

# ── PostgreSQL ───────────────────────────────────────────────────────────────
# Одна строка на всё: pg_stat_database даёт накопительные счётчики по текущей БД,
# pg_database_size — её объём, отдельным подзапросом идёт возраст самого долгого
# активного запроса. Свой backend исключён (pg_backend_pid), иначе метрика мерила
# бы саму себя; backend_type ограничен клиентскими, иначе в максимум попадали бы
# вечно живущие autovacuum launcher и walwriter.
PG_UP=0
PG_BACKENDS=0
PG_DEADLOCKS=0
PG_ROLLBACK=0
PG_SIZE=0
PG_LONGEST=0

pg_raw="$(docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
    -At -F'|' -c "
        SELECT d.numbackends,
               d.deadlocks,
               d.xact_rollback,
               pg_database_size(d.datname),
               COALESCE((
                   SELECT round(max(extract(epoch from (now() - a.query_start)))::numeric, 1)
                     FROM pg_stat_activity a
                    WHERE a.state = 'active'
                      AND a.backend_type = 'client backend'
                      AND a.query_start IS NOT NULL
                      AND a.pid <> pg_backend_pid()
               ), 0)
          FROM pg_stat_database d
         WHERE d.datname = current_database()
    " </dev/null 2>/dev/null | head -1)"

IFS='|' read -r _backends _deadlocks _rollback _size _longest <<<"$pg_raw"
if is_uint "${_backends:-}"; then
    PG_UP=1
    PG_BACKENDS="$_backends"
    is_uint "${_deadlocks:-}" && PG_DEADLOCKS="$_deadlocks"
    is_uint "${_rollback:-}" && PG_ROLLBACK="$_rollback"
    is_uint "${_size:-}" && PG_SIZE="$_size"
    is_num "${_longest:-}" && PG_LONGEST="$_longest"
fi

# ── Neo4j ────────────────────────────────────────────────────────────────────
NEO_UP=0
NEO_NODES=0
NEO_RELS=0
NEO_TS=0

if [ "$DRY_RUN" = "1" ]; then
    # Ручная проверка обязана мерить, а не пересказывать кэш, поэтому опрашиваем
    # всегда и кэш не пишем: иначе `--dry-run` подавил бы следующий штатный опрос.
    poll_graph=1
else
    if [ -r "$CACHE" ]; then
        read -r _c_ts _c_up _c_nodes _c_rels <"$CACHE"
        is_uint "${_c_ts:-}" && NEO_TS="$_c_ts"
        is_uint "${_c_up:-}" && NEO_UP="$_c_up"
        is_uint "${_c_nodes:-}" && NEO_NODES="$_c_nodes"
        is_uint "${_c_rels:-}" && NEO_RELS="$_c_rels"
    fi
    poll_graph=0
    [ $((NOW - NEO_TS)) -ge "$NEO4J_MIN_INTERVAL" ] && poll_graph=1
fi

if [ "$poll_graph" = "1" ]; then
    _npw="$(read_env NEO4J_PASSWORD)"
    if [ -n "$_npw" ]; then
        # Два CALL-подзапроса, а не `MATCH (n) ... MATCH ()-[r]->()`: во второй
        # форме нулевое число связей даёт пустой результат вместо строки с нулём,
        # и «граф пуст» стало бы неотличимо от «запрос не отработал».
        graph_raw="$(docker exec "$GRAPH_CONTAINER" cypher-shell \
            -u "${NEO4J_USER:-neo4j}" -p "$_npw" --format plain \
            "CALL { MATCH (n) RETURN count(n) AS nodes } CALL { MATCH ()-[r]->() RETURN count(r) AS rels } RETURN nodes, rels" \
            </dev/null 2>/dev/null | tail -1 | tr -d ' \r')"
        _n="${graph_raw%%,*}"
        _r="${graph_raw##*,}"
        if is_uint "$_n" && is_uint "$_r"; then
            NEO_UP=1
            NEO_NODES="$_n"
            NEO_RELS="$_r"
            NEO_TS="$NOW"
            if [ "$DRY_RUN" != "1" ]; then
                printf '%s %s %s %s\n' "$NEO_TS" "$NEO_UP" "$NEO_NODES" "$NEO_RELS" >"$CACHE"
            fi
        else
            # Опрос не удался. Счётчики оставляем прежними (последнее известное),
            # но up сбрасываем в 0, а measured_timestamp не двигаем — по паре
            # up=0 + застывший timestamp видно и что данные старые, и насколько.
            # Кэш при неудаче не переписывается, поэтому следующий прогон снова
            # увидит просроченный NEO_TS и попробует опросить граф ещё раз.
            NEO_UP=0
        fi
    fi
fi

emit() {
    # У каждого блока свой _up — это главное требование к этому экспортёру.
    # Prometheus не умеет отличать «значение равно нулю» от «серии нет»: при
    # `absent()`-логике молчащий экспортёр выглядит ровно как здоровая система
    # с нулевым eviction. Именно так 31.07 нулевой lag выдал себя за порядок.
    # Поэтому опрос каждого хранилища всегда печатает свой признак успеха, и все
    # остальные значения блока читаются только вместе с ним: при up=0 нули ниже
    # означают «не измеряли», а не «пусто».
    echo '# HELP frontier_redis_up Опрос Redis удался (1) или нет (0). Все frontier_redis_* ниже действительны только при 1.'
    echo '# TYPE frontier_redis_up gauge'
    echo "frontier_redis_up $REDIS_UP"

    echo '# HELP frontier_redis_used_memory_bytes Занято памяти Redis, байт.'
    echo '# TYPE frontier_redis_used_memory_bytes gauge'
    echo "frontier_redis_used_memory_bytes $REDIS_USED"

    echo '# HELP frontier_redis_maxmemory_bytes Потолок памяти Redis. 0 = лимит не задан.'
    echo '# TYPE frontier_redis_maxmemory_bytes gauge'
    echo "frontier_redis_maxmemory_bytes $REDIS_MAXMEMORY"

    echo '# HELP frontier_redis_evicted_keys_total Ключей вытеснено с последнего старта Redis.'
    echo '# TYPE frontier_redis_evicted_keys_total counter'
    echo "frontier_redis_evicted_keys_total $REDIS_EVICTED"

    echo '# HELP frontier_redis_connected_clients Клиентских подключений к Redis.'
    echo '# TYPE frontier_redis_connected_clients gauge'
    echo "frontier_redis_connected_clients $REDIS_CLIENTS"

    echo '# HELP frontier_redis_rdb_last_bgsave_ok Последний bgsave завершился успешно (1) или нет (0).'
    echo '# TYPE frontier_redis_rdb_last_bgsave_ok gauge'
    echo "frontier_redis_rdb_last_bgsave_ok $REDIS_BGSAVE_OK"

    echo '# HELP frontier_pg_up Опрос PostgreSQL удался (1) или нет (0). Все frontier_pg_* ниже действительны только при 1.'
    echo '# TYPE frontier_pg_up gauge'
    echo "frontier_pg_up $PG_UP"

    echo '# HELP frontier_pg_numbackends Активных подключений к базе.'
    echo '# TYPE frontier_pg_numbackends gauge'
    echo "frontier_pg_numbackends $PG_BACKENDS"

    echo '# HELP frontier_pg_deadlocks_total Взаимоблокировок с момента сброса статистики.'
    echo '# TYPE frontier_pg_deadlocks_total counter'
    echo "frontier_pg_deadlocks_total $PG_DEADLOCKS"

    echo '# HELP frontier_pg_xact_rollback_total Откаченных транзакций с момента сброса статистики.'
    echo '# TYPE frontier_pg_xact_rollback_total counter'
    echo "frontier_pg_xact_rollback_total $PG_ROLLBACK"

    echo '# HELP frontier_pg_database_size_bytes Размер базы на диске, байт.'
    echo '# TYPE frontier_pg_database_size_bytes gauge'
    echo "frontier_pg_database_size_bytes $PG_SIZE"

    echo '# HELP frontier_pg_longest_query_seconds Возраст самого долгого активного клиентского запроса, секунд.'
    echo '# TYPE frontier_pg_longest_query_seconds gauge'
    echo "frontier_pg_longest_query_seconds $PG_LONGEST"

    echo '# HELP frontier_neo4j_up Последний опрос Neo4j удался (1) или нет (0). Все frontier_neo4j_* ниже действительны только при 1.'
    echo '# TYPE frontier_neo4j_up gauge'
    echo "frontier_neo4j_up $NEO_UP"

    echo '# HELP frontier_neo4j_node_count Узлов в графе.'
    echo '# TYPE frontier_neo4j_node_count gauge'
    echo "frontier_neo4j_node_count $NEO_NODES"

    echo '# HELP frontier_neo4j_relationship_count Связей в графе.'
    echo '# TYPE frontier_neo4j_relationship_count gauge'
    echo "frontier_neo4j_relationship_count $NEO_RELS"

    # Без этой метрики два значения графа неотличимы от свежих: они переписываются
    # в файл на каждом прогоне, хотя измеряются раз в NEO4J_MIN_INTERVAL.
    echo '# HELP frontier_neo4j_measured_timestamp_seconds Когда граф опрашивался в последний раз (значения выше могут быть из кэша).'
    echo '# TYPE frontier_neo4j_measured_timestamp_seconds gauge'
    echo "frontier_neo4j_measured_timestamp_seconds $NEO_TS"
}

if [ "$DRY_RUN" = "1" ]; then
    emit
    exit 0
fi

# Пишем атомарно: textfile collector читает каталог на каждом скрейпе и на
# частично записанном файле выдал бы parse error вместо всех метрик разом.
emit >"$TMP"
mv "$TMP" "$OUT"
chmod 0644 "$OUT"
