#!/usr/bin/env bash
# Прогоняет юнит-тесты правил Prometheus (`prometheus/alerts.test.yml`).
#
# Зачем отдельный скрипт, если правила уже накрыты тестами в tests/:
# те разбирают правило СТАТИЧЕСКИ — что метрику кто-то публикует, что порог не
# разъехался с текстом, что `for` в границах. Ни один из них выражение не
# вычисляет. А молча ломается именно вычисление: `- ignoring(window)` без
# `and on(source_id)` даёт пустой вектор при истинном условии, потому что метки
# левой и правой части не совпадают. Правило при этом синтаксически цело,
# валидно и не может сработать никогда — класс FrontierS3QuotaCritical (04.08.2026).
#
# promtool живёт в образе prometheus, отдельно его на хост ставить не нужно.
# Файлы копируются в /tmp контейнера: /etc/prometheus смонтирован пофайлово и
# на запись не рассчитан.
#
# Использование (на сервере):
#   bash scripts/verify-alert-rules.sh
#
# Ненулевой код возврата означает, что правило либо не грузится, либо ведёт себя
# не так, как записано в alerts.test.yml. Молчаливого «ok» здесь быть не может:
# set -e плюс явная проверка обоих шагов.

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/frontier-intelligence}"
CONTAINER="${PROMETHEUS_CONTAINER:-frontier-intelligence-prometheus-1}"
WORK=/tmp/frontier-alert-rule-check

RULES="$PROJECT_DIR/prometheus/alerts.yml"
TESTS="$PROJECT_DIR/prometheus/alerts.test.yml"

for path in "$RULES" "$TESTS"; do
    if [ ! -f "$path" ]; then
        echo "missing $path" >&2
        exit 1
    fi
done

if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
    echo "container $CONTAINER is not running; start the monitor profile first" >&2
    exit 1
fi

# `docker exec` без -i забирает stdin у вызывающего скрипта: heredoc или пайп
# выше по течению теряет остаток. Поэтому каждому вызову явно даётся /dev/null.
docker exec "$CONTAINER" rm -rf "$WORK" </dev/null
docker exec "$CONTAINER" mkdir -p "$WORK" </dev/null
docker cp "$RULES" "$CONTAINER:$WORK/alerts.yml"
docker cp "$TESTS" "$CONTAINER:$WORK/alerts.test.yml"

echo "== promtool check rules =="
docker exec "$CONTAINER" promtool check rules "$WORK/alerts.yml" </dev/null

echo
echo "== promtool test rules =="
docker exec -w "$WORK" "$CONTAINER" promtool test rules alerts.test.yml </dev/null

docker exec "$CONTAINER" rm -rf "$WORK" </dev/null
echo
echo "alert rules: ok"
