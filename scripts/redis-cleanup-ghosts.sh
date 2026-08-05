#!/usr/bin/env bash
# Разовая уборка накопленного мусора в Redis: призрачные консьюмеры и
# осиротевший стрим. Пункты 9 и 37 реестра.
#
# ЭТО ЗАПИСЬ В REDIS, поэтому без --confirm скрипт только показывает, что сделал бы.
#
# Что убирается и почему это накопилось:
#
#   1. Призрачные консьюмеры в stream:posts:crawl и stream:posts:reindex.
#      Уборка мёртвых консьюмеров (_cleanup_dead_consumers) написана только в двух
#      потребителях из четырёх — enrichment и vision. У crawl4ai и reindex её нет,
#      а имя консьюмера генерируется на старт процесса, поэтому каждый рестарт
#      оставляет запись навсегда. У crawl4ai 84 рестарта за 45 суток — отсюда
#      и 85 записей. Побочный эффект: десятки мёртвых серий в
#      frontier_redis_stream_consumer_idle_seconds.
#
#      Корень чинится кодом (заход 9: общий хелпер + стабильное имя консьюмера).
#      Этот скрипт разгребает то, что уже накопилось.
#
#   2. Ключ stream:posts:enriched. Продюсер снят 2026-08-05, consumer-групп
#      у стрима не было ни одной за всю историю: entries-added 47 635 при длине
#      10 004, то есть ~37 тысяч событий вытеснены триммингом непрочитанными.
#      Данные в нём никому не нужны по построению — их никто ни разу не читал.
#
# Инвариант удаления консьюмера — тот же, что у _cleanup_dead_consumers, и он
# консервативный: удаляем ТОЛЬКО pending == 0 И idle > порога. Консьюмер
# с ненулевым pending держит неподтверждённые сообщения, и его удаление
# осиротило бы их в PEL — то есть превратило бы уборку мусора в потерю данных.
#
# Использование:
#   bash scripts/redis-cleanup-ghosts.sh              # показать план
#   bash scripts/redis-cleanup-ghosts.sh --confirm    # выполнить

set -uo pipefail

PROJECT_DIR="${FRONTIER_ROOT:-/opt/frontier-intelligence}"
REDIS_CONTAINER="${REDIS_CONTAINER:-frontier-intelligence-redis-1}"
IDLE_MIN_MS="${IDLE_MIN_MS:-3600000}"   # 1 час, как у _cleanup_dead_consumers
ORPHAN_STREAM="stream:posts:enriched"

CONFIRM=0
for arg in "$@"; do
    case "$arg" in
        --confirm) CONFIRM=1 ;;
        *) echo "usage: $(basename "$0") [--confirm]" >&2; exit 2 ;;
    esac
done

redis() { docker exec -i "$REDIS_CONTAINER" redis-cli "$@" </dev/null; }

if [ "$CONFIRM" != "1" ]; then
    echo "=== ПЛАН (ничего не меняется, добавь --confirm) ==="
else
    echo "=== ВЫПОЛНЕНИЕ ==="
fi

total_removed=0

# Потоки, у которых в коде НЕТ автоматической уборки. enrichment и vision
# сюда намеренно не входят: они убирают за собой сами, и у них по одному
# консьюмеру — трогать их значило бы дублировать работающий механизм.
for stream in stream:posts:crawl stream:posts:reindex; do
    group="$(redis XINFO GROUPS "$stream" 2>/dev/null | sed -n '2p' | tr -d '\r')"
    if [ -z "$group" ]; then
        echo "  $stream: группы нет, пропуск"
        continue
    fi

    # Плоский вывод redis-cli: пары ключ/значение построчно. Разбираем питоном,
    # а не awk: у consumer-записей разное число полей между версиями Redis,
    # и позиционный разбор ломается молча.
    plan="$(redis XINFO CONSUMERS "$stream" "$group" 2>/dev/null | python3 -c "
import sys
lines=[l.rstrip('\r\n') for l in sys.stdin if l.strip()]
items=[]; cur={}
for i in range(0, len(lines)-1, 2):
    k, v = lines[i], lines[i+1]
    if k == 'name' and cur:
        items.append(cur); cur = {}
    cur[k] = v
if cur:
    items.append(cur)
idle_min = int('$IDLE_MIN_MS')
for it in items:
    try:
        pending = int(it.get('pending', '1'))
        idle = int(it.get('idle', '0'))
    except ValueError:
        continue
    if pending == 0 and idle > idle_min:
        print(it.get('name', ''))
")"

    count="$(printf '%s' "$plan" | grep -c . || true)"
    echo "  $stream / $group: под удаление $count консьюмеров (pending=0, idle>$((IDLE_MIN_MS/60000))м)"

    if [ "$CONFIRM" = "1" ] && [ "${count:-0}" -gt 0 ]; then
        while IFS= read -r consumer; do
            [ -z "$consumer" ] && continue
            redis XGROUP DELCONSUMER "$stream" "$group" "$consumer" >/dev/null 2>&1
            total_removed=$((total_removed + 1))
        done <<<"$plan"
        left="$(redis XINFO CONSUMERS "$stream" "$group" 2>/dev/null | grep -c '^name$' || true)"
        echo "    удалено $count, осталось ${left:-?}"
    fi
done

# Осиротевший стрим. Проверяем инвариант ПЕРЕД удалением, а не полагаемся
# на то, что продюсера сняли: если группа появилась, значит у стрима есть
# потребитель, и удалять его нельзя.
groups_count="$(redis XINFO GROUPS "$ORPHAN_STREAM" 2>/dev/null | grep -c '^name$' || true)"
exists="$(redis EXISTS "$ORPHAN_STREAM" 2>/dev/null | tr -d '\r')"

if [ "${exists:-0}" != "1" ]; then
    echo "  $ORPHAN_STREAM: ключа нет, пропуск"
elif [ "${groups_count:-0}" != "0" ]; then
    echo "  $ORPHAN_STREAM: ПОЯВИЛАСЬ consumer-группа ($groups_count) — НЕ удаляю."
    echo "    Значит у стрима есть потребитель, и предпосылка «читателей нет» больше неверна."
else
    len="$(redis XLEN "$ORPHAN_STREAM" 2>/dev/null | tr -d '\r')"
    echo "  $ORPHAN_STREAM: ключ есть, длина $len, consumer-групп 0 → под удаление"
    if [ "$CONFIRM" = "1" ]; then
        redis DEL "$ORPHAN_STREAM" >/dev/null 2>&1
        echo "    удалён"
    fi
fi

if [ "$CONFIRM" = "1" ]; then
    echo "=== итого удалено консьюмеров: $total_removed ==="
fi
