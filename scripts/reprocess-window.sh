#!/usr/bin/env bash
# Серверная половина процедуры переобработки окна (scripts/server-reprocess-window.ps1).
#
# Читает список post_id со stdin, по одному в строке, и ставит каждый в очередь
# переобработки через admin API. Печатает итог и ЗАВЕРШАЕТСЯ НЕНУЛЕВЫМ КОДОМ,
# если хоть один вызов не удался.
#
# Зачем отдельным файлом, а не строкой внутри .ps1. Прежняя редакция звала
# `ssh $Server "curl -fsS -X POST ..."` и отправляла результат в Out-Null. Два
# следствия: (1) админка закрыта авторизацией и отвечала 401, то есть боевой
# прогон не переобрабатывал ничего; (2) в PowerShell 5.1 ненулевой код нативного
# exe не бросает исключение даже под $ErrorActionPreference='Stop', поэтому
# «reprocess ok N/N» печаталось при любом исходе. Второе опаснее первого:
# инструмент восстановления выглядел исправным ровно до момента, когда понадобится.
#
# Учётные данные читаются ЗДЕСЬ, на сервере, и уходят curl через `--config -`
# (stdin). В argv они не попадают, значит не видны в `ps` у соседних процессов
# и не оседают в истории вызывающей стороны.
#
# Использование:
#   printf '%s\n' id1 id2 | bash scripts/reprocess-window.sh [задержка_в_секундах]

set -euo pipefail

PROJECT_DIR="${FRONTIER_PROJECT_DIR:-/opt/frontier-intelligence}"
ADMIN_BASE="${FRONTIER_ADMIN_BASE:-http://127.0.0.1:8101}"
DELAY_SEC="${1:-0.2}"

ENV_FILE="$PROJECT_DIR/.env"
if [ ! -r "$ENV_FILE" ]; then
    echo "cannot read $ENV_FILE" >&2
    exit 78
fi

# Значение берём после первого '=' и снимаем обрамляющие кавычки и CR.
# Целиком .env не sourc'им намеренно: в нём есть строки, которые не должны
# исполняться в этом шелле.
read_env_value() {
    sed -n "s/^$1=//p" "$ENV_FILE" | head -1 | tr -d '\r' | sed 's/^"\(.*\)"$/\1/; s/^'"'"'\(.*\)'"'"'$/\1/'
}

admin_user="$(read_env_value ADMIN_USER)"
# ADMIN_USER в серверном .env НЕ ЗАДАН — там только ADMIN_PASSWORD. Имя пользователя
# подставляет сам сервис: admin/backend/main.py:66 `os.environ.get("ADMIN_USER", "admin")`.
# Без этой строки curl уходил с пустым именем, `secrets.compare_digest(user, "admin")`
# не сходился, и КАЖДЫЙ вызов возвращал 401 — то есть заход 7 вылечил молчание
# (скрипт теперь честно падает), но переобработать окно по-прежнему было нельзя.
# Проверено вживую 06.08.2026: с пустым именем 401, с "admin" — 200.
# Соседний scripts/admin_api_auth.py:35 этот дефолт держит с самого начала;
# разъехались именно две реализации одного и того же.
admin_user="${admin_user:-admin}"
admin_password="$(read_env_value ADMIN_PASSWORD)"

if [ -z "$admin_password" ]; then
    # Fail-closed. Пустой пароль означает, что авторизация не сложится, и молча
    # получать 401 на каждый пост — ровно то поведение, ради которого этот файл
    # и появился.
    echo "ADMIN_PASSWORD is empty or absent in $ENV_FILE — refusing to run" >&2
    exit 78
fi
if [ -z "$admin_user" ]; then
    admin_user=admin  # тот же дефолт, что в admin/backend/main.py::_admin_user
fi

ok=0
bad=0

while read -r post_id; do
    case "$post_id" in
        "" ) continue ;;
    esac

    code="$(
        printf 'user = "%s:%s"\n' "$admin_user" "$admin_password" |
            curl -sS -o /dev/null -w '%{http_code}' \
                -X POST --config - \
                "$ADMIN_BASE/api/pipeline/reprocess/$post_id" || echo 000
    )"

    case "$code" in
        200|202)
            ok=$((ok + 1))
            ;;
        *)
            bad=$((bad + 1))
            echo "reprocess FAILED post_id=$post_id http=$code" >&2
            ;;
    esac

    sleep "$DELAY_SEC"
done

echo "reprocessed_ok=$ok reprocessed_failed=$bad"

# Единственное, что доедет до вызывающей стороны как отказ. Без этой строки
# цикл выше молчит точно так же, как молчал прежний скрипт.
if [ "$bad" -gt 0 ]; then
    exit 1
fi
