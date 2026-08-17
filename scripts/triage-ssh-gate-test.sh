#!/usr/bin/env bash
# triage-ssh-gate-test.sh — набор обоих полюсов для scripts/triage-ssh-gate.sh.
#
# Белый список в шлюзе — это и есть граница безопасности ключа alert-triage: ключ
# получает три глагола вместо шелла. Проверка, состоящая только из «разрешённое
# работает», такую границу не измеряет — она обязана показывать, что запрещённое
# ОТКЛОНЯЕТСЯ. Поэтому здесь оба полюса, и отрицательных случаев большинство.
#
# Запуск на сервере:  bash scripts/triage-ssh-gate-test.sh
# Код выхода 0 — все случаи сошлись; 1 — есть расхождение.
#
# Случай `logs admin; whoami` тут ключевой: точка с запятой обязана стать частью
# ИМЕНИ СЕРВИСА и упереться в белый список, а не выполниться отдельной командой.

set -uo pipefail

GATE="$(dirname "$0")/triage-ssh-gate.sh"
PASS=0
FAIL=0

# expect_refuse <команда> — шлюз обязан отказать (ненулевой код).
expect_refuse() {
  local cmd="$1" rc
  SSH_ORIGINAL_COMMAND="$cmd" bash "$GATE" </dev/null >/dev/null 2>&1
  rc=$?
  if [ "$rc" -ne 0 ]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    printf 'ПРОВАЛ: ожидался отказ, но команда прошла: %s\n' "$cmd" >&2
  fi
}

# expect_allow <команда> — шлюз обязан пропустить (нулевой код).
expect_allow() {
  local cmd="$1" rc
  SSH_ORIGINAL_COMMAND="$cmd" bash "$GATE" </dev/null >/dev/null 2>&1
  rc=$?
  if [ "$rc" -eq 0 ]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    printf 'ПРОВАЛ: ожидался пропуск, но получен отказ (rc=%s): %s\n' "$rc" "$cmd" >&2
  fi
}

# --- полюс «отклонить» ---
expect_refuse ''                          # пустая строка = попытка получить шелл
expect_refuse 'rm -rf /'
expect_refuse 'bash'
expect_refuse 'sh -c whoami'
expect_refuse 'collect; rm -rf /tmp/x'    # инъекция через ;
expect_refuse 'collect && whoami'
expect_refuse 'collect extra'
expect_refuse 'logs'                      # нет обязательного аргумента
expect_refuse 'logs admin; whoami'        # ; уходит в имя сервиса и не проходит белый список
expect_refuse 'logs ../../etc/passwd'     # обход пути
expect_refuse 'logs nosuchservice'
expect_refuse 'logs admin abc'            # число строк не число
expect_refuse 'logs admin 10 extra'
expect_refuse 'deliver'                   # нет режима
expect_refuse 'deliver bogus'
expect_refuse 'deliver send'              # пустой stdin: дайджеста нет
expect_refuse 'DELIVER send'              # регистр значим
expect_refuse 'Collect'

# --- полюс «пропустить» ---
expect_allow 'logs admin 5'
expect_allow 'logs postgres 3'
expect_allow 'logs worker 1'
expect_allow 'logs admin'                 # число строк по умолчанию
expect_allow 'collect'

printf 'triage-ssh-gate: сошлось %d, расхождений %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
