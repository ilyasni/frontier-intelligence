#!/usr/bin/env bash
# triage-gate-hook-test.sh — набор обоих полюсов для scripts/triage-gate-hook.sh.
#
# Хук снимает последний человеческий чекпойнт для перечисленных строк, поэтому проверка,
# состоящая только из «легальное проходит», тут бесполезна: она не измеряет границу.
# Отрицательных случаев здесь больше, и они важнее.
#
# Два случая несут особый вес, оба — уже случавшийся в проекте класс ошибки:
#   * многострочная команда с легальной ПЕРВОЙ строкой. Сопоставление по строкам считает
#     совпадение, если совпала любая строка, поэтому такая команда прошла бы целиком;
#   * инъекция через `;`, `&&`, `|`, backticks и `$()` — сторож проекта однажды искал
#     подстроку «-n» и при этом пропускал `push -f`, то есть проверял не тот признак.
#
# Запуск:  bash scripts/triage-gate-hook-test.sh
# Код 0 — всё сошлось; 1 — есть расхождение.
#
# ПРИ РАСКАТКЕ прогонять не только по этому файлу, но и по РАЗВЁРНУТОМУ значению из
# settings.json: опечатка в пути к хуку даёт 127, действие идёт обычным потоком, и
# гейт молча не работает. Путь берётся так:
#   python3 -c "import json;print(json.load(open('.claude/settings.json'))['hooks']['PreToolUse'][0]['hooks'][0]['command'])"

set -uo pipefail

# Путь переопределяем намеренно: (1) при раскатке прогнать набор по РАЗВЁРНУТОМУ из
# settings.json значению, а не только по файлу в репозитории; (2) мутационная проверка
# гоняет копии и не имеет физической возможности испортить оригинал — первый заход
# 17.08.2026 правил файл на месте, упал по таймауту и оставил хук принимающим любой хост.
HOOK="${TRIAGE_HOOK_PATH:-$(dirname "$0")/triage-gate-hook.sh}"
DIGEST="/tmp/frontier-triage-digest.md"
PASS=0
FAIL=0
ALLOWED_SEEN=0

# Отдаёт решение хука: allow | deny | ask | defer | none | MALFORMED
run_hook() {
  python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":sys.argv[1]}}))' "$1" \
    | TRIAGE_HOOK_AUDIT=/dev/null TRIAGE_HOOK_STRICT="${STRICT_MODE:-0}" bash "$HOOK" 2>/dev/null \
    | python3 -c '
import sys, json
raw = sys.stdin.read().strip()
if not raw:
    print("none")
else:
    try:
        print(json.loads(raw)["hookSpecificOutput"]["permissionDecision"])
    except Exception:
        print("MALFORMED")'
}

expect_allow() {
  local got; got=$(run_hook "$1")
  if [ "$got" = "allow" ]; then
    PASS=$((PASS + 1)); ALLOWED_SEEN=$((ALLOWED_SEEN + 1))
  else
    FAIL=$((FAIL + 1)); printf 'ПРОВАЛ: ждали allow, получили %-9s <- %s\n' "$got" "$1" >&2
  fi
}

# Не «ждали deny», а «ждали ЧТО УГОДНО, КРОМЕ allow»: в нестрогом профиле это none,
# в строгом deny, и оба варианта означают «человеческий чекпойнт на месте».
expect_not_allow() {
  local got; got=$(run_hook "$1")
  if [ "$got" != "allow" ]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1)); printf 'ПРОВАЛ: РАЗРЕШЕНО то, что разрешать нельзя <- %s\n' "$1" >&2
  fi
}

echo '--- полюс «разрешить»: три верба петли ---'
expect_allow 'ssh frontier-intelligence collect'
expect_allow 'ssh frontier-intelligence logs admin'
expect_allow 'ssh frontier-intelligence logs admin 200'
expect_allow 'ssh frontier-intelligence logs postgres 1'
expect_allow 'ssh frontier-intelligence logs worker 400'
expect_allow "ssh frontier-intelligence deliver send < ${DIGEST}"
expect_allow "ssh frontier-intelligence deliver skip < ${DIGEST}"

# Перечисляем ВСЕ сервисы: усечённый список в хуке иначе прошёл бы незамеченным,
# а проявился бы запросом разрешения посреди ночного прогона.
echo '--- полюс «разрешить»: все 18 сервисов ---'
for svc in admin alertmanager crawl4ai gpt2giga-proxy grafana ingest mcp mcp-gateway \
           neo4j node-exporter paddleocr postgres prometheus qdrant redis searxng worker xray; do
  expect_allow "ssh frontier-intelligence logs $svc 50"
done

echo '--- полюс «не разрешать»: инъекция и составные команды ---'
expect_not_allow 'ssh frontier-intelligence collect; whoami'
expect_not_allow 'ssh frontier-intelligence collect && cat /etc/shadow'
expect_not_allow 'ssh frontier-intelligence collect | tee /root/x'
expect_not_allow 'ssh frontier-intelligence collect & whoami'
expect_not_allow 'ssh frontier-intelligence logs postgres 200 | tee /root/x'
expect_not_allow 'ssh frontier-intelligence logs $(whoami)'
expect_not_allow 'ssh frontier-intelligence logs `whoami`'
expect_not_allow 'ssh frontier-intelligence logs ${USER}'
expect_not_allow 'ssh frontier-intelligence collect > /root/out'
expect_not_allow 'ssh frontier-intelligence collect $(id)'
# Ключевой случай: первая строка легальна, вторая — нет.
expect_not_allow 'ssh frontier-intelligence collect
whoami'
expect_not_allow 'whoami
ssh frontier-intelligence collect'

# Эти четыре случая добавлены ПО РЕЗУЛЬТАТУ мутационной проверки: без них удаление
# защиты от многострочности и выключение set -f оставались незамеченными — прочие
# многострочные формы отсекались проверкой числа слов, то есть другой защитой.
# Здесь число слов после разбиения СОВПАДАЕТ с легальным, поэтому ловит ровно то,
# для чего защита и стоит. Удалять эти строки нельзя, они держат конкретных мутантов.
echo '--- полюс «не разрешать»: различающие случаи (найдены мутациями) ---'
expect_not_allow 'ssh frontier-intelligence
collect'
expect_not_allow 'ssh frontier-intelligence logs
admin'
expect_not_allow 'ssh frontier-intelligence logs *'
expect_not_allow 'ssh frontier-intelligence logs ?dmin'

echo '--- полюс «не разрешать»: подмена цели ---'
expect_not_allow 'ssh other-host collect'
expect_not_allow 'ssh frontier-intelligence.evil.tld collect'
expect_not_allow 'ssh -o ProxyCommand=/bin/sh frontier-intelligence collect'
expect_not_allow 'scp frontier-intelligence:/etc/passwd .'
expect_not_allow 'bash -c "ssh frontier-intelligence collect"'
expect_not_allow 'ENV=1 ssh frontier-intelligence collect'
expect_not_allow 'sudo ssh frontier-intelligence collect'

echo '--- полюс «не разрешать»: аргументы вербов ---'
expect_not_allow 'ssh frontier-intelligence'
expect_not_allow 'ssh frontier-intelligence whoami'
expect_not_allow 'ssh frontier-intelligence collect extra'
expect_not_allow 'ssh frontier-intelligence Collect'
expect_not_allow 'ssh frontier-intelligence COLLECT'
expect_not_allow 'ssh frontier-intelligence logs'
expect_not_allow 'ssh frontier-intelligence logs nosuchservice'
expect_not_allow 'ssh frontier-intelligence logs ../../etc/passwd'
expect_not_allow 'ssh frontier-intelligence logs admin abc'
expect_not_allow 'ssh frontier-intelligence logs admin -1'
expect_not_allow 'ssh frontier-intelligence logs admin 4000'
expect_not_allow 'ssh frontier-intelligence logs admin 200 extra'
expect_not_allow 'ssh frontier-intelligence logs admin admin'

echo '--- полюс «не разрешать»: верб deliver ---'
expect_not_allow 'ssh frontier-intelligence deliver'
expect_not_allow 'ssh frontier-intelligence deliver send'
expect_not_allow 'ssh frontier-intelligence deliver bogus < /tmp/frontier-triage-digest.md'
expect_not_allow 'ssh frontier-intelligence deliver send < /tmp/other.md'
expect_not_allow 'ssh frontier-intelligence deliver send < /etc/shadow'
expect_not_allow "ssh frontier-intelligence deliver send < ${DIGEST} extra"
expect_not_allow "ssh frontier-intelligence deliver send ${DIGEST}"

echo '--- строгий профиль: неперечисленное должно ОТВЕРГАТЬСЯ, а не ждать ---'
# Без этого беспилотный прогон на неперечисленной команде уходит в запрос и висит
# до утра — ровно то, что случилось с тремя прогонами 17.08.2026.
STRICT_MODE=1
got=$(run_hook 'ssh frontier-intelligence whoami')
if [ "$got" = "deny" ]; then PASS=$((PASS + 1)); else
  FAIL=$((FAIL + 1)); printf 'ПРОВАЛ: в строгом профиле ждали deny, получили %s\n' "$got" >&2
fi
got=$(run_hook 'ssh frontier-intelligence collect')
if [ "$got" = "allow" ]; then PASS=$((PASS + 1)); else
  FAIL=$((FAIL + 1)); printf 'ПРОВАЛ: строгий профиль сломал легальный верб (%s)\n' "$got" >&2
fi
STRICT_MODE=0

# Тест про тесты. Если бы обвязка молча ломалась — не нашла python3, перепутала путь
# к хуку — все expect_not_allow прошли бы «успешно» на пустом выводе, и набор показал
# бы зелёное на неработающем гейте. Утверждаем, что положительный полюс реально
# наблюдался.
echo '--- проверка самой обвязки ---'
if [ "$ALLOWED_SEEN" -ge 25 ]; then
  PASS=$((PASS + 1))
else
  FAIL=$((FAIL + 1))
  printf 'ПРОВАЛ: положительный полюс не наблюдался (allow=%s) — обвязка не работает, зелёное было бы ложным\n' "$ALLOWED_SEEN" >&2
fi

printf '\ntriage-gate-hook: сошлось %d, расхождений %d (из них разрешений наблюдалось %d)\n' "$PASS" "$FAIL" "$ALLOWED_SEEN"
[ "$FAIL" -eq 0 ]
