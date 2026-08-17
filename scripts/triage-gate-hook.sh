#!/usr/bin/env bash
# triage-gate-hook.sh — PreToolUse-хук, разрешающий БЕЗ ВОПРОСА ровно те обращения
# к шлюзу на .222, из которых состоит петля разбора алертов. Всё прочее не решает.
#
# ЗАЧЕМ. Беспилотный прогон в окружении bridge упирается в запрос разрешения, отвечать
# на который некому: 17.08.2026 три прогона подряд сели в worker_status=requires_action
# и висели часами. Тело триггера рутины рычагом не является — запрошенная модель
# вернулась другой, allowed_tools запрос разрешения не остановил (измерено). Рычаг —
# слой настроек, и самый узкий из доступных механизмов это хук: он видит команду
# целиком и сверяет её буквально, а не по образцу.
#
# ПОЧЕМУ ИМЕННО РАЗРЕШАЮЩИЙ ХУК, А НЕ СМЕНА РЕЖИМА. У любого отказа этого хука
# последствие одно и то же: решения нет → работает обычный поток разрешений → запрос
# → прогон встал. Все три документированных пути отказа ведут туда же:
#   * exit 1 НЕ блокирует (считается неблокирующей ошибкой);
#   * недоступный или неисполнимый файл хука даёт 127 — действие идёт обычным потоком;
#   * таймаут хука не блокирует.
# То есть сломанный хук не выполняет неотсмотренную команду, а останавливает работу.
# У ЗАПРЕЩАЮЩЕГО хука те же ловушки инвертированы в опасную сторону, поэтому запрет
# здесь не основной механизм: жёсткий пол держит forced command ключа на .222 плюс
# правила deny, которые хук пробить не может по построению.
#
# КОНТРАКТ (code.claude.com/docs/en/hooks — PreToolUse decision control):
#   stdin  — JSON вызова; команда лежит в .tool_input.command
#   stdout — JSON, начинающийся с '{': {"hookSpecificOutput":{"hookEventName":"PreToolUse",
#            "permissionDecision":"allow|deny|ask|defer","permissionDecisionReason":"..."}}
#   exit 0 с пустым stdout — решения нет, работает обычный поток
#   exit 2 — блокировка
# Значений ровно четыре; "escalate" не существует.
#
# ЖУРНАЛ. Своего журнала решений о разрешениях продукт не ведёт (есть лишь opt-in
# отладочный лог), поэтому хук обязан вести его сам — иначе разрешения невидимы.
#
# РАСКАТКА. Файл сам по себе ничего не меняет: он вступает в силу только будучи
# прописан в hooks.PreToolUse нужного слоя настроек. Раскатка — отдельное решение
# владельца, см. docs. Набор проверок обоих полюсов: scripts/triage-gate-hook-test.sh.
# При раскатке прогонять набор по РАЗВЁРНУТОМУ значению из settings.json, а не только
# по этому файлу: путь в настройках можно опечатать, и тогда гейт молча не работает.
#
# ПЕРЕМЕННЫЕ:
#   TRIAGE_HOOK_AUDIT      — куда писать журнал решений (по умолч. /tmp/triage-gate-hook.log)
#   TRIAGE_HOOK_STRICT=1   — отвергать неперечисленное с причиной вместо «решения нет».
#     Нужно беспилотному профилю: иначе неперечисленная команда уходит в запрос и
#     прогон висит до утра. В интерактивном профиле оставлять выключенным.
#   TRIAGE_HOOK_DIGEST     — путь к дайджесту для верба deliver (по умолч. /tmp/frontier-triage-digest.md)

set -uo pipefail

AUDIT="${TRIAGE_HOOK_AUDIT:-/tmp/triage-gate-hook.log}"
STRICT="${TRIAGE_HOOK_STRICT:-0}"
DIGEST="${TRIAGE_HOOK_DIGEST:-/tmp/frontier-triage-digest.md}"
SSH_HOST="frontier-intelligence"

# Тот же закрытый список, что и в triage-ssh-gate.sh. Держать синхронно: расхождение
# даёт либо запрос разрешения на легальный вызов, либо разрешение того, что шлюз потом
# отвергнет — и то и другое читается как «петля сломалась».
ALLOWED_SERVICES="admin alertmanager crawl4ai gpt2giga-proxy grafana ingest mcp mcp-gateway neo4j node-exporter paddleocr postgres prometheus qdrant redis searxng worker xray"

audit() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$AUDIT" 2>/dev/null || true; }

emit_allow() {
  audit "allow: $1"
  printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow","permissionDecisionReason":"triage gate verb (closed set)"}}'
  exit 0
}

# Не решаем: обычный поток разрешений отработает как обычно. В строгом профиле —
# отвергаем, потому что в беспилотном прогоне «обычный поток» означает вечное ожидание.
no_decision() {
  if [ "$STRICT" = "1" ]; then
    audit "deny: $1"
    printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Беспилотная петля: разрешены только collect, logs <service> [n], deliver <send|skip>. Отвечать на запрос разрешения здесь некому, поэтому команда отклонена, а не поставлена в ожидание."}}'
  else
    audit "defer: $1"
  fi
  exit 0
}

# reconfigure(newline='') обязателен и найден трассировкой мутанта: под Windows print()
# переводит \n в \r\n, из-за чего многострочная команда приезжала как
# `ssh frontier-intelligence\r\ncollect`, слово хоста не совпадало и отказ выдавала
# ПРОВЕРКА ХОСТА вместо защиты от многострочности. Набор при этом был зелёным, измеряя
# не то, что заявлено, — а на Linux, где хук и живёт, повёл бы себя иначе.
payload=$(cat)
cmd=$(printf '%s' "$payload" | python3 -c 'import sys,json
sys.stdout.reconfigure(newline="")
print(json.load(sys.stdin).get("tool_input",{}).get("command",""))' 2>/dev/null) || cmd=""
[ -n "$cmd" ] || exit 0   # не Bash-вызов или пустая команда: не наше дело

# Многострочное отвергаем ДО любого сопоставления. Это не формальность: сравнение
# по строкам (grep -x и родня) считает совпадение, если совпала ЛЮБАЯ строка, поэтому
# команда из легальной первой строки и `whoami` во второй прошла бы проверку целиком.
case "$cmd" in
  *$'\n'*|*$'\r'*) no_decision "multiline: $cmd" ;;
esac

# Метасимволы отвергаем целиком, кроме '<' — он нужен вербу deliver, который ждёт
# дайджест на stdin. Дальше разбор идёт по СЛОВАМ, а не регуляркой по всей строке:
# после этой отсечки токен не может нести в себе вторую команду.
#
# ЭТА ОТСЕЧКА — ЭКВИВАЛЕНТНЫЙ МУТАНТ, и это проверено, а не предположено. Мутационный
# прогон 17.08.2026: из шести мутантов пять убиты набором, а удаление ЭТОЙ проверки
# выживает. Причина не в дырке набора: ниже каждый токен сравнивается с ТОЧНЫМ литералом,
# поэтому токен, содержащий метасимвол, не может быть равен ни 'collect', ни имени
# сервиса из списка, ни 'send'. Двенадцать состязательных строк, дающих легальную
# арность (`collect;whoami`, `$(collect)`, `${x-collect}`, `logs admin 5;id`,
# `deliver send < …;id` и прочие), на мутанте дали ноль разрешений.
# Оставлено осознанно: эшелонированность плюс внятный журнал (метасимвольная команда
# пишется как 'metachar', а не как 'unknown verb'), и страховка на случай, если кто-то
# позже ослабит сравнение токенов — тогда проверка снова станет несущей.
# НЕ УДАЛЯТЬ как «мёртвый код»: она мёртвая ровно пока точные сравнения ниже целы.
case "$cmd" in
  *';'*|*'&'*|*'|'*|*'`'*|*'$('*|*'${'*|*'>'*|*'('*|*')'*) no_decision "metachar: $cmd" ;;
esac

# set -f ОБЯЗАТЕЛЕН и найден мутационной проверкой: `set --` делает не только разбиение
# на слова, но и подстановку имён файлов, поэтому без него `logs *` подставился бы по
# содержимому текущего каталога, и имя случайного файла могло совпасть с сервисом из
# списка. Отсечка метасимволов этого не покрывает — глоб-символы в ней не перечислены.
set -f
# shellcheck disable=SC2086
set -- $cmd   # безопасно: метасимволы отсечены, подстановка имён выключена
[ "$1" = "ssh" ] || no_decision "not ssh: $cmd"
[ "${2:-}" = "$SSH_HOST" ] || no_decision "other host: $cmd"

case "${3:-}" in
  collect)
    [ "$#" -eq 3 ] || no_decision "collect with args: $cmd"
    emit_allow "$cmd"
    ;;
  logs)
    svc="${4:-}"
    [ -n "$svc" ] || no_decision "logs without service: $cmd"
    case " $ALLOWED_SERVICES " in
      *" $svc "*) : ;;
      *) no_decision "service not allowed: $cmd" ;;
    esac
    if [ "$#" -eq 4 ]; then
      emit_allow "$cmd"
    elif [ "$#" -eq 5 ]; then
      case "$5" in
        ''|*[!0-9]*) no_decision "non-numeric line count: $cmd" ;;
        *) [ "${#5}" -le 3 ] || no_decision "line count too long: $cmd"
           emit_allow "$cmd" ;;
      esac
    else
      no_decision "logs with extra args: $cmd"
    fi
    ;;
  deliver)
    # Ровно `ssh <host> deliver <send|skip> < <digest>` — шесть слов. Путь к дайджесту
    # фиксирован: иначе вербом можно было бы отправить произвольный файл.
    [ "$#" -eq 6 ] || no_decision "deliver arity: $cmd"
    case "${4:-}" in
      send|skip) : ;;
      *) no_decision "deliver mode: $cmd" ;;
    esac
    [ "${5:-}" = "<" ] || no_decision "deliver without stdin redirect: $cmd"
    [ "${6:-}" = "$DIGEST" ] || no_decision "deliver from unexpected path: $cmd"
    emit_allow "$cmd"
    ;;
  *)
    no_decision "unknown verb: $cmd"
    ;;
esac
