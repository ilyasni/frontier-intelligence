#!/usr/bin/env bash
# Единственное определение наборов COMPOSE_PROFILES. Подключать через source.
#
# Зачем отдельный файл. Все 18 сервисов объявлены с директивой `profiles:`, а
# COMPOSE_PROFILES в серверном .env не задан — значит любая compose-команда без
# явного набора резолвится в ноль сервисов. Наборы приходилось повторять в каждой
# точке входа, они разъехались, и на 04.08.2026 три из них падали ещё до запуска:
#
#   scripts/server-deploy-rebuild.sh   core,worker,mcp,crawl,paddleocr
#       -> service "crawl4ai" depends on undefined service "xray"
#   Makefile ALL_PROFILES              core,ingest,worker,mcp,admin
#       -> service "ingest" depends on undefined service "xray"
#   .env.example (пример в комментарии) core,ingest,worker,crawl,paddleocr,mcp,admin
#       -> service "admin" depends on undefined service "xray"
#
# Причина у всех одна: `ingest`, `admin` и `crawl4ai` объявляют depends_on: xray,
# поэтому профиль xray нельзя выкинуть, даже если сам прокси в этой операции не нужен.
# Ошибка возникает на этапе разбора проекта, то есть ломается вся команда целиком,
# а не отдельный сервис.
#
# Использование:
#   . "$(dirname "$0")/compose-profiles.sh"      # даст COMPOSE_PROFILES по умолчанию
#   COMPOSE_PROFILES="$FRONTIER_PROFILES_BUILD" frontier_assert_profiles || exit 1

# Полный стек: всё, что объявлено в docker-compose.yml.
FRONTIER_PROFILES_FULL="core,ingest,xray,worker,crawl,paddleocr,mcp,admin,searxng,monitor"

# Сборка и раскатка сервисов с COPY-кодом. Без monitor и searxng — они не пересобираются.
FRONTIER_PROFILES_BUILD="core,ingest,xray,worker,crawl,paddleocr,mcp,admin"

# Точечные операции. Валидны потому, что ни worker, ни mcp не объявляют depends_on: xray.
FRONTIER_PROFILES_WORKER="core,worker"
FRONTIER_PROFILES_MCP="core,mcp"
FRONTIER_PROFILES_MONITOR="monitor"

export FRONTIER_PROFILES_FULL FRONTIER_PROFILES_BUILD \
       FRONTIER_PROFILES_WORKER FRONTIER_PROFILES_MCP FRONTIER_PROFILES_MONITOR

# Уважаем уже заданное значение: вызывающий мог выбрать набор осознанно.
export COMPOSE_PROFILES="${COMPOSE_PROFILES:-$FRONTIER_PROFILES_FULL}"

# Проверка набора ДО того, как что-то запустится. Без неё невалидный набор
# обнаруживается только в момент деплоя, когда откатываться дороже.
frontier_assert_profiles() {
  local set="${1:-$COMPOSE_PROFILES}"
  local out
  if ! out="$(COMPOSE_PROFILES="$set" docker compose config --services 2>&1)"; then
    echo "ОСТАНОВ: невалидный набор профилей: $set" >&2
    printf '%s\n' "$out" | head -3 >&2
    echo "Рабочие наборы объявлены в scripts/compose-profiles.sh" >&2
    return 1
  fi
  if [ -z "$out" ]; then
    echo "ОСТАНОВ: набор '$set' не даёт ни одного сервиса" >&2
    return 1
  fi
  return 0
}
