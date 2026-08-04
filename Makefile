.PHONY: up-core up-all up-ingest up-worker up-mcp up-admin up-monitor check-profiles down logs init shell ps restart

COMPOSE=docker compose

# Наборы профилей читаются из scripts/compose-profiles.sh — единственного места,
# где они объявлены. Раньше Makefile хранил свою копию, и она разъехалась:
# ALL_PROFILES без xray падал на `service "ingest" depends on undefined service "xray"`.
# Ошибка возникает при разборе проекта, то есть `make up-all` не поднимал НИЧЕГО.
PROFILES_FILE=scripts/compose-profiles.sh
ALL_PROFILES=$(shell . ./$(PROFILES_FILE) >/dev/null 2>&1; echo $$FRONTIER_PROFILES_FULL)
BUILD_PROFILES=$(shell . ./$(PROFILES_FILE) >/dev/null 2>&1; echo $$FRONTIER_PROFILES_BUILD)

# Точечные цели: worker, mcp и admin сами по себе depends_on: xray не объявляют,
# но admin — объявляет, поэтому у него набор шире, чем core+admin.
CORE_PROFILES=core
INGEST_PROFILES=core,ingest,xray
WORKER_PROFILES=core,worker
MCP_PROFILES=core,mcp
ADMIN_PROFILES=core,admin,xray
MONITOR_PROFILES=monitor

up-core:
	COMPOSE_PROFILES=$(CORE_PROFILES) $(COMPOSE) up -d

up-all:
	COMPOSE_PROFILES=$(ALL_PROFILES) $(COMPOSE) up -d

up-ingest:
	COMPOSE_PROFILES=$(INGEST_PROFILES) $(COMPOSE) up -d

up-worker:
	COMPOSE_PROFILES=$(WORKER_PROFILES) $(COMPOSE) up -d

up-mcp:
	COMPOSE_PROFILES=$(MCP_PROFILES) $(COMPOSE) up -d

up-admin:
	COMPOSE_PROFILES=$(ADMIN_PROFILES) $(COMPOSE) up -d

# Проверить, что все объявленные здесь наборы валидны, ничего не запуская.
check-profiles:
	@. ./$(PROFILES_FILE); \
	for s in "$(CORE_PROFILES)" "$(INGEST_PROFILES)" "$(WORKER_PROFILES)" \
	         "$(MCP_PROFILES)" "$(ADMIN_PROFILES)" "$(MONITOR_PROFILES)" \
	         "$(ALL_PROFILES)" "$(BUILD_PROFILES)"; do \
	  if COMPOSE_PROFILES="$$s" $(COMPOSE) config --services >/dev/null 2>&1; then \
	    echo "  OK       $$s"; \
	  else \
	    echo "  СЛОМАН   $$s"; exit 1; \
	  fi; \
	done

up-monitor:
	COMPOSE_PROFILES=$(MONITOR_PROFILES) $(COMPOSE) up -d

down:
	$(COMPOSE) --profile core --profile ingest --profile worker --profile mcp --profile admin --profile crawl --profile searxng --profile monitor down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

logs-%:
	$(COMPOSE) logs -f --tail=100 $*

restart-%:
	$(COMPOSE) restart $*

init:
	$(COMPOSE) $(CORE_PROFILES) up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	docker compose exec -T postgres psql -U frontier -d frontier -c "SELECT 1" > /dev/null 2>&1 || (echo "Waiting for postgres..." && sleep 10)
	python3 scripts/init_storage.py

shell-%:
	$(COMPOSE) exec $* /bin/sh

healthz:
	@curl -sf http://localhost:8090/v1/models && echo "gpt2giga-proxy: OK" || echo "gpt2giga-proxy: FAIL"
	@curl -sf http://localhost:6333/readyz && echo "qdrant: OK" || echo "qdrant: FAIL"
	@docker compose exec -T redis redis-cli ping | grep -q PONG && echo "redis: OK" || echo "redis: FAIL"

stream-info:
	docker compose exec redis redis-cli XLEN stream:posts:parsed
	docker compose exec redis redis-cli XINFO GROUPS stream:posts:parsed 2>/dev/null || echo "No consumer groups yet"
