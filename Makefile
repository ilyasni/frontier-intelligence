.PHONY: up-core up-all up-ingest up-worker up-mcp up-admin down logs init shell ps restart

COMPOSE=docker compose
CORE_PROFILES=--profile core
ALL_PROFILES=--profile core --profile ingest --profile worker --profile mcp --profile admin
MONITOR_PROFILES=--profile monitor

up-core:
	$(COMPOSE) $(CORE_PROFILES) up -d

up-all:
	$(COMPOSE) $(ALL_PROFILES) up -d

up-ingest:
	$(COMPOSE) $(CORE_PROFILES) --profile ingest up -d

up-worker:
	$(COMPOSE) $(CORE_PROFILES) --profile worker up -d

up-mcp:
	$(COMPOSE) $(CORE_PROFILES) --profile mcp up -d

up-admin:
	$(COMPOSE) $(CORE_PROFILES) --profile admin up -d

up-monitor:
	$(COMPOSE) $(MONITOR_PROFILES) up -d

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
