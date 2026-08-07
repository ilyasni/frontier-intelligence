# Снимок веток перед уборкой — 2026-08-07

Все ветки ниже **влиты в main**, то есть их содержимое из истории никуда
не девается — удаляется только указатель. Восстановить любую:

```bash
git branch <имя> <sha>          # локально
git push origin <sha>:refs/heads/<имя>   # в origin
```

Снимок сделан потому, что удаление ветки в origin рефлогом уже не
восстанавливается — рефлог живёт только локально.

| ветка | sha | последний коммит |
|---|---|---|
| origin/WORMSOFT | f1addca | 2026-05-05 Refine Wormsoft alerts and pipeline UX  |
| multi-llm-routing-rollout | 664489b | 2026-06-26 Normalize line endings to LF via .gitattributes (end CRLF churn)  |
| origin/multi-llm-routing-rollout | 664489b | 2026-06-26 Normalize line endings to LF via .gitattributes (end CRLF churn)  |
| docs/harness-saas-strategy | f8d7bbf | 2026-08-03 docs: harness / SaaS strategy pack (June 2026 research)  |
| fix/admin-graph-view-cytoscape-render | 22f6f01 | 2026-08-03 admin: fix blank Graph screen and main-thread freeze in cytoscape  |
| ops/daily-alert-triage-loop | 7a198e4 | 2026-08-03 ops: daily alert-triage loop (collect bundle + gated delivery)  |
| ops/s3-bucket-quota-tooling | 79b5049 | 2026-08-03 s3: bucket usage, lifecycle apply, dead-snapshot pruning  |
| origin/docs/harness-saas-strategy | f8d7bbf | 2026-08-03 docs: harness / SaaS strategy pack (June 2026 research)  |
| origin/fix/admin-graph-view-cytoscape-render | 22f6f01 | 2026-08-03 admin: fix blank Graph screen and main-thread freeze in cytoscape  |
| origin/ops/daily-alert-triage-loop | 7a198e4 | 2026-08-03 ops: daily alert-triage loop (collect bundle + gated delivery)  |
| origin/ops/s3-bucket-quota-tooling | 79b5049 | 2026-08-03 s3: bucket usage, lifecycle apply, dead-snapshot pruning  |
| docs/s3-backup-quota-runbook | c0f2301 | 2026-08-03 docs: S3 bucket quota and backup lifecycle runbook (QuotaExceeded)  |
| feat/provenance-independence-layer | 4c2671c | 2026-08-03 provenance: de-syndicated companions to source_count (schema+Neo4j)  |
| fix/llm-json-truncation-repair | 2614426 | 2026-08-03 llm_json: recover objects from responses truncated by max_tokens  |
| fix/openrouter-rate-limit-reset | 52c049a | 2026-08-03 openrouter: parse X-RateLimit-Reset as ms, cap quarantine at 24h  |
| origin/docs/s3-backup-quota-runbook | c0f2301 | 2026-08-03 docs: S3 bucket quota and backup lifecycle runbook (QuotaExceeded)  |
| origin/feat/provenance-independence-layer | 4c2671c | 2026-08-03 provenance: de-syndicated companions to source_count (schema+Neo4j)  |
| origin/fix/llm-json-truncation-repair | 2614426 | 2026-08-03 llm_json: recover objects from responses truncated by max_tokens  |
| origin/fix/openrouter-rate-limit-reset | 52c049a | 2026-08-03 openrouter: parse X-RateLimit-Reset as ms, cap quarantine at 24h  |
| feat/provenance-clustering | f798749 | 2026-08-03 provenance: clustering-side wiring for the de-syndication companions  |
| origin/feat/provenance-clustering | f798749 | 2026-08-03 provenance: clustering-side wiring for the de-syndication companions  |
| feat/source-freshness-metric | 64be67a | 2026-08-03 observability: frontier_source_freshness_hours per enabled source  |
| fix/enrichment-poison-pending | 3dfdab0 | 2026-08-03 enrichment: stop poison messages wedging the PEL forever  |
| fix/openrouter-credit-balance | b034518 | 2026-08-03 finops: read OpenRouter balance from /credits, not key limit_remaining  |
| fix/redis-stream-maxlen-oom | a780101 | 2026-08-03 redis: cap event-bus streams at 10k and raise maxmemory to 2gb  |
| origin/feat/source-freshness-metric | 64be67a | 2026-08-03 observability: frontier_source_freshness_hours per enabled source  |
| origin/fix/enrichment-poison-pending | 3dfdab0 | 2026-08-03 enrichment: stop poison messages wedging the PEL forever  |
| origin/fix/openrouter-credit-balance | b034518 | 2026-08-03 finops: read OpenRouter balance from /credits, not key limit_remaining  |
| origin/fix/redis-stream-maxlen-oom | a780101 | 2026-08-03 redis: cap event-bus streams at 10k and raise maxmemory to 2gb  |
| fix/silent-analysis-failure | 5eda35e | 2026-08-03 analysis: stop reporting failed clustering runs as success  |
| origin/fix/silent-analysis-failure | 5eda35e | 2026-08-03 analysis: stop reporting failed clustering runs as success  |
| fix/missing-signals-gap-normalization | 263edae | 2026-08-03 missing_signals: normalize gap arithmetic to [0,1], add counters  |
| origin/fix/missing-signals-gap-normalization | 263edae | 2026-08-03 missing_signals: normalize gap arithmetic to [0,1], add counters  |
| feat/auto-hmi-workspace | a812d88 | 2026-08-03 workspace: add auto_hmi, the sixth workspace (all sources disabled)  |
| feat/editorial-card-feedback | eb1d4cb | 2026-08-03 editorial: card_feedback table + 3 MCP tools for the weekly pick  |
| feat/own-stake-second-axis | 3ba2653 | 2026-08-03 search: own_stake second axis over the author's own corpus  |
| origin/feat/auto-hmi-workspace | a812d88 | 2026-08-03 workspace: add auto_hmi, the sixth workspace (all sources disabled)  |
| origin/feat/editorial-card-feedback | eb1d4cb | 2026-08-03 editorial: card_feedback table + 3 MCP tools for the weekly pick  |
| origin/feat/own-stake-second-axis | 3ba2653 | 2026-08-03 search: own_stake second axis over the author's own corpus  |
| chore/sync-exclude-hardening | e21c2dd | 2026-08-03 sync: stop --delete from wiping server-only artifacts; fix rsync syntax  |
| docs/env-example-provider-knobs | 70f6fa1 | 2026-08-03 docs(env): provider knobs for OpenRouter, Polza and vision routing  |
| fix/qdrant-backup-dead-collections | e98d035 | 2026-08-03 backup: skip superseded and excluded qdrant collections  |
| origin/chore/sync-exclude-hardening | e21c2dd | 2026-08-03 sync: stop --delete from wiping server-only artifacts; fix rsync syntax  |
| origin/docs/env-example-provider-knobs | 70f6fa1 | 2026-08-03 docs(env): provider knobs for OpenRouter, Polza and vision routing  |
| origin/fix/qdrant-backup-dead-collections | e98d035 | 2026-08-03 backup: skip superseded and excluded qdrant collections  |
| test/config-contract-regressions | 4458c8c | 2026-08-03 test: contract tests for the silent-failure classes, + fix the arXiv flag they found  |
| origin/test/config-contract-regressions | 4458c8c | 2026-08-03 test: contract tests for the silent-failure classes, + fix the arXiv flag they found  |
| chore/cron-debt-and-dead-telegram-filters | 206b7d1 | 2026-08-03 config: pay the cron debt and delete the dead telegram filters  |
| fix/searxng-empty-cache-and-alert-label | 9c31201 | 2026-08-03 searxng: short TTL for empty results, engine set in the cache key, honest metrics  |
| security/mcp-rest-loopback-only | d90d5cd | 2026-08-03 security: bind the MCP REST port to loopback (it has no auth)  |
| origin/chore/cron-debt-and-dead-telegram-filters | 206b7d1 | 2026-08-03 config: pay the cron debt and delete the dead telegram filters  |
| origin/fix/searxng-empty-cache-and-alert-label | 9c31201 | 2026-08-03 searxng: short TTL for empty results, engine set in the cache key, honest metrics  |
| origin/security/mcp-rest-loopback-only | d90d5cd | 2026-08-03 security: bind the MCP REST port to loopback (it has no auth)  |
| test/searxng-ttl-field-contract | a9e8436 | 2026-08-03 test(searxng): pin SEARXNG_EMPTY_CACHE_TTL as a declared field, not a getattr default  |
| origin/test/searxng-ttl-field-contract | a9e8436 | 2026-08-03 test(searxng): pin SEARXNG_EMPTY_CACHE_TTL as a declared field, not a getattr default  |
| chore/drop-own-corpus-from-init-storage | 5d972f5 | 2026-08-04 storage: stop creating the author-corpus collection at init  |
| origin/chore/drop-own-corpus-from-init-storage | 5d972f5 | 2026-08-04 storage: stop creating the author-corpus collection at init  |
| docs/audit-2026-08-04 | 303575a | 2026-08-04 docs: audit against the running stack, status marks, unfinished register  |
| fix/admin-reprocess-stream-cap | c64f1a5 | 2026-08-04 admin: cap the reprocess stream and stop the invalid escape in SQL  |
| fix/sync-pull-ps1-parses | 3c51aea | 2026-08-04 scripts: make sync-pull.ps1 parse at all  |
| origin/docs/audit-2026-08-04 | 303575a | 2026-08-04 docs: audit against the running stack, status marks, unfinished register  |
| origin/fix/admin-reprocess-stream-cap | c64f1a5 | 2026-08-04 admin: cap the reprocess stream and stop the invalid escape in SQL  |
| origin/fix/sync-pull-ps1-parses | 3c51aea | 2026-08-04 scripts: make sync-pull.ps1 parse at all  |
| docs/route-step0-done | 244948e | 2026-08-04 docs: mark step 0 of the route done, with what the run actually showed  |
| origin/docs/route-step0-done | 244948e | 2026-08-04 docs: mark step 0 of the route done, with what the run actually showed  |
| ops/alerting-second-path | 3092efb | 2026-08-04 alerting: a second delivery path that does not go through admin, plus a watchdog  |
| origin/ops/alerting-second-path | 3092efb | 2026-08-04 alerting: a second delivery path that does not go through admin, plus a watchdog  |
| docs/route-step1-done | f1b397b | 2026-08-04 docs: close step 1 — both delivery paths verified against the live stack  |
| origin/docs/route-step1-done | f1b397b | 2026-08-04 docs: close step 1 — both delivery paths verified against the live stack  |
| ops/restore-path-and-backup-metrics | a161023 | 2026-08-04 backup: write the restore half, prove it works, and put backups on the dashboard  |
| origin/ops/restore-path-and-backup-metrics | a161023 | 2026-08-04 backup: write the restore half, prove it works, and put backups on the dashboard  |
| ops/s3-backup-retention-one-day | 81b94a0 | 2026-08-04 s3: cut backup retention to one day — two never fit the quota  |
| origin/ops/s3-backup-retention-one-day | 81b94a0 | 2026-08-04 s3: cut backup retention to one day — two never fit the quota  |
| feat/disruption-clustering-ceiling | 0efce29 | 2026-08-04 clustering: give disruption a ceiling sized to its volume, and measure coverage  |
| origin/feat/disruption-clustering-ceiling | 0efce29 | 2026-08-04 clustering: give disruption a ceiling sized to its volume, and measure coverage  |
| docs/readme-rewritten-against-code | 620465e | 2026-08-04 docs: rewrite README against the code, and stop keeping copies that drift  |
| origin/docs/readme-rewritten-against-code | 620465e | 2026-08-04 docs: rewrite README against the code, and stop keeping copies that drift  |
| chore/compose-profiles-single-source | ff5b81a | 2026-08-04 compose: declare the profile sets once instead of five times  |
| origin/chore/compose-profiles-single-source | ff5b81a | 2026-08-04 compose: declare the profile sets once instead of five times  |
| fix/regressions-from-todays-changes | 61d9c5c | 2026-08-04 fix: repair the regressions today's own changes introduced  |
| origin/fix/regressions-from-todays-changes | 61d9c5c | 2026-08-04 fix: repair the regressions today's own changes introduced  |
| feat/rsi-approval-loop-in-gateway | da3b069 | 2026-08-04 mcp: expose the RSI approval loop through the gateway  |
| origin/feat/rsi-approval-loop-in-gateway | da3b069 | 2026-08-04 mcp: expose the RSI approval loop through the gateway  |
| fix/alert-rules-house-contract | 11d68ca | 2026-08-04 alerts: make today's new rules satisfy the house contract  |
| origin/fix/alert-rules-house-contract | 11d68ca | 2026-08-04 alerts: make today's new rules satisfy the house contract  |
| docs/register-progress-and-8102-decision | 418a840 | 2026-08-04 docs: record what today closed, and close the 8102 question for good  |
| origin/docs/register-progress-and-8102-decision | 418a840 | 2026-08-04 docs: record what today closed, and close the 8102 question for good  |
| docs/refuted-entries-need-recheck | 9ee5257 | 2026-08-04 docs: the two refuted entries are provisional, not settled  |
| origin/docs/refuted-entries-need-recheck | 9ee5257 | 2026-08-04 docs: the two refuted entries are provisional, not settled  |
| docs/route-ii-open-items | c25be26 | 2026-08-05 step 12, item 28: N copies of one article are not N confirmations  |
| main | c25be26 | 2026-08-05 step 12, item 28: N copies of one article are not N confirmations  |
| origin/docs/route-ii-open-items | c25be26 | 2026-08-05 step 12, item 28: N copies of one article are not N confirmations  |
| origin/main | c25be26 | 2026-08-05 step 12, item 28: N copies of one article are not N confirmations  |
| fix/audit-2026-08-06 | c501cb8 | 2026-08-06 clustering: three measurements that each contradicted the expected answer  |
| origin/fix/audit-2026-08-06 | c501cb8 | 2026-08-06 clustering: three measurements that each contradicted the expected answer  |
