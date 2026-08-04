# Незаконченный и нереализованный функционал

> Сверено с рабочим стеком **2026-08-04**. Каждый пункт проверен на живом сервере
> (`ssh frontier-intelligence`), в БД и в коде — доказательства приведены прямо в пункте.
> Метод и полный разбор документации: [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).
>
> **Порядок работ — не этот список, а [маршрут](./AUDIT-2026-08-04.md#7-маршрут)** (заходы 0–5,
> утверждены 2026-08-04). Здесь фактура; там очередь и критерии готовности.
> Пункт 3 (MCP-шлюз 8102) выведен из очереди решением владельца — см.
> [принятые решения](./AUDIT-2026-08-04.md#8-принятые-решения).

Всего пунктов: **52**.

| Срез | Разбивка |
|---|---|
| Тяжесть | блокеры 5 · высокий 24 · средний 18 · низкий 5 |
| Состояние | работает частично 30 · только спроектировано 8 · код есть, не подключён 7 · брошено 3 · подключено, выключено флагом 3 · раскатано, не зафиксировано 1 |
| Слой | ops 18 · search 8 · enrichment 7 · ingest 7 · mcp 6 · graph 2 · provenance 2 · editorial 1 · docs 1 |
| Объём работ | S 20 · M 26 · L 6 |

**Как читать состояние.** `только спроектировано` — есть замысел/контракт, продюсера нет. `код есть, не подключён` — модуль написан, но его никто не вызывает. `подключено, выключено флагом` — работает, но выключено в проде. `работает частично` — отрабатывает, но с дырой, которую видно на данных. `раскатано, не зафиксировано` — живёт в проде мимо git. `брошено` — начато и оставлено.

---

## Блокеры (5)

### 1. Кластеризация видит максимум 400 постов на воркспейс в сутки — трендовый слой питается ~29% корпуса

`enrichment` · работает частично · объём M

- [ ] **Есть.** Полный конвейер semantic → trend → emerging работает и запускается по крону: worker/services/semantic_clustering.py:2474 run_semantic_clustering, admin/backend/scheduler.py:960-969 job run_semantic_clusters, per-workspace обёртка scheduler.py:674-678. За 7 дней в cluster_runs: stage='full' success 32, error 10.
- [ ] **Не хватает.** Выборка постов жёстко ограничена: semantic_clustering.py:2486-2494 → _fetch_posts(..., limit=max(semantic_cluster_max_posts, 50)), а semantic_cluster_max_posts=400 (shared/config.py:542, в .env и admin_runtime_settings не переопределён — psql: 0 строк по ключам '%cluster%'). Крон один раз в сутки: shared/config.py:435-438 ADMIN_SEMANTIC_CLUSTER_CRON='35 3 * * *' (живые cluster_runs стартуют 03:35–03:39). _fetch_posts (semantic_clustering.py:491-522) берёт ORDER BY published_at DESC LIMIT 400 из 30-дневного окна. Нет ни backfill-джобы, ни метрики покрытия, ни алерта.

<details><summary>Доказательства</summary>

psql: eligible за 24ч по воркспейсам → disruption 1086 / clustered ровно 400; ai_research 190/7; design 60/10. Итого по всем eligible-постам (embedding_status='done' AND relevance_score>=0.6 AND published_at IS NOT NULL): clustered 55308 / eligible 187944 = 29%. По месяцам публикации: 2026-04 10747/26073, 2026-05 3314/52091 (6.4%), 2026-06 6977/51475 (13.6%), 2026-07 26339/44326. Файлы: worker/services/semantic_clustering.py:2486-2494, :491-522, shared/config.py:542-543, :435-438

</details>

### 2. RSI-контур одобрения: 10 инструментов работают по REST, но не выведены в MCP-шлюз — нажать «approve» не из чего

`mcp` · подключено, выключено флагом · объём S

- [ ] **Есть.** mcp/tools/threshold_proposals.py (316 строк) и mcp/tools/graph_health.py (151) подключены роутерами в mcp/server.py:62-63 с tags=['rsi'] и отвечают по REST: `curl http://127.0.0.1:8100/tools` → 32 инструмента, среди них get_graph_health, list/approve/reject_entity_merge, list_threshold_proposals, approve/reject_threshold_change, list_underrated_signals, list/mark_relevance_audit. Данные для них накоплены: 86 pending entity_merge_proposals, 1762 weak-сигнала с judge_verdict.underrated=true, 26911 relevance_decisions.
- [ ] **Не хватает.** В mcp/mcp_gateway.py объявлено ровно 22 @mcp.tool (grep 'async def .*(' по файлу — строки 68..553), ни одного из этих десяти имён нет. Единственный MCP-клиент (шлюз 8102) их не видит. Следствие в БД: 86 entity_merge_proposals висят pending с 2026-07-07 по 2026-08-04, одобренных за это время ноль (последнее approved 2026-07-05); у всех 26911 relevance_decisions audit_status IS NULL — ни одного человеческого вердикта за всю историю.

<details><summary>Доказательства</summary>

mcp/mcp_gateway.py:62-553 (22 @mcp.tool); mcp/server.py:62-63; ssh: curl -sSL http://127.0.0.1:8100/tools → 32 имени; psql: select status,count(*),min(created_at),max(created_at) from entity_merge_proposals group by 1 → pending 86 (2026-07-07..2026-08-04), approved 179 (последний 2026-07-05); select audit_status,count(*) from relevance_decisions group by 1 → NULL | 26911; select count(*) filter (where (judge_verdict->>'underrated')::boolean) from weak_signal_snapshots → 1762

</details>

### 3. ~~MCP-шлюз 8102 остался единственной поверхностью без аутентификации~~ — ОТЛОЖЕНО

> **Решение владельца 2026-08-04: не актуально, отложено.** Хост в локальной сети
> (`192.168.31.222/24`), наружу не проброшен. Принятый риск и условия пересмотра —
> в [AUDIT-2026-08-04.md, раздел 8](./AUDIT-2026-08-04.md#8-принятые-решения).
> Пункт оставлен в реестре с фактурой, но из очереди работ выведен. Не поднимать заново
> без нового основания.

`mcp` · подключено, выключено флагом · объём M

- [ ] **Есть.** Шлюз работает и отдаёт 22 инструмента. Соседний REST-порт 8100 УЖЕ закрыт на loopback веткой security/mcp-rest-loopback-only (коммит d90d5cd, 2026-08-03) именно потому, что 'mcp/server.py has no authentication of any kind'. То есть проблема осознана, половина работы сделана.
- [ ] **Не хватает.** Аутентификация шлюза и/или бинд на loopback. Проверено вживую без единого credential: POST /mcp initialize → HTTP 200 + mcp-session-id, затем tools/list → 22 инструмента, среди них record_card_feedback (пишет в Postgres), list_card_feedback (отдаёт содержимое таблицы) и ingest_url (инициирует серверную загрузку). Транспорт не хардённый: allowed_hosts=['*'], allowed_origins=['*'], enable_dns_rebinding_protection=False. Сам автор закрывающего коммита записал в его теле: 'STILL OPEN, deliberately not changed here: mcp-gateway remains on 0.0.0.0:8102 carrying the same three tools.' Смягчающее: ingest_url прикрыт assert_public_http_url (mcp/tools/ingest_url.py:10), хост в LAN 192.168.31.222, не в интернете.

<details><summary>Доказательства</summary>

mcp/mcp_gateway.py:20-25 (host=0.0.0.0, allowed_hosts/origins=['*'], dns_rebinding=False); docker-compose.yml mcp-gateway ports ['8102:8102'] (для сравнения mcp — ['127.0.0.1:8100:8100']); `docker ps` → 0.0.0.0:8102->8102/tcp; `sudo ufw status` → Status: inactive; git show security/mcp-rest-loopback-only (d90d5cd) — секция 'STILL OPEN'; живой handshake на 127.0.0.1:8102 без токена → 200 + 22 инструмента

</details>

### 4. ✅ СДЕЛАНО 2026-08-04 — ~~Весь алертинг ходит через admin~~

> **Закрыто заходом 1.** У 11 критических правил появился второй путь доставки:
> receiver `telegram-direct` ходит в Telegram сам через socks5, минуя `admin`.
> Проверено `amtool config routes test`: критический алерт уходит в
> `telegram-direct,telegram-admin` — оба пути сразу. Плюс dead man's switch:
> правило `FrontierWatchdog` (firing всегда, глушится в blackhole) и внешний
> наблюдатель `scripts/alert-watchdog.sh` в cron каждые 10 минут.
> Подробности — [маршрут, заход 1](./AUDIT-2026-08-04.md#7-маршрут).

`ops` · работает частично · объём M

- [ ] **Есть.** Alertmanager настроен, default receiver = telegram-admin, blackhole только по явному notify="never". 56 правил, targets все up, cluster ready.
- [ ] **Не хватает.** Резервный путь доставки и dead-man's-switch. Единственный receiver — webhook на http://admin:8101/api/monitoring/alertmanager/webhook, то есть цепочка Prometheus → Alertmanager → admin → Telegram завязана на тот самый сервис, падение которого и надо сообщать (правило FrontierAdminDown, alerts.yml:531). Замер: increase(alertmanager_notifications_failed_total{integration="webhook"}[7d]) = 135 при increase(alertmanager_notifications_total[7d]) = 296 → 46% провалов. По дням: 2026-07-31 = 129, 2026-08-01 = 6 — ровно окно Redis-OOM, когда admin лежал. Прямого telegram_configs в Alertmanager нет, отдельного heartbeat/watchdog-правила нет.

<details><summary>Доказательства</summary>

prometheus/alertmanager.yml — receivers: только blackhole и telegram-admin (webhook_configs.url → admin:8101); PromQL increase(alertmanager_notifications_failed_total{integration="webhook"}[7d])=135 vs increase(...notifications_total...)=296; query_range по 21д → провалы только 2026-07-31 (129) и 2026-08-01 (6); prometheus/alerts.yml:531 FrontierAdminDown

</details>

### 5. Бэкапы делаются нотариально, но восстановление не написано и ни разу не проверено

`ops` · работает частично · объём L

- [ ] **Есть.** scripts/backup-stack.sh в cron 03:30 работает: последний прогон `=== backup OK 2026-08-04_033001 ===`, 6 файлов (postgres.dump 607 MB, neo4j_data.tar.gz 629 MB, 2 снапшота Qdrant, secrets_config.tar.gz, MANIFEST) выгружены в s3://bucket-467940/backups/2026-08-04/.
- [ ] **Не хватает.** Обратный путь целиком. В scripts/ нет ни одного файла с 'restore' в имени; в docs/ нет ни dr-restore-runbook.md, ни любого раннбука восстановления (ls docs/ — 30 записей, ничего про restore). В самом backup-stack.sh нет шага верификации дампа (единственное упоминание restore — комментарий 'crash-consistent, restore прогонит recovery'), то есть pg_restore --list или пробный разворот не выполняется никогда. WAL-архивации у postgres нет. Restore-drill не запланирован ни в crontab (там только backup-stack.sh и export-analysis-freshness.sh), ни где-либо ещё.

<details><summary>Доказательства</summary>

ssh: `ls scripts/ | grep -i restore` → пусто; `ls docs/ | grep -iE 'restore|dr-'` → пусто; `grep -iE 'restore|verify|pg_restore|--list' scripts/backup-stack.sh` → одна строка-комментарий; `crontab -l` → 2 задания; backups/cron.log tail → backup OK 2026-08-04

</details>

---

## Высокий приоритет (24)

### 6. Редакторская петля card_feedback односторонняя: 0 строк за сутки после раската, гейт калибровки в 60 пар не может сдвинуться в принципе

`editorial` · работает частично · объём M

- [ ] **Есть.** Раскатано 03.08.2026: таблица card_feedback (миграция storage/postgres/migrations/20260803_card_feedback.sql), три инструмента в mcp/tools/editorial.py (744 строки) — export_inbox_cards, record_card_feedback, list_card_feedback, все три выведены в шлюз (mcp_gateway.py:482,516,553). Инструмент честно отдаёт диагностику axes/axes_note/last_run/rows_before_window.
- [ ] **Не хватает.** select count(*) from card_feedback → 0. Ни одного потребителя разметки: grep 'card_feedback' по репозиторию даёт только запись/чтение самой таблицы, калибратора порогов нет (docstring editorial.py прямо говорит «Калибровка порогов по накопленной разметке — отдельное задание»). Обе оси карточки структурно пустые: _axes_availability (editorial.py:223-236) возвращает own_stake_at_pick=None для всех трёх видов карточек, а own_stake на хитах поиска недоступен из-за OWN_STAKE_ENABLED=false. То есть гейт «60 пар с обоими числами» (editorial.py:93, 282-283) не может набраться, пока не сделан пункт про own_stake.

<details><summary>Доказательства</summary>

psql: select count(*) from card_feedback → 0; mcp/tools/editorial.py:90-96 (COUNTS_NOTE про гейт 60 пар), :223-236 (_axes_availability), :239-284 (_axes_note); storage/postgres/migrations/20260803_card_feedback.sql; grep card_feedback → 12 файлов, потребителя-калибратора среди них нет

</details>

### 7. Vision запускается ДО relevance-гейта: 42% vision-обогащений сделаны для постов, которые тут же выбросили

`enrichment` · работает частично · объём S

- [ ] **Есть.** Vision-контур работает: enrichment публикует PostVisionEvent сразу после сохранения поста (worker/tasks/enrichment_task.py:721-737, комментарий прямо декларирует «Publish vision event before relevance check — vision is independent»), VisionTask обрабатывает, ставит vision_status и шлёт reindex (worker/tasks/vision_task.py:474-478).
- [ ] **Не хватает.** Гейт релевантности стоит ПОСЛЕ, поэтому платный vision (GigaChat/OpenRouter/Polza) и PaddleOCR отрабатывают на постах, которые через секунду получат embedding_status='dropped'. Результат не используется никогда: reindex_task.py:351-356 явно возвращает {'status':'skipped','reason':'embedding_status_not_done'} для всего, что не 'done'. Ни отложенной публикации vision-события после вердикта, ни отмены уже поставленного — нет.

<details><summary>Доказательства</summary>

psql: SELECT i.embedding_status, count(*) FROM post_enrichments pe JOIN indexing_status i ON i.post_id=pe.post_id WHERE pe.kind='vision' GROUP BY 1 → done 3365, dropped 2423, pending 1 (то есть 2423 из 5789 = 42% выброшены). Файлы: worker/tasks/enrichment_task.py:721-737 и :781-838 (drop-ветка идёт ниже публикации), worker/tasks/reindex_task.py:351-356

</details>

### 8. crawl4ai молча теряет ~41% задач: ни ретрая, ни записи в БД, ни метрики

`enrichment` · работает частично · объём M

- [ ] **Есть.** Crawl4AIService потребляет stream:posts:crawl, сохраняет kind='crawl' в post_enrichments и триггерит reindex (crawl4ai/crawl4ai_service.py:76-136). В БД 88319 crawl-обогащений, за 24ч сохранено 1068.
- [ ] **Не хватает.** Все неуспехи — тупик. crawl4ai_service.py:96-102 ловит исключение, пишет log.warning и идёт дальше; :133-136 при пустом results пишет log.info('No crawl results') и делает XACK. EnrichmentEngine.enrich_url (crawl4ai/enrichment_engine.py:239-292) возвращает None на rate-limit, 429, любой не-200 и на ошибку браузерного фетча — тоже только log.info. Нигде не пишется статус в post_enrichments/indexing_status, нет счётчика в shared/metrics.py (единственная crawl-метрика — frontier_crawl_session_recreates_total), нет DLQ и нет повторной попытки. Отличить «у поста нечего краулить» от «источник отдал 403» из данных невозможно.

<details><summary>Доказательства</summary>

ssh, docker logs frontier-intelligence-crawl4ai-1 --since 24h: 'Crawl enrichment saved' 1068, 'No crawl results' 422, 'Browser HTTP error' 472, 'HTTP error' 152, 'Rate limited' 111, 'Crawl failed' 0. Файлы: crawl4ai/crawl4ai_service.py:96-102, :133-136; crawl4ai/enrichment_engine.py:239-292

</details>

### 9. stream:posts:enriched — стрим, в который только пишут: 10024 события, ноль consumer-групп

`ingest` · код есть, не подключён · объём S

- [ ] **Есть.** EnrichmentTask объявляет STREAM_OUT='stream:posts:enriched' (worker/tasks/enrichment_task.py:28) и публикует туда событие на КАЖДОМ успешно обогащённом посте (enrichment_task.py:929-936: post_id, workspace_id, source_id, category, relevance_score, concept_count). Стрим живой и упирается в потолок тримминга.
- [ ] **Не хватает.** Подписчика нет ни одного. `XINFO GROUPS stream:posts:enriched` возвращает пустой список, `XLEN` = 10024 (ровно потолок STREAM_MAXLEN=10_000, shared/redis_client.py:19). Grep 'posts:enriched' по всему дереву вне docs даёт единственное совпадение — саму строку объявления. То есть точка fan-out для downstream-потребителей (аналитика, доставка, алерты) размечена, но ни один сервис на неё не подписан; событие просто вытесняется тримом.

<details><summary>Доказательства</summary>

ssh: `docker compose exec redis redis-cli XINFO GROUPS stream:posts:enriched` → пусто; `XLEN` → 10024. Grep 'posts:enriched' (glob !docs) → worker/tasks/enrichment_task.py:28. Живые стримы: KEYS 'stream:*' → parsed, crawl, vision, reindex, enriched

</details>

### 10. Воркспейс auto_hmi заведён и описан, но не наполняется: 1 из 10 источников включён, 3 поста, 0 трендов — все продуктовые инструменты по нему пусты

`ingest` · работает частично · объём S

- [ ] **Есть.** Воркспейс полностью описан в config/workspaces.yml:190-247 (6 категорий, threshold 0.55, ослабленные пороги кластеризации, persist_weak_signals: true), заведены 10 источников, кластеризация по нему отрабатывает успешно, есть 1 missing_signal.
- [ ] **Не хватает.** Включён ровно один источник — auto_rss_arxiv_cs_hc_automotive; девять (5 telegram ru, techcrunch transportation, insideevs, ieee spectrum, automotiveworld) выключены. Всего 3 поста в БД (published_at 2026-08-03..2026-08-04), 1 semantic_cluster, 1 emerging_signal, 0 trend_clusters, 0 записей в weak_signal_snapshots кроме одной. post_candidates последнего прогона = 1. Любой вызов search_frontier/list_clusters/search_trend_clusters с workspace='auto_hmi' вернёт пусто, и отличить это от поломки клиенту нечем.

<details><summary>Доказательства</summary>

psql: select id,source_type,is_enabled from sources where workspace_id='auto_hmi' → 1 t / 9 f; select workspace_id,count(*) from posts where workspace_id='auto_hmi' group by 1 → 3; select workspace_id,summary from cluster_runs ... → auto_hmi "post_candidates":1; config/workspaces.yml:190-247

</details>

### 11. Три MCP-инструмента читают по id без скоупа воркспейса, два пишущих доступны без аутентификации на 0.0.0.0:8102

`mcp` · работает частично · объём M

- [ ] **Есть.** Гвард mcp.guards.assert_known_workspace применяется в 7 местах observability.py (161,265,332,412,626,654,856) и во всех search-инструментах; в БД workspace_id — обязательная колонка везде.
- [ ] **Не хватает.** ClusterEvidenceRequest (observability.py:43-46), ClusterDetailsRequest (:60-62) и MissingSignalDetailsRequest (:65-66) не содержат поля workspace вовсе, а обработчики get_cluster_evidence (:717) и get_cluster_details не вызывают assert_known_workspace — SELECT идёт `WHERE id = :id` без фильтра по воркспейсу. Шлюз при этом опубликован как 0.0.0.0:8102 без единой проверки токена (mcp/mcp_gateway.py:18-27: allowed_hosts=['*'], allowed_origins=['*'], enable_dns_rebinding_protection=False; ни Depends, ни middleware), workspace приходит от клиента как есть (shared/search_contracts.py: default='disruption'), и среди 22 инструментов шлюза два пишущих — record_card_feedback (INSERT в card_feedback) и ingest_url (XADD в Redis). Для сравнения сам mcp опубликован на 127.0.0.1:8100.

<details><summary>Доказательства</summary>

mcp/tools/observability.py:43-46,60-66,717-749 (нет assert_known_workspace и нет фильтра по workspace_id); mcp/mcp_gateway.py:18-27,244,516; ssh: docker ps → mcp-gateway '0.0.0.0:8102->8102/tcp', mcp '127.0.0.1:8100->8100/tcp'

</details>

### 12. DLQ объявлен и подключён к коду, но невидим и неразгребаем: ни консьюмера, ни метрики, ни алерта, ни экрана

`ops` · работает частично · объём M

- [ ] **Есть.** Полноценный poison-detection в enrichment: shared/config.py:534 indexing_dlq_stream='stream:posts:parsed:dlq' + vision_dlq_stream; worker/tasks/enrichment_task.py:602-638 _drop_poison_message (XADD в DLQ с payload, error, delivery_count + XACK), :640-653 _drop_poison_pending с порогом indexing_max_deliveries. Логика написана после инцидента с застрявшими disabled-source событиями и покрыта тестами (tests/test_enrichment_task.py:119,392).
- [ ] **Не хватает.** Ничего не читает DLQ и никто о нём не узнает: ключа stream:posts:parsed:dlq в Redis нет вообще (`EXISTS` → 0, `KEYS '*dlq*'` → пусто), consumer-группы нет, счётчика в shared/metrics.py нет, правила в prometheus/alerts.yml нет (слово DLQ встречается только в тексте описания FrontierRedisStreamPendingHigh, alerts.yml:392), в admin/backend/routers/ нет ни одного роута для просмотра/re-drive. Механизм «сохранить и разобрать позже» существует только в половине «сохранить».

<details><summary>Доказательства</summary>

ssh: `redis-cli EXISTS stream:posts:parsed:dlq` → 0; `redis-cli KEYS '*dlq*'` → пусто. Grep 'dlq' (case-insens) по репозиторию: shared/config.py:534, worker/tasks/enrichment_task.py:617, worker/tasks/vision_task.py, prometheus/alerts.yml:392 (только текст), admin/backend/routers/settings.py:302 (только отдача значения в конфиг). Роутов и метрик нет.

</details>

### 13. У пайплайна нет ни одного счётчика стадий: пропускную способность и долю дропов измерить нечем

`ops` · работает частично · объём M

- [ ] **Есть.** Метрик много — 78 семейств на /metrics воркера. Есть подробная телеметрия LLM и провайдеров (frontier_llm_requests_total, frontier_gigachat_*, frontier_openrouter_*), свежесть данных frontier_last_post_age_seconds и состояние стримов frontier_redis_stream_lag / _pending / _oldest_pending_age_seconds / _consumer_idle_seconds (shared/metrics.py:335-358).
- [ ] **Не хватает.** Ни одного счётчика самого конвейера: нет posts_ingested, posts_enriched, posts_dropped_by_relevance, qdrant_upserts, neo4j_writes, crawl_results, vision_results, indexing_errors. Из-за этого 39% дропа по релевантности (122034 из 312041), 41% потерь краула и 400-й потолок кластеризации не видны ни на одном дашборде и не могут стать алертом — деградация обнаруживается только когда данные вообще перестают поступать (FrontierNoNewPosts, 6–24ч).

<details><summary>Доказательства</summary>

ssh, worker:9090/metrics → 78 семейств, среди них нет ни одного с корнем posts_/ingest_/enrich_/index_. Grep 'crawl|vision|ingest|stream' по shared/metrics.py даёт только CRAWL_SESSION_RECREATES_TOTAL и блок REDIS_STREAM_*. psql: indexing_status по embedding_status → done 189997, dropped 122034, pending 7, error 3

</details>

### 14. Redis, Postgres и Neo4j не скрейпятся Prometheus — отказ хранилища виден только по вторичным признакам через 6–24 часа

`ops` · работает частично · объём M

- [ ] **Есть.** Мониторинг развёрнут и работает: 8 активных таргетов, все up (worker, ingest, crawl4ai, mcp, admin, qdrant, alertmanager, node-exporter), 56 правил в prometheus/alerts.yml, алерты на падение сервисов FrontierCoreServiceDown (alerts.yml:495-504, up{job=~"worker|ingest|qdrant"}), FrontierControlPlaneServiceDown, FrontierSupportServiceDown.
- [ ] **Не хватает.** В prometheus/prometheus.yml нет job'ов для redis, postgres и neo4j — ни экспортёров, ни self-metrics. Соответственно нет ни up{job="redis"}, ни метрики used_memory: Redis-OOM 31.07.2026, который положил ingest+enrichment на 9 часов при lag/pending=0, до сих пор нечем поймать. Приняты только смягчения (STREAM_MAXLEN=10k в shared/redis_client.py:19, maxmemory 2g), детекция не сделана. Postgres и Neo4j в том же положении.

<details><summary>Доказательства</summary>

ssh: curl 127.0.0.1:9090/api/v1/targets → 8 таргетов, redis/postgres/neo4j отсутствуют. prometheus/prometheus.yml: scrape_configs = worker, ingest, crawl4ai, mcp, admin, qdrant, alertmanager, node-exporter. redis-cli INFO memory → used_memory 255.99M при maxmemory 2147483648, maxmemory-policy volatile-ttl

</details>

### 15. Мониторинг свежести аналитики не покрывает trend_clusters, missing_signals и card_feedback — трендовая тишина в пяти воркспейсах никем не замечена

`ops` · работает частично · объём S

- [ ] **Есть.** После инцидента 31.07-02.08 сделан textfile-экспортер scripts/export-analysis-freshness.sh и два алерта FrontierAnalysisStale / FrontierAnalysisStaleCritical (48ч/96ч) — они реально работают, 12 живых серий, возраст 2.6-2.7ч по всем воркспейсам.
- [ ] **Не хватает.** SQL экспортера охватывает ровно две таблицы: emerging_signals и semantic_clusters. trend_clusters, missing_signals и card_feedback не отслеживаются. Именно поэтому 21 день без единого тренда в design и 10 дней в ai_products_media прошли молча — метрика по этим воркспейсам всё это время показывала свежесть 2-3 часа. Тот же класс молчаливого отказа, против которого экспортер и вводился.

<details><summary>Доказательства</summary>

ssh: cat scripts/export-analysis-freshness.sh — SELECT 'emerging_signals' ... UNION ALL SELECT 'semantic_clusters' ... (третьей таблицы нет); prometheus/alerts.yml FrontierAnalysisStale expr по frontier_analysis_last_update_timestamp_seconds; curl 9090/api/v1/query frontier_analysis_last_update_timestamp_seconds → 12 серий, только emerging_signals+semantic_clusters, все ~2.6ч

</details>

### 16. Продуктовый слой без телеметрии: ни одной метрики вызовов MCP-инструментов, латентности и ошибок; у шлюза 8102 /metrics вообще нет

`ops` · только спроектировано · объём M

- [ ] **Есть.** Сервис mcp — цель Prometheus (http://mcp:8100/metrics, health=up) и экспортирует общие LLM-метрики: по job='mcp' видно task='mcp_synthesis'. Есть 56 алертов в prometheus/alerts.yml.
- [ ] **Не хватает.** В shared/metrics.py нет ни одного счётчика/гистограммы про MCP-инструменты: поиск 'duration' по файлу не даёт ни одного имени, метрик вида rag_query_duration_seconds / trend_clusters_detected_total / missing_signals_detected_total (заявлены в .cursor/rules/02-tech-stack.mdc:126 и 06-devops.mdc:133) не существует. Ни один из 56 алертов не покрывает поверхность доставки. У mcp-gateway эндпоинта метрик нет физически: curl 127.0.0.1:8102/metrics → 404, и в списке scrape-целей Prometheus его нет. Единственный источник данных о реальном использовании — логи контейнера: за 7 суток на REST прошло 2 вызова (оба /tools/list_emerging_signals), на /mcp — 16 POST, преимущественно ListToolsRequest.

<details><summary>Доказательства</summary>

ssh: curl 9090/api/v1/targets → 8 целей, mcp-gateway отсутствует; curl 127.0.0.1:8102/metrics → HTTP 404; docker logs frontier-intelligence-mcp-1 --since 168h | grep 'POST /tools' → 2 строки; grep 'duration' shared/metrics.py → 0; .cursor/rules/02-tech-stack.mdc:126

</details>

### 17. У бэкапов и квоты S3 нет ни метрики, ни алерта — при 13.1 из ~15 GiB отказ будет молчаливым

`ops` · только спроектировано · объём S

- [ ] **Есть.** Механизм для дешёвой метрики уже развёрнут и работает: node-exporter смонтирован с ./prometheus/textfile:/var/lib/node_exporter/textfile и флагом --collector.textfile.directory, а через него уже живёт frontier_analysis_last_update_timestamp_seconds (cron */10, алерт FrontierAnalysisStale). Есть scripts/s3_bucket_usage.py, считающий занятое место.
- [ ] **Не хватает.** Ни одного правила про бэкапы или S3 среди 56: `grep -iE 'backup|s3|restore' prometheus/alerts.yml` → пусто (есть только FrontierHostRootDiskFilling/Critical про корень хоста). Замер сейчас: 13.1 GiB из квоты ~15 GiB, в бакете 3 дня бэкапов по 3.5-3.6 GiB — свободного места меньше одного суточного бэкапа. Когда backup-stack.sh упадёт на переполнении квоты или на выгрузке, узнать об этом будет неоткуда: cron пишет только в backups/cron.log, метрики свежести бэкапа нет.

<details><summary>Доказательства</summary>

prometheus/alerts.yml (56 правил, ни одного backup/s3); docker-compose.yml node-exporter volumes ['./prometheus/textfile:/var/lib/node_exporter/textfile:ro'] + command --collector.textfile.directory; s3_bucket_usage.py → 'TOTAL: 28456 objects, 13.1GiB', backups 10.6GiB, days=3 по ~3.5GiB

</details>

### 18. Петля alert-triage мертва с 2026-08-03, и о её смерти не сигнализирует ничто

`ops` · работает частично · объём S

- [ ] **Есть.** Задача FrontierAlertTriage заведена в Task Scheduler (State: Ready, NextRunTime 04.08.2026 9:15), скрипты scripts/alert-triage-collect.sh и alert-triage-deliver.sh на месте, дайджесты копятся в docs/ops/alert-digests/, раннбук docs/runbooks/alert-triage-daily.md описывает процедуру.
- [ ] **Не хватает.** Сигнал живости самой петли. LastTaskResult = 3221225786 (0xC000013A, STATUS_CONTROL_C_EXIT); .claude/alert-triage.log обрывается строкой '===== [2026-08-03 09:15:02] alert-triage start (/alert-triage) =====' без парной 'exit='. Последний дайджест на сервере — 2026-08-02.md, и это не первый сбой: в ряду отсутствуют 2026-07-24, 07-27, 07-31. Ни в 56 правилах Prometheus, ни в textfile-коллекторе нет метрики свежести дайджеста — ровно тот класс молчаливого отказа, против которого в проекте уже вводили FrontierAnalysisStale.

<details><summary>Доказательства</summary>

Get-ScheduledTaskInfo FrontierAlertTriage → LastRunTime 03.08.2026 9:15:01, LastTaskResult 3221225786; D:\Workspace\frontier-intelligence\.claude\alert-triage.log (хвост — start без exit); ssh `ls docs/ops/alert-digests/` → последний 2026-08-02.md, пропуски 07-24/07-27/07-31; grep по prometheus/alerts.yml на digest/triage → пусто

</details>

### 19. ✅ СДЕЛАНО 2026-08-04 — ~~Месяц продовых правок не влит в main~~

> **Закрыто заходом 0.** `main` перемотан `595a977` → `244948e` одним fast-forward
> (стопка оказалась строго линейной, 0 merge-коммитов) и запушен. Заодно впервые
> за месяц отработал CI: перед слиянием прогнаны тесты (908 passed, 0 failed) и
> ruff. Ветки остаются на месте как история тем.

`ops` · работает частично · объём L

- [ ] **Есть.** Все 26 веток ЗАПУШЕНЫ в origin — исходная гипотеза про «27 незапушенных веток» не подтвердилась: посравнению rev-parse каждая ветка SYNCED с origin/<той же>. Это уже не риск потери кода.
- [ ] **Не хватает.** Слияние. Продовый чекаут стоит на ветке test/searxng-ttl-field-contract, `git rev-list --count main..HEAD` = 26, `git merge-base --is-ancestor <ветка> main` даёт UNMERGED для всех 26. Ветки уложены стопкой, а не параллельно: HEAD содержится ровно в одной ветке. Следствия: (а) `git checkout main` на рабочей копии сервера откатит месяц исправлений (Redis-OOM maxlen, poison-pending, silent-analysis-failure, openrouter rate-limit reset и т.д.); (б) origin/main как источник правды показывает состояние на 3 июля; (в) любой процесс, привязанный к merge — например Context7-гейт из docs/llm-orchestrator-context7-gate.md — не может сработать в принципе.

<details><summary>Доказательства</summary>

ssh git: `git rev-parse --abbrev-ref HEAD` → test/searxng-ttl-field-contract; `git rev-list --count main..HEAD` → 26; цикл сравнения → 26×'SYNCED UNMERGED', main и multi-llm-routing-rollout — 'SYNCED MERGED'; `git log -1 main` → 595a977-эпоха 2026-07-03; `git branch --contains HEAD` → одна ветка

</details>

### 20. ✅ СДЕЛАНО 2026-08-04 — ~~Незакоммиченный код, раскатанный в образ~~

> **Закрыто заходом 0.** Правка `pipeline.py` (`maxlen=STREAM_MAXLEN` в ручном
> reprocess — защита от Redis-OOM) закоммичена веткой `fix/admin-reprocess-stream-cap`
> вместе с `rf`-строками в `posts.py`. Пересборка больше её не снимет.
> В дереве намеренно остаются `mcp/Dockerfile` и `mcp/tools/ingest_url.py`
> (чистый CRLF, ноль содержательных строк) и `admin/frontend-legacy.html`.

`ops` · раскатано, не зафиксировано · объём S

- [ ] **Есть.** Правка admin/backend/routers/pipeline.py: ручной reprocess пишет в стрим с явным потолком (`await client.xadd("stream:posts:parsed", event, maxlen=STREAM_MAXLEN, approximate=True)` вместо голого xadd) — это защита ровно от того Redis-OOM, что положил стек 31.07 на 9 часов. Плюс rf-строки в posts.py/pipeline.py. Правка присутствует в работающем контейнере: `docker exec admin grep -c STREAM_MAXLEN /app/admin/backend/routers/pipeline.py` → 3.
- [ ] **Не хватает.** Коммит. `git status --short` в /opt/frontier-intelligence: ' M admin/backend/routers/pipeline.py', ' M admin/backend/routers/posts.py', плюс неотслеживаемый admin/frontend-legacy.html (161 КБ). Любая пересборка из git HEAD молча вернёт xadd без maxlen и снимет защиту. (Диффы mcp/Dockerfile и mcp/tools/ingest_url.py, наоборот, чисто CRLF-шумовые — содержательных изменений там нет.)

<details><summary>Доказательства</summary>

ssh `git status --short` (5 строк); `git diff -- admin/backend/routers/pipeline.py` — импорт shared.redis_client.STREAM_MAXLEN и xadd(..., maxlen=STREAM_MAXLEN, approximate=True) в _queue_post_reprocess; `docker exec frontier-intelligence-admin-1 grep -c STREAM_MAXLEN ...` → 3

</details>

### 21. Три из четырёх точек входа деплоя собраны с невалидным набором compose-профилей

`ops` · код есть, не подключён · объём S

- [ ] **Есть.** Рабочий эталон один — scripts/server-build-stack.sh с COMPOSE_PROFILES="core,ingest,xray,worker,crawl,paddleocr,mcp,admin". Он проходит валидацию.
- [ ] **Не хватает.** Тот же набор в остальных трёх. Проверено прямо на сервере, все три падают на этапе `docker compose config`: (1) scripts/server-deploy-rebuild.sh — COMPOSE_PROFILES=core,worker,mcp,crawl,paddleocr → 'service "crawl4ai" depends on undefined service "xray": invalid compose project'; (2) Makefile ALL_PROFILES=--profile core --profile ingest --profile worker --profile mcp --profile admin → 'service "ingest" depends on undefined service "xray"'; (3) `--profile monitor` (команда раскатки Grafana из docs/monitoring-runtime-dashboard.md) → 'service "alertmanager" depends on undefined service "admin"'. При этом docs/ops-server-troubleshooting.md §5 предлагает именно server-deploy-rebuild.sh как основную практику деплоя правок кода.

<details><summary>Доказательства</summary>

ssh: `COMPOSE_PROFILES=core,worker,mcp,crawl,paddleocr docker compose config --services` → invalid; `docker compose --profile core --profile ingest --profile worker --profile mcp --profile admin config --services` → invalid; `docker compose --profile monitor config --services` → invalid; grep Makefile ALL_PROFILES; grep COMPOSE_PROFILES scripts/server-build-stack.sh (эталон)

</details>

### 22. Хостового файрвола нет, а бинды портов разъехались: три сервиса на 0.0.0.0, остальные на loopback

`ops` · работает частично · объём M

- [ ] **Есть.** Работа по сужению поверхности начата и заметно продвинута: mcp, prometheus, alertmanager, neo4j, qdrant, paddleocr, gpt2giga-proxy опубликованы как 127.0.0.1:<port>.
- [ ] **Не хватает.** Доведение до конца и хотя бы один слой сетевого контроля. `ufw status` → Status: inactive, правил INPUT нет (-P INPUT ACCEPT). На 0.0.0.0 остаются grafana ['3000:3000'], admin ['8101:8101'], mcp-gateway ['8102:8102'] — то есть весь сегмент 192.168.31.0/24 видит админку и шлюз. Отдельно: TLS-терминации в стеке нет принципиально (Caddy исключён), админка ходит по plain HTTP, а сессионная кука ставится без secure=True (только httponly=True, samesite="lax") — .cursor/rules/00-principles.mdc требует 'HTTPS + secure cookies в продакшне'. Grafana хотя бы не на дефолтных креденшелах (admin:admin → 401).

<details><summary>Доказательства</summary>

ssh `sudo ufw status` → inactive; `sudo iptables -S` → -P INPUT ACCEPT; `ip -4 addr` → 192.168.31.222/24; docker-compose.yml grafana/admin/mcp-gateway ports без префикса 127.0.0.1; admin/backend/main.py set_cookie(httponly=True, samesite="lax", max_age=_SESSION_TTL, path="/") — без secure; curl -u admin:admin http://127.0.0.1:3000/api/org → 401

</details>

### 23. ✅ СДЕЛАНО 2026-08-04 — ~~Токен вебхука в query string; fail-open при пустом значении~~

> **Закрыто заходом 1.** Токен уехал из query string в Basic-auth
> (`http_config.basic_auth` у receiver'а `telegram-admin`) — в логах `admin`
> он больше не появляется. Fail-open заменён на 503 при пустом токене;
> образ `admin` пересобран и раскатан (`/api/health` = 200, `RestartCount=0`).
> Проверено после пересборки: верный Basic → 200, без токена → 403.

`ops` · работает частично · объём S

- [ ] **Есть.** Контроль доступа на эндпоинте есть и сейчас включён: ALERTMANAGER_WEBHOOK_TOKEN задан (длина 43 в контейнере admin), проверка _assert_alertmanager_token умеет читать и Basic-auth, и заголовок x-alertmanager-token.
- [ ] **Не хватает.** Перевод на заголовок и закрытие fail-open. Фактически Alertmanager шлёт токен параметром URL, и uvicorn логирует полный путь: в `docker logs admin` строки вида 'POST /api/monitoring/alertmanager/webhook?token=WAOPEHk9swvMg3VrJafD0vYBJdbS7DbbAlEsiASVwQk HTTP/1.1 200 OK' — секрет в открытых логах, ротации логов на демоне не настроено (/etc/docker/daemon.json без log-opts). Второе: admin/backend/routers/monitoring.py — `expected = settings.alertmanager_webhook_token.strip(); if not expected: return`, то есть при пустой переменной единственный контроль доступа на этом эндпоинте молча отключается, а не отказывает.

<details><summary>Доказательства</summary>

ssh `docker logs frontier-intelligence-admin-1 --since 24h | grep alertmanager` → 4 строки с token= в URL; admin/backend/routers/monitoring.py:63-67 (ранний return при пустом токене); prometheus/alertmanager.yml webhook_configs.url; cat /etc/docker/daemon.json (нет log-opts)

</details>

### 24. RSI-метрики объявлены, но не отдают ни одной серии — два алерта на них не сработают никогда

`ops` · код есть, не подключён · объём M

- [ ] **Есть.** Имена зарегистрированы и видны в экспозиции: на http://127.0.0.1:8101/metrics присутствуют '# HELP/# TYPE frontier_novelty_judge_total', 'frontier_relevance_audit', 'frontier_graph_health'. На них уже построены дашборд grafana/dashboards/frontier-rsi.json и группа правил frontier_rsi (7 алертов).
- [ ] **Не хватает.** Экспорт значений из дочерних процессов. Ни одного сэмпла: `curl /metrics | grep '^frontier_(graph_health|relevance_audit|novelty_judge)'` → пусто, и в Prometheus count() по всем трём = 0. Причина — значения выставляются внутри субпроцесса планировщика (admin/backend/scheduler.py _run_job_subprocess → admin.backend.manual_jobs), а prometheus_client multiprocess-режим или pushgateway не подключены. Следствие: FrontierGraphDuplicateClustersRising (alerts.yml:542) и FrontierNoveltyJudgeFailing (alerts.yml:552) физически не могут перейти в firing — это дырка в наблюдаемости, замаскированная под работающий мониторинг.

<details><summary>Доказательства</summary>

ssh `curl -sS http://127.0.0.1:8101/metrics | grep -E '^# (HELP|TYPE) frontier_(graph_health|relevance_audit|novelty_judge)'` → 6 строк; тот же grep без '#' → пусто; PromQL count(frontier_graph_health)=0, count(frontier_relevance_audit)=0, count(frontier_novelty_judge_total)=0 (для контроля count(frontier_llm_requests_total)=17); prometheus/alerts.yml:542,552

</details>

### 25. Слой провенанса/независимости не доходит ни до одной поисковой поверхности — только до трёх Postgres-инструментов observability

`provenance` · работает частично · объём M

- [ ] **Есть.** shared/provenance.py считает deduped_source_count, distinct_voices, echo_ratio, arrival_dispersion, distinct_originators, independence_score; колонки есть в trend_clusters, emerging_signals, semantic_clusters (миграция 20260714_provenance_dedup.sql); mcp/tools/observability.py:421,443,462,635,767,810,842 выбирает и отдаёт independence_score в list_clusters / list_emerging_signals / get_cluster_details.
- [ ] **Не хватает.** shared.provenance импортируется РОВНО одним модулем — worker/services/semantic_clustering.py:16; в mcp/ его нет вообще. В payload Qdrant-коллекции trend_clusters 39 ключей и ни одного провенансного (проверено scroll: burst_score, source_count, source_diversity_score есть — independence_score, distinct_originators, echo_ratio нет), поэтому search_trend_clusters физически не может вернуть независимость. frontier_brief._compact_workspace (frontier_brief.py:67-104) вырезает её из трендов и emerging при сборке брифа. search_frontier / search_balanced работают на уровне постов, где провенанса нет вовсе.

<details><summary>Доказательства</summary>

grep 'from shared.provenance' по репозиторию → worker/services/semantic_clustering.py:16 (единственное); qdrant scroll trend_clusters__embeddingsgigar__dense_2560 → список 39 ключей payload без independence_*; mcp/tools/frontier_brief.py:67-104; mcp/tools/observability.py:767,810,842

</details>

### 26. independence_score проставлен у 16 из 374 trend_clusters, остальные отдаются как 0.0 — «не измерено» неотличимо от «полностью синдицировано»

`provenance` · работает частично · объём S

- [ ] **Есть.** Колонки провенанса добавлены миграцией 20260714_provenance_dedup.sql с DEFAULT 0.0; расчёт заработал в проде 2026-08-02 (первые заполненные строки этой даты). Для emerging_signals покрытие заметно лучше — 6156 из 32853 с independence_score>0, 881 с distinct_originators.
- [ ] **Не хватает.** Бэкфилла исторических строк не было. По trend_clusters: 16 из 374 имеют deduped_source_count>0, 8 — distinct_originators, 9 — echo_ratio; у 358 строк (детекция 2026-03-28..2026-07-31) все поля лежат в дефолте, independence_score=0.0. Инструменты list_clusters/get_cluster_details отдают этот 0.0 без признака «не измерено», а сам документ провенанса запрещает доверять independence_score до валидации на размеченном наборе — набора в репозитории нет (Glob по tests/ и scripts/ не даёт файла с ручной разметкой independent/syndicated).

<details><summary>Доказательства</summary>

psql: select (deduped_source_count>0) has_prov,count(*),min(detected_at)::date,max(detected_at)::date from trend_clusters group by 1 → f|358|2026-03-28|2026-07-31, t|16|2026-08-02|2026-08-04; select count(*),count(distinct_originators),count(*) filter (where echo_ratio>0) from trend_clusters → 374|8|9; shared/provenance.py:19 (порог дедупа 0.60 заимствован, не измерен)

</details>

### 27. own_stake (вторая ось «свой замер») построена целиком, но выключена флагом и личный корпус не проиндексирован — квадранты недостижимы

`search` · подключено, выключено флагом · объём M

- [ ] **Есть.** Полная реализация: mcp/tools/search_frontier.py:253-412 (_own_stake_enabled, _stake_quadrant с квадрантами post/run_your_own/personal_blind_spot/noise, _own_corpus_size, _attach_own_stake), слияние блоков в search_balanced.py:_merge_own_corpus, клиент Qdrant worker/integrations/qdrant_client.py:651-720 (upsert_own_corpus_chunk, own_corpus_size), скрипт заливки scripts/index_own_corpus.py на 1422 строки, настройки shared/config.py:51-54 + own_stake_* , тесты tests/test_own_stake.py.
- [ ] **Не хватает.** В живом контейнере mcp OWN_STAKE_ENABLED=false. Коллекции own_corpus__embeddingsgigar__dense_2560 в Qdrant НЕТ вообще (`curl /collections` → только frontier_docs*, trend_clusters* ×2; прямой GET по имени → "doesn't exist"), то есть index_own_corpus.py в проде не запускался ни разу. Пороги квадрантов не откалиброваны: комментарий в _stake_quadrant прямо пишет, что relevance_high=0.40 — «единственное наблюдавшееся живое значение score», а при включённом sparse score это RRF-балл масштаба 0.02-0.06, и порог 0.40 схлопнет всю выдачу в два нижних квадранта.

<details><summary>Доказательства</summary>

ssh: docker exec frontier-intelligence-mcp-1 env | grep OWN_STAKE → OWN_STAKE_ENABLED=false; curl http://127.0.0.1:6333/collections/own_corpus__embeddingsgigar__dense_2560 → {"error":"Collection ... doesn't exist!"}; mcp/tools/search_frontier.py:253-412; shared/config.py:51-54; scripts/index_own_corpus.py (1422 строки)

</details>

### 28. Поиск не дедуплицирует ре-синдикацию: 24% постов окна — кросс-источниковые дубли, в выдаче они идут как независимые подтверждения

`search` · только спроектировано · объём M

- [ ] **Есть.** Механика де-синдикации написана и работает на уровне кластеров (shared/provenance.py, worker/services/semantic_clustering.py). В search_frontier есть дедуп по semantic_cluster_id, но только для отбора 6 хитов в промпт синтеза (_select_synthesis_hits, search_frontier.py:172-186) — на возвращаемый список он не влияет.
- [ ] **Не хватает.** run_search_request (search_frontier.py:416-483) отдаёт hits из hybrid_search как есть: гидрация source_score → сортировка по score → возврат. Ни canonical-URL дедупа, ни near-dup фильтра, ни пометки «это перепечатка». Замер на живой БД: за 30 дней в disruption 46772 поста, из них 11291 (24%) попадают в 4781 группу с одинаковыми первыми 90 символами контента и >1 источником. Дополнительно 86303 из 190166 точек Qdrant имеют пустой semantic_cluster_id, то есть даже кластерный дедуп в синтезе работает меньше чем на половине корпуса.

<details><summary>Доказательства</summary>

mcp/tools/search_frontier.py:416-483, :172-186; psql: with t as (select lower(left(regexp_replace(content,'\s+',' ','g'),90)) k,count(*) c,count(distinct source_id) s from posts where workspace_id='disruption' and published_at>now()-interval '30 days' group by 1 having count(*)>1 and count(distinct source_id)>1) select count(*),sum(c) from t → 4781 | 11291; qdrant count filter semantic_cluster_id='' → 86303 из 190166

</details>

### 29. Стабильные тренды формируются только в disruption — пять из шести воркспейсов отдают пустую trend-поверхность

`search` · работает частично · объём L

- [ ] **Есть.** Кластеризация запускается по всем шести воркспейсам и завершается успехом: cluster_runs stage='full' status='success' за сутки есть у каждого. Semantic-слой и emerging-слой живые: ai_trends 9404 semantic / 2515 emerging, design 2470 / 2077, ai_products_media 3322 / 2250.
- [ ] **Не хватает.** Промоушен в stable не срабатывает. В последнем суточном прогоне signals_promoted_to_stable=0 и stable_trends_created_or_updated=0 у пяти воркспейсов из шести (только disruption: 4/4); signals_promoted_to_emerging=0 у четырёх. Накопленный итог: disruption 357 trend_clusters, ai_research 6, ai_trends 6, design 3 (последний 2026-07-14, 21 день назад), ai_products_media 2 (2026-07-25), auto_hmi 0 за всю историю. То есть search_trend_clusters и list_clusters(kind='trend') для воркспейсов, обслуживающих внешних MCP-клиентов ai-researcher и design-director, возвращают почти пустоту. Отдельно: signals_merged=0 и semantic_clusters_merged=0 у ВСЕХ шести — слияние кандидатов не срабатывает нигде.

<details><summary>Доказательства</summary>

psql: select workspace_id,summary from cluster_runs where stage='full' and status='success' and started_at>now()-interval '1 day' → у 5 из 6 "signals_promoted_to_stable":0, у всех 6 "signals_merged":0; select workspace_id,count(*),max(detected_at) from trend_clusters group by 1 → disruption 357/2026-08-04, ai_research 6, ai_trends 6/2026-07-28, design 3/2026-07-14, ai_products_media 2/2026-07-25, auto_hmi отсутствует; пороги: config/workspaces.yml (trend_cluster_stable_threshold 0.56-0.62)

</details>

---

## Средний приоритет (18)

### 30. Собственное правило security-git-preflight «после смены auth posture обновить документ» не выполнено — снимок экспозиции описывает прошлую картину

`docs` · работает частично · объём S

- [ ] **Есть.** Документ docs/security-git-preflight.md ведётся, в нём есть раздел Server Exposure Snapshot и явное обязательство: 'After changing firewall, compose ports, or auth posture, update this document and docs/ops-server-troubleshooting.md'.
- [ ] **Не хватает.** Собственно обновление после двух изменений posture. Снимок перечисляет admin:8101, mcp:8100, mcp-gateway:8102 одним рядом, не фиксируя, что (а) mcp с 2026-08-03 переведён на 127.0.0.1 и больше не доступен извне, (б) mcp-gateway остался на 0.0.0.0 БЕЗ аутентификации и теперь несёт пишущие инструменты, (в) admin закрыт Basic-auth. Оперативное последствие не косметическое: единственный документ, по которому оценивают поверхность атаки, показывает симметрию там, где её нет, и не помечает 8102 как остаточный риск.

<details><summary>Доказательства</summary>

ssh `grep -E 'curl|8100|8101|8102|0\.0\.0\.0' docs/security-git-preflight.md` → три строки списка портов без пометок про auth; фактическое состояние: docker ps (mcp 127.0.0.1:8100, mcp-gateway 0.0.0.0:8102), /api/pipeline/stats → 401, /mcp на 8102 → 200 без токена

</details>

### 31. Медиа и vision существуют только для Telegram — 98% корпуса не имеет ни одного изображения

`enrichment` · работает частично · объём L

- [ ] **Есть.** Полный vision-контур: скачивание медиа в S3 (ingest/sources/telegram_source.py:200,377-438), стрим stream:posts:vision, VisionTask, отдельный контейнер paddleocr (up, healthy), маршрутизация vision по провайдерам, MCP-инструмент search_by_vision. 4845 постов с медиа обработаны, 917 пропущены.
- [ ] **Не хватает.** media_urls заполняет ТОЛЬКО telegram_source. В rss_source.py / web_source.py / api_source.py грепа по media_urls/has_media нет ни одного совпадения — ни скачивания og:image, ни загрузки в S3. crawl4ai достаёт og:image (crawl4ai/enrichment_engine.py:153) в JSON-поле og, но никуда его не подаёт: ни в S3, ни в media_urls, ни в stream:posts:vision. В итоге весь vision-стек обслуживает 5762 telegram-поста из 312041 (1.8%), а search_by_vision по факту telegram-only.

<details><summary>Доказательства</summary>

psql: SELECT s.source_type, count(*) FILTER (WHERE p.has_media), count(*) FROM posts p JOIN sources s ON s.id=p.source_id GROUP BY 1 → api 0/99624, rss 0/201887, telegram 5762/9511, web 0/1023. Grep 'media_urls|has_media' по ingest/sources → совпадения только в telegram_source.py. crawl4ai/enrichment_engine.py:153

</details>

### 32. Реактивный пайплайн детекции трендов — выдолбленный слот в схеме без единой строки продюсера

`enrichment` · только спроектировано · объём L

- [ ] **Есть.** Под фичу зарезервировано место во всех слоях: CHECK-констрейнт pipeline IN ('reactive','stable') в storage/postgres/init.sql, колонка в модели (shared/models/trend_cluster.py:20 с комментарием '# reactive | stable'), параметр запроса в MCP (mcp/tools/search_trend_clusters.py:25 Literal['stable','reactive'], описание в mcp/server.py:125), payload-индекс 'pipeline' и 'burst_score' в Qdrant (worker/integrations/qdrant_client.py:58,61).
- [ ] **Не хватает.** Продюсера нет. Grep 'reactive' (case-insens) по всем *.py даёт ровно три совпадения — все три потребители/объявления типа. В APScheduler-конфигурации admin/backend/scheduler.py задачи с 30-минутным кроном под burst-детекцию нет. В БД pipeline='stable' 374 строк, 'reactive' — ноль. Значение pipeline='reactive' в search_trend_clusters всегда вернёт пусто.

<details><summary>Доказательства</summary>

Grep 'reactive' по *.py → mcp/tools/search_trend_clusters.py:25, mcp/server.py:125, shared/models/trend_cluster.py:20. psql: select pipeline, count(*) from trend_clusters group by 1 → stable | 374 (единственная строка)

</details>

### 33. Album Assembler: события описаны, ни разу не импортированы; 270 альбомов навсегда «несобранные»

`enrichment` · код есть, не подключён · объём M

- [ ] **Есть.** Таблица media_groups с флагом assembled и счётчиком item_count заполняется на приёме (worker/tasks/enrichment_task.py:297-314 _upsert_media_group, вставка всегда с assembled=FALSE), а флаг переводится в TRUE побочным эффектом vision-обработки (worker/tasks/vision_task.py:239-241). Есть админ-экран /albums (admin/backend/routers/albums.py) с фильтром по assembled.
- [ ] **Не хватает.** Самого сборщика нет: shared/events/album_assembled_v1.py и shared/events/albums_parsed_v1.py не импортируются нигде (grep по *.py вне old_docs — ноль), живут только posts_parsed_v1 и posts_vision_v1. Никакой процесс не добирает группы, у которых vision не отработал: 270 записей media_groups остаются assembled=false бессрочно, и нет ни джобы, ни алерта на этот хвост.

<details><summary>Доказательства</summary>

psql: select assembled, count(*) from media_groups group by 1 → f 270, t 1198. Grep 'album_assembled|albums_parsed' по *.py → ноль. Grep 'assembled' по *.py → admin/backend/routers/albums.py:16,28-30,43; admin/backend/routers/posts.py:125; worker/tasks/vision_task.py:239-241; worker/tasks/enrichment_task.py:305; shared/models/media.py:26

</details>

### 34. indexing_status.vision_status непригоден как индикатор: 306211 строк 'pending' — это посты без медиа

`enrichment` · работает частично · объём S

- [ ] **Есть.** Колонка vision_status ведётся: worker/tasks/enrichment_task.py:396-405 _update_vision_status и :407-419 _mark_vision_skipped, VisionTask ставит done/skipped/error (vision_task.py:149,326,344,474). Реальная очередь vision пуста — все посты с медиа обработаны.
- [ ] **Не хватает.** Значение по умолчанию 'pending' проставляется каждому посту при первом же UPSERT из общего пути индексации (enrichment_task.py:486-517 не трогает vision_status), поэтому 306211 строк 'pending' — это посты, у которых медиа нет и не будет. Отличить настоящий backlog vision от шума по этой колонке нельзя ни SQL-запросом, ни метрикой; ни одного алерта или счётчика на vision-очередь нет.

<details><summary>Доказательства</summary>

psql кросс-таб: SELECT p.has_media, i.vision_status, count(*) ... → (f, pending) 306211, (f, skipped) 59, (f, done) 9, (t, done) 4845, (t, skipped) 917. Постов с непустым media_urls — 5752. Файлы: worker/tasks/enrichment_task.py:396-419, :486-517

</details>

### 35. Из Neo4j ничего никогда не удаляется — при дропе поста Qdrant чистится, граф нет

`graph` · работает частично · объём M

- [ ] **Есть.** Двусторонняя согласованность сделана для Qdrant: worker/integrations/qdrant_client.py:576 delete_document, вызывается и при откате неудачного обогащения (worker/tasks/enrichment_task.py:941-947), и при переходе поста в 'dropped' на переобработке (enrichment_task.py:800-811). Neo4j при дропе получает graph_status='skipped'.
- [ ] **Не хватает.** У Neo4jFrontierClient (worker/integrations/neo4j_client.py, 445 строк) нет ни одного метода удаления — только MERGE/SET и чтения. Узел (:Document) с рёбрами MENTIONS и накрученными счётчиками RELATED_TO.count остаётся в графе навсегда, даже когда пост признан нерелевантным и вычищен из Qdrant. То есть co-occurrence-граф монотонно растёт и включает материал, которого нет в поиске; счётчики связей завышаются.

<details><summary>Доказательства</summary>

Grep 'def (delete|remove)' по worker/integrations → единственное совпадение qdrant_client.py:576. cypher-shell: MATCH (d:Document) RETURN count(d) → 190160 при psql indexing_status embedding_status='done' = 189997 (разрыв 163). Concept-узлов 758750

</details>

### 36. Граф Neo4j: метки (:Source) и (:TrendCluster) созданы констрейнтами и остались пустыми, связи FROM_SOURCE/EVOLVED_FROM/BRIDGES не пишутся

`graph` · только спроектировано · объём L

- [ ] **Есть.** Граф живой и наполненный: 758750 (:Concept), 190160 (:Document), связи MENTIONS, RELATED_TO, CONTAINS и ECHO_OF (224 ребра, провенансный слой). Инструмент get_concept_graph (mcp/tools/graph.py, 49 строк) работает.
- [ ] **Не хватает.** MATCH (s:Source) RETURN count(s) → 0; MATCH (t:TrendCluster) → 0. CALL db.relationshipTypes() возвращает ровно четыре типа — FROM_SOURCE, EVOLVED_FROM, BRIDGES отсутствуют. Метки существуют только потому, что их породили констрейнты source_id/trend_id. Следствие для продукта: единственный графовый инструмент отдаёт co-occurrence концептов, а не происхождение сигнала — по нему нельзя ни отследить, откуда пришёл тренд, ни построить эволюцию, ни найти мост между воркспейсами. Слой community detection / иерархических summary (GraphRAG) отсутствует: grep -i 'community|louvain|leiden|gds\.' по *.py → 0 совпадений (только упоминания в CLAUDE.md/AGENTS.md/.cursor).

<details><summary>Доказательства</summary>

ssh cypher-shell: MATCH (s:Source) RETURN count(s) → 0; MATCH (t:TrendCluster) → 0; CALL db.relationshipTypes() → MENTIONS, RELATED_TO, CONTAINS, ECHO_OF; CALL db.labels() → Workspace, Concept, Document, Source, TrendCluster; mcp/tools/graph.py (49 строк, один роут)

</details>

### 37. Мёртвые consumer'ы копятся в crawl и reindex: 85 и 14 «призраков» с простоем до 26 суток

`ingest` · работает частично · объём S

- [ ] **Есть.** Уборка мёртвых консьюмеров написана и работает — но только в двух из четырёх потребителей: worker/tasks/enrichment_task.py:655-671 _cleanup_dead_consumers (idle>1ч и pending=0 → XGROUP DELCONSUMER, вызывается из run_loop:997) и worker/tasks/vision_task.py:561-571 (вызов :517). Обёртка есть в shared/redis_client.py:214-216.
- [ ] **Не хватает.** В worker/tasks/reindex_task.py и crawl4ai/crawl4ai_service.py такого метода нет вовсе — их run_loop (reindex_task.py:438-457, crawl4ai_service.py:145-166) только читает и реклеймит. Имя консьюмера генерируется на каждый старт процесса (reindex_task.py:26, crawl4ai_service.py:23), поэтому каждый рестарт оставляет запись навсегда. Побочный эффект — метрика frontier_redis_stream_consumer_idle_seconds засоряется десятками мёртвых серий.

<details><summary>Доказательства</summary>

ssh: XINFO GROUPS stream:posts:crawl → consumers 85 (XINFO CONSUMERS: idle до 2302641030 мс ≈ 26.6 сут); stream:posts:reindex → consumers 14 (idle до 51473846 мс ≈ 14 сут); при этом stream:posts:parsed и stream:posts:vision → по 1 консьюмеру. Grep '_cleanup_dead_consumers|xdel_consumer' по *.py → только enrichment_task.py:655,668 и vision_task.py:561,571

</details>

### 38. linked_urls нигде не персистятся — любой reprocess/replay сужает crawl-обогащение

`ingest` · работает частично · объём M

- [ ] **Есть.** Ingest аккуратно собирает внешние ссылки: из HTML-якорей и из plain-текста (ingest/sources/base.py:456,461 html_to_text → finalize_linked_urls(urls + extract_urls_from_plain_text)), кладёт в PostParsedEvent.linked_urls, а enrichment по ним ставит задачу краула (worker/tasks/enrichment_task.py:918-927).
- [ ] **Не хватает.** В таблице posts нет колонки linked_urls (psql \d posts — 16 колонок: id, workspace_id, source_id, external_id, grouped_id, content, category, relevance_score, has_media, media_urls, published_at, tags, extra, created_at, updated_at, semantic_cluster_id), и в extra они тоже не пишутся (enrichment_task.py:268-272 кладёт в extra только url и author). Поэтому admin/backend/routers/pipeline.py:183 при reprocess восстанавливает их только из plain-текста: finalize_linked_urls(extract_urls_from_plain_text(content)) — все href из HTML теряются. Переобработка RSS/web-поста тихо даёт меньше краула, чем первичная обработка.

<details><summary>Доказательства</summary>

psql \d posts (колонки linked_urls нет). admin/backend/routers/pipeline.py:183; ingest/sources/base.py:456,461; worker/tasks/enrichment_task.py:268-272, :918-927. Массовый reprocess идёт тем же путём: pipeline.py:219-257 → _queue_post_reprocess

</details>

### 39. Тримминг стримов не отличает прочитанное от непрочитанного — переполнение = тихая потеря событий

`ingest` · работает частично · объём M

- [ ] **Есть.** После Redis-OOM 31.07 введён жёсткий потолок: shared/redis_client.py:19 STREAM_MAXLEN=10_000 применяется во всех XADD (:47-51 и :113-117, maxlen=..., approximate=True), включая ручной reprocess (admin/backend/routers/pipeline.py:206-208). Есть алерты на лаг и pending (prometheus/alerts.yml:363-402).
- [ ] **Не хватает.** MAXLEN режет по длине независимо от того, прочитаны ли записи консьюмер-группой: если потребитель встанет дольше, чем на 10k событий, старые сообщения будут удалены до доставки и это нигде не отразится — lag и pending при этом останутся нулевыми (ровно тот класс молчания, что был у OOM-инцидента). Ни метрики «сколько урезано», ни правила «trim при ненулевом лаге» нет. Что тримминг реально срабатывает, видно по стримам, стоящим ровно на потолке.

<details><summary>Доказательства</summary>

ssh: XLEN stream:posts:parsed 10000 при entries-read 69375; stream:posts:crawl 10004 при entries-read 42319; stream:posts:reindex 10005 при entries-read 32673; stream:posts:enriched 10024. Файлы: shared/redis_client.py:19,47-51,113-117; prometheus/alerts.yml:363-402 (только lag/pending/oldest-pending)

</details>

### 40. ingest_url не умеет ингестить URL: требует уже существующий post_id и не проверяет его — очевидный сценарий «принеси ссылку» невозможен

`mcp` · работает частично · объём M

- [ ] **Есть.** mcp/tools/ingest_url.py (50 строк) выведен в шлюз (mcp_gateway.py:244), SSRF-защита работает (assert_public_http_url), событие кладётся в stream:posts:crawl с trace_id.
- [ ] **Не хватает.** post_id — обязательное поле без дефолта (ingest_url.py:23-26), и инструмент НЕ создаёт пост: он только ставит crawl-обогащение к уже существующей строке. Пользователь Claude-проекта, который хочет добавить найденную статью, сделать этого не может. Плюс post_id не валидируется — существование строки в posts не проверяется до xadd, поэтому произвольное значение принимается с ответом status='queued', а разбираться будет консьюмер enrichment (тот самый класс FK-violation, из-за которого чинили poison-pending 2026-07-12).

<details><summary>Доказательства</summary>

mcp/tools/ingest_url.py:20-50 (post_id: str = Field(..., description=...), никакого SELECT перед xadd); mcp/server.py:170-186 (inputSchema, required: [url, post_id]); mcp/mcp_gateway.py:244

</details>

### 41. Синтез (главная фича search/brief) вызывался 8 раз за 60 дней и КАЖДЫЙ раз падал на primary-провайдере

`mcp` · работает частично · объём S

- [ ] **Есть.** Синтез реализован в трёх инструментах: search_frontier._synthesize_results (search_frontier.py:210-244, task='mcp_synthesis'), search_balanced._synthesize_balanced (:230), frontier_brief._synthesize_brief (:134); маршрут mcp_synthesis настроен в shared/llm_routing.py:274 с фолбэком.
- [ ] **Не хватает.** За 60 дней по job='mcp' ровно 8 вызовов task='mcp_synthesis': wormsoft status=error 4 и polza status=ok 4 — то есть 100% вызовов упирались в ошибку основного провайдера и доезжали только фолбэком. Ни одного успешного вызова wormsoft за два месяца. Ни алерта, ни метрики на это нет (FrontierWormsoftFallbackBurst смотрит на общий поток, где enrichment перекрывает восемь вызовов шумом). Причина ошибки из метрик не восстанавливается — фасет reason отсутствует.

<details><summary>Доказательства</summary>

ssh: curl 9090/api/v1/query 'sum by (job,task,provider,status)(increase(frontier_llm_requests_total{task="mcp_synthesis"}[60d]))' → {job=mcp,provider=polza,status=ok} 4.0, {job=mcp,provider=wormsoft,status=error} 4.0; mcp/tools/search_frontier.py:210-244; shared/llm_routing.py:274

</details>

### 42. SLO/error-budget документ неизмерим: recording-правил ноль, а ретенция Prometheus 200 часов при 30-дневных окнах

`ops` · только спроектировано · объём M

- [ ] **Есть.** docs/sre/llm-orchestrator-sli-slo.md задаёт целевые значения (99.0% / 8% / 97.5% / 20% / 99.5%) и формулы SLI; Prometheus поднят, 3 группы правил, 56 алертов, 8 таргетов up.
- [ ] **Не хватает.** Recording rules и достаточная глубина хранения. `/api/v1/rules?type=record` → groups=0: ни одного recording-правила под формулы документа и ни одного алерта на burn-rate/нарушение SLO. Отдельно и независимо: prometheus запущен с --storage.tsdb.retention.time=200h (≈8.3 суток), поэтому 30-дневное окно error budget арифметически не посчитать, и разбор инцидентов старше недели невозможен — например, чтобы посмотреть провалы доставки алертов 31.07, окно уже почти исчерпано.

<details><summary>Доказательства</summary>

ssh `curl http://127.0.0.1:9090/api/v1/rules?type=record` → recording groups: 0; полный список групп — frontier_analysis_freshness(3), frontier_rsi(7), frontier_runtime_resilience(46), TOTAL 56; docker-compose.yml prometheus command → '--storage.tsdb.retention.time=200h'

</details>

### 43. 11 из 18 сервисов без healthcheck, а единственная внешняя поверхность (mcp-gateway) не покрыта ни healthcheck, ни метриками, ни алертом

`ops` · работает частично · объём M

- [ ] **Есть.** Healthcheck есть у 7 сервисов (postgres, redis, qdrant, neo4j, searxng, paddleocr, gpt2giga-proxy). Скрейп настроен на 8 таргетов, все up. Есть FrontierCoreServiceDown / FrontierControlPlaneServiceDown / FrontierSupportServiceDown / FrontierAdminDown.
- [ ] **Не хватает.** Healthcheck у admin, alertmanager, crawl4ai, grafana, ingest, mcp, mcp-gateway, node-exporter, prometheus, worker, xray (.cursor/rules/06-devops.mdc объявляет их 'обязательными для всех сервисов'). Хуже конкретно для mcp-gateway: он не входит в список скрейп-таргетов Prometheus (admin, alertmanager, crawl4ai, ingest, mcp, node-exporter, qdrant, worker — восемь, шлюза нет), значит у сервиса, опубликованного на 0.0.0.0:8102, нет ни healthcheck, ни /metrics в мониторинге, ни правила падения — его отказ будет замечен только пользователем.

<details><summary>Доказательства</summary>

ssh python по docker-compose.yml → 'total 18 with healthcheck 7', WITHOUT: admin, alertmanager, crawl4ai, grafana, ingest, mcp, mcp-gateway, node-exporter, prometheus, worker, xray; `curl http://127.0.0.1:9090/api/v1/targets` → 8 таргетов, mcp-gateway отсутствует

</details>

### 44. Процедура replay окна после сбоя не отработает: скрипт не умеет авторизоваться в закрытой админке

`ops` · код есть, не подключён · объём S

- [ ] **Есть.** scripts/server-reprocess-window.ps1 существует, ветка -DryRun корректно строит список id, документ docs/ops-server-troubleshooting.md подаёт её как штатную процедуру переобработки окна после 402/сбоя провайдера.
- [ ] **Не хватает.** Передача учётных данных. Скрипт вызывает `ssh $Server "curl -fsS -X POST http://127.0.0.1:8101/api/pipeline/reprocess/$id"` без креденшелов, а auth-middleware admin закрывает все /api/* кроме /api/health и /api/auth/login. Проверено на сервере: GET /api/pipeline/stats → 401. То есть боевой прогон молча не переобработает ничего, а dry-run будет выглядеть исправным — сломанный инструмент восстановления, о котором узнают в момент, когда он нужен.

<details><summary>Доказательства</summary>

ssh `curl -o /dev/null -w '%{http_code}' http://127.0.0.1:8101/api/pipeline/stats` → 401; scripts/server-reprocess-window.ps1 (вызов curl без -u/cookie); admin/backend/main.py auth-middleware; docs/ops-server-troubleshooting.md — раздел про replay окна

</details>

### 45. Коллекция own_corpus в Qdrant не создана — вторая ось карточки (own_stake) не запускалась ни разу

`search` · код есть, не подключён · объём M

- [ ] **Есть.** Код готов целиком: shared/config.py:39-53 (qdrant_own_corpus_collection='own_corpus', own_stake_enabled, own_stake_top_k=3, own_stake_high=0.60), отдельная схема payload-индексов _OWN_CORPUS_PAYLOAD_INDEXES (worker/integrations/qdrant_client.py:72-76) со страховкой от пустого имени (:81), скрипт индексации scripts/index_own_corpus.py, потребление в mcp/tools/search_frontier.py.
- [ ] **Не хватает.** В живом Qdrant коллекции own_corpus нет вообще: GET /collections отдаёт только frontier_docs, trend_clusters и две версионированные. Флаг в контейнере mcp — OWN_STAKE_ENABLED=false. Пороги own_stake_high/low сам код помечает как «ПЛЕЙСХОЛДЕРЫ, а не измеренные величины» (shared/config.py:44-53) — то есть даже при включении калибровки не было.

<details><summary>Доказательства</summary>

ssh: curl 127.0.0.1:6333/collections → ['frontier_docs','trend_clusters','frontier_docs__embeddingsgigar__dense_2560','trend_clusters__embeddingsgigar__dense_2560']. docker compose exec mcp env | grep OWN → OWN_STAKE_ENABLED=false, QDRANT_OWN_CORPUS_COLLECTION=own_corpus, OWN_STAKE_HIGH=0.60, OWN_STAKE_TOP_K=3

</details>

### 46. pipeline="reactive" в search_trend_clusters — принимаемое значение без производителя, всегда пустая выдача

`search` · только спроектировано · объём M

- [ ] **Есть.** Параметр объявлен в трёх местах контракта: mcp/tools/search_trend_clusters.py:25 `pipeline: Literal["stable","reactive"] | None = "stable"`, mcp/server.py:125 в inputSchema с описанием «stable | reactive», storage/postgres/init.sql:110 CHECK (pipeline IN ('reactive','stable')). Фильтр доезжает до Qdrant (search_trend_clusters.py:51 pipeline=req.pipeline).
- [ ] **Не хватает.** Ни одной строки, которая пишет pipeline='reactive'. grep -i 'reactive' по репозиторию вне docs/ и вне vendor-бандлов Vue: только объявления типов и .cursor/rules. В БД `select pipeline,count(*) from trend_clusters group by 1` → единственная строка «stable | 374». В Qdrant у всех точек payload pipeline='stable'. Клиент, передавший reactive, получает пустой список без объяснения.

<details><summary>Доказательства</summary>

mcp/tools/search_trend_clusters.py:25,51; mcp/server.py:125; storage/postgres/init.sql:110; shared/models/trend_cluster.py:20; psql: select pipeline,count(*) from trend_clusters group by 1 → stable|374; qdrant scroll trend_clusters__embeddingsgigar__dense_2560 → payload.pipeline='stable'

</details>

### 47. cross_workspace_bridges: хранилище и три пути записи есть, потребителя нет — «тренд из ai_trends может попасть в disruption» не делает никто

`search` · только спроектировано · объём M

- [ ] **Есть.** Колонка workspaces.cross_workspace_bridges (storage/postgres/init.sql:11), модель shared/models/workspace.py:17, заполнена у 5 из 6 воркспейсов в config/workspaces.yml (строки 22,45,93,129,173), пишется тремя путями (bootstrap_configs.py:45-70, workspaces.py:83-146, init_storage.py:56-77), редактируется в админке (WorkspacesView.js:121,296) и отображается в двух MCP-инструментах.
- [ ] **Не хватает.** Ни одного чтения ради поведения. В mcp/tools/observability.py:149,337 значение только выводится в ответ list_workspaces/get_workspace_overview. mcp/tools/frontier_brief.py берёт исключительно явно переданный список (workspace_ids(), строки 58-66) и мостами не расширяет его. В search_frontier / search_balanced / search_trend_clusters обращений нет. В воркспейсе auto_hmi поле сознательно не заполнено с комментарием «их не читает никто» — то есть отсутствие потребителя уже зафиксировано в конфиге, но фича из контракта не убрана.

<details><summary>Доказательства</summary>

grep 'cross_workspace_bridges' по репозиторию вне docs/ → 30 совпадений, все на запись/отображение; mcp/tools/observability.py:149,337; mcp/tools/frontier_brief.py:58-66,152-190; config/workspaces.yml:198 («cross_workspace_bridges намеренно не заданы — их не читает никто»)

</details>

---

## Низкий приоритет (5)

### 48. Пять батчей источников не раскатаны: 26 из 27 заведённых id выключены с 31.05

`ingest` · брошено · объём S

- [ ] **Есть.** Источники заведены в БД с меткой extra->>'rollout_batch', процедура включения описана и технически работает (PATCH /toggle + правка config/sources.yml). Живой ingest здоров: 202 включённых источника, у всех last_success_at свежее 7 дней, ноль last_error.
- [ ] **Не хватает.** Ни один батч не начали. Из 27 источников шести батчей включён ровно один (auto_hmi). auto_ru 6/0, ev_tesla 4/0, global_mobility 3/0, smart_city 2/0, design_ux 2/0 — с 31.05 и 28.06 соответственно. Тематические покрытия (авто-РФ, EV, городская мобильность) остаются нулевыми, хотя конфигурация под них уже написана и провалидирована.

<details><summary>Доказательства</summary>

psql: SELECT extra->>'rollout_batch', count(*), count(*) FILTER (WHERE is_enabled) FROM sources WHERE extra ? 'rollout_batch' GROUP BY 1 → batch:auto_hmi 10/1, batch:auto_ru 6/0, batch:design_ux 2/0, batch:ev_tesla 4/0, batch:global_mobility 3/0, batch:smart_city 2/0. Здоровье включённых: 202 enabled, with_error 0, ни одного с last_success_at старше 7 дней

</details>

### 49. Коннектор email реализован, задиспатчен и допущен CHECK-констрейнтом, но не исполнялся ни разу

`ingest` · код есть, не подключён · объём S

- [ ] **Есть.** ingest/sources/email_source.py (97 строк), импорт и ветка диспетчера ingest/main.py:19 и :124-125 (elif source_type == 'email': cls = EmailSource), тип 'email' в shared/source_definitions.py и в CHECK sources_source_type_check (миграция storage/postgres/migrations/20260328_source_connectors.sql).
- [ ] **Не хватает.** Ни одного источника этого типа не заведено — в sources только api/rss/telegram/web. Код никогда не выполнялся, значит не проверены ни аутентификация, ни разбор писем, ни чекпоинты; тестового покрытия на живой ящик тоже нет. Формально «поддерживаемый тип», фактически — непроверенная ветка в проде.

<details><summary>Доказательства</summary>

psql: select source_type, is_enabled, count(*) from sources group by 1,2 → api 4/1, rss 129/23, telegram 53/10, web 16/11; строк с source_type='email' нет. Файлы: ingest/main.py:19,124-125; ingest/sources/email_source.py

</details>

### 50. Альбомные vision-сводки (1191 файл в S3) не доступны ни одному MCP-инструменту — только через админку

`mcp` · работает частично · объём M

- [ ] **Есть.** Сборка альбомов работает: 1468 media_groups, 1198 assembled, 1191 с vision_summary_s3_key, свежие (max created_at 2026-08-04 06:03). Сводка формируется в worker/tasks/vision_task.py:258-300,444-470 и кладётся в S3 ключом vision/{ws}/albums/{grouped_id}/summary.json.gz. Экран /albums в админке умеет её показать.
- [ ] **Не хватает.** Единственный vision-инструмент MCP — search_by_vision — читает исключительно post_enrichments kind='vision' (mcp/tools/search_by_vision.py:76-104), альбомного уровня не касается. Ключ vision_summary_s3_key фигурирует только в admin/backend/routers/{posts,albums}.py и во фронте админки. Пользователь Claude-проекта агрегированную сводку по альбому получить не может. Плюс 270 media_groups застряли в assembled=false (последняя 2026-08-01) и ни у одной нет summary.

<details><summary>Доказательства</summary>

psql: select assembled,count(*),count(vision_summary_s3_key),max(created_at) from media_groups group by 1 → t|1198|1191|2026-08-04, f|270|0|2026-08-01; grep 'vision_summary_s3_key' → admin/backend/routers/posts.py, albums.py, worker/tasks/vision_task.py, фронт; mcp/tools/search_by_vision.py:76-104 (только post_enrichments)

</details>

### 51. Осиротевшие Qdrant-коллекции после перехода на алиасы: frontier_docs держит 46264 устаревшие точки

`search` · брошено · объём S

- [ ] **Есть.** Переход на версионированные коллекции с алиасами выполнен и работает: aliases frontier_docs_active → frontier_docs__embeddingsgigar__dense_2560 (190162 точки) и trend_clusters_active → trend_clusters__embeddingsgigar__dense_2560 (374). Есть скрипты scripts/qdrant_alias_cutover.py и scripts/qdrant_backfill_versioned.py.
- [ ] **Не хватает.** Базовые коллекции с прежними именами остались и не удалены: frontier_docs — 46264 точки, status green, с полным набором payload-индексов; trend_clusters — тоже жива. Они не обновляются с момента cutover, но занимают место и продолжают отвечать на прямые запросы по имени, то есть любой код или скрипт, обратившийся к 'frontier_docs' вместо алиаса, тихо получит срез полугодовой давности.

<details><summary>Доказательства</summary>

ssh: curl 127.0.0.1:6333/aliases → две записи *_active на версионированные коллекции; curl 127.0.0.1:6333/collections/frontier_docs → points_count 46264, indexed_vectors_count 91919, status green; активная — 190162 точки

</details>

### 52. Поле global_signals в ответе search_balanced — буквальная копия signals, отдельной глобальной полосы нет

`search` · брошено · объём S

- [ ] **Есть.** search_balanced честно строит три полосы параллельными запросами: main (valence positive/neutral), counter (valence negative + COUNTER_SIGNAL_TYPES, окно ≥30д), ru (source_region='ru', окно ≥30д) — search_balanced.py:305-330, плюс внешнее заземление через SearXNG.
- [ ] **Не хватает.** В ответе рядом с "signals" отдаётся "global_signals": main_search["results"] — тот же самый объект (search_balanced.py:370). Это единственное вхождение имени во всём репозитории: ни фильтра source_region='global', ни отдельной задачи под него нет. Клиент видит два ключа и вправе считать, что получил две разные выборки. Либо полосу надо сделать (симметрично ru_task, с source_region_override='global'), либо ключ убрать из контракта.

<details><summary>Доказательства</summary>

mcp/tools/search_balanced.py:370 ("global_signals": main_search["results"]); grep 'global_signals' по репозиторию → 1 совпадение; для сравнения ru_task на :325-329 использует source_region_override='ru'

</details>

---

## Что в этот список НЕ вошло

- **Стратегические пакеты `docs/harness/` и `docs/saas/`.** Это планы продукта, а не долг по коду: ни одна фаза не начата, и это нормальное состояние замысла. Их статус зафиксирован пометками в самих файлах.
- **Рефакторинг и косметика.** Список только про функциональность, которая заявлена или начата, но не доведена.
- **Архив `docs/old_docs ilyasni-telegram-assistant.git/`** — материалы предыдущего проекта, справочные по определению.
