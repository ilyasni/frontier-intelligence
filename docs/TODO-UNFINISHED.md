# Незаконченный и нереализованный функционал

> Сверено с рабочим стеком **2026-08-04**. Каждый пункт проверен на живом сервере
> (`ssh frontier-intelligence`), в БД и в коде — доказательства приведены прямо в пункте.
> Метод и полный разбор документации: [AUDIT-2026-08-04.md](./AUDIT-2026-08-04.md).
>
> **Порядок работ — не этот список, а [маршрут](./AUDIT-2026-08-04.md#7-маршрут)** (заходы 0–5,
> утверждены 2026-08-04). Здесь фактура; там очередь и критерии готовности.
> Пункт 3 (MCP-шлюз 8102) выведен из очереди решением владельца — см.
> [принятые решения](./AUDIT-2026-08-04.md#8-принятые-решения).

## Состояние на 2026-08-06 (сверено измерением)

> **Полная сверка и новые находки — [AUDIT-2026-08-06.md](./AUDIT-2026-08-06.md).**
> Там же реестр предложений по рефакторингу, три архитектурные позиции и черновик
> маршрута III. Ниже — только статусы.
>
> Предыдущая редакция шапки не сходилась: объявляла «Сделано 27 · Открыто 27» при
> заявленных 57 пунктах (это 61) и перечисляла 23 номера вместо 27, без 26 и 28.
> Числа ниже пересчитаны по телу документа и проверены на живом стеке 06.08.

Всего пунктов **76** (52 из аудита + 3 найдено при вечерней перепроверке
+ 2 найдено при построении маршрута II и его исполнении + **19 найдено при сверке,
раскате и разборе 06–07.08**).

| | Пунктов | Номера |
|---|---|---|
| ✅ **Сделано** | 33 | 1, 2, 4, 5, 9, 11, 13, 14, 15, 16, 19, 20, 21, 23, 24, 26, 28, 30, 37, 39, 41, 44, 47, **54**, 56, 58, 59, 60, 61, 63, 68, 70, 71 |
| 🟡 **Частично** | 10 | 8 исходы краула · **10 auto_hmi** (5 источников из 10, 54 поста, трендов ноль) · 12 DLQ · 18 петля alert-triage · 25 провенанс · **43 healthcheck** (шлюз закрыт, 10 сервисов остались) · 48 раскат батчей (16 из 27) · 49 email-коннектор · 53 crawl4ai · **62 контракт инструментов** (ось «шлюз ↔ модель» закрыта, `inputSchema` остаётся) |
| 🔒 **Отложено решением владельца** | 2 | 3 аутентификация шлюза 8102 · 22 внешняя экспозиция и файрвол |
| ♻️ **Откатилось и исправлено 06.08** | 1 | **17** метрики бэкапа и квоты S3 — метрика была зажата в INT32_MAX, оба квотных алерта недостижимы |
| ⬜ **Открыто** | 30 | 6, 7, 27, 29, 31, 32, 33, 34, 35, 36, 38, 40, 42, 45, 46, 50, 51, 52, 55, 57, 64, 65, 66, 67, 69, 72, 73, 74, 75, **76** |

**Маршрут II пройден целиком** — заходы 7–12 выполнены, хотя галочки в
`AUDIT-2026-08-04.md` при исполнении не проставлялись (исправлено 06.08).
Вне маршрута закрыты пункты 47 и 49 решением владельца от 2026-08-05,
пункт 29 отложен им же до следующей сессии.

**Что осталось хвостами от маршрута II** (сверено 06.08):

| Откуда | Осталось |
|---|---|
| заход 10 | 16 (телеметрия MCP-инструментов, `/metrics` у шлюза), хвосты 32/46/52 (сужение контракта), self-scrape Prometheus из 43 |
| заход 11 | 10 и 48 — раскат дошёл до 16 источников из 27, не раскатан целиком только `global_mobility` (0/3); 38 (`linked_urls`) не начат |
| заход 12 | 25 — остаток намеренный, но строка контракта инструментов, которую он предписывал написать, до сих пор не написана ни в `description`, ни в `docs/README.md` |
| 31 | контур og:image → vision + процесс очистки старых изображений (решение владельца: строить) |

**Блокеров не осталось:** из пяти исходных четыре закрыты, пятый (8102) выведен из
очереди решением владельца. Всё оставшееся — `high` и ниже.

**Порядок работ дальше — [черновик маршрута III](./AUDIT-2026-08-06.md#6-что-предлагается-делать-дальше-маршрут-iii-черновик)**,
там же три новые развилки для владельца.

✅ **Обе предварительно опровергнутые записи закрыты измерением 2026-08-04 вечером.**
Ротация docker-логов есть у всех 18 контейнеров; знаменатель метрики покрытия верен.
Процедуры и результаты — в разделе [«Опровергнуто и закрыто»](#-опровергнуто-и-закрыто-измерением)
в конце файла. Реестр разобран полностью.

**Порядок работ по открытым пунктам — [маршрут II, заходы 7–12](./AUDIT-2026-08-04.md#маршрут-ii--заходы-712-построен-2026-08-04-ночью).**
Там же список того, что сознательно отложено до следующего маршрута, и шесть
развилок, требующих решения владельца.

Числа внутри пунктов — на момент аудита (утро 04.08). Там, где перепроверка дала
другое, стоит пометка «пересняно». Пункты, перепроверенные при построении маршрута II
(вечер 04.08), несут пометку «сверено при маршруте II» — их числа свежее заголовка.

| Срез (исходный, по 52 пунктам) | Разбивка |
|---|---|
| Тяжесть | блокеры 5 · высокий 24 · средний 18 · низкий 5 |
| Состояние | работает частично 30 · только спроектировано 8 · код есть, не подключён 7 · брошено 3 · подключено, выключено флагом 3 · раскатано, не зафиксировано 1 |
| Слой | ops 18 · search 8 · enrichment 7 · ingest 7 · mcp 6 · graph 2 · provenance 2 · editorial 1 · docs 1 |
| Объём работ | S 20 · M 26 · L 6 |

**Как читать состояние.** `только спроектировано` — есть замысел/контракт, продюсера нет. `код есть, не подключён` — модуль написан, но его никто не вызывает. `подключено, выключено флагом` — работает, но выключено в проде. `работает частично` — отрабатывает, но с дырой, которую видно на данных. `раскатано, не зафиксировано` — живёт в проде мимо git. `брошено` — начато и оставлено.

---

## Блокеры (5)

### 1. ✅ СДЕЛАНО 2026-08-04 — ~~Кластеризация видит максимум 400 постов на воркспейс~~

> **Закрыто заходом 3, но диагноз уточнён.** «29% корпуса» — величина по всей
> истории; кластеризация по построению смотрит только в окно. Покрытие В ОКНЕ
> показало, что проблема адресная: `disruption` 40.7% при потолке 400, все
> остальные 95–99% при настроенных под их объём потолках 500–650. Единственному
> крупному воркспейсу потолок просто не настроили — он остался на дефолте.
>
> `disruption` получил `semantic_cluster_max_posts: 2000`. Ночной прогон по
> `cluster_runs`: **523с против 37**, кластеров 1636 против 323, покрытие
> 40.7% → 44.5% и растёт (backfill не нужен, окно скользящее).
>
> **Осталось хвостом:** 523с — это 58% от таймаута субпроцесса в 900с (было 4%),
> прогон идёт внутри окна ночного бэкапа на трёх ядрах. Дальше потолок поднимать
> только вместе с `ADMIN_JOB_SUBPROCESS_TIMEOUT_SEC`. Джоб живёт в cgroup **admin**
> (лимит 4 ГБ), а не worker — при разборе памяти смотреть туда.
> Подробности — [маршрут, заход 3](./AUDIT-2026-08-04.md#7-маршрут).

`enrichment` · работает частично · объём M

- [ ] **Есть.** Полный конвейер semantic → trend → emerging работает и запускается по крону: worker/services/semantic_clustering.py:2474 run_semantic_clustering, admin/backend/scheduler.py:960-969 job run_semantic_clusters, per-workspace обёртка scheduler.py:674-678. За 7 дней в cluster_runs: stage='full' success 32, error 10.
- [ ] **Не хватает.** Выборка постов жёстко ограничена: semantic_clustering.py:2486-2494 → _fetch_posts(..., limit=max(semantic_cluster_max_posts, 50)), а semantic_cluster_max_posts=400 (shared/config.py:542, в .env и admin_runtime_settings не переопределён — psql: 0 строк по ключам '%cluster%'). Крон один раз в сутки: shared/config.py:435-438 ADMIN_SEMANTIC_CLUSTER_CRON='35 3 * * *' (живые cluster_runs стартуют 03:35–03:39). _fetch_posts (semantic_clustering.py:491-522) берёт ORDER BY published_at DESC LIMIT 400 из 30-дневного окна. Нет ни backfill-джобы, ни метрики покрытия, ни алерта.

<details><summary>Доказательства</summary>

psql: eligible за 24ч по воркспейсам → disruption 1086 / clustered ровно 400; ai_research 190/7; design 60/10. Итого по всем eligible-постам (embedding_status='done' AND relevance_score>=0.6 AND published_at IS NOT NULL): clustered 55308 / eligible 187944 = 29%. По месяцам публикации: 2026-04 10747/26073, 2026-05 3314/52091 (6.4%), 2026-06 6977/51475 (13.6%), 2026-07 26339/44326. Файлы: worker/services/semantic_clustering.py:2486-2494, :491-522, shared/config.py:542-543, :435-438

</details>

### 2. ✅ СДЕЛАНО 2026-08-04 — ~~RSI-контур одобрения недостижим через шлюз~~

> **Закрыто заходом 6.** Все десять инструментов выведены в `mcp/mcp_gateway.py`;
> шлюз отдаёт 32 против прежних 22 — столько же, сколько REST. Проверено живым
> хендшейком: `list_entity_merge_proposals` вернул настоящие данные
> (`MMAO` ↔ `Metabolic Multi-Agent Optimizer (MMAO)`).
>
> Против повторения — контрактный тест `tests/test_mcp_gateway_contract.py`
> (сравнение множеств в обе стороны + поимённая проверка контура одобрения).
> Попутно: `mcp-gateway` отсутствовал в `DEFAULT_SERVICES` скрипта сборки, из-за
> чего штатный деплой не обновлял его никогда — добавлен.
>
> **Внимание:** четыре из десяти — пишущие (правка графа Neo4j и порогов
> детектора), и это меняет риск, принятый по пункту 3.
> См. [принятые решения](./AUDIT-2026-08-04.md#8-принятые-решения).

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
>
> **ПОДТВЕРЖДЕНО ПОВТОРНО тем же вечером, уже с полным знанием последствий.**
> Заходом 6 в шлюз выведены четыре пишущих инструмента тяжелее прежних —
> `approve_entity_merge` (слияние понятий в графе Neo4j, обратно автоматически
> не разделяется), `approve_threshold_change` (правка порога детектора),
> `reject_entity_merge`, `mark_relevance_audit`. Формально это то самое условие
> пересмотра, которое было записано утром. Владельцу оно предъявлено дословно,
> вместе с ценой закрытия (бинд на `127.0.0.1` + SSH-туннель, из-за чего MCP
> перестаёт работать при опущенном туннеле). **Решение подтверждено: сервис
> в локальной сети, посторонних в ней нет.**
>
> Пункт остаётся в реестре как фактура, но из очереди работ выведен окончательно.
> **Не поднимать заново** — вопрос обсуждён дважды, второй раз с полным составом
> пишущих инструментов на руках. Основание для нового разговора — только смена
> сетевого контура: проброс порта наружу или доступ в эту сеть с недоверенных
> устройств.

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

### 5. ✅ СДЕЛАНО 2026-08-04 — ~~Восстановление не написано и не проверено~~

> **Закрыто заходом 2.** Появился `scripts/restore-stack.sh` (режимы `verify`,
> `drill`, восстановление в прод под флагом, `fetch` из S3) и проверка дампа
> через `pg_restore --list` прямо в `backup-stack.sh` — непустой файл больше
> не считается доказательством. **Учение проведено: RTO для БД 84 секунды**,
> схема восстановилась целиком, числа сходятся. Подробности —
> [маршрут, заход 2](./AUDIT-2026-08-04.md#7-маршрут).

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

### 8. 🟡 ЧАСТИЧНО 2026-08-05 — исходы стали видимыми; ретрая и записи в БД по-прежнему нет

> **Заходом 9 закрыта наблюдаемость.** `frontier_crawl_outcomes_total{outcome,reason}`
> считает **каждый** из одиннадцати выходов `enrich_url`. Тип возврата не меняли
> сознательно: одиннадцать точек выхода и один вызывающий, трактующий `None` как
> «нечего добавить», — типизированный результат тронул бы всю цепочку ради того же,
> что даёт счётчик.
>
> **AST-тест нашёл ветку, которую я пропустил** — `304 Not Modified`, и это успех,
> а не отказ. Именно необследованные ветки давали расхождение замеров: класс
> `timeout` (399/сут) раньше не считали вовсе, и доля отказов гуляла между 41% и 35%.
> Свежий замер: 1403 успеха против 1497 неуспехов за сутки.
>
> Алерт `FrontierCrawlFailureRateHigh` — на **долю**, а не на число: краул отказывает
> всегда (часть ссылок мертва по природе), и абсолютный порог означал бы либо вечное
> firing, либо слепоту.
>
> **Осталось:** запись признака «краул не дал контента» в `post_enrichments`/`indexing_status`
> и ретрай. Без этого «у поста нечего краулить» и «источник отдал 403» по-прежнему
> неотличимы **в данных** — различимы стали только в метриках. Плюс не сделан
> дешёвый фильтр шеринг-виджетов (`facebook.com/sharer` и т.п., ~90 отказов/сут).

### 8-bis. Исходная формулировка

> **Пересняно вечером 04.08:** доля **35%**, не 41 (728 провалов на 1337 успехов
> за сутки). Механизм не изменился, код тот же. Разбивка отказов: 403 — 514,
> 404 — 56, 401 — 18, 503 — 3. Уточнение к исходному замеру: `HTTP error` тогда
> считался с наложением на `Browser HTTP error`; раздельно — 406 браузерных
> и 190 обычных. По крауле в Prometheus по-прежнему ровно одна непустая серия,
> и та про пересоздание сессий.

`enrichment` · работает частично · объём M

- [ ] **Есть.** Crawl4AIService потребляет stream:posts:crawl, сохраняет kind='crawl' в post_enrichments и триггерит reindex (crawl4ai/crawl4ai_service.py:76-136). В БД 88319 crawl-обогащений, за 24ч сохранено 1068.
- [ ] **Не хватает.** Все неуспехи — тупик. crawl4ai_service.py:96-102 ловит исключение, пишет log.warning и идёт дальше; :133-136 при пустом results пишет log.info('No crawl results') и делает XACK. EnrichmentEngine.enrich_url (crawl4ai/enrichment_engine.py:239-292) возвращает None на rate-limit, 429, любой не-200 и на ошибку браузерного фетча — тоже только log.info. Нигде не пишется статус в post_enrichments/indexing_status, нет счётчика в shared/metrics.py (единственная crawl-метрика — frontier_crawl_session_recreates_total), нет DLQ и нет повторной попытки. Отличить «у поста нечего краулить» от «источник отдал 403» из данных невозможно.

<details><summary>Доказательства</summary>

ssh, docker logs frontier-intelligence-crawl4ai-1 --since 24h: 'Crawl enrichment saved' 1068, 'No crawl results' 422, 'Browser HTTP error' 472, 'HTTP error' 152, 'Rate limited' 111, 'Crawl failed' 0. Файлы: crawl4ai/crawl4ai_service.py:96-102, :133-136; crawl4ai/enrichment_engine.py:239-292

</details>

### 9. ✅ СДЕЛАНО 2026-08-05 — ~~stream:posts:enriched: стрим, в который только пишут~~

> **Закрыто досрочно, вне очереди захода 9, и по конкретному поводу.** Детектор
> осиротевшего продюсера, поставленный заходом 8, поймал этот стрим на первом же
> скрейпе и встал в pending — то есть через полчаса начал бы слать в Telegram про
> известную проблему каждые 6 часов. Это ровно то нытьё на известном состоянии,
> от которого заход 7 лечил карантинный алерт OpenRouter, поэтому дешевле было
> снять причину, чем терпеть симптом.
>
> **Сделано:** публикация в `stream:posts:enriched` убрана из `enrichment_task`,
> объявление `STREAM_OUT` снято. Consumer-группу заводить **не** стали: группа без
> потребителя перестанет терять данные и начнёт копить pending, то есть проблема
> сменит форму и станет шумом в алертах.
>
> **Что не сделано намеренно:** ключ в Redis не удалён. `DEL` — запись в Redis,
> и она сведена в одну согласованную операцию вместе с разовой зачисткой 85
> призрачных консьюмеров (заход 9). Пока ключ жив, `entries-added` заморожен на
> 47 635 и `increase[1h]` обнулится сам примерно через час после раската —
> тогда же погаснет и алерт. Это и есть проверка детектора: он обязан потухнуть.

### 9-bis. Исходная формулировка

> **Пересняно вечером 04.08:** `entries-added` **47 313** при длине 10 004 —
> то есть **37 309 событий вытеснены триммингом, не прочитанные никем**.
> Это прямая мера бесполезной работы продюсера, в исходном пункте её не было.

`ingest` · код есть, не подключён · объём S

- [ ] **Есть.** EnrichmentTask объявляет STREAM_OUT='stream:posts:enriched' (worker/tasks/enrichment_task.py:28) и публикует туда событие на КАЖДОМ успешно обогащённом посте (enrichment_task.py:929-936: post_id, workspace_id, source_id, category, relevance_score, concept_count). Стрим живой и упирается в потолок тримминга.
- [ ] **Не хватает.** Подписчика нет ни одного. `XINFO GROUPS stream:posts:enriched` возвращает пустой список, `XLEN` = 10024 (ровно потолок STREAM_MAXLEN=10_000, shared/redis_client.py:19). Grep 'posts:enriched' по всему дереву вне docs даёт единственное совпадение — саму строку объявления. То есть точка fan-out для downstream-потребителей (аналитика, доставка, алерты) размечена, но ни один сервис на неё не подписан; событие просто вытесняется тримом.

<details><summary>Доказательства</summary>

ssh: `docker compose exec redis redis-cli XINFO GROUPS stream:posts:enriched` → пусто; `XLEN` → 10024. Grep 'posts:enriched' (glob !docs) → worker/tasks/enrichment_task.py:28. Живые стримы: KEYS 'stream:*' → parsed, crawl, vision, reindex, enriched

</details>

### 10. 🟡 ЧАСТИЧНО 2026-08-06 — auto_hmi наполняется, но трендовая поверхность по-прежнему пуста

> **Заходом 11 включено пять источников из десяти**, и поток пошёл: постов **54**
> вместо 3, `post_candidates` ночного прогона 14 вместо 1, накопилось 8 emerging_signals
> и 8 missing_signals. Флаги совпадают с YAML (`config/sources.yml:2373, 2389, 2405,
> 2429, 2456`), то есть включение сделано по правилу «PATCH + YAML» и bootstrap его
> не собьёт. Выключены все пять telegram-каналов.
>
> **Формулировку ниже надо сузить:** «любой вызов search_frontier/list_clusters
> с workspace='auto_hmi' вернёт пусто» сегодня неверно — 25 постов проиндексированы,
> `search_frontier` и `list_emerging_signals` отвечают. Пусто возвращают только
> `list_clusters(kind='trend')` и `search_trend_clusters`: `trend_clusters` по auto_hmi
> нет ни одного за всю историю, `signals_promoted_to_stable` последнего прогона — 0.
>
> **Замечено попутно:** у включённого `auto_rss_insideevs_ux` в собственном комментарии
> (`config/sources.yml:2447-2449`) написано «фид мёртв у источника, включать смысла
> нет» — а флаг стоит `true`. Замер подтверждает комментарий: 1 пост, свежайший
> от 2025-07-15. Развилка для владельца в [AUDIT-2026-08-06.md §6](./AUDIT-2026-08-06.md).

### 10-bis. Исходная формулировка

`ingest` · работает частично · объём S

- [ ] **Есть.** Воркспейс полностью описан в config/workspaces.yml:190-247 (6 категорий, threshold 0.55, ослабленные пороги кластеризации, persist_weak_signals: true), заведены 10 источников, кластеризация по нему отрабатывает успешно, есть 1 missing_signal.
- [ ] **Не хватает.** Включён ровно один источник — auto_rss_arxiv_cs_hc_automotive; девять (5 telegram ru, techcrunch transportation, insideevs, ieee spectrum, automotiveworld) выключены. Всего 3 поста в БД (published_at 2026-08-03..2026-08-04), 1 semantic_cluster, 1 emerging_signal, 0 trend_clusters, 0 записей в weak_signal_snapshots кроме одной. post_candidates последнего прогона = 1. Любой вызов search_frontier/list_clusters/search_trend_clusters с workspace='auto_hmi' вернёт пусто, и отличить это от поломки клиенту нечем.

<details><summary>Доказательства</summary>

psql: select id,source_type,is_enabled from sources where workspace_id='auto_hmi' → 1 t / 9 f; select workspace_id,count(*) from posts where workspace_id='auto_hmi' group by 1 → 3; select workspace_id,summary from cluster_runs ... → auto_hmi "post_candidates":1; config/workspaces.yml:190-247

</details>

### 11. ✅ СДЕЛАНО 2026-08-05 — ~~Инструменты читают по id без скоупа воркспейса~~

> **Закрыто заходом 10.** Часть про аутентификацию 8102 из пункта исключена —
> она отложена решением владельца (пункт 3).
>
> Дыра оказалась шире, чем в формулировке: не три инструмента, а четыре читающих
> плюс `get_signal_timeline`, который звал `assert_known_workspace` (то есть проверял
> сам слаг) и следом тянул кластер тем же голым `WHERE id = :id`. При этом
> `workspace_id` в проекте объявлен обязательным в каждой таблице — изоляция
> существовала в схеме и не существовала на поверхности доступа.
>
> `mcp/guards.assert_row_workspace` отдаёт **404, а не 403**: 403 подтвердил бы,
> что объект с таким id существует, и превратил бы гвард в оракул для перебора.
> Поле `workspace` в четырёх Request-моделях опциональное — обратная совместимость
> с уже настроенными клиентами; ужесточение до обязательного ломает работающие
> вызовы и вынесено в отдельное решение.
>
> Структурный тест держит инвариант: инструмент, читающий строку по id и не
> проверяющий её воркспейс, роняет прогон.

### 11-bis. Исходная формулировка

`mcp` · работает частично · объём M

- [ ] **Есть.** Гвард mcp.guards.assert_known_workspace применяется в 7 местах observability.py (161,265,332,412,626,654,856) и во всех search-инструментах; в БД workspace_id — обязательная колонка везде.
- [ ] **Не хватает.** ClusterEvidenceRequest (observability.py:43-46), ClusterDetailsRequest (:60-62) и MissingSignalDetailsRequest (:65-66) не содержат поля workspace вовсе, а обработчики get_cluster_evidence (:717) и get_cluster_details не вызывают assert_known_workspace — SELECT идёт `WHERE id = :id` без фильтра по воркспейсу. Шлюз при этом опубликован как 0.0.0.0:8102 без единой проверки токена (mcp/mcp_gateway.py:18-27: allowed_hosts=['*'], allowed_origins=['*'], enable_dns_rebinding_protection=False; ни Depends, ни middleware), workspace приходит от клиента как есть (shared/search_contracts.py: default='disruption'), и среди 22 инструментов шлюза два пишущих — record_card_feedback (INSERT в card_feedback) и ingest_url (XADD в Redis). Для сравнения сам mcp опубликован на 127.0.0.1:8100.

<details><summary>Доказательства</summary>

mcp/tools/observability.py:43-46,60-66,717-749 (нет assert_known_workspace и нет фильтра по workspace_id); mcp/mcp_gateway.py:18-27,244,516; ssh: docker ps → mcp-gateway '0.0.0.0:8102->8102/tcp', mcp '127.0.0.1:8100->8100/tcp'

</details>

### 12. 🟡 ЧАСТИЧНО 2026-08-05 — DLQ стала видимой; консьюмера и экрана по-прежнему нет

> **Заходом 8 закрыта видимость, не разгребание.** `frontier_redis_dlq_length{service,stream}`
> печатает **ноль явно** для несуществующего ключа — в этом весь смысл. Ключей DLQ
> в Redis нет ни одного, и до сих пор «poison не случался» было неотличимо от
> «механизм сломан и никогда ничего не запишет». Правило `FrontierDlqNotEmpty`.
>
> Тест `test_missing_dlq_key_still_publishes_a_zero` держит именно этот инвариант:
> мутация «печатать только ненулевые» ловится.
>
> **Осталось:** консьюмера DLQ и экрана re-drive в админке нет. Разбирать придётся
> руками (`XRANGE <stream> - + COUNT 10`), и это записано в описании алерта. Делать
> автоматический re-drive до того, как в DLQ впервые что-то попадёт, — писать код
> под несуществующие данные.

`ops` · работает частично · объём M

- [ ] **Есть.** Полноценный poison-detection в enrichment: shared/config.py:534 indexing_dlq_stream='stream:posts:parsed:dlq' + vision_dlq_stream; worker/tasks/enrichment_task.py:602-638 _drop_poison_message (XADD в DLQ с payload, error, delivery_count + XACK), :640-653 _drop_poison_pending с порогом indexing_max_deliveries. Логика написана после инцидента с застрявшими disabled-source событиями и покрыта тестами (tests/test_enrichment_task.py:119,392).
- [ ] **Не хватает.** Ничего не читает DLQ и никто о нём не узнает: ключа stream:posts:parsed:dlq в Redis нет вообще (`EXISTS` → 0, `KEYS '*dlq*'` → пусто), consumer-группы нет, счётчика в shared/metrics.py нет, правила в prometheus/alerts.yml нет (слово DLQ встречается только в тексте описания FrontierRedisStreamPendingHigh, alerts.yml:392), в admin/backend/routers/ нет ни одного роута для просмотра/re-drive. Механизм «сохранить и разобрать позже» существует только в половине «сохранить».

<details><summary>Доказательства</summary>

ssh: `redis-cli EXISTS stream:posts:parsed:dlq` → 0; `redis-cli KEYS '*dlq*'` → пусто. Grep 'dlq' (case-insens) по репозиторию: shared/config.py:534, worker/tasks/enrichment_task.py:617, worker/tasks/vision_task.py, prometheus/alerts.yml:392 (только текст), admin/backend/routers/settings.py:302 (только отдача значения в конфиг). Роутов и метрик нет.

</details>

### 13. ✅ СДЕЛАНО 2026-08-05 — ~~У пайплайна нет ни одного счётчика стадий~~

> **Закрыто заходом 8.** Одно семейство `frontier_pipeline_stage_total{service,stage,workspace,outcome}`,
> а не восемь имён из исходной формулировки: кардинальность ~180 серий, разрез строится
> одним запросом, а новая стадия не требует нового имени.
>
> Инструментировано две точки: `emit_to_stream` в ingest (`published` / `publish_failed` —
> вход конвейера, то есть знаменатель, которого не существовало) и `_update_indexing_status`
> в worker (`pending` / `done` / `dropped` / `error` — та самая дыра, где жили 39% дропа).
> `workspace_id` протащен аргументом на всех путях, где событие в области видимости;
> где нет — метка честно становится `unknown`, а не теряется молча.
>
> Счётчик инкрементируется **до** записи в БД намеренно: это счётчик попыток перевести
> пост в стадию, и расхождение между ним и содержимым `indexing_status` само по себе
> сигнал — запись обёрнута в `try/except` у трёх вызывающих из пяти, то есть её провал
> сейчас не виден ниоткуда.
>
> Правила: `FrontierPipelineErrorBurst` (порог абсолютный: ошибки редки, 32 за всю историю)
> и `FrontierIngestPublishFailing`. Регрессия доли дропа **не** заведена сознательно —
> базовая линия требует окна длиннее ретенции Prometheus в 200ч, это пункт 42.
>
> Тесты: `tests/test_pipeline_stage_metrics.py`. Мутационный прогон поймал дыру в первой
> редакции — тесты проверяли помощник, но не то, что его кто-то вызывает, то есть
> воспроизводили ровно дефект пункта 24. Добавлены кейсы на реальные точки вызова.

`ops` · работает частично · объём M

- [ ] **Есть.** Метрик много — 78 семейств на /metrics воркера. Есть подробная телеметрия LLM и провайдеров (frontier_llm_requests_total, frontier_gigachat_*, frontier_openrouter_*), свежесть данных frontier_last_post_age_seconds и состояние стримов frontier_redis_stream_lag / _pending / _oldest_pending_age_seconds / _consumer_idle_seconds (shared/metrics.py:335-358).
- [ ] **Не хватает.** Ни одного счётчика самого конвейера: нет posts_ingested, posts_enriched, posts_dropped_by_relevance, qdrant_upserts, neo4j_writes, crawl_results, vision_results, indexing_errors. Из-за этого 39% дропа по релевантности (122034 из 312041), 41% потерь краула и 400-й потолок кластеризации не видны ни на одном дашборде и не могут стать алертом — деградация обнаруживается только когда данные вообще перестают поступать (FrontierNoNewPosts, 6–24ч).

<details><summary>Доказательства</summary>

ssh, worker:9090/metrics → 78 семейств, среди них нет ни одного с корнем posts_/ingest_/enrich_/index_. Grep 'crawl|vision|ingest|stream' по shared/metrics.py даёт только CRAWL_SESSION_RECREATES_TOTAL и блок REDIS_STREAM_*. psql: indexing_status по embedding_status → done 189997, dropped 122034, pending 7, error 3

</details>

### 14. ✅ СДЕЛАНО 2026-08-05 — ~~Redis, Postgres и Neo4j не скрейпятся Prometheus~~

> **Закрыто заходом 8, но не тем способом, который напрашивался.** Оба канонических
> пути на этом хосте закрыты, и это проверено, а не предположено: экспортёры-контейнеры
> не притащить (`docker pull` падает по I/O timeout, Docker Hub недоступен), self-metrics
> нет ни у одного образа — `neo4j:5.15-community` отдаёт `/metrics` только в Enterprise,
> у `redis`/`postgres` alpine эндпоинта нет вовсе. Остался третий, уже трижды обкатанный
> путь: `scripts/export-storage-metrics.sh` в textfile-коллектор, cron `*/10`.
>
> Шестнадцать метрик, **у каждого блока свой `_up`** — это главное требование: Prometheus
> не отличает «значение равно нулю» от «серии нет», и молчащий экспортёр выглядел бы
> ровно как здоровая система с нулевым вытеснением. Именно так 31.07 нулевой lag выдал
> себя за порядок.
>
> **Периодичность разведена по замеру, а не по интуиции:** `docker exec redis-cli INFO` —
> 0.061с, `docker exec cypher-shell` — 1.772с. Тридцатикратная разница не в запросе
> (count-store отвечает за O(1)), а в клиенте: JVM стартует заново на каждый вызов.
> Redis и Postgres снимаются каждый прогон, граф — не чаще раза в 540с, между опросами
> значение берётся из кэша. Порог 540, а не 600, чтобы джиттер крона `*/10` не отбрасывал
> опрос до следующего тика. Рядом лежит `frontier_neo4j_measured_timestamp_seconds`:
> без неё кэшированные значения были бы неотличимы от свежих.
>
> Семь правил в группе `frontier_storage`, включая `FrontierRedisMemoryHigh` — то самое,
> чего не хватало 31.07 — и `FrontierStorageMetricsExporterDown` как страховку от тихой
> смерти самого экспортёра.

`ops` · работает частично · объём M

- [ ] **Есть.** Мониторинг развёрнут и работает: 8 активных таргетов, все up (worker, ingest, crawl4ai, mcp, admin, qdrant, alertmanager, node-exporter), 56 правил в prometheus/alerts.yml, алерты на падение сервисов FrontierCoreServiceDown (alerts.yml:495-504, up{job=~"worker|ingest|qdrant"}), FrontierControlPlaneServiceDown, FrontierSupportServiceDown.
- [ ] **Не хватает.** В prometheus/prometheus.yml нет job'ов для redis, postgres и neo4j — ни экспортёров, ни self-metrics. Соответственно нет ни up{job="redis"}, ни метрики used_memory: Redis-OOM 31.07.2026, который положил ingest+enrichment на 9 часов при lag/pending=0, до сих пор нечем поймать. Приняты только смягчения (STREAM_MAXLEN=10k в shared/redis_client.py:19, maxmemory 2g), детекция не сделана. Postgres и Neo4j в том же положении.

<details><summary>Доказательства</summary>

ssh: curl 127.0.0.1:9090/api/v1/targets → 8 таргетов, redis/postgres/neo4j отсутствуют. prometheus/prometheus.yml: scrape_configs = worker, ingest, crawl4ai, mcp, admin, qdrant, alertmanager, node-exporter. redis-cli INFO memory → used_memory 255.99M при maxmemory 2147483648, maxmemory-policy volatile-ttl

</details>

### 15. ✅ СДЕЛАНО 2026-08-04 — ~~Мониторинг свежести аналитики не покрывает trend_clusters, missing_signals и card_feedback~~

> **Закрыто заходом 7.** Экспортёр расширен с двух таблиц до пяти, и — важнее — выборка
> идёт от списка воркспейсов через `CROSS JOIN`, а не от `GROUP BY` по самой таблице.
> Прежняя форма пропускала воркспейс, у которого строк нет вовсе: серии не возникало,
> `max by (table)` по несуществующей серии не считается, и алерт не мог сработать никогда.
> Добавлен второй гейдж `frontier_analysis_rows_total{table,workspace}` — отличие
> «таблица пуста» от «экспортёр до неё не дошёл».
>
> **Первый же замер подтвердил, ради чего это делалось:** у `design` последний стабильный
> тренд датирован **14.07.2026** (21 день), у `ai_products_media` — 02.08. Всё это время
> метрика свежести по ним показывала 2–3 часа, потому что считалась по другим таблицам.
>
> Трендовая тишина выведена в отдельное правило `FrontierTrendClustersStale`
> (`severity: info`, **без** `notify`, порог 7 суток), а будящие `FrontierAnalysisStale`
> и `...Critical` сужены селектором `table=~`. Причина: тишина в `trend_clusters` — не
> отказ, а следствие структурных ворот, и решение по ней за владельцем
> ([развилка 29](./AUDIT-2026-08-04.md#2026-08-04--открытые-развилки--ждут-решения-владельца)).
> Правило с `notify` висело бы firing неделями — та же болезнь, что лечил пункт 56.
> По `card_feedback` правила нет вовсе: ноль строк там ожидаем.
>
> Тесты: `tests/test_alert_rules_contract.py` — селектор по `table` сверяется со списком
> таблиц, которые экспортёр реально печатает; отдельный кейс держит `FrontierTrendClustersStale`
> тихим, пока развилка 29 открыта. Оба проверены мутацией.

`ops` · работает частично · объём S

- [ ] **Есть.** После инцидента 31.07-02.08 сделан textfile-экспортер scripts/export-analysis-freshness.sh и два алерта FrontierAnalysisStale / FrontierAnalysisStaleCritical (48ч/96ч) — они реально работают, 12 живых серий, возраст 2.6-2.7ч по всем воркспейсам.
- [ ] **Не хватает.** SQL экспортера охватывает ровно две таблицы: emerging_signals и semantic_clusters. trend_clusters, missing_signals и card_feedback не отслеживаются. Именно поэтому 21 день без единого тренда в design и 10 дней в ai_products_media прошли молча — метрика по этим воркспейсам всё это время показывала свежесть 2-3 часа. Тот же класс молчаливого отказа, против которого экспортер и вводился.

<details><summary>Доказательства</summary>

ssh: cat scripts/export-analysis-freshness.sh — SELECT 'emerging_signals' ... UNION ALL SELECT 'semantic_clusters' ... (третьей таблицы нет); prometheus/alerts.yml FrontierAnalysisStale expr по frontier_analysis_last_update_timestamp_seconds; curl 9090/api/v1/query frontier_analysis_last_update_timestamp_seconds → 12 серий, только emerging_signals+semantic_clusters, все ~2.6ч

</details>

### 16. ✅ СДЕЛАНО 2026-08-06 — ~~Продуктовый слой без телеметрии~~

> У шлюза появились `/healthz` и `/metrics` (`@mcp.custom_route`, механизм штатный —
> сверено по докам MCP Python SDK), счётчик `frontier_mcp_tool_calls_total{tool,outcome}`
> и гистограмма `frontier_mcp_tool_duration_seconds{tool}`. Верхний бакет 120с не
> случаен: синтез в `search_balanced` ходит в LLM с таймаутом 120, без него весь
> хвост попадал бы в `+Inf` одной кучей.
>
> **Точка инструментации одна на все 32 обёртки** — `_finish(r)`. Имя инструмента
> берётся из URL запроса, а не из аргумента: так его нельзя забыть, добавляя
> тридцать третью обёртку.
>
> Проверено вживую: рукопожатие MCP → `list_clusters` ok 2, `get_cluster_details`
> ok 1 / error 1, гистограмма считает; Prometheus эти серии видит.

### 16-bis. Исходная формулировка

`ops` · только спроектировано · объём M

- [ ] **Есть.** Сервис mcp — цель Prometheus (http://mcp:8100/metrics, health=up) и экспортирует общие LLM-метрики: по job='mcp' видно task='mcp_synthesis'. Есть 56 алертов в prometheus/alerts.yml.
- [ ] **Не хватает.** В shared/metrics.py нет ни одного счётчика/гистограммы про MCP-инструменты: поиск 'duration' по файлу не даёт ни одного имени, метрик вида rag_query_duration_seconds / trend_clusters_detected_total / missing_signals_detected_total (заявлены в .cursor/rules/02-tech-stack.mdc:126 и 06-devops.mdc:133) не существует. Ни один из 56 алертов не покрывает поверхность доставки. У mcp-gateway эндпоинта метрик нет физически: curl 127.0.0.1:8102/metrics → 404, и в списке scrape-целей Prometheus его нет. Единственный источник данных о реальном использовании — логи контейнера: за 7 суток на REST прошло 2 вызова (оба /tools/list_emerging_signals), на /mcp — 16 POST, преимущественно ListToolsRequest.

<details><summary>Доказательства</summary>

ssh: curl 9090/api/v1/targets → 8 целей, mcp-gateway отсутствует; curl 127.0.0.1:8102/metrics → HTTP 404; docker logs frontier-intelligence-mcp-1 --since 168h | grep 'POST /tools' → 2 строки; grep 'duration' shared/metrics.py → 0; .cursor/rules/02-tech-stack.mdc:126

</details>

### 17. ♻️ ОТКАТИЛОСЬ, ИСПРАВЛЕНО 2026-08-06 — ~~У бэкапов и квоты S3 нет ни метрики, ни алерта~~

> **Бэкапная половина работает, квотная была обезоружена собственной починкой.**
> Замер 06.08: в кэше `10717881069`, в экспозиции `frontier_s3_bucket_bytes 2147483647` —
> ровно INT32_MAX. Причина: `scripts/export-backup-metrics.sh:78` читал байты через
> `awk 'printf "%d"'`, а на хосте `awk` = **mawk 1.3.4**, который печатает `%d` через
> int32 и насыщает. Отношение выходило `0.133` вместо `0.665`, поэтому
> `FrontierS3QuotaHigh` (`> 0.85`, `for: 1h`) не могла сработать **ни при каком
> наполнении**, а `FrontierS3QuotaCritical` считала свободное место от обрезанного
> числа. За окно ретенции бакет доходил до **88% квоты** — с исправной метрикой алерт
> бы сработал.
>
> Класс тот же, что у всего реестра, только вывернутый: предыдущая редакция той же
> строки (`$2+0`) давала `1.03665e+10`, и это выглядело как поломка. Насыщенное
> `2147483647` выглядит как честное измерение.
>
> **Исправлено 06.08:** кэш читается встроенным `read` без числовых конверсий,
> `\r` срезается. Проверено на сервере — `10717881069`, отношение `0.6655`.
> Тесты `tests/test_export_backup_metrics.py`: три поведенческих кейса плюс
> структурный (поведенческий на хосте с `gawk` позеленел бы и на сломанном коде).
> Разбор — [AUDIT-2026-08-06.md §3.1](./AUDIT-2026-08-06.md).
>
> **Устарело в тексте ниже:** «алерты по квоте уже pending, бакет заполнен на 87.9%» —
> на 06.08 реальное наполнение 66.5%, и решения владельца этот пункт больше не ждёт.

> **Исходное закрытие (заход 2, 04.08).** `scripts/export-backup-metrics.sh` в cron
> пишет свежесть бэкапа, размеры артефактов и занятость бакета в textfile-коллектор;
> добавлена группа правил `frontier_backup` (5 штук): протухший бэкап, падающий прогон,
> квота >85%, свободного места меньше одного бэкапа, смерть самого экспортёра.

`ops` · только спроектировано · объём S

- [ ] **Есть.** Механизм для дешёвой метрики уже развёрнут и работает: node-exporter смонтирован с ./prometheus/textfile:/var/lib/node_exporter/textfile и флагом --collector.textfile.directory, а через него уже живёт frontier_analysis_last_update_timestamp_seconds (cron */10, алерт FrontierAnalysisStale). Есть scripts/s3_bucket_usage.py, считающий занятое место.
- [ ] **Не хватает.** Ни одного правила про бэкапы или S3 среди 56: `grep -iE 'backup|s3|restore' prometheus/alerts.yml` → пусто (есть только FrontierHostRootDiskFilling/Critical про корень хоста). Замер сейчас: 13.1 GiB из квоты ~15 GiB, в бакете 3 дня бэкапов по 3.5-3.6 GiB — свободного места меньше одного суточного бэкапа. Когда backup-stack.sh упадёт на переполнении квоты или на выгрузке, узнать об этом будет неоткуда: cron пишет только в backups/cron.log, метрики свежести бэкапа нет.

<details><summary>Доказательства</summary>

prometheus/alerts.yml (56 правил, ни одного backup/s3); docker-compose.yml node-exporter volumes ['./prometheus/textfile:/var/lib/node_exporter/textfile:ro'] + command --collector.textfile.directory; s3_bucket_usage.py → 'TOTAL: 28456 objects, 13.1GiB', backups 10.6GiB, days=3 по ~3.5GiB

</details>

### 18. 🟡 ЧАСТИЧНО 2026-08-06 — причина найдена и наполовину устранена; вторая половина требует прав администратора

> **Диагноз, записанный ниже 04.08, оказался неполным: режимов отказа ДВА, а не один.**
> Разбор — [AUDIT-2026-08-06.md §3.4](./AUDIT-2026-08-06.md).
>
> *Режим A — процесс умер после старта.* 03.08 и 04.08 в `.claude/alert-triage.log` есть
> строка `start` в 09:15:02 без парной `exit=`. Это и есть исходная гипотеза про консольный
> сигнал при закрытии сессии.
>
> *Режим B — задача не стартовала вовсе.* 06.08 строки `start` в 09:15 **нет ни одной**,
> а дайджест написан в 21:39 — то есть прогон отложился и отработал позже. Против этого
> режима `RestartCount=2` бессилен по построению: перезапускать нечего.
>
> Смягчения, которые реестр оставлял на владельца, оказались **уже применёнными**:
> `RestartCount=2`, `RestartInterval=PT10M`, `StartWhenAvailable=True`. Именно последнее
> и вытащило прогон 06.08 на вечер.
>
> **Сделано 06.08:** `WakeToRun = True` — задача теперь будит машину к 09:15, а не ждёт,
> пока её включат. Против режима B это прямое лечение.
>
> **Осталось — режим A, и требует прав администратора.** Перевод на `LogonType = S4U`
> («Run whether user is logged on») из непривилегированной сессии отдаёт
> `Set-ScheduledTask: Access is denied`. Команда для запуска **в консоли от администратора**:
>
> ```powershell
> $p = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Limited
> Set-ScheduledTask -TaskName 'FrontierAlertTriage' -Principal $p
> ```
>
> Откат — из снимка `FrontierAlertTriage.backup.xml`, снятого перед правкой
> (`Register-ScheduledTask -Xml (Get-Content … -Raw) -TaskName … -Force`).
>
> **Риск, который надо назвать вслух:** под S4U задача исполняется в неинтерактивной
> сессии. Петля запускает `claude -p /alert-triage`, и если CLI чего-то ждёт от
> интерактивной сессии, режим A сменится на «не работает всегда» вместо «не работает
> через раз». Поэтому после переключения первый прогон проверять руками:
> `Start-ScheduledTask -TaskName 'FrontierAlertTriage'` и хвост `.claude/alert-triage.log`
> на парную `exit=`.
>
> **Отдельно:** порог алерта 26ч не переживает отложенный прогон. 05.08 дайджест в 09:19,
> 06.08 в 21:40 — разрыв 36ч, и `FrontierAlertTriageStale` честно горел с 08:19 UTC.
> Пока режим B не закрыт полностью, это ожидаемое поведение, а не ложное срабатывание.

### 18-bis. Исходная формулировка (04.08)

> **Заходом 7 закрыта поставка, не причина.** Разделено намеренно: «разобрать
> `LastTaskResult 3221225786`» не имеет проверяемого исхода, и если бы критерием
> готовности был «дайджест пришёл два дня подряд», пункт закрылся бы случайно.
>
> **Сделано.** `scripts/export-analysis-freshness.sh` пишет
> `frontier_alert_triage_last_digest_timestamp_seconds` — время записи последнего
> дайджеста **на сервере**; правило `FrontierAlertTriageStale` (порог 26ч, `absent()`
> в условии) будит, если дайджеста нет дольше суток с запасом.
>
> Наблюдатель намеренно снаружи наблюдаемого: петля живёт на Windows и умирает
> по консольному сигналу, то есть сообщить о собственной смерти не может физически.
> Порог 26ч, а не 24: петля ходит раз в сутки в 09:15, и в здоровом режиме возраст
> дайджеста доходит почти до 24 часов — порог 24ч давал бы ложное срабатывание каждую
> ночь. Тест `test_alert_triage_threshold_exceeds_a_full_day` держит это (мутация
> «опустить до 86400» ловится).
>
> **Осталось: причина.** Две очевидные гипотезы сняты измерением, третья осталась
> без подтверждения.
>
> *Не таймаут задачи.* `ExecutionTimeLimit = PT30M`, а здоровый прогон по логу
> занимает **1.3–2.7 минуты** (14 замеров, 19.07–02.08). До лимита не близко.
>
> *Не батарея.* `StopIfGoingOnBatteries = True`, но `Win32_Battery` пуст — машина
> стационарная.
>
> *Что видно вместо этого.* Падают только прогоны **по расписанию в 09:15**;
> ручные — нет. Последние успешные запуски датированы 01.08 18:25 и 02.08 10:35,
> оба вне расписания. Ряд сбоев: 24.07, 27.07, 31.07, 03.08, 04.08 — каждый
> `start` без парного `exit=`, то есть `powershell.exe` умер между строкой 19 и
> строкой 30 `run-alert-triage.ps1`, не дойдя до записи кода возврата. Значит его
> убили снаружи: `0xC000013A` (`STATUS_CONTROL_C_EXIT`) — это то, что получает
> консольный процесс при завершении сессии или остановке задачи планировщиком.
>
> Совместимая с фактами гипотеза: задача заведена с `LogonType: Interactive` и
> `WakeToRun: False`, то есть исполняется в сессии пользователя, а в 09:15 машина
> может спать или только просыпаться — процесс стартует и погибает на переходе
> питания или сессии. Не подтверждено: воспроизводить надо на живом переходе.
>
> Дешёвые смягчения, не требующие знания причины (**решение за владельцем**,
> потому что это правка настроек его машины): `RestartCount = 2` с
> `RestartInterval = PT10M` — транзиентное убийство самолечится;
> `StartWhenAvailable = True` — пропущенный запуск отрабатывает при первой
> возможности; `WakeToRun = True` — будит машину, но именно этого можно и не хотеть.

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

### 21. ✅ СДЕЛАНО 2026-08-04 — ~~Три из четырёх точек входа деплоя невалидны~~

> **Закрыто заходом 5.** Наборы профилей объявлены один раз в
> `scripts/compose-profiles.sh` и читаются оттуда всеми точками входа; добавлена
> проверка `frontier_assert_profiles` перед сборкой и цель `make check-profiles`.
> Набор `--profile monitor` починился сам в заходе 1, когда у `alertmanager` сняли
> `depends_on: admin`. Проверено: все восемь наборов валидны.
> Подробности — [маршрут, заход 5](./AUDIT-2026-08-04.md#7-маршрут).

`ops` · код есть, не подключён · объём S

- [ ] **Есть.** Рабочий эталон один — scripts/server-build-stack.sh с COMPOSE_PROFILES="core,ingest,xray,worker,crawl,paddleocr,mcp,admin". Он проходит валидацию.
- [ ] **Не хватает.** Тот же набор в остальных трёх. Проверено прямо на сервере, все три падают на этапе `docker compose config`: (1) scripts/server-deploy-rebuild.sh — COMPOSE_PROFILES=core,worker,mcp,crawl,paddleocr → 'service "crawl4ai" depends on undefined service "xray": invalid compose project'; (2) Makefile ALL_PROFILES=--profile core --profile ingest --profile worker --profile mcp --profile admin → 'service "ingest" depends on undefined service "xray"'; (3) `--profile monitor` (команда раскатки Grafana из docs/monitoring-runtime-dashboard.md) → 'service "alertmanager" depends on undefined service "admin"'. При этом docs/ops-server-troubleshooting.md §5 предлагает именно server-deploy-rebuild.sh как основную практику деплоя правок кода.

<details><summary>Доказательства</summary>

ssh: `COMPOSE_PROFILES=core,worker,mcp,crawl,paddleocr docker compose config --services` → invalid; `docker compose --profile core --profile ingest --profile worker --profile mcp --profile admin config --services` → invalid; `docker compose --profile monitor config --services` → invalid; grep Makefile ALL_PROFILES; grep COMPOSE_PROFILES scripts/server-build-stack.sh (эталон)

</details>

### 22. 🔒 ОТЛОЖЕНО решением владельца 2026-08-04 — ~~Хостового файрвола нет, бинды разъехались, кука без secure~~

> **Решение владельца: принять как осознанный риск, из очереди снять.** Основание то же,
> что у пункта 3: хост в локальной сети `192.168.31.0/24`, наружу не проброшен.
> Принято: неактивный `ufw`, `grafana:3000` и `admin:8101` на `0.0.0.0`, plain HTTP
> и сессионная кука без `secure` (TLS в стеке нет принципиально — Caddy исключён).
> Формулировка и условия пересмотра —
> [AUDIT-2026-08-04.md, раздел 8](./AUDIT-2026-08-04.md#2026-08-04--внешняя-экспозиция-и-отсутствие-файрвола--принятый-риск-пункт-22).
>
> Пункт остаётся в реестре как фактура. Основание для нового разговора — смена
> сетевого контура либо появление второго пользователя.

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

### 24. ✅ СДЕЛАНО 2026-08-05 — ~~RSI-метрики не отдают ни одной серии~~

> **Закрыто заходом 8 третьим путём — ни multiprocess, ни textfile.**
>
> `PROMETHEUS_MULTIPROC_DIR` отпал: он переключает **весь** реестр процесса, и голые
> Gauge, на которых висят живые правила, начинают требовать явного `multiprocess_mode` —
> цена несоразмерна выгоде. Textfile отпал: каталог не смонтирован в `admin`, а
> `frontier_novelty_judge_total` — Counter, монотонность которого пришлось бы держать
> руками.
>
> Оказалось, что родитель уже получает разобранный JSON ребёнка — этого достаточно.
> `_run_job_subprocess` теперь раскладывает полезную нагрузку по тем же сеттерам
> с `service="admin"`. Ключевая проверка перед работой: при `workspace_id=__all__` все три
> джоба отдают разбивку `results` с `workspace_id` внутри каждого элемента, то есть метки
> есть чем размечать — без этого пункт стоил бы правки контракта дочернего процесса.
> `confirmed_weak` не приходит отдельным полем, но выводится точно как `judged - underrated`.
>
> Оба зависящих правила — `FrontierGraphDuplicateClustersRising` и `FrontierNoveltyJudgeFailing` —
> не фильтруют по `service`, поэтому `admin` их устраивает.
>
> **Попутно закрыта вторая половина, которой в формулировке не было:** при исключении
> `manual_jobs` пишет JSON в **stderr** и возвращает 1, то есть перепубликовывать нечего,
> и провальный прогон не оставлял в метриках ничего. Заведён
> `frontier_admin_job_runs_total{job,outcome}` с инкрементом **до** `raise`, плюс правило
> `FrontierAdminJobFailing`, различающее `failed` и `timeout` (последнее важно: ночная
> кластеризация `disruption` уже занимает 523–673с из 900с таймаута).

`ops` · код есть, не подключён · объём M

- [ ] **Есть.** Имена зарегистрированы и видны в экспозиции: на http://127.0.0.1:8101/metrics присутствуют '# HELP/# TYPE frontier_novelty_judge_total', 'frontier_relevance_audit', 'frontier_graph_health'. На них уже построены дашборд grafana/dashboards/frontier-rsi.json и группа правил frontier_rsi (7 алертов).
- [ ] **Не хватает.** Экспорт значений из дочерних процессов. Ни одного сэмпла: `curl /metrics | grep '^frontier_(graph_health|relevance_audit|novelty_judge)'` → пусто, и в Prometheus count() по всем трём = 0. Причина — значения выставляются внутри субпроцесса планировщика (admin/backend/scheduler.py _run_job_subprocess → admin.backend.manual_jobs), а prometheus_client multiprocess-режим или pushgateway не подключены. Следствие: FrontierGraphDuplicateClustersRising (alerts.yml:542) и FrontierNoveltyJudgeFailing (alerts.yml:552) физически не могут перейти в firing — это дырка в наблюдаемости, замаскированная под работающий мониторинг.

<details><summary>Доказательства</summary>

ssh `curl -sS http://127.0.0.1:8101/metrics | grep -E '^# (HELP|TYPE) frontier_(graph_health|relevance_audit|novelty_judge)'` → 6 строк; тот же grep без '#' → пусто; PromQL count(frontier_graph_health)=0, count(frontier_relevance_audit)=0, count(frontier_novelty_judge_total)=0 (для контроля count(frontier_llm_requests_total)=17); prometheus/alerts.yml:542,552

</details>

### 25. 🟡 ЧАСТИЧНО 2026-08-05 — провенанс дошёл до брифа; поиск по постам не покрыт намеренно

> **Формулировка пункта оказалась неточной.** Замер 05.08: payload trend-коллекции
> провенанс УЖЕ несёт — 48 точек из 404, ровно столько же, сколько измеренных строк
> в Postgres. То есть `_trend_cluster_index_payload` пишет его правильно, а терялся
> он дальше по пути.
>
> **Сделано.** `_compact_workspace` в брифе вырезал провенанс whitelist'ом — теперь
> пропускает вложенным блоком `provenance`, и **None, а не словарь нулей** при
> неизмеренном: на 05.08 измерены 48 из 404, и словарь нулей отправил бы синтезатору
> 88% выдуманных измерений. В промпт добавлено, как этот блок читать: низкая
> независимость при высоком `echo_ratio` — это ре-синдикация, а не слабый сигнал,
> и она снижает доверие к широте, а не к силе.
>
> Проверено вживую: `disruption` отдаёт настоящие 0.8 / 0.885 / 0.65, а у emerging
> нашёлся случай `independence_score 0.15` при `echo_ratio 0.75` и одном
> дедуплицированном источнике — перепечатка, которую синтезатор раньше принял бы
> за сигнал. У `design` — `provenance: null` при живом `signal_score` 0.70.
>
> **Обезврежена мина.** `scripts/sync_trend_clusters_to_qdrant.py` был ДЕСТРУКТИВЕН:
> не выбирал провенансные колонки, а upsert заменяет payload целиком — то есть
> прогон «для бэкфилла» затёр бы 48 точек с настоящими числами, схлопнул `evidence`
> в пустой список и подменил `title_ru`/`insight`/`opportunity` автозаглушками.
> Три ухудшения разом и молча. Скрипт теперь выбирает и переносит все поля;
> `distinct_originators` передаётся без `or 0`, потому что NULL у 380 строк из 404
> обязан остаться NULL.
>
> **Осталось намеренно:** `search_frontier`/`search_balanced` работают на уровне
> ПОСТОВ, где провенанса нет вовсе, а считать его в онлайне дорого. Это надо
> записать строкой контракта инструментов, а не пытаться доделать.

### 25-bis. Исходная формулировка

`provenance` · работает частично · объём M

- [ ] **Есть.** shared/provenance.py считает deduped_source_count, distinct_voices, echo_ratio, arrival_dispersion, distinct_originators, independence_score; колонки есть в trend_clusters, emerging_signals, semantic_clusters (миграция 20260714_provenance_dedup.sql); mcp/tools/observability.py:421,443,462,635,767,810,842 выбирает и отдаёт independence_score в list_clusters / list_emerging_signals / get_cluster_details.
- [ ] **Не хватает.** shared.provenance импортируется РОВНО одним модулем — worker/services/semantic_clustering.py:16; в mcp/ его нет вообще. В payload Qdrant-коллекции trend_clusters 39 ключей и ни одного провенансного (проверено scroll: burst_score, source_count, source_diversity_score есть — independence_score, distinct_originators, echo_ratio нет), поэтому search_trend_clusters физически не может вернуть независимость. frontier_brief._compact_workspace (frontier_brief.py:67-104) вырезает её из трендов и emerging при сборке брифа. search_frontier / search_balanced работают на уровне постов, где провенанса нет вовсе.

<details><summary>Доказательства</summary>

grep 'from shared.provenance' по репозиторию → worker/services/semantic_clustering.py:16 (единственное); qdrant scroll trend_clusters__embeddingsgigar__dense_2560 → список 39 ключей payload без independence_*; mcp/tools/frontier_brief.py:67-104; mcp/tools/observability.py:767,810,842

</details>

### 26. ✅ СДЕЛАНО 2026-08-05 — ~~«не измерено» неотличимо от «полностью синдицировано»~~

> **Закрыто заходом 12.** Свежий замер: измерено 48 из 404 `trend_clusters`,
> 7368 из 34039 `emerging_signals`, 5721 из 51427 `semantic_clusters` — отсечка
> ровно 02.08.2026, смешанных дней нет. У `ai_trends` и `design` неизмеренными
> были ВСЕ trend_clusters, то есть клиент получал сто процентов фальшивых нулей.
>
> **Правка в ОДНОЙ точке, а не в тринадцати.** Реестр насчитал семь мест отдачи —
> это только те, где имя колонки набрано буквально. Ещё шесть путей отдают строку
> целиком через `SELECT *` (`get_cluster_details`, `get_signal_timeline`), и
> правка «по семи местам» оставила бы их отдавать сырой ноль, а новая ручка
> завтра добавила бы четырнадцатое. Разметка стоит в `_fetch_rows`/`_fetch_one`.
>
> Предикат тройной (`deduped_source_count` ИЛИ `distinct_voices` ИЛИ
> `independence_score`), хотя эмпирически три признака совпадают побитово —
> расхождений ноль во всех трёх таблицах в обе стороны. Механизм расхождения
> существует: `_provenance_fields` клампит deduped через `min(deduped, raw_source_count)`.
> Маркер на одном столбце был бы завязан на случайность.
>
> Бэкфилла исторических строк нет и не будет: они вне окна принятия решений.
>
> Проверено вживую: `design` → `provenance_measured=false` и `null` вместо нулей;
> `disruption` → `true` и настоящие 0.8 / 0.885 / 0.65; `get_cluster_details`
> размечен наравне со списками; хиты поиска лишнего ключа не получили.
>
> Радиус поражения проверен: смены типа `float → float|null` не увидит ни один
> потребитель — совпадения по `independence_score` вне репозитория нашлись только
> в кэшах транскриптов, ни в скилах, ни в промптах его нет.

### 26-bis. Исходная формулировка

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

### 28. ✅ СДЕЛАНО 2026-08-05 — ~~Поиск не дедуплицирует ре-синдикацию~~

> **Закрыто заходом 12.** Цифра реестра подтверждена свежим замером: за 30 дней
> в disruption 11 220 постов из 47 301 (23.7%) лежат в 4779 группах с одинаковым
> содержимым и РАЗНЫМИ source_id. В реальной выдаче это 10–13% топ-30.
>
> Схлопывание по canonical URL стоит **после сортировки и ДО среза по limit**,
> то есть до всех четырёх потребителей — own_stake, синтеза, самой выдачи и
> entity_evidence. Обе ловушки закрыты статическим тестом: дедуп после среза молча
> недодаёт (limit=10 → 7–9), дедуп после сборки промпта оставляет модель считать
> копии за подтверждения, и проверка состава выдачи этого не увидит.
>
> Над-выборка скромная (2×, потолок 120) намеренно: `hybrid_search` зовётся внутри
> цикла по воркспейсам, и при включённых мостах множитель умножается на их число.
>
> Хит без пригодного ключа не схлопывается никогда: голый хост без пути ключом не
> считается (под него подпадают разные материалы одного сайта), а хиты без url
> получают уникальный сторож — иначе слиплись бы в один.
>
> **Проверено вживую:** три запроса, выдача ровно 30 у каждого, схлопнуто 6/10/11.
> Группы настоящие: одна статья arXiv через две категорийные ленты
> (`rss_arxiv_cs_ai_cs_ro` + `rss_arxiv_cs_ro`), один материал через
> `api_hn_topstories` + `api_hn_beststories`, один через `rss_medium_design` +
> `rss_medium_ui`. При `limit=10` выдано ровно 10.
>
> **Не сделано намеренно:** текстовые перепечатки без общего URL (те самые 23.7%
> по содержимому против 22.8% по URL) в онлайне не ловятся — записано в контракт
> как «дедуп идёт только по canonical URL», а не как недоделка.

### 28-bis. Исходная формулировка

`search` · только спроектировано · объём M

- [ ] **Есть.** Механика де-синдикации написана и работает на уровне кластеров (shared/provenance.py, worker/services/semantic_clustering.py). В search_frontier есть дедуп по semantic_cluster_id, но только для отбора 6 хитов в промпт синтеза (_select_synthesis_hits, search_frontier.py:172-186) — на возвращаемый список он не влияет.
- [ ] **Не хватает.** run_search_request (search_frontier.py:416-483) отдаёт hits из hybrid_search как есть: гидрация source_score → сортировка по score → возврат. Ни canonical-URL дедупа, ни near-dup фильтра, ни пометки «это перепечатка». Замер на живой БД: за 30 дней в disruption 46772 поста, из них 11291 (24%) попадают в 4781 группу с одинаковыми первыми 90 символами контента и >1 источником. Дополнительно 86303 из 190166 точек Qdrant имеют пустой semantic_cluster_id, то есть даже кластерный дедуп в синтезе работает меньше чем на половине корпуса.

<details><summary>Доказательства</summary>

mcp/tools/search_frontier.py:416-483, :172-186; psql: with t as (select lower(left(regexp_replace(content,'\s+',' ','g'),90)) k,count(*) c,count(distinct source_id) s from posts where workspace_id='disruption' and published_at>now()-interval '30 days' group by 1 having count(*)>1 and count(distinct source_id)>1) select count(*),sum(c) from t → 4781 | 11291; qdrant count filter semantic_cluster_id='' → 86303 из 190166

</details>

### 29. Стабильные тренды формируются только в disruption — и дело НЕ в порогах

> **Диагноз уточнён замером 07.08.2026, и он опровергает напрашивавшееся объяснение.**
> Гипотеза была «пороги настроены под disruption и для остальных слишком высоки».
> Замер её не подтверждает.
>
> Гейт промоушена — шесть конъюнктов (`semantic_clustering.py:1836-1844`). Считаем
> по кандидатам за трое суток, сколько промахиваются **ровно на одном** условии —
> это ровно то, что сдвинулось бы от правки порога:
>
> | воркспейс | кандидатов | промах на одном | промах на трёх и более |
> |---|---|---|---|
> | disruption | 5226 | **90** | 4867 |
> | design | 830 | **1** | 827 |
> | ai_trends | 568 | **1** | 563 |
> | ai_research | 447 | **3** | 438 |
> | ai_products_media | 768 | **0** | 764 |
> | auto_hmi | 8 | 1 | 7 |
>
> То есть у тихих воркспейсов кандидаты не «чуть-чуть не дотягивают» — они
> структурно далеко. Понижение любого одного порога разблокировало бы 0–3 кандидата.
>
> **Почему далеко** — второй замер, состав кандидата:
>
> | воркспейс | активных источников за 3д | постов за 3д | кандидатов с ОДНИМ документом | среднее число источников на кандидата |
> |---|---|---|---|---|
> | disruption | **101** | 5450 | 80% | 1.22 |
> | ai_research | 15 | 820 | 91% | 1.06 |
> | ai_trends | 11 | 794 | 93% | 1.04 |
> | ai_products_media | 16 | 328 | 94% | 1.02 |
> | design | **8** | 170 | 88% | 1.03 |
> | auto_hmi | 2 | 16 | 75% | 1.00 |
>
> Emerging-слой почти целиком состоит из **одиночек**: один документ, один
> семантический кластер, один источник. Гейт требует темы, подхваченной минимум
> двумя кластерами, тремя-четырьмя документами и двумя источниками. Одиночка не
> пройдёт его никогда — ни при каком пороге, потому что `source_diversity` при
> одном источнике равен нулю **по определению** (`semantic_clustering.py:1703-1710`),
> а не по величине.
>
> **Вывод: это дефицит покрытия источниками, а не калибровки.** У disruption активных
> источников в 6.7 раза больше, чем у следующего воркспейса, и постов в 6.6 раза
> больше, чем у ai_research. Вероятность, что одну тему за окно подхватят два разных
> источника, у восьми источников design'а мала — отсюда 1.03 источника на кандидат.
>
> **Что из этого следует для плана.** Предложение «калибровать пороги по перцентилям
> воркспейса» ([AUDIT-2026-08-06.md §5.3](./AUDIT-2026-08-06.md)) на этих данных
> **вредно**: перцентиль по распределению, где 88–94% кандидатов — одиночки, начнёт
> промотировать одиночек в «стабильные тренды». Проверить это будет нечем:
> `card_feedback` пуст, `relevance_decisions.audit_status` NULL у всех 29 119 строк,
> то есть человеческого сигнала качества нет ни одного.
>
> **Пункт 48 (раскат источников) становится предусловием этого пункта, а не соседом.**
> У design включено 22 источника, а публиковали за трое суток — 8; у `global_mobility`
> не включён ни один из трёх. Сначала покрытие, потом разговор о порогах.

### 29-bis. Исходная формулировка

`search` · работает частично · объём L

- [ ] **Есть.** Кластеризация запускается по всем шести воркспейсам и завершается успехом: cluster_runs stage='full' status='success' за сутки есть у каждого. Semantic-слой и emerging-слой живые: ai_trends 9404 semantic / 2515 emerging, design 2470 / 2077, ai_products_media 3322 / 2250.
- [ ] **Не хватает.** Промоушен в stable не срабатывает. В последнем суточном прогоне signals_promoted_to_stable=0 и stable_trends_created_or_updated=0 у пяти воркспейсов из шести (только disruption: 4/4); signals_promoted_to_emerging=0 у четырёх. Накопленный итог: disruption 357 trend_clusters, ai_research 6, ai_trends 6, design 3 (последний 2026-07-14, 21 день назад), ai_products_media 2 (2026-07-25), auto_hmi 0 за всю историю. То есть search_trend_clusters и list_clusters(kind='trend') для воркспейсов, обслуживающих внешних MCP-клиентов ai-researcher и design-director, возвращают почти пустоту. Отдельно: signals_merged=0 и semantic_clusters_merged=0 у ВСЕХ шести — слияние кандидатов не срабатывает нигде.

<details><summary>Доказательства</summary>

psql: select workspace_id,summary from cluster_runs where stage='full' and status='success' and started_at>now()-interval '1 day' → у 5 из 6 "signals_promoted_to_stable":0, у всех 6 "signals_merged":0; select workspace_id,count(*),max(detected_at) from trend_clusters group by 1 → disruption 357/2026-08-04, ai_research 6, ai_trends 6/2026-07-28, design 3/2026-07-14, ai_products_media 2/2026-07-25, auto_hmi отсутствует; пороги: config/workspaces.yml (trend_cluster_stable_threshold 0.56-0.62)

</details>

---

## Средний приоритет (18)

### 30. ✅ СДЕЛАНО 2026-08-04 — ~~Снимок экспозиции описывает прошлую картину~~

> **Закрыто заходом 7.** Раздел «Server Exposure Snapshot» переписан: вместо одного
> плоского списка «всё опубликовано на всех интерфейсах» — три группы с указанием,
> что чем закрыто. Зафиксированы обе смены posture, которые документ пропустил
> (`mcp` на loopback 03.08, `admin` под Basic-auth), и остаточный риск 8102 назван
> прямо, вместе с полным списком пишущих RSI-инструментов, появившихся там заходом 6.
> Туда же ушло решение владельца по пункту 22.
>
> Копия состояния заменена командой сверки: снимок в документе объясняет **почему**
> так, а «как сейчас» отдаёт `docker ps`. Копия расходится с оригиналом всегда,
> вопрос лишь в сроке — то же лечение, что применили к `docs/README.md` заходом 4.

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

### 37. ✅ СДЕЛАНО 2026-08-05 — ~~Мёртвые consumer'ы копятся в crawl и reindex~~

> **Закрыто заходом 9 в корне, а не разово.** Уборка вынесена в
> `shared/stream_consumers.py` и подключена ко всем четырём потребителям —
> корреляция была прямая: где она есть, консьюмер один; где нет, накопилось 85 и 14.
> Инвариант остался консервативным: `idle > 1ч` **И** `pending == 0`, потому что
> консьюмер с ненулевым pending держит неподтверждённые сообщения, и его удаление
> осиротило бы их в PEL — превратив уборку мусора в потерю данных.
>
> Накопленные 97 призраков сняты отдельной согласованной операцией
> (`scripts/redis-cleanup-ghosts.sh`, без `--confirm` только показывает план).

### 37-bis. Исходная формулировка

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

### 39. ✅ СДЕЛАНО 2026-08-05 — ~~Тримминг не отличает прочитанное от непрочитанного~~

> **Закрыто заходом 8 детекцией, а не изменением тримминга.** `MAXLEN` трогать не стали:
> у стримов с группой 5–10 суток запаса, и подъём потолка вернул бы риск Redis-OOM,
> ради которого он и вводился. Вместо этого потеря стала наблюдаемой.
>
> `frontier_redis_stream_delivery_gap{stream,group}` = 1, когда `last-delivered-id`
> группы **старше** первой уцелевшей записи стрима: значит `MAXLEN` срезал промежуток
> между ними до доставки. `lag` и `pending` при этом нулевые — с точки зрения Redis
> группа не отстаёт, потому что отстающих записей больше не существует. Ровно та тишина,
> что сопровождала Redis-OOM 31.07. Правило `FrontierRedisStreamDeliveryGap`, severity
> `critical` — это безвозвратная потеря данных.
>
> Заодно `frontier_redis_stream_groups` + `_entries_added` и правило
> `FrontierRedisStreamOrphanProducer`: продюсер, пишущий в стрим без единой
> consumer-группы, тоже невидим для `lag`/`pending` (групп нет — отставать нечему).
> Это готовая проверка для пункта 9.
>
> Инвариант покрыт восемью параметризованными кейсами, включая тот, на котором наивная
> реализация даёт ложное срабатывание на каждом новом консьюмере: у только что созданной
> группы `last-delivered-id` равен `0-0`, и это не потеря.

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

### 41. ✅ СДЕЛАНО 2026-08-05 — ~~Синтез каждый раз падал на primary-провайдере~~

> **Закрыто заходом 10. Причина оказалась однострочной, как и предполагал маршрут.**
> `worker/llm_router_client.py` при ГОЛОМ `model_override` (без `provider_override`)
> подменял только модель у ПЕРВОГО кандидата семейства, оставляя его провайдера.
> Для `mcp_synthesis` первым идёт wormsoft, а override приходил с идентификатором
> GigaChat — то есть wormsoft получал чужое имя модели и отвечал 404. Замер: 8 вызовов
> за 60 дней, 4 ошибки wormsoft и 4 успеха polza, ни одного успеха первого кандидата.
> Каждый вызов синтеза гарантированно тратил круг впустую.
>
> Голый override снят с трёх call-site'ов (`search_frontier`, `search_balanced`,
> `frontier_brief`). Против повторения — защита в самом роутере: если модель
> объявлена в политике семейства, маршрут идёт к её ВЛАДЕЛЬЦУ; если владелец
> неизвестен, override игнорируется с `log.warning`, а не приписывается произвольному
> провайдеру. Внешних вызывающих с голым override было ровно три, все три — синтез.

### 41-bis. Исходная формулировка

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

### 43. 🟡 ЧАСТИЧНО 2026-08-06 — внешняя поверхность закрыта, десять сервисов остались

> **Закрыта та часть, ради которой пункт и заводился.** У `mcp-gateway` появился
> healthcheck (питоном из того же образа — `curl` в нём нет), девятый скрейп-таргет
> и место в правиле `FrontierControlPlaneServiceDown`. Живой замер: `health=healthy`,
> таргетов 9, все `up`.
>
> Healthcheck намеренно бьёт в `/healthz` шлюза и **не** ходит в REST 8100: он
> отвечает на вопрос «жив ли этот процесс», а не «жив ли весь стек». Иначе падение
> REST перезапускало бы шлюз по кругу, ничего этим не исправляя.
>
> **Новый инвариант вместо разовой правки:** тест
> `test_every_scrape_target_is_covered_by_a_down_rule` — у каждой цели из
> `prometheus.yml` обязано быть правило на `up == 0`. Следующий сервис не окажется
> вне наблюдения так же тихо.
>
> **Осталось:** healthcheck у десяти сервисов — admin, worker, ingest, crawl4ai, mcp,
> prometheus, alertmanager, node-exporter, xray, grafana.

### 43-bis. Исходная формулировка

`ops` · работает частично · объём M

- [ ] **Есть.** Healthcheck есть у 7 сервисов (postgres, redis, qdrant, neo4j, searxng, paddleocr, gpt2giga-proxy). Скрейп настроен на 8 таргетов, все up. Есть FrontierCoreServiceDown / FrontierControlPlaneServiceDown / FrontierSupportServiceDown / FrontierAdminDown.
- [ ] **Не хватает.** Healthcheck у admin, alertmanager, crawl4ai, grafana, ingest, mcp, mcp-gateway, node-exporter, prometheus, worker, xray (.cursor/rules/06-devops.mdc объявляет их 'обязательными для всех сервисов'). Хуже конкретно для mcp-gateway: он не входит в список скрейп-таргетов Prometheus (admin, alertmanager, crawl4ai, ingest, mcp, node-exporter, qdrant, worker — восемь, шлюза нет), значит у сервиса, опубликованного на 0.0.0.0:8102, нет ни healthcheck, ни /metrics в мониторинге, ни правила падения — его отказ будет замечен только пользователем.

<details><summary>Доказательства</summary>

ssh python по docker-compose.yml → 'total 18 with healthcheck 7', WITHOUT: admin, alertmanager, crawl4ai, grafana, ingest, mcp, mcp-gateway, node-exporter, prometheus, worker, xray; `curl http://127.0.0.1:9090/api/v1/targets` → 8 таргетов, mcp-gateway отсутствует

</details>

### 44. ✅ СДЕЛАНО 2026-08-04 — ~~Процедура replay окна не отработает: скрипт не умеет авторизоваться~~

> **Закрыто заходом 7, но диагноз оказался вдвое хуже исходного.** Авторизация была
> вторым дефектом, а не первым. В боевой ветке стояло
> `ssh $Server "curl -fsS -X POST ..." | Out-Null` **без проверки `$LASTEXITCODE`**,
> а в PowerShell 5.1 ненулевой код нативного exe не бросает исключение даже под
> `$ErrorActionPreference = 'Stop'`. Строка `reprocess ok N/N` печаталась независимо
> от 401, 500 и таймаута. Одна авторизация вылечила бы симптом и оставила механизм
> молчания на месте.
>
> **Сделано:** серверная половина вынесена в `scripts/reprocess-window.sh` (читает
> учётные данные из серверного `.env`, отдаёт их curl через `--config -` на stdin —
> в `argv` они не попадают, значит не видны в `ps`; fail-closed при пустом пароле;
> ненулевой код возврата при любом отказе). `.ps1` стал тонким драйвером с
> `Assert-LastExitCode` после каждого `ssh`, и двадцать отдельных рукопожатий
> свернулись в одно. Файл пересохранён в UTF-8 **с BOM** — иначе PS 5.1 прочтёт
> кириллицу как ANSI.
>
> **Тем же дефектом были больны ещё два скрипта, и нашёл их не разбор, а тест:**
> `scripts/server_apply_vision_chain_policy.py` (без авторизации вовсе) и
> `scripts/reprocess_done_for_sparse.py` (логировал отказ по каждому посту и всё равно
> завершался нулём). Оба переведены на общий `scripts/admin_api_auth.py`; второй
> теперь падает, если не переобработал хоть один пост.
>
> Против возврата — `tests/test_ops_scripts_contract.py`: реестр вызывающих админку
> (незарегистрированный скрипт роняет тест), проверка **и** авторизации, **и** громкого
> отказа, отдельный кейс на `ssh` без разбора `$LASTEXITCODE`. Мутация «снять проверку
> кода возврата» ловится; проверка только на наличие заголовка на прежней версии
> прошла бы зелёной.

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

### 47. ✅ СДЕЛАНО 2026-08-05 — ~~cross_workspace_bridges без единого потребителя~~

> **Решение владельца: реализовывать (ветка Б).** Добавлен `include_bridges: bool = False`
> в `search_frontier` и `get_frontier_brief`; мосты читаются из БД, а не из YAML
> (YAML запечён в образ и расходится с базой).
>
> Обязательное условие соблюдено: каждый результат из воркспейса-моста несёт
> `origin_workspace` и `bridged`, иначе `disruption` молча получал бы сигналы
> `ai_trends` без атрибуции.
>
> **Решено по ходу, стоит знать:** `limit` — потолок ВСЕЙ выдачи, а не на воркспейс
> (иначе `include_bridges` у `disruption` с четырьмя мостами превращал бы `limit=10`
> в 50); транзитивности нет — мосты моста не раскрываются; ключи атрибуции
> появляются только при `include_bridges=True`, чтобы дефолтный ответ остался прежним.
>
> **Не сделано:** `search_balanced` мосты не поддерживает — он строит свой
> `SearchRequest`, и поле там остаётся дефолтным. Расширить дёшево.

### 47-bis. Исходная формулировка

`search` · только спроектировано · объём M

- [ ] **Есть.** Колонка workspaces.cross_workspace_bridges (storage/postgres/init.sql:11), модель shared/models/workspace.py:17, заполнена у 5 из 6 воркспейсов в config/workspaces.yml (строки 22,45,93,129,173), пишется тремя путями (bootstrap_configs.py:45-70, workspaces.py:83-146, init_storage.py:56-77), редактируется в админке (WorkspacesView.js:121,296) и отображается в двух MCP-инструментах.
- [ ] **Не хватает.** Ни одного чтения ради поведения. В mcp/tools/observability.py:149,337 значение только выводится в ответ list_workspaces/get_workspace_overview. mcp/tools/frontier_brief.py берёт исключительно явно переданный список (workspace_ids(), строки 58-66) и мостами не расширяет его. В search_frontier / search_balanced / search_trend_clusters обращений нет. В воркспейсе auto_hmi поле сознательно не заполнено с комментарием «их не читает никто» — то есть отсутствие потребителя уже зафиксировано в конфиге, но фича из контракта не убрана.

<details><summary>Доказательства</summary>

grep 'cross_workspace_bridges' по репозиторию вне docs/ → 30 совпадений, все на запись/отображение; mcp/tools/observability.py:149,337; mcp/tools/frontier_brief.py:58-66,152-190; config/workspaces.yml:198 («cross_workspace_bridges намеренно не заданы — их не читает никто»)

</details>

---

## Низкий приоритет (5)

### 48. 🟡 ЧАСТИЧНО 2026-08-06 — раскат дошёл до 16 источников из 27

> **Заходом 11 включено 16 из 27**, а не 1. Замер 06.08 по `extra->>'rollout_batch'`
> (всего / включено): `auto_hmi` 10/**5**, `auto_ru` 6/**4**, `ev_tesla` 4/**4**,
> `smart_city` 2/**2**, `design_ux` 2/**1**, `global_mobility` 3/**0**.
> Второе действие тоже выполнено — в `config/sources.yml` ровно 16 записей с
> `rollout_batch` несут `is_enabled: true`, иначе ближайший bootstrap молча
> откатил бы включение.
>
> **Осталось 11 id**, целиком не раскатан только `global_mobility` (0/3).
>
> **Оговорка, которой в пункте не было:** раскат источников сам по себе трендов
> не даёт. У `auto_hmi` при 54 постах `trend_clusters` нет вообще — это пункт 29,
> и он от раската не лечится.

### 48-bis. Исходная формулировка

`ingest` · брошено · объём S

- [ ] **Есть.** Источники заведены в БД с меткой extra->>'rollout_batch', процедура включения описана и технически работает (PATCH /toggle + правка config/sources.yml). Живой ingest здоров: 202 включённых источника, у всех last_success_at свежее 7 дней, ноль last_error.
- [ ] **Не хватает.** Ни один батч не начали. Из 27 источников шести батчей включён ровно один (auto_hmi). auto_ru 6/0, ev_tesla 4/0, global_mobility 3/0, smart_city 2/0, design_ux 2/0 — с 31.05 и 28.06 соответственно. Тематические покрытия (авто-РФ, EV, городская мобильность) остаются нулевыми, хотя конфигурация под них уже написана и провалидирована.

<details><summary>Доказательства</summary>

psql: SELECT extra->>'rollout_batch', count(*), count(*) FILTER (WHERE is_enabled) FROM sources WHERE extra ? 'rollout_batch' GROUP BY 1 → batch:auto_hmi 10/1, batch:auto_ru 6/0, batch:design_ux 2/0, batch:ev_tesla 4/0, batch:global_mobility 3/0, batch:smart_city 2/0. Здоровье включённых: 202 enabled, with_error 0, ни одного с last_success_at старше 7 дней

</details>

### 49. 🟡 ЧАСТИЧНО (переоценено 2026-08-06) — код доделан, ветка по-прежнему не исполнялась

> **Статус понижен со «сделано» до «частично».** Код действительно доведён целиком
> и покрыт тестами (`tests/test_email_source.py`), это подтверждено чтением:
> пароль в `Settings`, вся IMAP-сессия в одном `asyncio.to_thread`, `except: pass`
> вычищен, поле названо `auth_ref` в обход маски админки. Но заголовок пункта —
> «не исполнялся ни разу» — остаётся **буквально верным**: в `sources` нет ни одной
> строки с `source_type='email'` (замер 06.08: api 5, rss 152, telegram 63, web 27).
> Оговорка про это была в тексте закрытия, но статус ✅ её перекрывал.
>
> Закрывается заведением реального ящика: `sources.yml` + `PATCH /toggle` (включение —
> два действия), затем проверка рукопожатия, таймингов и чекпоинтов на живом сервере.

> **Решение владельца: доделать целиком, а не сужать контракт.** Все три дефекта
> устранены: пароль уехал в `Settings` (`IMAP_PASSWORD` / карта `IMAP_PASSWORDS`),
> вся IMAP-сессия обёрнута одним `asyncio.to_thread` (по-вызовная обёртка дала бы
> гонку — `imaplib` держит состояние соединения), `except Exception: pass` заменён
> на логирование.
>
> **Мина, найденная по ходу и стоившая бы источника.** Админка маскирует `***`
> любое поле, в имени которого есть `password`/`pass`/`secret`/`token`/`credential`.
> Поле, хранящее только ИМЯ ключа, попало бы под маску, вернулось в UI как `***`
> и уехало обратно в БД первым же PATCH — источник перестал бы аутентифицироваться
> после любой правки через админку. Поле названо `auth_ref`. Плюс `normalize_source_extra`
> теперь **вырезает** `fetch.password`: значение, однажды написанное в `sources.yml`,
> иначе доехало бы до `sources.extra` в PostgreSQL и осталось там навсегда.
>
> Доделано сверх задания: RFC 2047-декодирование `Subject`/`From` (без него
> кириллические темы уезжали как `=?utf-8?B?…?=`) и fallback на `text/html`
> (HTML-only письма давали пустой `content` и падали в `to_event()`).
>
> **Не проверено:** живой ящик. Реального IMAP-источника нет, рукопожатие с
> настоящим сервером и тайминги не измерены. Источник в БД не заведён.

### 49-bis. Исходная формулировка

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

---

## Найдено при перепроверке 2026-08-04, вечер

Три пункта, которых в утреннем аудите не было. Найдены проходом, который сверял
оставшиеся пункты с изменившимся стеком и проверял сегодняшние правки на регрессии.

### 53. 🟡 ЧАСТИЧНО 2026-08-05 — накопление призраков вылечено; healthcheck и причина падений остались

> **Заходом 9 закрыта первопричина в crawl-части.** Имя консьюмера больше не
> генерируется на старт процесса: `shared/stream_consumers.consumer_name()` собирает
> его из имени сервиса и номера экземпляра (`FRONTIER_CONSUMER_INSTANCE`, дефолт `1`).
>
> **Очевидное решение не работало, и это стоит записать.** Взять `HOSTNAME` мало:
> у этих сервисов в compose нет ни `hostname:`, ни `container_name`, поэтому внутри
> контейнера он равен короткому id контейнера — переживает `restart`, но меняется
> на каждом `--force-recreate`, а их в маршруте по нескольку на заход. Тест
> формулирует инвариант как «имя переживает пересоздание», а не «одинаково при
> одинаковом HOSTNAME»: вторая формулировка проходит и на нерабочем варианте.
>
> Проверено после раската: `crawl4ai-1`, `worker-1`, `vision-1`, `reindex-1`.
>
> **Осталось:** healthcheck у сервиса и разбор причины 84 падений (`ExitCode=0`,
> `OOMKilled=false`). Окна данных по прошлым падениям нет — ретенция 200ч.

### 53-bis. Исходная формулировка

`ops` · работает частично · объём S

- [ ] **Есть.** Сервис работает, последние 8 суток стабильно, политика `unless-stopped`.
- [ ] **Не хватает.** `RestartCount=84` при `ExitCode=0` и `OOMKilled=false` — то есть
  ~84 падения за 45 суток (12.06–27.07), причина в статусе выхода не отражена.
  Healthcheck не объявлен вовсе, метрик у сервиса в Prometheus одна непустая серия.
  Это **первопричина пункта 37** в его crawl-части: имя консьюмера генерируется
  на старт процесса, поэтому каждое падение оставляет мёртвую запись навсегда —
  84 падения плюс текущий процесс дают ровно те 85 «призраков».

<details><summary>Доказательства</summary>

`docker inspect frontier-intelligence-crawl4ai-1` → `Created 2026-06-12T13:54`,
`StartedAt 2026-07-27T04:00`, `RestartCount=84`, `ExitCode=0`, `OOMKilled=false`,
`RestartPolicy=unless-stopped`; `{{if .Config.Healthcheck}}` → пусто.
Для сравнения `RestartCount=0` у worker, ingest, admin, mcp, redis.
`XINFO GROUPS stream:posts:crawl` → consumers 85.

</details>

### 54. ✅ СДЕЛАНО 2026-08-07 — ~~Сорок веток в origin, все влиты — мёртвые указатели~~

> К моменту уборки их стало **47 локальных и 48 в origin**, и все, кроме текущей,
> влиты в `main`. Осталось по одной с каждой стороны: `main`.
>
> **Манифест записан до удаления, и это не формальность.** Локальное удаление
> обратимо через рефлог, удаление в origin — нет: рефлог живёт только локально.
> Поэтому путь назад зафиксирован файлом `docs/ops/git-branch-manifest-2026-08-07.md`
> (имя, sha, дата и тема всех 95 ссылок), и он лежит в git, а не рядом с ним.
>
> **Попутно нашлось то, чего в пункте не было: репозиторий никогда не паковался.**
> `count-objects` до уборки — 2243 свободных объекта, `in-pack` ноль, 14.53 МиБ.
> После `git gc` — 2240 объектов в паке, **2.23 МиБ**, то есть в шесть с половиной
> раз меньше.
>
> **Ветка аудита влита в main fast-forward** решением владельца: прод-чекаут снова
> стоит на `main`, `main == origin/main`, рабочее дерево чистое. Заодно снята
> странность, тянувшаяся с захода 0, — прод бежал на тематической ветке.
>
> Грабля порядка, стоившая одной попытки: `git branch --delete` сравнивает ветку
> с ЕЁ upstream, а не с `main`, и отказывается удалять, пока `origin/<ветка>`
> отстаёт. Последний коммит уехал в `main`, в ветку не пушился — и удаление
> упало. Правильный порядок: сначала снять ветку в origin, потом локально.

### 54-bis. Исходная формулировка

`ops` · брошено · объём S

- [ ] **Есть.** `git branch -r --merged origin/main` возвращает все 41 ссылку,
  то есть невлитых не осталось ни одной. Заход 19 отработал полностью.
- [ ] **Не хватает.** Уборки. Ветки больше не несут информации: тема каждой уже
  в истории `main`, а листинг из сорока строк мешает увидеть текущую работу.
  Это гигиена, а не долг — удаление безопасно и обратимо через рефлог.

<details><summary>Доказательства</summary>

`git branch -r | wc -l` → 41 (origin/main + 40 тематических).
`git branch -r --merged origin/main | wc -l` → 41.
Локальных веток 40.

</details>

### 55. Docker-образы: 159 верхнеуровневых при 18 активных, 129 dangling

`ops` · брошено · объём S

- [ ] **Есть.** Место пока не жмёт: `/` 216 ГБ, занято 67 (33%), свободно 140.
- [ ] **Не хватает.** Уборки. Среди именованных тегов — исторические хвосты
  `predeploy-*` и `overlay-base-*` возрастом 2–3 месяца, не используемые ни одним
  сервисом compose. Поле `Reclaimable` у `docker system df` доверия не заслуживает:
  оно отдаёт отрицательное значение из-за общих слоёв. Опираться на 129 dangling
  и на список тегов.

<details><summary>Доказательства</summary>

`docker system df` → Images TOTAL 159, ACTIVE 18, SIZE 19.57GB; Build Cache 70 записей / 434MB.
`docker images -f dangling=true -q | wc -l` → 129. Именованных тегов 34, среди них
`frontier-intelligence-admin:predeploy-20260506070803`, три `admin:overlay-base-2026050*`,
`worker:predeploy*`, `mcp:overlay-base-20260506071611`.

</details>

### 56. ✅ СДЕЛАНО 2026-08-04 — ~~Алерт `FrontierOpenRouterModelQuarantineBurst` шумит каждый день на штатном поведении~~

> Найден 2026-08-04 при построении маршрута II и закрыт заходом 7 в тот же день.
> В исходном аудите отсутствовал — утром правило не было firing.
>
> **Замер сузил задачу и удешевил решение.** Планировалось исключать из условия
> карантин, объяснимый суточным сбросом (`quarantine_until` равен ближайшей полуночи
> UTC). Оказалось, что `quarantine_until` наружу вообще не отдаётся — только гейдж
> 0/1, — то есть такой вариант требовал правки кода `admin` и пересборки.
>
> Но он и не понадобился. За 7 суток `frontier_openrouter_picker_skip_total` имел
> **ровно одну** серию, и та про отсутствие каталога. Причин `all_quarantined`,
> `no_capable_model`, `guard_quarantine` не было **ни разу**: ежесуточный карантин
> 14 из 16 бесплатных моделей не стоил ни одного отказанного запроса. Значит различать
> «объяснимый» карантин от «неисправного» незачем — достаточно потребовать, чтобы
> карантин сопровождался реальными пропусками picker'а.
>
> Правило заменено на `FrontierOpenRouterFreePoolUnusable`: пропуски picker'а по
> связанным с карантином причинам **И** широкий карантин. Пересборки не потребовалось.
> Гейдж карантина остаётся диагностическим — он на дашборде, но никого не будит.
>
> Против возврата — `test_no_rule_pages_on_bare_openrouter_quarantine`: ни одно
> правило с `notify` или `severity` выше `info` не имеет права ссылаться на гейдж
> карантина, не ссылаясь при этом на пропуски picker'а.
>
> **Найдено попутно, вынесено в пункт 57:** регулярка `FrontierOpenRouterPickerSkipBurst`
> перечисляет ровно те причины, которых за неделю не было ни одной, и не покрывает
> единственную, которая случалась.

`ops` · работает частично · объём S

- [ ] **Есть.** Правило работает ровно как написано и ловит настоящее состояние:
  `sum by (service) (frontier_openrouter_model_quarantine{service="admin"}) >= 3`
  за 10 минут, `severity: warning`, `notify: telegram`. Метрика живая, 16 серий.
- [ ] **Не хватает.** Различения отказа и штатного исчерпания квоты. Разбор состояния:
  14 из 16 бесплатных моделей имеют `in_quarantine_until = 1785888000.0` — это **ровно**
  ближайшая полночь UTC (`1785888000 / 86400 = 20670.0`), а `last_error` и
  `consecutive_errors` у всех пусты. То есть карантин наступил не от ошибок, а от
  исчерпания суточной квоты бесплатного тарифа, и снимется сам. Правило же трактует
  это как деградацию и шлёт предупреждение в Telegram: firing **6 суток из 7**, по
  3–11 часов в день. Починка 02.08 (`shared/openrouter_limits.py`) вылечила вечный
  карантин из-за миллисекунд, но условие алерта осталось прежним — и в шапке того же
  модуля прямо записано, что алерт «возвращался на следующие сутки».
  Это зеркало «молчаливого отказа», против которого построена половина реестра:
  алерт, который кричит каждый день на норме, обучает не смотреть на алерты вообще.

<details><summary>Доказательства</summary>

`curl 9090/api/v1/alerts` → firing с 2026-08-04T17:45:27Z.
`redis-cli HMGET or:health:<model> in_quarantine_until last_error consecutive_errors`
по всем 17 ключам `or:health:*` → у 14 значение `1785888000.0`, поля ошибок пусты;
`date -u -d "tomorrow 00:00" +%s` → `1785888000`.
`max(ALERTS{alertname="FrontierOpenRouterModelQuarantineBurst",alertstate="firing"})`
по суткам за 7 дней → 29.07 8ч, 30.07 3ч, 01.08 6ч, 02.08 11ч, 03.08 6ч, 04.08 3ч.
`shared/openrouter_limits.py:1-14` (шапка модуля), `prometheus/alerts.yml` — правило.

</details>

### 57. Алерт пропусков picker'а перечисляет причины, которых не бывает, и не покрывает единственную, которая случается

> Найден 2026-08-04 при разборе пункта 56. Тот же класс наизнанку: алерт есть,
> выглядит осмысленным, а поймать может только то, чего не происходит.

`ops` · работает частично · объём S

- [ ] **Есть.** `FrontierOpenRouterPickerSkipBurst` существует и настроен разумно по форме:
  `sum by (service, task_family, reason) (increase(frontier_openrouter_picker_skip_total{service="worker", reason=~"guard_.*|near_cap|all_quarantined|no_capable_model"}[15m])) >= 5`.
- [ ] **Не хватает.** Совпадения перечисленных причин с наблюдаемыми. За 7 суток метрика
  имела **единственную** серию — `reason="openrouter_fail_safe_catalog_missing"` на worker,
  значение 114. Ни одна из четырёх перечисленных в регулярке причин не встретилась ни разу.
  То есть правило покрывает пустое множество, а реально случающийся отказ проходит мимо него.
  Отдельно и, возможно, важнее: `frontier_openrouter_catalog_available{service="admin"}`
  всё то же окно равен **1** (`min_over_time[7d]` = 1), то есть worker фейлсейфится по
  каталогу, отсутствия которого admin не видит. `FrontierOpenRouterCatalogUnavailable`
  смотрит на admin и потому молчит. Расхождение между двумя видами одного каталога
  требует разбора на стороне worker — расширять регулярку до выяснения причины нельзя,
  иначе на месте молчащего правила окажется шумящее.

<details><summary>Доказательства</summary>

PromQL: `sum by (service,reason) (max_over_time(frontier_openrouter_picker_skip_total[7d]))`
→ одна серия `{service="worker", reason="openrouter_fail_safe_catalog_missing"} 114`;
`count(frontier_openrouter_picker_skip_total)` → пусто (серии нет сейчас);
`count_over_time(count(frontier_openrouter_picker_skip_total)[7d:1h])` → 58 (была в 58 часовых
интервалах из ~168); `min_over_time(frontier_openrouter_catalog_available{service="admin"}[7d])` → 1.
Правило — `prometheus/alerts.yml`, `FrontierOpenRouterPickerSkipBurst`.

</details>

---

## Найдено при сверке 2026-08-06

Двенадцать пунктов, которых в реестре не было. Найдены сплошным замером живого стека
при сверке всех 57 предыдущих; метод и полный разбор — [AUDIT-2026-08-06.md](./AUDIT-2026-08-06.md).
Три из них (58, 59, 60) закрыты в тот же день, их разбор оставлен как фактура.

### 58. ✅ СДЕЛАНО 2026-08-06 — ~~RSI-петля порогов падала на первичном ключе каждую ночь~~

> Разбор — [AUDIT-2026-08-06.md §3.2](./AUDIT-2026-08-06.md). `id` предложения собирался как
> `_digest(f"{ws}|{key}|pending")`, то есть константа на всё время жизни пары, а `ON CONFLICT`
> покрывает только частичный индекс `WHERE status = 'pending'`. Как только предложение уходило
> из `pending` — то есть как только человек его одобрял, ради чего петля и построена — следующий
> прогон падал на PRIMARY KEY и ронял весь прогон по воркспейсу.
>
> Замер: семь строк в `threshold_proposals`, все `superseded`, свежайшая от **29.06**. Сорок один
> день `disruption` не выпускал ни одного предложения. Починено добавлением `run_id` в ключ;
> проверено живым прогоном (`thrprop:f658b02b4b7b0678`, повторный прогон обновил ту же строку).

`enrichment` · работает частично · объём S

### 59. ✅ СДЕЛАНО 2026-08-06 — ~~Метка `job` зарезервирована Prometheus, а мы её заняли~~

> `sum by (job)` на живом Prometheus возвращал **один ряд** `{job="admin"}` при девяти рядах
> на `/metrics` сервиса: одноимённая метка приложения при скрейпе уезжает в `exported_job`.
> Отказы разных джобов складывались между собой, а `{{ $labels.job }}` печатал «Джоб **admin**
> падает повторно». Переименовано в `job_name`; инвариант держит тест.

`ops` · работает частично · объём S

### 60. ✅ СДЕЛАНО 2026-08-06 — ~~Алерт на падение суточного джоба не мог сработать~~

> `FrontierAdminJobFailing` требует `>= 3` отказов за 6 часов — суточный прогон столько
> не наберёт, даже падая каждую ночь. Именно поэтому 41 сутки пункта 58 не показал ни один
> из 82 алертов. Добавлено правило `FrontierAdminJobFailingDaily` на **повторяемость**:
> `increase(...[49h]) >= 2`. Окно 49ч, а не 48 — суточный крон плавает на минуты.

`ops` · работает частично · объём S

### 61. ✅ СДЕЛАНО 2026-08-06 — ~~Гвард изоляции воркспейсов недостижим~~

> `workspace` выведен в четыре обёртки шлюза и четыре `inputSchema`. Проверено
> сырым рукопожатием MCP (клиент сессии держит схему с прошлого подключения и
> новое поле не увидел бы): у всех четырёх инструментов поле в схеме есть,
> кластер `trend:9e5afee32589bcd5` из `disruption` от имени `design` отдаёт
> **404 «cluster not found»**, от своего имени — данные.
>
> **По ходу нашлась пятая дыра**, которой в пункте не было: `get_source_details`
> выбирал `WHERE s.id = :source_id` и гварда не звал вовсе. Пропущен он был потому,
> что структурный тест держал захардкоженный список из четырёх имён — теперь набор
> **выводится из кода**: любая функция, читающая строку по id без фильтра по
> воркспейсу, обязана звать гвард. Мутация проверена.

### 61-bis. Исходная формулировка

`mcp` · код есть, не подключён · объём S

- [ ] **Есть.** Гвард написан и раскатан: `mcp/guards.py:207` `assert_row_workspace(row, workspace, *, what=...)`, отдаёт **404, а не 403** (403 подтвердил бы существование объекта и стал бы оракулом для перебора). Вызывается из семи мест `mcp/tools/observability.py`; поле `workspace` объявлено в четырёх Request-моделях (`:49`, `:71`, `:81`, `:97`) с комментарием про чтение чужого воркспейса. Структурный тест `tests/test_workspace_row_scope.py` роняет прогон, если инструмент читает строку по id и не зовёт гвард.
- [ ] **Не хватает.** Поля `workspace` в сигнатурах `mcp/mcp_gateway.py` (`get_source_details`, `get_cluster_details`, `get_missing_signal_details`, `get_cluster_evidence`) и в `inputSchema` `mcp/server.py:277-324`. Единственный клиент ходит через шлюз, то есть прислать он может только `None`, а гвард при `None` делает ранний `return` (`guards.py:225-231`). **Защита написана, снабжена комментарием на 15 строк, задеплоена и не срабатывала ни разу.** Пятый инструмент того же класса, `get_signal_timeline`, поле имеет во всех трёх местах — значит это пропуск, а не решение.

<details><summary>Доказательства</summary>

`grep 'async def get_cluster_details' -A 8 mcp/mcp_gateway.py` — параметра `workspace` нет; то же у трёх соседей. Арифметика по всем инструментам: 118 полей уровня инструмента в моделях против 114 параметров шлюза и 114 свойств `/tools` — разница ровно 4.

</details>

### 62. 🟡 ЧАСТИЧНО 2026-08-06 — ось «шлюз ↔ модель» закрыта, третья копия остаётся

> Добавлен `test_gateway_signature_matches_the_request_model`: множество параметров
> обёртки сверяется с полями Request-модели REST-хендлера в обе стороны, с реестрами
> осознанных исключений (оба пусты). Мутация «убрать поле из обёртки» ловится.
>
> **Осталось:** третья копия контракта — рукописный `inputSchema` в `mcp/server.py`
> (~430 строк словарей). Ограничения `ge/le/min_length` и `Literal` из моделей
> в неё по-прежнему не попадают, поэтому клиент не знает границ и узнаёт о них
> только из 422. Правильное лечение — не третий тест, а вывод схемы из модели
> ([AUDIT-2026-08-06.md §5.2](./AUDIT-2026-08-06.md)).

### 62-bis. Исходная формулировка

`mcp` · работает частично · объём S

- [ ] **Есть.** `tests/test_mcp_gateway_contract.py` сравнивает множества имён REST и шлюза в обе стороны плюс поимённо проверяет контур одобрения RSI. Он работает: 32 = 32.
- [ ] **Не хватает.** Сверки параметров. Пункт 61 для этого теста зелёный. Из 45 полей с ограничениями (`ge/le/min_length/max_length`) и 10 полей с `Literal` до клиента не доезжает **ни одно**: в рукописном `inputSchema` ноль ключей `minimum`/`maximum`/`enum`, в сигнатурах шлюза ноль `Annotated`/`Literal`.

### 63. ✅ СДЕЛАНО 2026-08-06 — ~~Ошибка валидации 422 доходит без причины~~

> `_raise_for_status_with_detail` научился разбирать `detail`-**список** от FastAPI
> и собирать его в «поле: причина» (`limit: Input should be less than or equal to 100`),
> отбрасывая транспортный префикс `body`. Хелпер применён ко всем 32 обёрткам вместо
> 11 — через единую точку возврата `_finish(r)`.
>
> Проверено вживую заодно с гвардом: отказ пришёл как
> `404 Not Found from .../get_cluster_details: cluster not found`, то есть с причиной,
> а не голым статусом.

### 63-bis. Исходная формулировка

`mcp` · работает частично · объём S

- [ ] **Есть.** `_raise_for_status_with_detail` (`mcp/mcp_gateway.py:30-54`) написан именно затем, чтобы вернуть причину отказа.
- [ ] **Не хватает.** Он разбирает `detail` только при `isinstance(raw, str)`, а FastAPI при ошибке валидации кладёт туда **список объектов** — проверка не срабатывает, и управление уходит на голый `response.raise_for_status()`. Плюс сам хелпер применён к 11 обёрткам из 32; у остальных 21 стоит голый `raise_for_status()`. Показательно, что жертвой стал ровно тот инструмент, ради которого хелпер писали: `record_card_feedback` собирает карточки руками, промах по форме даёт 422 — а `card_feedback` до сих пор 0 строк.

### 64. crawl4ai — единственный потребитель без потолка доставок: poison блокирует голову PEL бессрочно

`ingest` · работает частично · объём M

- [ ] **Есть.** У enrichment полноценный poison-detection: `_drop_poison_message` и `_drop_poison_pending` с порогом `indexing_max_deliveries` (`worker/tasks/enrichment_task.py:602-653`), написанный после инцидента 12.07.
- [ ] **Не хватает.** В `crawl4ai/crawl4ai_service.py` нет ни счётчика доставок, ни DLQ. Сообщение, на котором обработчик падает воспроизводимо, будет реклеймиться бесконечно. Отдельно: у vision и reindex потолок считается по полю в payload, а на пути reclaim оно не растёт никогда.

### 65. Cron-прогоны не пишут в `admin_manual_jobs`, а два алерта построены на этой таблице

`ops` · работает частично · объём M

- [ ] **Есть.** `FrontierManualSignalJobRunningTooLong` и `FrontierManualSignalJobFailedRecently` существуют и настроены разумно по форме.
- [ ] **Не хватает.** Они видят только ручные клики в админке: плановые прогоны идут мимо таблицы. Плюс селектор обоих содержит `job_name="run_semantic_clusters"`, которого нет ни в одной живой серии (живые: `run_signal_analysis`, `run_missing_signals`). Комментарий в `alerts.yml` при этом утверждает, что `frontier_admin_manual_jobs_recent_failures` «висит на пустом векторе с 10.04.2026» — сегодня у метрики две живые серии, то есть устарел и комментарий.

### 66. Восемь из шестнадцати cron-джоб не считают исход, шесть возвращают захардкоженный `status="ok"`

`ops` · работает частично · объём S

- [ ] **Не хватает.** Исход прогона определяется отсутствием исключения, а не наличием эффекта. Это тот же корень, что у инцидента 31.07–02.08, и архитектурное лечение предложено отдельно — [AUDIT-2026-08-06.md §5.1](./AUDIT-2026-08-06.md), `RunOutcome`.

### 67. `linked_ratio` считает провал краула как отсутствие ссылки — и это едет в ранжирование

`enrichment` · работает частично · объём S

- [ ] **Не хватает.** `worker/tasks/enrichment_task.py:187-192` считает `AVG(CASE WHEN pe.id IS NOT NULL ...)` по `kind='crawl'`, то есть «краул упал» неотличимо от «ссылок не было». Значение едет в `source_quality_payload` (`:214`) → `source_score`/`source_authority` (`:883-884`) → payload Qdrant → ранжирование выдачи. **479 отказов краула в сутки прямо сейчас занижают качество источников, у которых ссылки как раз есть.** В пункте 8 этого следствия нет: там речь про наблюдаемость, а здесь отсутствие записи МЕНЯЕТ данные.

### 68. ✅ СДЕЛАНО 2026-08-06 — ~~`docs/ops/alert-digests/` исключены и из git, и из rsync~~

> Каталог был закрыт в `.gitignore` и в `.rsync-exclude` одной причиной —
> «серверные артефакты, которые генерируются на месте», — и в этой формулировке
> дайджесты стояли рядом с `prometheus/textfile`. Причина верна только наполовину:
> метрики коллектора перегенерируются каждые десять минут и в истории бесполезны,
> а дайджест пишется раз в сутки, никогда не переписывается, и **его отсутствие
> само является доказательством** — пункт 18 доказывается пропущенными днями.
>
> `.gitignore` сужен, 16 файлов (14 дайджестов + 2 снимка перед волнами раската)
> заведены в git. Исключение в `.rsync-exclude` **оставлено намеренно**: локально
> этих файлов нет, и `sync-push --delete` снёс бы историю — ровно так 03.08 погиб
> `prometheus/textfile`.
>
> Асимметрия закреплена тестом
> `test_server_written_history_is_excluded_from_rsync_but_kept_in_git`: файл обязан
> быть исключён из rsync И обязан НЕ быть в `.gitignore`. Обе половины проверены
> мутацией. Прежний парный инвариант `CLOSED_IN_BOTH` покраснел на этой правке —
> и правильно сделал, что спросил.

### 69. `_FALLBACK_WORKSPACE_SLUGS` — четвёртая копия списка воркспейсов, и она fail-open

`mcp` · работает частично · объём S

- [ ] **Не хватает.** `mcp/guards.py:123-125` держит захардкоженный набор слагов на случай, если `config/workspaces.yml` не окажется в образе. Срабатывает он ровно тогда, когда образ устарел, то есть когда заведомо неверен, — и при этом **пропускает** слаг дальше, а не отклоняет. Принцип уже сформулирован в том же файле (`guards.py:239-244`: «гвард, который не может проверить, обязан сказать об этом»), но к allowlist не применён. `tests/test_mcp_guards.py:57` проверяет набор литералом и отстал на `auto_hmi`.

### 74. Слияние сигналов недостижимо арифметически: предикат построен на признаках, равных нулю по построению

`enrichment` · работает частично · объём S

- [ ] **Есть.** `_merge_signal_candidates` (`worker/services/semantic_clustering.py:1262-1340`) написан, вызывается на каждом прогоне для stable и emerging (`:1981-1982`), результат пишется в `summary.signals_merged`.
- [ ] **Не хватает.** Хоть одного слияния. `signals_merged = 0` у всех шести воркспейсов за всю историю наблюдений.

<details><summary>Доказательства и разбор</summary>

Кандидаты строятся как **связные компоненты** над семантическими кластерами
(`:1678` `groups = _components(...)`), а компоненты **разбивают** множество: каждый
семантический кластер попадает ровно в одну группу. Значит у любых двух кандидатов
одного прогона `doc_ids` и `supporting_semantic_cluster_ids` **не пересекаются
по построению**.

Теперь веса предиката (`:1310-1316`):

```
similarity = doc_overlap*0.28 + semantic_overlap*0.22
           + concept_overlap*0.24 + title_overlap*0.16 + temporal_overlap*0.10
```

Первые два слагаемых тождественно нулевые, значит потолок similarity равен
**0.50** при пороге `signal_merge_similarity_threshold` = **0.72** (у одного
воркспейса 0.58). Ветка `doc_overlap >= doc_overlap_threshold` не срабатывает
по той же причине. Остаётся единственная лазейка — `semantic_title_merge`:
`concept_overlap >= 0.6 И title_overlap >= 0.45 И temporal_overlap >= 0.4`,
три жёстких условия по Жаккару разом.

**Решающее сравнение — с соседом.** `_merge_semantic_candidates` (`:1493-1560`)
делает то же самое уровнем ниже и работает: `semantic_clusters_merged` = 8
у disruption и 2 у ai_research за последний прогон. Его веса (`:1538-1544`):

```
similarity = centroid_similarity*0.45 + concept_overlap*0.25
           + title_overlap*0.15 + temporal_proximity*0.10 + doc_overlap*0.05
```

Разница ровно одна: **у семантического слияния есть косинус центроидов с весом
0.45, у сигнального его нет вообще.** Косинус на дизъюнктных множествах нулю не
равен — поэтому там потолок 0.95, а тут 0.50.

То есть сигнальный предикат опирается исключительно на признаки пересечения,
которых в его входных данных быть не может.

**Замер вместо рассуждения.** Двоичным поиском по порогу измерена фактическая
similarity пары дизъюнктных кандидатов с ПОЛНОСТЬЮ совпадающими ключевыми словами:
**0.34** (`concept 1.0*0.24 + title 0.0*0.16 + temporal 1.0*0.10`). При пороге 0.33
они сливаются, при 0.35 — нет. До боевых 0.72 не близко даже в идеальном случае.

**Лечение** — добавить центроидный член в `_merge_signal_candidates`, как у соседа
(центроид у кандидата уже посчитан, `:1699`), и перевзвесить. **Порог трогать до
этого нельзя:** понижение до 0.34 сделает решающими только совпадение слов
в ключевых словах и близость во времени, то есть изменит не чувствительность
предиката, а его смысл.

**Прежде чем выбирать веса — замерить распределение косинусов между центроидами
кандидатов.** Своих чисел на это сейчас нет: центроид в `emerging_signals` не
персистится, живёт только в памяти прогона. Выбирать веса без замера — ровно то,
чего этот реестр не разрешает.

Арифметика закреплена `tests/test_signal_merge_ceiling.py`: дизъюнктные кандидаты
не сливаются на боевом пороге; в формуле нет центроида (появится — тест упадёт,
и это будет означать, что пункт закрыт); понижение порога до потолка сливает
по одним словам.

</details>

### 75. Web-коннектор поставляет материал, который кластеризация не увидит никогда: 95% его постов без `published_at`

`ingest` · работает частично · объём M

- [ ] **Есть.** Web-источников 27, включено 25, прогоны идут и завершаются `success`, посты сохраняются и находятся поиском.
- [ ] **Не хватает.** Даты публикации. У **1126 постов из 1186** (95%) web-источников `published_at IS NULL` — против **2 из 207 787** у rss и **нуля** у telegram и api. А выборка кластеризации требует `p.published_at IS NOT NULL` (`worker/services/semantic_clustering.py`, `_fetch_posts`, вместе с `embedding_status='done'` и `relevance_score >= 0.6`). То есть **весь этот материал структурно невидим для трендового слоя** — не «редко проходит», а исключён предикатом.

<details><summary>Доказательства и механизм</summary>

Замер 07.08.2026:

| тип источника | постов | из них без `published_at` |
|---|---|---|
| web | 1186 | **1126 (95%)** |
| rss | 207 787 | 2 |
| api | 100 094 | 0 |
| telegram | 9 915 | 0 |

По воркспейсам доля NULL: `auto_hmi` **40.7%** (24 из 59), design 0.77%, disruption 0.38%.
Для `auto_hmi` это заметная часть: два из четырёх его активных источников — web-типа,
и именно поэтому 54 поста дали всего 8 кандидатов.

Механизм: `ingest/sources/web_source.py` берёт дату из элемента по `date_selector`
из конфига источника и зовёт `parse_datetime(...)`. Если селектор не совпал или
на странице даты нет — получается `None`, и он же уезжает в `posts.published_at`.
Ни ошибки, ни предупреждения при этом нет: `source_runs.status = 'success'`,
`fetched_count` растёт, пост сохраняется. Отказ виден только тем, что материал
не участвует в кластеризации.

Пример: `web_baymard_blog` — 20 постов, все без даты; `design_web_contentsquare` —
21 пост, все без даты. В отчётах по свежести источников они выглядят как «никогда
не публиковал», потому что `max(published_at)` по ним NULL.

</details>

> **Развилка для владельца — чем заменять отсутствующую дату.**
>
> *(а) Оставить как есть.* Материал web-источников не попадает в тренды; 25 включённых
> источников работают вхолостую относительно продуктовой поверхности.
>
> *(б) Подставлять время первого обнаружения и ПОМЕЧАТЬ подстановку* (например
> `extra.published_at_inferred = true`), а предикат кластеризации расширить на
> помеченные. Рекомендую: пост, у которого не прочиталась дата, всё равно существует,
> и время первого обнаружения — честная нижняя граница. Цена: временны́е метрики
> (velocity, change points) по таким постам считаются от момента обнаружения, а не
> публикации, и это надо не забыть при чтении.
>
> *(в) Чинить `date_selector` по каждому источнику.* Точнее всех, но 25 источников
> и хрупко: селектор ломается при первой же перевёрстке сайта, и ломается молча —
> ровно как сейчас.
>
> Вариант (б) без пометки делать нельзя: тогда «дата публикации» и «дата, когда мы
> это увидели» станут неразличимы, и класс молчаливого отказа просто сменит форму.

### 76. crawl4ai зависает живым: процесс работает, цикл потребления стоит, healthcheck'а нет

`ingest` · работает частично · объём M

- [ ] **Есть.** Отказ ВИДЕН: `FrontierRedisStreamOldestPendingTooOld` сработал корректно, а `frontier_crawl_outcomes_total` показал ноль за час. Детекция, поставленная заходом 8, отработала.
- [ ] **Не хватает.** Восстановления и признака живости самого цикла. 07.08.2026 контейнер простоял **4 часа 36 минут** в состоянии `status=running`, `RestartCount=0`, отдавая `/metrics` (скрейп-таргет `up`) — и не обработав ни одного сообщения. Последняя строка лога 02:59:33, `HTTP error status=403` на science.org, дальше тишина; `oldest_pending_age` 16 465с совпал с этим временем до секунды. Лечится перезапуском: после него `entries-read` 46 775 → 47 475, `lag` 956 → 256, за 15 минут 165 сохранено.

<details><summary>Почему healthcheck на /metrics не помог бы</summary>

Сервис отдаёт метрики на 9092, и Prometheus всё это время видел его `up`. То есть
HTTP-проба живости процесса на этом эндпоинте прошла бы успешно при полностью
вставшем цикле. Нужен признак самого ЦИКЛА: отметка времени последней итерации
(`frontier_crawl_loop_heartbeat_timestamp_seconds`) плюс правило на её протухание,
либо healthcheck, читающий ту же отметку. Это часть пункта 43, оставшаяся открытой:
`crawl4ai` — один из десяти сервисов без healthcheck.

Отдельно стоит понять первопричину зависания. Кандидат — ожидание, которое никогда
не вернётся, в браузерном фетче: последняя запись перед тишиной именно про внешний
запрос. У `enrich_url` есть таймауты, у объемлющего цикла — нет.

</details>

### 72. `asyncio.gather` в `worker/main.py` без `return_exceptions`: падение одного потребителя останавливает все три

`ops` · работает частично · объём S

- [ ] **Не хватает.** Три потребителя стримов запускаются одним `gather`; без
  `return_exceptions=True` исключение в любом из них отменяет остальные. То есть
  отказ vision уносит с собой enrichment и reindex, а в логе видно только первое
  исключение.

### 73. Выключение источника не останавливает опрос до перезапуска ingest

`ingest` · работает частично · объём S

- [ ] **Не хватает.** `PATCH /toggle` меняет флаг в БД, но планировщик ingest держит
  расписания с момента старта. До перезапуска выключенный источник продолжает
  опрашиваться, а его события отбрасываются ниже по конвейеру — тот самый класс,
  из-за которого 12.07 чинили застревание disabled-source событий в PEL.

### 70. ✅ СДЕЛАНО 2026-08-06 — ~~Эталонный скрипт сборки не передаёт зеркало PyPI~~

> Найдено попыткой пересобрать образ шлюза с новой зависимостью. `server-build-stack.sh`
> полагался на интерполяцию `${PIP_INDEX_URL:-}` из `docker-compose.yml` и ушёл на
> **pypi.org**, который с этого хоста недоступен (ReadTimeout — тот же класс, что
> с Docker Hub и Cloudflare). Соседний `server-build-mcp.sh` передавал `--build-arg`
> с самого начала: разъехались две реализации одного и того же.
>
> **Дефект был невидим месяцами**, потому что слой `pip install` у всех образов
> закэширован: скрипт успешно «пересобирал» что угодно ровно до первого изменения
> зависимостей. Починено явным `--build-arg` плюс дефолт на зеркало aliyun;
> инвариант держит `test_build_entrypoints_pass_the_pypi_mirror`.

`ops` · работает частично · объём S

### 71. ✅ СДЕЛАНО 2026-08-06 — ~~Открытая верхняя граница у mcp[cli] в образе шлюза~~

> Ограничение было `mcp[cli]>=1.0.0`. Первая же пересборка подтянула **mcp 2.0.0**,
> где `mcp.server.fastmcp` удалён (переименован в `mcp.server.mcpserver.MCPServer`),
> и контейнер ушёл в рестарт-луп с `ModuleNotFoundError`. Открытая граница ждала
> своего часа месяцами и выстрелила в момент, никак с ней не связанный: правка
> была про `prometheus-client`.
>
> Запинено `>=1.0.0,<2.0.0`, образ пересобран, в контейнере mcp **1.29.0**,
> `/healthz` и `/metrics` отвечают 200. Переход на v2 — отдельная работа с правкой
> кода, а не побочный эффект сборки.
>
> `mcp` (REST, 8100) не пострадал: он не импортирует `fastmcp` — во время аварии
> отдавал 200 и 32 инструмента.

`ops` · работает частично · объём S

---

## ✅ Опровергнуто и закрыто измерением

Две записи, которые вечером 04.08 держались на одном прочтении и потому не
закрывались. Обе перепроверены **измерением**, а не повторным чтением того же кода.
Оба долга не подтвердились.

### R1. Ротация docker-логов — ✅ ЗАКРЫТО, долга нет

Аудит смотрел в `/etc/docker/daemon.json`, где `log-opts` действительно нет. Ротация
объявлена якорем `x-docker-json-logging` в `docker-compose.yml` и применена ко всем
сервисам.

Проверено перебором всех работающих контейнеров:

```bash
ssh frontier-intelligence "cd /opt/frontier-intelligence && \
  for c in \$(docker ps --format '{{.Names}}'); do \
    printf '%-45s ' \"\$c\"; \
    docker inspect \"\$c\" --format '{{json .HostConfig.LogConfig.Config}}'; \
  done"
```

**Результат: 18 контейнеров из 18 отдали `{"max-file":"5","max-size":"10m"}`.**
Ни одного `{}`. Пункт закрыт.

### R2. Знаменатель метрики покрытия кластеризации — ✅ ЗАКРЫТО, знаменатель верен

Претензия состояла в том, что экспортёр берёт окно как `GREATEST(semantic, trend)`,
а кластеризация работает в `semantic`, то есть знаменатель завышен. Проверялась не
чтением — чтение и подвело автора претензии, — а сопоставлением того, сколько постов
выборка взяла **на самом деле** (`cluster_runs.summary->>'post_candidates'`), с потолком
и числом подходящих из textfile-метрик.

Инвариант при верном знаменателе: `post_candidates` равен потолку, когда подходящих
больше потолка, и равен числу подходящих, когда их меньше. Замер:

| Воркспейс | `max_posts` | `eligible` | `post_candidates` | |
|---|---|---|---|---|
| `disruption` | 2000 | — | 2000 | усечено потолком |
| `ai_trends` | 550 | 5643 | 550 | усечено потолком |
| `design` | 650 | 841 | 650 | усечено потолком |
| `ai_research` | 500 | 5558 | 500 | усечено потолком |
| `ai_products_media` | 500 | 1771 | 500 | усечено потолком |
| `auto_hmi` | 400 | 1 | **1** | подходящих меньше потолка |

Ни в одном воркспейсе `post_candidates` не оказался меньше обоих чисел — а именно это
было бы признаком короткого окна. Строка `auto_hmi` доказывает вторую половину
инварианта: когда подходящих меньше потолка, выборка берёт ровно их. Претензия
опровергнута, пункт закрыт.
