# Хостинг, резидентность данных и egress (можно ли убрать VPN)

<!-- audit-status:2026-08-04 -->
> **📐 ЗАМЫСЕЛ, НЕ РЕАЛИЗОВАНО · сверено 2026-08-04.**
> Замысел, а не описание системы: на дату сверки не реализован. Не читать как отчёт о готовом.
> Конкретных расхождений найдено: **3** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

> **Статус:** проект / план (НЕ реализовано). Дополнение к пакету (раунд 2).
> **Дата:** 2026-06-26 · Анализ гипотезы «убрать VPN, разместив инфру за рубежом». Индекс: [README](./README.md)
> ⚖️ Юридические выводы требуют подтверждения юристом по 152-ФЗ; вендорские/санкционные факты волатильны — проверять на момент покупки.

## ✅ Выбранный целевой сценарий (обновление 2026-06-26, раунд 3)

Владелец сознательно **снял три из четырёх якорей**, на которых анализ ниже строил дефолтную рекомендацию «Топология D». Поэтому **зафиксированный целевой сценарий — Топология B-refined: один зарубежный регион + residential-прокси только для Telegram.**

**Снятые владельцем условия:**
- **152-ФЗ ст.18 ч.5 (локализация) — вынесена за скобки** (решение владельца; остаточные обязанности 152-ФЗ ниже остаются).
- **Endpoint может быть за рубежом.** MCP потребляется через Claude/ChatGPT: облачные коннекторы (ChatGPT / claude.ai) вызывают MCP из зарубежного облака (зарубеж→зарубеж), а локальные клиенты (Claude Desktop/Cursor) принадлежат пользователю, у которого доступ к Claude/ChatGPT уже есть. Логика владельца: «если Claude/ChatGPT недоступны — MCP и не нужен».
- **Оплата хостинга решена** (зарубежная карта) — отдельно от приёма платежей клиентов (YooKassa+НПД = контур выручки).
- **Embeddings уходят с GigaChat** (workstream M-EMB в [09-roadmap](./09-roadmap.md)).

**Целевая форма:**
- **Один зарубежный узел** = store (Postgres/Qdrant/Neo4j/S3) + compute (worker/ingest HTTP) + MCP/admin endpoint.
- **+ residential-прокси только для Telethon-логина** (идеально РФ-residential, гео-стабильный под аккаунт) — маленькая подписка, НЕ xray-реестр. Причина не закон, а анти-бан Telegram: датацентр-IP и резкая смена гео → login-challenge / FloodWait / бан; 1–2 сессии без DR. Рычаг = residential гео-стабильный IP, не страна сервера.
- **Удаляется целиком** общий xray VLESS/REALITY HTTP-source failover (реестр профилей, health, hot-swap) — исходная цель владельца достигнута; открытые источники узел тянет напрямую.

**Остаточные условия (держать в плане):**
- **Latency:** если embeddings остаются на Wormsoft (РФ-endpoint), зарубежный worker платит трансграничный RTT на каждом эмбеддинге. Если латентность/международная фаза критична — брать эмбеддинг-провайдера рядом с worker (не-РФ). Выбор провайдера — открыт (см. M-EMB).
- **Остаток 152-ФЗ (снята только локализация):** уведомление РКН оператора ПДн (для самозанятого тоже; 5–10 тыс ₽ за неподачу) + уведомление о трансграничной передаче (ст.12) для зарубежного узла/OpenRouter. Бумаги, нужны в любом сценарии.
- **Вендоры** (Telegram-residential-прокси, зарубежный хостер) — проверять на момент покупки; исключить bulletproof/санкционные.

> Анализ ниже (Топологии A/B/C/D, дерево решения, приложения) сохранён как **обоснование и запись рассуждения**. Его дефолтная рекомендация (Топология D) предполагала, что локализация связывает и endpoint обязан быть в РФ — оба условия владелец снял осознанно, поэтому выбран B-refined.

---

# Хостинг, резидентность данных и egress: можно ли убрать xray, уехав за рубеж

> **Статус:** проект / решение архитектора (НЕ реализовано). Документ оценивает гипотезу владельца и фиксирует рекомендуемую топологию.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом + верификацией фактов 2026. Индекс: [README](./README.md)
> **Связанные:** [05-payments-npd-legal](./05-payments-npd-legal.md) §4 · [07-ops-ha-dr-compliance](./07-ops-ha-dr-compliance.md) §7 (B5) · [02-identity-tenancy](./02-identity-tenancy-data-isolation.md) §6 · [10-decision-log](./10-decision-log.md)

# Hosting, Data Residency & Egress — solo-operable verdict

> Scope: один человек под НПД (самозанятый) хочет **удалить боль с xray/VPN egress**, переехав на зарубежный хост, доступный и оплачиваемый из РФ. Этот документ адверсариально проверяет, законно ли это, и даёт конкретную целевую топологию. **Главный вывод вперёд:** xray-боль реальна и её можно сократить — но НЕ переездом всего за рубеж. Переезд только egress-слоя законен; переезд **базы данных** за рубеж нарушает 152-ФЗ для ПДн граждан РФ, и обезличивание этого НЕ спасает на этапе первичной записи сырого поста.

---

## 1. Постановка: что именно решает xray сегодня (the VPN-drop question)

Egress-слой — это **не одна функция, а две**, делящие один sidecar (подтверждено аудитом репозитория):

| Потребитель xray | Что делает | Файлы |
|---|---|---|
| **Telegram DC reachability** | `ConnectionTcpMTProxyRandomizedIntermediate` / SOCKS5 к MTProto DC, т.к. с голого RU-IP DC недоступны/нестабильны | `ingest/account_rotator.py`, `MTPROXY_*`/`TG_PROXY_*`, `ingest depends_on: xray` |
| **Заблокированные HTTP-источники** | SOCKS5 через VLESS/REALITY к Medium, иностранным newsroom, Google News RSS (~15 пинов `proxy_config: {type: socks5, host: xray}`) | `config/sources.yml`, `services/xray`, `shared/xray_profile_registry.py` |

Сверх этого — **runtime-реестр профилей с failover**: `xray_profile_registry.py` + `admin/.../xray_runtime.py` + `xray_health.py` + cron в scheduler. Это и есть та операционная масса, которую владелец хочет удалить: реестр профилей, health-чеки, hot-swap через `xray-active-profile.txt`. **Это операционная боль, не юридическая.**

> **Важно (verified 2026):** OpenRouter в конфиге **НЕ** ходит через xray — его API достижим из РФ напрямую. Проблема OpenRouter — **оплата**, а не доступность. Поэтому переезд за рубеж нельзя оправдывать «доступностью OpenRouter»: с RU-IP она уже есть.

---

## 2. Что переезд за рубеж РЕШАЕТ — и что ЛОМАЕТ (адверсариальная рамка)

Гипотеза оптимизирует **исходящий** столбец и игнорирует, что политика РФ-2026 сделала дорогим **входящий**.

| Ось | All-RU (статус-кво + xray) | All-foreign |
|---|---|---|
| Telegram DC connectivity | нужен MTProxy/SOCKS5 (xray) | напрямую ✅ |
| **Telegram account SAFETY** | лучше (ближе к residential/RU) | **хуже** — foreign DC-IP → больше FloodWait/банов ❌ |
| Medium / newsroom / GNews | нужен xray SOCKS5 | напрямую ✅ |
| RU LLM (Wormsoft/GigaChat/Polza) | напрямую ✅ | напрямую ✅ (но RTT на каждом embedding) |
| OpenRouter API reach | напрямую ✅ | напрямую ✅ |
| **Входящий: RU-клиент → endpoint** | напрямую ✅ (вы в РФ) | **риск блокировки РКН** (IP+SNI allowlist) ❌ |
| Админство из РФ | локально ✅ | пересекает границу (нужен тоннель) ⚠️ |
| 152-ФЗ локализация первичной БД | выполнено ✅ | нарушено, если данные не обезличены ❌ |

**Три «обратные» проблемы, которые добавляет чисто-зарубежный хост (это и есть реальная цена):**

1. **Входящая достижимость для RU-клиентов (Claude/Cursor/MCP).** «Иностранные endpoint доступны из РФ» — **уже неверно в 2026**. РФ разворачивает **IP+SNI allowlist** («белый список», полная версия опубликована 13.03.2026, активен в Москве и регионах); даже корректный SNI блокируется, если IP сервера не в allowlist. Cloudflare-обслуживаемые сайты блокируются массово. Ваш продукт — это endpoint для RU-клиентов; за рубежом вы **не можете** сделать им failover.
2. **Telegram account safety — это разворот, не ничья.** Anti-ban guidance 2026: residential-прокси предпочтительны, ≤2-3 аккаунта/IP, **datacenter-IP (особенно иностранные) → больше login-challenge, FloodWait, банов**. Ваши 1-2 hand-warmed сессии (`HOSTNAME=ingest-0`) — самый невосполнимый актив; бан = многодневный простой без DR. Connectivity к DC за рубежом **лучше**, выживаемость аккаунта **хуже**.
3. **Админство из РФ** всё равно пересекает деградирующую границу — тот же VLESS-стиль тоннель, только в обратную сторону.

> **Бонусный разворот по xray (verified 2026):** протокол, на котором стоит `xray_profile_registry` — **VLESS/REALITY — РКН начал блокировать в конце 2025** (с декабря 2025 — блокировка на уровне протоколов, не сервисов; к февралю 2026 заблокировано 469 VPN-сервисов). То есть Topology A **деградирует сама по себе** и требует постоянной ротации профилей. Это **усиливает** довод вынести открытый egress на напрямую-подключённый зарубежный узел — но не довод тащить туда БД.

---

## 3. 152-ФЗ: почему «уехать за рубеж = уйти от резидентности» — НЕВЕРНО

Это решающая юридическая ось, и здесь гипотезу нельзя ставить штампом «ок».

**Стабильная структура (вряд ли изменится скоро):**

- **ч.5 ст.18 152-ФЗ** требует, чтобы при **сборе** ПДн граждан РФ операции **запись, систематизация, накопление, хранение, уточнение, извлечение** выполнялись в БД **на территории РФ**. Это правило о **первичной БД**, не тотальный запрет на иностранную обработку.
- **ФЗ-23 от 28.02.2025, в силе с 01.07.2025** ввёл **жёсткий порядок: «сначала запись в РФ-БД, потом всё остальное»**. Verified: использование иностранной БД для **первичного сбора прямо запрещено** — любая форма/скрипт/intake, пишущий в зарубежный сервер до репликации в РФ, нарушает закон, **даже если копия в РФ создаётся потом**. Обязанность **расширена на обработчиков (processors)**, не только операторов.
- **Триггер — намерение (targeting RU-граждан), а не регистрация.** Зарубежная регистрация/хостинг **НЕ** выводит из-под обязанности. Это единственный факт, который убивает «go foreign to avoid 152-ФЗ».
- **Трансграничная передача — отдельный режим поверх локализации.** После записи в РФ копию можно передать за рубеж (ст.12, отдельное уведомление РКН). «Адекватные» страны — передача после подачи уведомления; «неадекватные» — нужно **дождаться решения РКН**, который может **запретить**. Большинство интересующих foreign-направлений, вероятно, неадекватны → это gating-зависимость, а не формальность.
- **Штраф за нарушение локализации:** до **6 млн ₽** (первое), до **18 млн ₽** (повтор) + блокировка ресурса РКН.

**Являются ли тела сообщений / @handles / медиа ПДн? — Да, косвенно.**
- @handle / отображаемое имя / личность канала, привязываемые к человеку с разумной доп. информацией — ПДн. **Тела сообщений рутинно называют третьих лиц** (ПДн людей, которые сами ничего не постили). Медиа несут лица/встроенный текст.
- **«Публичный канал» — НЕ индульгенция.** Нет общего изъятия для публичных данных. ст.10.1 («ПДн, разрешённые субъектом для распространения») требует **отдельного согласия** субъекта; человек, постящий в публичном канале, такого согласия вам не давал, и бремя доказывания законного основания — на обработчике. **Локализация применяется независимо от основания обработки** — она про то, *где лежит БД*, а не *можно ли* обрабатывать.

---

## 4. Escape hatch — обезличивание: реальный, но УЗКИЙ рычаг

Это load-bearing различие, ради которого нельзя ставить штамп.

- **Обезличивание (необратимое)** = действия, делающие невозможным **без доп. информации** соотнести данные с субъектом. Истинно обезличенные данные **выходят из-под 152-ФЗ**, локализация не действует. Методы кодифицированы (**ПП РФ №1154 от 01.08.2025** / приказ РКН №140 от 19.06.2025, в силе ~01.09.2025).
- **НО псевдонимизация / хеширование / токенизация с восстановимым ключом — это ВСЁ ЕЩЁ ПДн**, не обезличивание (ре-идентификация возможна по ключу/таблице соответствия). Набросок репозитория «drop/hash author identifiers» — это **псевдонимизация, не анонимизация**: снижает риск утечки (хорошая гигиена), но **не снимает обязанность локализации**.
- **Остаточный риск, который не закроет ни один ingest-фильтр (скажу прямо):** даже если хешировать автора и хранить «только производные сигналы», **сырое тело, которое нужно обработать для извлечения сигнала, в момент записи содержит ПДн третьих лиц** — и именно эта запись регулируется локализацией. NER-стрип @handles/email/phone (`_apply_pdn_gate`) — **best-effort, не исчерпывающий**: свободные имена, редкие сущности, контекст ре-идентифицируют.

> **Plain-language вывод (the de-identification escape hatch, честно):** обезличивание позволяет законно держать за рубежом **производные, агрегированные, НЕОБРАТИМО обезличенные trend-артефакты**. Оно **НЕ** позволяет **первичную запись сырых Telegram-постов** в зарубежной-только БД. Поэтому Topology B проваливается, а Topology C/D — работают.
>
> **Остаточный риск даже в C/D:** (1) самозанятый — **полноценный оператор ПДн** (уведомление РКН обязательно, 5 000–10 000 ₽ за неподачу с 30.05.2025); (2) тела сообщений упоминают третьих лиц — основание обработки публичного контента нужно подтверждать у юриста; (3) обезличивание — **регулируемый процесс** (ПП №1154), а не «дроп колонки» — нужна письменная методика, иначе foreign-артефакты могут счесть ПДн-за-рубежом.

---

## 5. Топологии: A / B / C / D

### A — All-RU + xray (статус-кво)
- **Egress:** xray VLESS/REALITY для Telegram + блок-HTTP; RU-LLM напрямую; OpenRouter напрямую.
- **Плюсы:** 152-ФЗ выполнен ✅; входящий из РФ локален ✅; Telegram на самом безопасном IP ✅; вы это уже эксплуатируете.
- **Минусы:** полный xray-failover surface (реестр/health/reload/cron) — соло-нагрузка; **VLESS деградирует (РКН-таргет с конца 2025)** → постоянная ротация профилей.
- **Solo:** средне (уже работает, но xray-failover — рекуррентный пожар).

### B — All-foreign, без xray, обезличено (ГИПОТЕЗА ВЛАДЕЛЬЦА)
- **Плюсы:** простейший **исходящий** сценарий; xray удалён ✅; OpenRouter/Medium тривиально достижимы.
- **Минусы:** (1) первичная запись сырых постов в **иностранную БД** → прямое нарушение ч.5 ст.18 (6/18 млн ₽ + блок) ❌; (2) обезличивание НЕ спасает (репо-хеш = псевдонимизация = ПДн) ❌; (3) Telegram account safety деградирует (foreign DC-IP) ❌; (4) **входящий endpoint под риском блокировки РКН (IP+SNI allowlist)** — клиенту не сделать failover ❌; (5) каждый embedding идёт через границу в GigaChat (latency) ⚠️.
- **Вердикт:** **ОТКЛОНЕНО** — и юридически, и инженерно, независимо друг от друга. То, что B удаляет (исходящий xray), дешевле того, что B ломает (резидентность + account safety + входящий доступ).

### C — RU-primary БД + один зарубежный fetch/compute-узел
- **Форма:** Postgres/Qdrant/Neo4j/S3 остаются **RU**; **один** иностранный stateless-узел делает прямой fetch открытых не-ПДн источников (Medium/newsroom/GNews) + OpenRouter/обезличенные hops, пишет нормализованные/обезличенные payload обратно в РФ. Telegram-ingest и customer-endpoint — **RU**.
- **Жёсткие условия легальности:** (1) сырой контент **транзитный/in-memory** на foreign-узле, system-of-record пишется **сначала в РФ**; (2) то, что foreign-узел **хранит или шлёт в иностранный LLM — необратимо обезличено**, не псевдонимизировано; (3) трансграничное уведомление РКН на легах РФ→foreign / РФ→OpenRouter; (4) общее уведомление об обработке — безусловно.
- **Плюсы:** xray-HTTP-failover surface удалён ✅; первичная БД локализована ✅; Telegram на безопасном RU-IP ✅; customer-endpoint в РФ ✅.
- **Минусы:** xray НЕ удалён полностью — **сжат до Telegram-only**; cross-border DB-write link — новая failure-поверхность ⚠️.
- **Solo:** средне (меньше узлов, чем D, но cross-border линк — реальный риск).

### D — Two-region (RU-узел: ПДн+Telegram+endpoint; foreign-узел: только открытые источники) — **РЕКОМЕНДУЕТСЯ**
- **RU-узел** = канонические Postgres/Qdrant/Neo4j/S3 (`ru-central-1`), **Telegram-ingest** (безопасный IP; минимальный MTProxy/SOCKS5 **только** для Telegram), **GigaChat embeddings** (без RTT на горячем пути), **customer-facing MCP/admin endpoint** (лучший входящий из РФ), ПДн-хранилище.
- **Foreign-узел** = stateless, **dumb-and-disposable** fetch/compute-воркер: только открытые не-ПДн-источники (Medium/newsroom/GNews) + OpenRouter/обезличенные LLM-hops; пушит нормализованные/обезличенные payload в РФ по аутентифицированному тоннелю.
- **Плюсы:** **удаляет general-purpose xray HTTP-failover surface** (большой ops-выигрыш, которого хотел владелец, и treadmill теперь, когда VLESS таргетят) ✅; Telegram на безопасном RU-IP ✅; первичная БД + endpoint в РФ (резидентность + входящий чисты) ✅; foreign-узел **stateless и одноразов** — если РКН заблокирует его IP, это не важно (он outbound-only, переразворачивается на новом IP) ✅; embeddings RU-локальны ✅.
- **Минусы:** **два узла** (высший ops-surface из четырёх) ⚠️; нужен закалённый RU↔foreign линк + чёткая ПДн-граница (foreign-узел НИКОГДА не получает сырые ПДн граждан РФ) ⚠️; платёжеспособность/живучесть foreign-узла — vendor-риск (volatile).
- **Solo:** средне-низко **при условии**, что foreign-узел держится тупым: один stateless-контейнер, IaC-provisioned, без state, авто-redeploy на свежий IP. Тогда маржинальная ops-цена над A ≈ «ещё один `deploy-tag.sh --node foreign`».

### Сравнительная таблица

| | A (All-RU+xray) | B (All-foreign) | C (RU-БД + 1 foreign worker) | D (Two-region) |
|---|---|---|---|---|
| 152-ФЗ резидентность | ✅ выполнено | ❌ нарушено | ✅ (первичная запись в РФ) | ✅ (первичная запись в РФ) |
| Удаляет xray HTTP-failover | ❌ нет | ✅ да | ✅ да (xray → Telegram-only) | ✅ да (xray → Telegram-only) |
| Telegram account safety | ✅ лучший IP | ❌ foreign DC-IP | ✅ RU-IP | ✅ RU-IP |
| Входящий из РФ (клиент→endpoint) | ✅ локально | ❌ риск allowlist | ✅ endpoint в РФ | ✅ endpoint в РФ |
| Embedding latency | ✅ RU-локально | ⚠️ RTT через границу | ✅ RU-локально | ✅ RU-локально |
| Трансграничное уведомление РКН | только OpenRouter | вся БД | foreign leg + OpenRouter | foreign leg + OpenRouter |
| Кол-во узлов / ops-surface | 1 / средний (xray-пожар) | 1 / низкий* | 2 / средний | 2 / средне-низкий (если foreign тупой) |
| Sanctions/reputation риск | низкий | зависит от хоста | зависит от хоста | зависит от хоста |
| **Вердикт** | приемлемо, но деградирует | **ОТКЛОНЕНО** | viable (fallback) | **РЕКОМЕНДУЕТСЯ** |

\* B «низкий ops-surface» обманчив: скрытая цена — Telegram-баны + недоступность для клиентов, это не строка бюджета, а реальная потеря.

---

## 6. РЕКОМЕНДАЦИЯ

**Принять Topology D (two-region), fallback — C.** Гипотезу владельца в её буквальной форме (Topology B: всё за рубеж, xray удалён) — **ОТКЛОНИТЬ** по двум независимым причинам: (1) юридически — первичная запись сырых Telegram-постов (ПДн третьих лиц в телах) попадёт в иностранную БД → прямое нарушение порядка «сначала-в-РФ» ч.5 ст.18 (ФЗ-23, 01.07.2025); обезличивание этого НЕ спасает, т.к. репо-хеш — псевдонимизация (ПП №1154), а не необратимое обезличивание; (2) инженерно — foreign-only ухудшает Telegram account safety и подставляет customer-endpoint под IP+SNI allowlist РКН-2026.

**Что делать:**
1. **БД остаётся в РФ.** Канонические Postgres/Qdrant/Neo4j + S3 `ru-central-1`. Желательно перенести Postgres/Redis на **управляемый RU 152-ФЗ/УЗ-1 сервис** (Cloud.ru — уже ваш S3-вендор — либо Yandex/Selectel), чтобы соло-оператору не нянчить БД. Это закрывает резидентность и совпадает с уже зафиксированной позицией репо (07-ops §7 B5, 02-identity §6).
2. **Telegram-ingest и customer/MCP-endpoint остаются в РФ.** Сохранить **минимальный MTProxy/SOCKS5 только для Telegram** (не предполагать, что голый RU-IP достанет MTProto при throttling-2026).
3. **Добавить один тупой, stateless, одноразовый foreign-узел** для прямого fetch открытых не-ПДн-источников (Medium/newsroom/GNews) и OpenRouter/обезличенных hops. Он **удаляет xray HTTP-failover surface** — реальную боль владельца. Сырые ПДн он **никогда** не персистит; пишет в РФ first.
4. **Перед переездом — измерить, какой потребитель xray реально болит** (см. decision tree): если это Telethon-MTProxy (stateful recorder), вынос за рубеж почти ничего не даёт (он всё равно пишет RU-primary); если это блок-HTTP-fetch — D/C его чисто снимают.
5. **Безусловно и независимо:** подать **уведомление об обработке ПДн** в РКН до запуска (самозанятый — полноценный оператор) и **отдельное трансграничное уведомление** на foreign-leg/OpenRouter.
6. **Vendor-гигиена (verified 2026):** **жёстко исключить aeza/Hypercore** (OFAC-designation 01.07.2025 / 19.11.2025, bulletproof-hosting) и любой reseller, фронтящий их ёмкость — это IP-репутация + риск бана платежей/OpenRouter. Foreign-узел — чистая репутация в дружественной юрисдикции (KZ/RS/AM/TR), оплачиваемый RU-reseller'ом в ₽ (MIR/СБП) или криптой; **Hetzner/западные мейджоры РФ не онбордят и заблокированы входящим.** НЕ оправдывать переезд «доступностью OpenRouter» — его API уже достижим из РФ, ограничена только оплата.

---

## 7. Чеклист верификации (перед коммитом в топологию)

**STABLE (структурное, перепроверять редко):**
- Локализация = RU-primary-БД для сбора/записи/хранения/уточнения/извлечения; триггер по намерению; не тотальный запрет иностранной обработки.
- Псевдонимизация ≠ обезличивание; из-под 152-ФЗ выводит только необратимое обезличивание.
- Трансграничная передача — отдельный режим поверх локализации (адекватные/неадекватные страны).
- Самозанятый — полноценный «оператор» с обязанностями уведомления + трансгранички + локализации.

**VOLATILE (verified June 2026 — перепроверить на момент решения):**
- ✅ verified: порядок «запись-в-РФ-первой» (ФЗ-23, 01.07.2025), processors теперь в scope.
- ✅ verified: VLESS/REALITY таргетится РКН с конца 2025; протокол-блокинг с дек.2025 (Topology A деградирует).
- ✅ verified: IP+SNI allowlist («белый список», 13.03.2026), Cloudflare блокируется (foreign-endpoint рискован).
- ✅ verified: самозанятый = оператор; 5 000–10 000 ₽ за неподачу с 30.05.2025.
- ✅ verified: aeza/Hypercore — OFAC (01.07.2025 / 19.11.2025); Hetzner отказывает РФ, 12 foreign-хостов заблокированы входящим.
- **VERIFY (юрист):** считается ли транзитная in-memory обработка сырых ПДн на foreign-узле ДО RU-записи иностранным «сбором/записью» (граница между легальным C/D и нарушающим B).
- **VERIFY (юрист):** квалифицируется ли ваше обезличивание производных артефактов как необратимое по ПП №1154 — письменная методика.
- **VERIFY (юрист):** законное основание обработки публичного Telegram-контента без согласия субъекта (ст.10.1 vs законный интерес) для **платного** перепродаваемого продукта.
- **VERIFY (РКН):** на момент покупки — на каком IP-провайдере дружественной юрисдикции, платёжеспособном из РФ (₽/MIR/СБП или крипта), с чистой IP-репутацией, не заблокированном целевыми источниками; РКН «адекватность» по каждому foreign-направлению.
- **VERIFY (ops):** какой потребитель xray реально болит — блок-HTTP-fetch vs Telethon-MTProxy; достаёт ли голый RU-IP MTProto при throttling-2026.
- **VERIFY (owner):** как реально топится OPENROUTER_API_KEY (USDC/крипта ~5% vs RU-rail) и не создаёт ли эта схема трансграничного следа, который надо декларировать.
- **VERIFY (ops):** добавленный border-RTT на embedding-hot-path, если что-то из compute уезжает (embeddings — GigaChat-only, на каждый пост) — держать embeddings на RU-узле.

Источники (verified June 2026): [lidings — localization tightening 01.07.2025](https://www.lidings.com/media/legalupdates/localization_pd_update/) · [konsu — record-first requirement](https://konsugroup.com/en/news/new-requirements-personal-data-protection-russia-2025-07/) · [zona.media — RU censorship 2026 (VLESS block)](https://en.zona.media/article/2026/04/07/russian_internet_censorship_2026) · [HRW — disrupted/throttled/blocked](https://www.hrw.org/report/2025/07/30/disrupted-throttled-and-blocked/state-censorship-control-and-increasing-isolation) · [iz.ru — full white list published 13.03.2026](https://en.iz.ru/en/2058779/2026-03-13/full-version-white-list-websites-and-internet-services-has-been-published) · [reclaimthenet — Moscow internet whitelist](https://reclaimthenet.org/russia-mobile-internet-whitelist-moscow-censorship) · [habr (МойСклад) — штрафы за ПДн с 30.05.2025](https://habr.com/ru/companies/moysklad/articles/994568/) · [biznesinalogi — РКН-регистрация для самозанятых 2025](https://biznesinalogi.ru/lenta/post/54270/) · [US Treasury sb0185 — aeza OFAC](https://home.treasury.gov/news/press-releases/sb0185) · [US Treasury sb0319 — Hypercore front company](https://home.treasury.gov/news/press-releases/sb0319) · [iz.ru — 12 foreign hosts blocked in RU](https://en.iz.ru/en/1866459/2025-04-07/websites-12-foreign-hosting-providers-were-blocked-russia) · [russiable — VPS payable from RU 2026](https://russiable.com/russian-vps-hosting/).


## Рекомендуемая топология (резюме)

Adopt Topology D (two-region): keep ALL canonical data in RU — Postgres/Qdrant/Neo4j + S3 ru-central-1 (preferably on a managed RU 152-FZ/УЗ-1 service like Cloud.ru/Yandex/Selectel), Telegram ingest on the safe RU IP behind a minimal Telegram-only MTProxy/SOCKS5, GigaChat embeddings RU-local, and the customer-facing MCP/admin endpoint in RU — while adding ONE dumb, stateless, disposable foreign node that directly fetches only open non-PDn sources (Medium, foreign newsrooms, Google News RSS) and runs OpenRouter/irreversibly-anonymized LLM hops, pushing de-identified payloads back to RU first. This deletes the general-purpose xray VLESS/REALITY HTTP-source failover surface (the owner's actual ops pain, now a treadmill because RKN began blocking VLESS in late 2025) while keeping the primary database 152-FZ-localized, Telegram on a ban-safe RU IP, and the customer endpoint reachable from RU (avoiding the 2026 IP+SNI allowlist that kills any foreign-hosted endpoint). The single decisive factor: 152-FZ ст.18 ч.5 as amended 01.07.2025 forces the FIRST recording of raw Telegram posts (which contain third-party PDn in their bodies) into a RU database, and de-identification cannot rescue an all-foreign topology because the raw body must be parsed before it can be anonymized — so the database simply cannot leave RU, which collapses the choice to "move only the egress/fetch function abroad, never the store."

## Дерево решения

## Decision tree

**Q0 — Does the corpus ever record RU-citizen PDn (Telegram bodies/@handles, customer email/INN)?**
- NO (purely org/product/topic signals, provably no individuals) → de-identification at source is trivial; foreign primary store MIGHT be legal — but this is almost never true for a Telegram trends product. → get lawyer sign-off, else treat as YES.
- YES (the realistic case) → go to Q1.

**Q1 — Can the FIRST recording of the raw post be made irreversibly anonymous BEFORE it touches any database?**
- NO (you must parse the raw body to extract entities → raw PDn is recorded at ingest) → **all-foreign (Topology B) is ILLEGAL** under the 01.07.2025 "record-in-RU-first" rule. → RU-primary DB is MANDATORY → go to Q2.
- YES (you can prove necessary irreversible обезличивание pre-DB, written method per ПП №1154) → rare; still file уведомление; lawyer must sign the method statement → then Q2 for egress anyway.

**Q2 — Which xray consumer is the real pain (MEASURE first)?**
- Blocked-HTTP fetch (Medium/newsrooms/GNews) is the pain → a foreign FETCH node cleanly retires that → go to Q3.
- Telethon MTProxy/SOCKS5 to Telegram DCs is the pain → moving the stateful recorder abroad saves little (it must still write RU-primary AND a foreign DC-IP raises ban risk) → KEEP Telegram ingest in RU behind a minimal MTProxy; only move HTTP fetch → go to Q3.

**Q3 — Is the customer/MCP endpoint and the DB stayable in RU, and can you operate two nodes solo?**
- Two nodes OK + foreign node kept dumb/stateless/disposable → **Topology D (RECOMMENDED).**
- Two-node ops too heavy at MVP → **Topology C (fallback):** RU DB + RU endpoint + RU Telegram + one foreign worker for open sources only; treat as temporary, plan to evolve to D.

**Unconditional gates (independent of topology):** file RKN уведомление об обработке ПДн before launch (самозанятый is a full operator; 5k–10k ₽ for non-filing); file a SEPARATE cross-border-transfer notification for the foreign-node/OpenRouter legs; hard-exclude aeza/Hypercore and any OFAC/bulletproof-linked host; do NOT justify going abroad on "OpenRouter reachability" (already reachable from RU — only payment is constrained).

## Чек-лист проверки (юрист / вендор)

- LAWYER: Confirm whether transient in-memory handling of raw PDn on a foreign fetch node BEFORE the RU-primary write counts as foreign 'collection/recording' under ч.5 ст.18 as amended 01.07.2025 — this is the exact dividing line between compliant Topology C/D and breaching B.
- LAWYER: Confirm whether the system's de-identification of derived trend artifacts qualifies as IRREVERSIBLE обезличивание under ПП РФ №1154 (01.08.2025) / приказ РКН №140 such that foreign-stored artifacts leave the PDn regime — obtain a WRITTEN method statement; pseudonymization/hashing with a recoverable key does NOT qualify.
- LAWYER: Confirm lawful basis for processing public-Telegram-channel author/body PDn without subject consent (ст.10.1 vs legitimate interest) and that it is defensible for a PAID, resold-synthesis product (ties to decision-log item 9 data-licensing).
- LAWYER/RKN: Confirm exact scope/wording of the уведомление об обработке ПДн for a самозанятый processing Telegram author handles + customer email + buyer INN, and FILE it before launch (B5 gate; 5,000–10,000 ₽ for non-filing since 30.05.2025; самозанятый is a full operator with no carve-out).
- LAWYER/RKN: Determine whether each foreign destination (candidate foreign host, OpenRouter) is on the RKN 'adequate countries' list — adequate = transfer after filing; non-adequate = must await RKN approval and RKN can prohibit it (gates whether C/D's cross-border legs are even permitted). File the SEPARATE cross-border-transfer notification.
- LAWYER: Confirm the foreign worker in C/D can PROVABLY never receive raw RU-citizen PDn (Telegram bodies/@handles) — i.e. 'fetch open foreign non-PDn sources + receive only pre-anonymized text' keeps it outside localization; the RU↔foreign link payload must be classified before it leaves RU.
- LAWYER/ACCOUNTANT: Confirm самозанятый/НПД is an adequate legal vehicle for a multi-tenant PDn operator and clarify КоАП exposure bands (no legal entity yet registered); confirm statutory-retention vs 152-FZ-erasure tension for receipts/revenue_ledger does not force foreign storage of any financial PDn — keep all financial PDn RU-primary.
- OWNER/OPS: MEASURE which xray consumer is the real pain — blocked-HTTP source fetch (Medium/Google News) vs Telethon MTProxy/SOCKS5 to Telegram DCs — because C/D only cleanly retires the former; moving the stateful Telethon recorder abroad saves little and raises ban risk.
- OPS: Test the live VLESS/REALITY profiles from the prod RU node NOW — RKN began protocol-level blocking of VLESS in late 2025; if your relay path is TSPU-blocked, Topology A's HTTP egress is already failing and the case for D strengthens.
- OPS: Confirm whether the 1-2 shared Telegram sessions can tolerate ANY datacenter/foreign IP, or must stay on the RU/residential-adjacent IP — a single ban is a multi-day, no-DR outage; get the owner's risk tolerance in writing before moving ingest. Also verify a bare RU IP still reaches MTProto under 2026 throttling (do not assume).
- VENDOR: At purchase time, pick a foreign node that is (a) payable from RU (MIR/СБП/RUB via a RU-reseller, or crypto — Hetzner and Western majors refuse RU and are blocked inbound), (b) clean IP reputation in a friendly jurisdiction (KZ/RS/AM/TR), (c) HARD-EXCLUDE aeza/Hypercore and any OFAC/bulletproof-linked stock, (d) egress not blocked by OpenRouter/Telegram/target newsrooms.
- OPS/VENDOR: At purchase time, re-verify the chosen RU cloud (Cloud.ru/Yandex/Selectel) still holds ФСТЭК УЗ-1 certification AND offers the specific managed services needed (Postgres, Redis; Qdrant/Neo4j likely self-managed VMs).
- OWNER: Verify how OPENROUTER_API_KEY is actually funded/topped-up (USDC/crypto ~5% fee vs any RU rail) post the 2026 RU-region payment block, and whether that arrangement itself creates a cross-border data-export footprint to declare — do NOT justify going abroad on OpenRouter reachability (API already reachable from RU; only payment is constrained).
- OPS: If any compute moves to the foreign node, MEASURE the added border RTT on the GigaChat embedding hot path (GigaChat-only, 2560d, every post) and keep embeddings on the RU node if latency is unacceptable. Note GIGACHAT_VERIFY_SSL_CERTS=False still works cross-border but adds RTT.

---

## Приложения: детальный разбор по направлениям

### Приложение A. 152-ФЗ: локализация ПДн и де-идентификация

_Уверенность: **medium**_

## TL;DR verdict

The owner's hypothesis — "pick a foreign provider reachable+payable from RU and drop the xray/VPN egress layer" — **collides head-on with 152-FZ localization and, as a *primary-store* topology, is not legally viable for raw Telegram/customer PDn.** De-identification is a *real* lever but a **narrow** one: only **irreversible обезличивание** (not hashing/pseudonymization) takes data out of the PDn regime, and Telegram message *bodies* can still name third parties, so a "store only de-identified trend signals" pipeline reduces but does not cleanly eliminate the localization trigger. The defensible answer is **topology (C): RU-primary DB + foreign-compute split** — keep the canonical record-of-first-collection in RU, allow a foreign node only for *source-fetching egress and de-identified compute*, which simultaneously satisfies the existing docs' "PDn routing gate" and lets the foreign node reach Telegram/Medium/OpenRouter directly for the *non-PDn* legs.

This is consistent with — and sharpens — the existing repo plan: `docs/saas/07-ops-ha-dr-compliance.md` §7 and `docs/saas/05-payments-npd-legal.md` §4 already commit to RU-primary stores (Postgres on RU node, Cloud.ru S3 `ru-central-1`, GigaChat) + a PDn gate. The owner's foreign-hosting idea does **not** overturn that; it only changes *which* node does the blocked-source fetch.

---

## (1) What localization (ст.18.5 / ч.5 ст.18 152-ФЗ) actually requires — STABLE legal structure

**Covered processing operations (the exact verbs in the statute):** when collecting PDn of RU citizens, including via the Internet, the operator must ensure **запись (recording), систематизация (systematization), накопление (accumulation), хранение (storage), уточнение/обновление/изменение (update), извлечение (retrieval)** are performed **using databases located in the territory of the RF**. (Confirmed against the current statute text and the Feb 2025 amendment.)

**It is a RU-PRIMARY-DATABASE rule, not a total ban on foreign processing.** The law does **not** forbid all foreign touching of the data. It forbids using a *foreign database* for those first-collection operations. The crux change: **Law №23-ФЗ of 28.02.2025, effective 01.07.2025**, rewrote ч.5 ст.18 to impose an **explicit ordering** — RU-citizen PDn must **first be recorded and stored in a RU database**, and only *after* that may it be subject to cross-border transfer (and foreign replicas). RKN (24.03.2025) and Mincifry (12.05.2025) clarifications confirm: restrictions are on the **primary record**, and data already recorded via RU databases may then flow cross-border under the separate transgranichnaya rules. So **foreign replicas / subsequent foreign processing are allowed *after* the RU primary recording** — this is the legal seam topology (C) rides on.

**Penalties (VOLATILE but current):** localization breach → admin fine up to **6M RUB** first offense, up to **18M RUB** repeat, plus RKN-ordered blocking. (This is the новая редакция penalty tier, not the old ~6M-only.)

**Reach is intent-based, not registration-based:** the duty attaches to any operator that *targets RU citizens* regardless of where the legal entity sits. So registering/hosting abroad does **not** escape the obligation — a foreign-hosted product aimed at RU users is still bound. This is the single fact that kills "go foreign to avoid 152-FZ."

---

## (2) Are Telegram bodies / @handles / media personal data, and does de-identification at ingest work? — STABLE structure + a hard residual-risk caveat

**Yes, this is PDn (indirect).** An author @handle / display name / channel identity that can, with reasonable additional info, be tied to a person is PDn. Message **bodies** routinely name third parties (PDn of people who never posted). **Media** can carry faces/biometric-adjacent content and embedded text. The existing repo register (`07-ops...md` §7 table) already classifies all three as "Yes (indirect)." That classification is correct and conservative.

**"Public channel" is NOT a free pass.** There is no blanket public-data exemption. The relevant regimes:
- **ст.10.1** ("ПДн, разрешённые субъектом для распространения") — needs a *specific* consent-to-distribute by the subject; a person posting in a public channel has **not** given you that consent, and the burden of proving lawful basis for further processing is **on the processor**.
- Общедоступные источники (ст.8) is a narrow construct and does not auto-legalize scraping.
- Practically: ingest of public-channel content rests on **законный интерес / legitimate-interest-style** footing at best, and **localization still applies regardless of the lawful-processing basis** — localization is orthogonal to *whether* you may process; it governs *where the database sits*.

**The de-identification escape hatch — viable but NARROW (this is the load-bearing distinction):**
- **Обезличивание (irreversible anonymization)** = actions making it impossible, *without additional information*, to attribute data to a subject. Truly anonymized data **falls outside the PDn regime**, so localization does not bite. **PP RF №1154 of 01.08.2025** now codifies approved обезличивание methods (incl. "метод введения идентификаторов").
- **BUT pseudonymization / hashing / tokenization with a recoverable key is explicitly treated as STILL PERSONAL DATA**, not обезличивание — because re-identification remains possible with the key/mapping table. The repo's own sketch ("drop/hash author identifiers") is therefore **pseudonymization, not anonymization** — it reduces breach risk and is good security hygiene, but it does **not** remove the data from the localization obligation. (Sources are explicit that the identifier-introduction method "cannot be said to anonymize 100%.")
- **Residual risk that no ingest filter fully closes:** even if you hash the author and store "only derived trend signals," the **raw message body you must process to derive the signal still contains third-party PDn** at the moment of recording — and that recording is exactly the operation localization governs. A NER strip of @handles/emails/phones (the repo's `_apply_pdn_gate` pseudonymizer) is **best-effort**, not exhaustive: free-text names, rare-entity descriptions, and context can re-identify. So "what is *stored* is not PDn" is achievable for the **derived signal artifacts**, but the **collection/recording step over the raw post is not de-identifiable away** — that step is what must use a RU database.

**Net viability:** de-identification lets you keep **derived, aggregated, irreversibly-anonymized trend artifacts** in a foreign store legally. It does **not** let the **first recording of raw Telegram posts** happen in a foreign-only database. This is precisely why topology (B) fails and topology (C) works.

---

## (3) RKN operator notification (уведомление об обработке ПДн) for a самозанятый — VOLATILE, recently tightened

- **Mandatory for самозанятые with NO exemption.** As of the 2024–2025 changes, the duty to file the уведомление об обработке ПДн extends to **organizations, ИП, AND самозанятые** alike. The old "small operator / employees-only" carve-outs that many relied on were narrowed; the prevailing 2025–2026 guidance is **"no exceptions for самозанятые."**
- **Timing:** file **before** processing begins (practically, no later than ~10 days after start).
- **Enforcement risk is now REAL, not theoretical:** from **30.05.2025** there are dedicated fines for *failure to notify*: **самозанятый/физлицо 5 000–10 000 RUB**, ИП/юрлицо **100 000–300 000 RUB**. RKN actively enforces the registry. For a paid SaaS handling customer emails + Telegram author PDn, **filing the уведомление is a launch gate**, which matches the repo's "B5 / РКН operator notification" flag in `07-ops...md` §7 and `05-payments...md` §4.
- The уведомление itself must declare cross-border transfer destinations if any (ties to item 4).

---

## (4) Cross-border transfer (трансграничная передача) if PDn reaches a foreign LLM / foreign host — STABLE structure

Any flow of RU-citizen PDn to a **foreign jurisdiction** (foreign host, OpenRouter, or Polza/any LLM whose compute is abroad) is **трансграничная передача** and triggers a **separate, dedicated regime on top of localization**:
- **A distinct RKN notification of intent to transfer cross-border is required** (separate from the general processing уведомление).
- **"Adequate" countries** (RKN list, those with adequate protection / Convention 108 members): transfer may begin **after filing** the notification (no need to await RKN's answer).
- **"Non-adequate" countries**: must **await RKN's decision**; RKN can **prohibit** the transfer for that operator/destination. Most of the foreign LLM/host destinations of interest are likely non-adequate, making this a gating dependency, not a formality.
- **Sequencing rule (post-01.07.2025):** you must **record in RU first**, *then* transfer. A foreign-only host that records first abroad violates the ordering even before you reach the transfer analysis.
- **Practical consequence for providers:** GigaChat (Sber, RU) and Wormsoft (`ai.wormsoft.ru`, RU) and a RU-facing Polza are **not** cross-border. **OpenRouter (foreign)** and any **foreign host** are. Embeddings being **GigaChat-only (RU)** is compliant by construction — keep it that way. The repo's PDn gate (RU-pin `pdn_high`, pseudonymize before any non-RU hop) is the correct mechanism; note the gate must rely on **anonymization for the leg that leaves RU**, since pseudonymized-still-PDn text crossing the border re-triggers the transfer regime.

---

## Verdict per topology

### (A) All-RU (status quo + xray egress) — **COMPLIANT, RECOMMENDED for the primary store**
- Localization: satisfied (Postgres on RU node, Cloud.ru S3 `ru-central-1`, GigaChat Sber). Already the repo's committed posture.
- Cross-border: only the OpenRouter/Polza-foreign LLM hops, closed by the existing PDn routing gate.
- Cost: keeps the **xray/VLESS/MTProxy egress complexity** the owner wants to delete. This is an *operational* burden, not a *legal* one.
- **This is the legally safest and lowest-novelty option. The xray layer is the price of RU-primary hosting given blocked sources.**

### (B) All-foreign + aggressive de-identification — **NOT VIABLE as described; HIGH legal risk**
- **Fails localization ordering:** the *first recording* of raw Telegram posts (which contain third-party PDn in bodies) would occur in a **foreign** database → direct ч.5 ст.18 breach (up to 6M/18M RUB + blocking).
- De-identification does **not** rescue it, because (a) the operations that must be RU-located include the **recording of the raw post you de-identify from**, and (b) the repo's "hash the author" is **pseudonymization (still PDn)**, not обезличивание. Only if you could prove **irreversible anonymization *before* any RU-citizen PDn is ever recorded in a database** — impossible when the raw body must be parsed to extract entities — would this hold.
- Customer-side PDn (email, buyer INN) recorded foreign-first independently breaks it.
- **Verdict: do not pursue B.** The egress simplification is not worth a 6M+ RUB localization exposure and a blockable resource.

### (C) RU-primary DB + foreign-compute split — **VIABLE, the right way to get most of the egress simplification**
- **Keep the canonical record-of-first-collection in RU** (Postgres + S3 `ru-central-1` + GigaChat embeddings) — satisfies localization ordering.
- **Add a foreign *compute/fetch* node** that: (i) performs the **source-fetch egress** to Telegram/Medium/Google-News/OpenRouter that currently needs xray — letting you **retire most of the xray sidecar for the fetch leg** (the foreign node reaches blocked sources directly); (ii) runs only **de-identified / non-PDn** synthesis legs, with the RU node remaining the system-of-record.
- **Hard constraints that make C legal:** (1) raw fetched content must be **shipped to the RU primary and recorded there first**, before any durable foreign storage; the foreign node treats raw PDn as **transient, in-memory, non-persisted**; (2) anything the foreign node *stores* or sends to a foreign LLM must be **irreversibly anonymized**, not just pseudonymized; (3) the cross-border leg (RU→foreign node, RU→OpenRouter) needs the **трансграничная notification** and, for non-adequate destinations, RKN approval. (4) file the **general уведомление об обработке** regardless.
- **This converges with the existing repo plan** (`07-ops...md` §7 PDn gate + RU-primary stores; `02-identity...md` keeps RU stores) — it does not require rearchitecting the data model, only relocating the *egress/fetch* function to a foreign worker while pinning the *database* in RU.
- **Caveat:** C only deletes the **fetch-egress** portion of xray. If Telethon itself (MTProxy/SOCKS5 to Telegram DCs) is the part you wanted gone, note the **ingest/Telethon session is a stateful recorder** — if it runs on the foreign node it must still treat raw posts as transient and write the primary record to RU, which may not save much over keeping ingest in RU behind xray. Quantify which xray consumer (fetch vs Telethon) is the real pain before committing.

---

## Stable vs volatile separation (for the owner)

**STABLE (structural, unlikely to change soon):**
- Localization = RU-primary-DB for collection/recording/storage/update/retrieval; intent-based reach; not a total foreign ban.
- Pseudonymization ≠ обезличивание; only irreversible anonymization exits the PDn regime.
- Cross-border = a separate regime layered on top, with adequate/non-adequate distinction.
- Самозанятый is a full "operator" with notification + cross-border + localization duties.

**VOLATILE (verify at decision time — these moved in 2025 and RKN guidance evolves):**
- The Feb 2025 / July 2025 "record-in-RU-first" ordering and its exact RKN/Mincifry clarifications.
- Penalty amounts (6M/18M localization; 5k–10k самозанятый non-notification) — current as of 2025 reforms.
- The PP RF №1154 (01.08.2025) approved anonymization-method list.
- The RKN "adequate countries" list and whether your specific foreign host/LLM destination is on it.

Sources: [comply.ru — localization & cross-border changes from 01.07.2025](https://comply.ru/tpost/c43ezsout1-lokalizatsiya-i-transgranichnaya-peredac); [normativ.kontur.ru — 152-ФЗ red. 24.06.2025](https://normativ.kontur.ru/document?moduleId=1&documentId=501173); [wcr-consulting.com — localization requirements 2026](https://wcr-consulting.com/blog/2026/03/13/lokalizaciya-baz-dannyh-personalnyh-dannyh/); [b-152.ru — storing PDn abroad / localization 2025](https://b-152.ru/hranenie-personalnyh-dannyh-za-granicej); [b-152.ru — what changed 2025–2026](https://b-152.ru/zakon-o-personalnyh-dannyh-2025); [consultant.ru — ст.3 определения / обезличивание](https://www.consultant.ru/document/cons_doc_LAW_61801/4f41fe599ce341751e4e34dc50a4b676674c1416/); [e-office24.ru — anonymization requirements (PP №1154)](https://e-office24.ru/news/trebovaniya-k-obezlichivaniyu-personalnykh-dannykh/); [altcor.ru — обезличивание vs pseudonymization 2025](https://altcor.ru/personal-data/obezlichivanie-pdn); [data-sec.ru — cross-border transfer 2026 / adequate countries](https://data-sec.ru/personal-data/cross-border-countries/); [e-office24.ru — cross-border notification to RKN](https://e-office24.ru/news/transgranichnaya-peredacha-personalnykh-dannykh/); [roskom24.ru — RKN registration 2026, самозанятые included](https://roskom24.ru/registratsiya_v_roskomnadzore_v_2025_godu/); [ic-tech.ru — PDn 2026 changes & non-notification fines from 30.05.2025](https://ic-tech.ru/blog/personalnye-dannye-2026-izmeneniya-2025-i-chto-srochno-proverit/); [consultant.ru — ст.10.1 ПДн разрешённые для распространения](https://www.consultant.ru/document/cons_doc_LAW_61801/591acc70f577873c1ee54765eda110b7a0271eaf/).

**Рекомендация (направление):** Reject topology (B) all-foreign — it breaks the post-01.07.2025 "record-in-RU-first" rule because the first recording of raw Telegram posts (third-party PDn in bodies) would land in a foreign DB; de-identification does NOT save it because the repo's "hash the author" is pseudonymization (still PDn under PP №1154), not irreversible обезличивание. Keep the canonical primary store in RU (topology A is fully compliant and is already the repo's committed posture in 07-ops-ha-dr-compliance.md §7 + 02-identity-tenancy §6). If the goal is to delete the xray egress pain, adopt topology (C): a RU-primary DB + a foreign FETCH/COMPUTE node that (1) reaches Telegram/Medium/OpenRouter directly, (2) treats raw PDn as transient/in-memory and writes the system-of-record to RU FIRST, (3) only persists or LLM-processes IRREVERSIBLY-ANONYMIZED derived signals abroad. Before committing C, measure which xray consumer actually hurts: if it is Telethon's MTProxy (a stateful recorder), moving it abroad saves little because it must still write the RU primary record — in that case keep ingest in RU behind xray and only move the blocked-HTTP-source fetch to the foreign node. Independently and unconditionally: file the RKN уведомление об обработке ПДн before launch (mandatory for самозанятые, 5k–10k fine for non-filing), and a SEPARATE cross-border-transfer notification for the OpenRouter/foreign-node legs. Implement the existing PDn routing gate so the only data crossing the border is irreversibly anonymized (not merely pseudonymized).

**Проверить:**

- LAWYER: Confirm the post-01.07.2025 ч.5 ст.18 'record-in-RU-first' ordering as applied to a topology-C foreign fetch node — specifically whether transient in-memory handling of raw PDn on a foreign node BEFORE the RU primary write counts as foreign 'recording/collection' (the dividing line between compliant C and breaching B).
- LAWYER: Confirm whether the system's de-identification of derived trend artifacts qualifies as irreversible 'обезличивание' under PP RF №1154 (01.08.2025) such that the foreign-stored artifacts are outside the PDn regime — and get a written method statement; pseudonymization/hashing with a recoverable key does NOT qualify.
- LAWYER: Confirm lawful basis for processing public-Telegram-channel author/body PDn without subject consent (ст.10.1 vs legitimate-interest) and that this basis is defensible for a PAID resold-synthesis product (ties to decision-log item 9 data-licensing).
- LAWYER/RKN: Confirm scope and exact wording of the уведомление об обработке ПДн for a самозанятый processing Telegram author handles + customer email + buyer INN, and file before launch (B5 gate already flagged in the repo).
- LAWYER/RKN: Determine whether each foreign destination (the candidate foreign host, OpenRouter) is on the RKN 'adequate countries' list — adequate = transfer after filing; non-adequate = must await RKN approval and RKN can prohibit it (gates whether topology C's cross-border legs are even permitted).
- OWNER/OPS: Measure which xray consumer is the real pain — blocked-HTTP source fetch (Medium/Google News) vs Telethon MTProxy/SOCKS5 to Telegram DCs — because topology C only cleanly retires the former; moving the stateful Telethon recorder abroad saves little since it must still write the RU primary record.
- ACCOUNTANT/LAWYER: Confirm the statutory-retention vs 152-FZ-erasure tension for receipts/revenue_ledger (decision-log item 8) does not force foreign storage of any financial PDn — keep all financial PDn RU-primary too.
- OWNER: Verify how OPENROUTER_API_KEY payment/access was actually solved (already configured) and whether that arrangement itself creates a cross-border data-export footprint that must be declared in the RKN cross-border notification.

---

### Приложение B. Egress и топологии хостинга (RU vs зарубеж)

_Уверенность: **medium**_

## TL;DR (engineering verdict)

The "drop xray by hosting abroad" hypothesis is **half-right and half-wrong, and the wrong half got worse in 2026**. A foreign node *does* cleanly solve outbound reachability to Telegram DCs / Medium / blocked newsrooms / OpenRouter. But (a) it **does not** save the LLM egress story (RU providers were always globally reachable, so RU hosting never needed a proxy *to* them), and (b) it **introduces two new failure modes that 2026 RU censorship makes severe**: foreign customer/MCP endpoints are now actively at risk of RKN IP/SNI-allowlist blocking *inbound from RU*, and a foreign-DC IP is the **worst** IP class for Telegram account safety. The honest framing is not "RU+xray vs foreign-no-xray" — it is **"where do you pay the egress tax: outbound-from-RU (xray, and xray's protocol is itself now being blocked) vs inbound-to-RU (RKN blocking your foreign endpoint)."** The engineering-best answer is a **two-region split (Topology D)**, with a fallback of **C (RU-primary-DB + foreign compute)** if D's ops cost is too high for a solo operator. Pure-foreign (B) is rejected on Telegram-stability + inbound-reachability grounds, independent of the legal question.

---

## STABLE STRUCTURE (architecture-level, won't change with vendor news)

### (1) What the xray / MTProxy / SOCKS5 layer solves TODAY — verified against the repo
The egress layer is **not one thing**; it is two distinct jobs sharing one sidecar:

- **Telegram DC reachability (MTProxy / SOCKS5).** `ingest/account_rotator.py` builds the Telethon client with either `ConnectionTcpMTProxyRandomizedIntermediate` (MTProxy: host/port/secret) or a SOCKS5 dict, resolved from `MTPROXY_*` / `TG_PROXY_*` / `TG_SOCKS5` env or per-source `proxy_config`. `ingest` `depends_on: xray` in `docker-compose.yml`. This path exists because **Telegram's MTProto DCs are unreliable/blocked from a bare RU IP**.
- **Blocked/unreliable HTTP sources (SOCKS5 via xray, VLESS/REALITY upstream).** `config/sources.yml` pins a concrete set through `proxy_config: {type: socks5, host: xray}`: **Medium feeds** (`medium.com/feed/tag/*` — future/design/mobility), several **foreign newsrooms** (e.g. `mobilityhouse.com` newsroom), and ~12+ other web/RSS sources. These are sources blocked or geo-throttled from RU. The xray sidecar (`services/xray`, `shared/xray_profile_registry.py`) terminates a **VLESS/REALITY** tunnel to a foreign relay and exposes a local SOCKS inbound (`socks-in`, port 10808) that both ingest and crawl4ai dial.
- **There is a runtime failover registry**: `shared/xray_profile_registry.py` + `admin/backend/services/xray_runtime.py` + `xray_health.py` + a scheduler cron — admin can hot-swap VLESS profiles by writing `xray-active-profile.txt` and touching a reload trigger. This is real operational weight: a profile registry, health-checks, and an admin failover workflow you maintain **only because egress is hard from RU**.
- **OpenRouter region access**: NOT currently routed through xray in the config (no `openrouter`/`polza` proxy pin found). The configured `OPENROUTER_API_KEY` works because OpenRouter's *API* is reachable from RU directly; the constraint is *payment*, not *reachability* (see volatile section).

**What disappears if the node is abroad (direct egress):**
- ✅ Telegram DC reachability — a foreign IP reaches MTProto DCs directly (no MTProxy/SOCKS5 needed for *connectivity*). **But see Telegram-account-safety reversal below — connectivity ≠ safety.**
- ✅ Medium / foreign newsrooms / Google News RSS — directly fetchable from a foreign IP; the ~15 `host: xray` source pins become plain direct fetches.
- ✅ The entire **VLESS/REALITY profile registry + failover ops surface** (`xray_runtime.py`, `xray_health.py`, the reload-trigger dance, the scheduler cron) — this is the single biggest *ops-complexity* win of going abroad, and it matters a lot for a solo operator.

### (2) The REVERSE problems a foreign host introduces (these are the real cost)
- **Inbound reachability for RU customers / Claude / Cursor.** Your product surface is the MCP gateway (:8102 SSE/Streamable-HTTP) consumed by Claude/Cursor, plus admin. "Foreign endpoints are normally reachable from RU" **was** true and is **no longer reliably true in 2026** (see volatile). A foreign customer endpoint now carries real **RKN-blocking risk** — by IP, by SNI, or by being swept up in a Cloudflare/foreign-ASN block. If your *customers* are in RU and your endpoint is abroad, you have moved the censorship tax from *your* outbound (which you control via xray) to *your customer's* inbound (which you do **not** control and cannot fail-over for them).
- **Management-from-RU.** You (the solo operator) administer from RU. Managing a foreign node means *your* admin/SSH/CI path crosses the same degrading border — the same VLESS-style tunnel you were trying to delete, now pointed the other way. Net ops-complexity may not drop as much as hoped.
- **Telegram-account risk from a foreign DC IP — this is a genuine reversal, not a wash.** Telethon/anti-ban guidance (2026) is explicit: prefer **residential** proxies, **≤2-3 accounts per IP**, and that **datacenter IPs** (especially foreign DC ranges) draw **more login challenges, FloodWait, and bans** than residential/local IPs. The shared 1-2 accounts (`HOSTNAME=ingest-0`, `AccountRotator`) are your scarcest, least-replaceable asset. Hosting `ingest` on a foreign DC IP **raises** ban/challenge probability vs the status quo. Connectivity to the DC is *better* abroad; *account survival* is *worse*. For a solo op with 1-2 hand-warmed sessions, an account ban is a multi-day outage with no DR.

### (3) LLM egress — a foreign host reaches RU providers fine (confirmed, no new proxy)
- Wormsoft (`https://ai.wormsoft.ru/api/gpt`), GigaChat (Sber: `gigachat.devices.sberbank.ru`, `ngw.devices.sberbank.ru:9443`), Polza (`polza.ai`) are **RU-facing public HTTPS endpoints**. RU endpoints are **globally reachable** (RKN censors *inbound to RU*, not *outbound from RU to RU services*). A foreign node calls them over plain TLS — **no inbound-to-RU proxy is ever needed**, and no new egress layer is introduced on the LLM axis by going abroad.
- One caveat to carry: `GIGACHAT_VERIFY_SSL_CERTS=False` (confirmed `shared/config.py` L54) — Sber's chain uses the Russian Trusted Root CA, typically unverified to tolerate RU MITM/CA quirks. From a foreign node this still works (you're disabling verification anyway) but it's a latency/RTT change (every LLM call now crosses the border), and embeddings are **GigaChat-only (RU)** — so a foreign-hosted worker pays an RU round-trip on **every embedding**, which is the hot path. This is a real **latency/throughput** cost of foreign compute, not a reachability blocker.

### Where the egress tax actually lands (the core trade)
| Axis | All-RU (status quo) | All-foreign |
|---|---|---|
| Telegram DC connectivity | needs MTProxy/SOCKS5 (xray) | direct ✅ |
| Telegram account *safety* | better (closer to residential/RU) | **worse** (foreign DC IP → more bans/challenges) ❌ |
| Medium / newsrooms / GNews | needs xray SOCKS5 | direct ✅ |
| RU LLM providers (Wormsoft/GigaChat/Polza) | direct ✅ | direct ✅ (RTT cost on embeddings) |
| OpenRouter API reach | direct ✅ | direct ✅ |
| **Inbound: RU customers→endpoint** | direct ✅ (you're in RU) | **RKN-block risk** ❌ |
| Admin/management from RU | local ✅ | crosses border (needs tunnel) ⚠️ |
| 152-FZ primary-DB localization | satisfied ✅ | violated unless de-identified ❌ |

The hypothesis optimizes the **outbound** column and ignores that 2026 RU policy made the **inbound** column the expensive one.

---

## TOPOLOGIES

### A — All-RU + xray (status quo)
- **Egress:** xray VLESS/REALITY for Telegram + blocked HTTP; RU LLMs direct; OpenRouter direct.
- **Pros:** 152-FZ primary DB in RU ✅; inbound from RU customers is local/reliable ✅; Telegram account IPs are RU/closest-to-residential (best account safety of the four) ✅; you already run it.
- **Cons:** carries the full **xray failover ops surface** (registry/health/reload/cron) — a solo maintenance burden; **and the protocol it relies on (VLESS/REALITY) is the one RKN reportedly started blocking in late 2025** (volatile) — so this layer is **degrading and needs active profile churn** to stay alive.
- **Solo-operable:** medium (you already operate it, but xray failover is a recurring fire).

### B — All-foreign, no xray, de-identified (the hypothesis)
- **Egress:** everything direct; xray deleted.
- **Pros:** simplest **outbound** story; deletes the xray ops surface ✅; OpenRouter/Medium/newsrooms trivially reachable.
- **Cons:** (1) **Telegram account safety degrades** (foreign DC IP) — your 1-2 sessions at higher ban risk, no DR ❌; (2) **inbound RKN-block risk** on the customer/MCP endpoint — you can't fail that over for RU customers ❌; (3) requires **provable de-identification of all PDn before storage** to not violate 152-FZ localization — a hard, ongoing classification guarantee, not a config flag ❌; (4) **every embedding/LLM call crosses the border to RU providers** (latency/throughput) ⚠️; (5) admin-from-RU still needs a tunnel ⚠️.
- **Verdict:** **rejected on engineering grounds alone** (Telegram safety + inbound reachability), before the legal question. The thing it deletes (outbound xray) is cheaper than the things it breaks (account safety, customer inbound).

### C — RU-primary-DB + foreign ingest/compute (split by data, not by source)
- **Shape:** Postgres/Qdrant/Neo4j/S3 stay **RU** (152-FZ primary store satisfied). A **foreign compute node** runs the parts that need direct foreign egress (open-foreign-source crawling, OpenRouter calls). Telegram ingest stays **RU** (keep MTProxy/SOCKS5 for it — see below) OR a thin xray only for Telegram.
- **Pros:** keeps the **best Telegram account IP** (RU) ✅; primary DB localized ✅; foreign compute reaches Medium/newsrooms/OpenRouter direct ✅; customer endpoint can stay **RU** (good inbound) ✅.
- **Cons:** you now run **two nodes** and a **secure RU↔foreign link** for the foreign worker to write to the RU DB (that link is itself a cross-border channel that must be tunneled and must not carry raw PDn) ⚠️; **still keeps a Telegram egress helper** (so xray isn't fully deleted, just shrunk to Telegram-only) ⚠️.
- **Solo-operable:** medium — fewer moving parts than D, but the cross-border DB-write link is a real new failure surface.

### D — Two-region: RU node (PDn-bearing + Telegram) + foreign node (open foreign sources & compute) — **RECOMMENDED**
- **Shape:**
  - **RU node** = canonical Postgres/Qdrant/Neo4j/S3 (`ru-central-1`), **Telegram ingest** (best account-safety IP; keep a *minimal* MTProxy/SOCKS5 just for Telegram), GigaChat embeddings (no border RTT on the hot path), the **customer-facing MCP/admin endpoint** (best RU inbound), and the PDn store.
  - **Foreign node** = a stateless fetch/compute worker that handles **only open, non-PDn-sensitive foreign sources** (Medium, foreign newsrooms, Google News RSS) and **OpenRouter/pseudonymized non-RU LLM hops**. It pushes normalized/de-identified payloads back to the RU node via an authenticated, tunneled channel.
- **Pros:** **deletes the general-purpose xray HTTP-source failover surface** (foreign node fetches those directly) — the big ops win the owner wanted ✅; **keeps Telegram on the safe RU IP** ✅; **keeps the customer endpoint and primary DB in RU** (inbound + 152-FZ both clean) ✅; the foreign node is **stateless and disposable** — if RKN blocks its IP it doesn't matter, it's *outbound-only* and re-provisionable ✅; latency-critical embeddings stay RU-local ✅.
- **Cons:** **two nodes to operate** (highest ops surface of the four) ⚠️; needs a hardened RU↔foreign link + a clear PDn boundary (foreign node must never receive raw RU-citizen PDn) ⚠️; foreign node payability/longevity is a vendor risk (volatile).
- **Solo-operable:** medium-low **unless** the foreign node is kept deliberately dumb (one stateless container, IaC-provisioned, no state, auto-redeploys on a fresh IP). If you keep it dumb, the marginal ops cost over A is roughly "one more `deploy-tag.sh --node foreign`."

### Cost sketch (relative, not absolute)
- A: 1 RU node + xray relay rental. Lowest infra $, highest *ops-time* (xray failover).
- B: 1 foreign node. Lowest infra $, **hidden cost = Telegram bans + customer-inbound failures** (not a $ line but the real cost).
- C: 1 RU + 1 foreign + cross-link. ~2× infra.
- D: 1 RU + 1 (small, stateless) foreign. ~1.3-1.5× infra (foreign node is tiny/disposable), lowest *recurring ops-time* once the foreign node is dumb-and-disposable.

---

## VOLATILE FACTS (2026 — verify before acting; these drove the recommendation)

- **[VERIFY — high-impact] RKN reportedly began blocking VLESS in late 2025.** VLESS/REALITY is *exactly* the transport `shared/xray_profile_registry.py` builds. If confirmed for your relay's path, **Topology A's egress is actively degrading** and the profile-registry churn is a treadmill — which *strengthens* the case to move open-foreign egress to a directly-connected foreign node (C/D) and shrink xray to Telegram-only. (Source: zona.media 2026 censorship overview.)
- **[VERIFY — high-impact] Telegram is being throttled/blocked from *inside* RU (Feb-June 2026), with channels losing ~half their audience.** This means a **RU IP is now a *worse* path for Telegram DC *connectivity*** — but Telethon's bot-DCs/MTProto reachability differs from the consumer-app throttling, and **account *safety* still favors RU/residential IPs**. Net: keep Telegram ingest in RU but **keep its MTProxy/SOCKS5 helper** (don't assume bare-RU-IP works for MTProto just because you're in RU). (Sources: Meduza, ipi.media, The Record, Amnesty.)
- **[VERIFY — high-impact] RU is rolling out an IP+SNI *allowlist* ("белый список"), live in ~57 regions, plus Cloudflare/foreign-ASN blocking.** This is the core reason **a foreign customer endpoint (B) is risky**: even a correct SNI is blocked if the server IP isn't allowlisted. **Keep the customer-facing endpoint in RU.** (Sources: iz.ru, reclaimthenet, The Record, OSW "Great Russian Firewall".)
- **[VERIFY] OpenRouter: API reachable from RU; *payments* blocked for RU-region accounts since 2026-05-11** ("does not support billing/payment associated with your geography"). API keeps working on balance; top-up via crypto-USDC (~5% fee) or foreign instrument. This explains the already-configured `OPENROUTER_API_KEY`. **A foreign host does NOT improve OpenRouter API *reach* (already fine from RU); it only changes the *payment/compliance optics*.** Don't justify going abroad on OpenRouter reachability. (Sources: habr, dtf, OpenRouter docs.)
- **[VERIFY] Foreign VPS payability from RU is shrinking.** Hetzner and others have blocked/refused RU customers; many foreign hosts no longer accept RU cards. Viable: RU-owned providers with foreign DCs that bill in RUB/MIR (e.g. RUVDS — DCs in DE/CH/NL/TR/UK/KZ, accepts non-RU cards), or crypto-paid hosts. **This is the practical constraint on C/D's foreign node** — pick a provider that is *payable from RU AND not on RKN's hit-list*. (Sources: russiable, ihc.host, hostadvice.)
- **[VERIFY] Telethon 2026 anti-ban guidance:** residential proxies preferred, ≤2-3 accounts/IP, **datacenter IPs draw more FloodWait/bans**, FloodWait is per-account (rotating accounts/sessions does NOT bypass it — matches `AccountRotator.handle_error` rotating on FloodWait, which only helps spread load, not evade the wait). (Source: docs.telethon.dev, telegramscraper anti-ban guide.)

## Sources
- [zona.media — RU internet censorship 2026 (VLESS block, Telegram, Max)](https://en.zona.media/article/2026/04/07/russian_internet_censorship_2026)
- [Meduza — Russia blocking Telegram for months](https://meduza.io/amp/en/feature/2026/06/11/russia-has-been-blocking-telegram-for-months-meduza-asked-five-popular-channel-admins-if-it-s-working)
- [The Record — Russia throttles Telegram, pushes Max](https://therecord.media/russia-throttles-telegram-pushes-its-own-messaging-app)
- [The Record — Moscow seeks state-approved-sites whitelist](https://therecord.media/moscow-seeks-to-limit-internet-to-state-approved-sites)
- [Izvestia — full "white list" published](https://en.iz.ru/en/2058779/2026-03-13/full-version-white-list-websites-and-internet-services-has-been-published)
- [The Record — Russia blocks Cloudflare-served sites](https://therecord.media/russia-blocks-thousands-of-websites-that-use-cloudflare-service)
- [Habr — OpenRouter stopped accepting payments for RU-region accounts](https://habr.com/ru/news/1034012/)
- [OpenRouter — Sovereign AI / in-region routing docs](https://openrouter.ai/docs/guides/features/sovereign-ai)
- [russiable — foreign VPS payable from RU 2026](https://russiable.com/russian-vps-hosting/)
- [docs.telethon.dev — datacenters / FAQ](https://docs.telethon.dev/en/v2/concepts/datacenters.html)
- [telegramscraper — how to avoid Telegram ban 2026](https://telegramscraper.shop/blog/how-to-avoid-telegram-ban)

**Рекомендация (направление):** Adopt **Topology D (two-region)** as the engineering-best target, assuming the legal question is resolved: keep the **RU node** as the canonical store + Telegram ingest (safest account IP) + GigaChat embeddings (no border RTT on the hot path) + the **customer-facing MCP/admin endpoint** (avoids the 2026 RKN inbound-allowlist risk that kills any foreign endpoint), and add a **small, stateless, disposable foreign node** that directly fetches open/non-PDn foreign sources (Medium, foreign newsrooms, Google News RSS) and runs OpenRouter/pseudonymized hops, pushing de-identified payloads back to RU over a hardened tunnel. This **deletes the general-purpose xray VLESS/REALITY HTTP-source failover surface** (the owner's actual ops pain, and a treadmill now that VLESS is reportedly RKN-targeted) while **avoiding every reverse problem** of pure-foreign hosting: Telegram stays on a safe RU IP, the customer endpoint stays reachable from RU, and the primary DB stays 152-FZ-localized. Keep a **minimal MTProxy/SOCKS5 helper for Telegram only** (do not assume a bare RU IP reaches MTProto given the 2026 throttling). **Reject Topology B (all-foreign)** outright — it trades the cheap outbound-xray problem for the expensive inbound-RKN-block + foreign-DC-Telegram-ban problems. If two-node ops is too heavy for a solo operator at MVP, fall back to **Topology C** (RU DB + RU customer endpoint + RU Telegram, single foreign worker for open sources), and only consider it temporary. Do NOT justify any move abroad on "OpenRouter reachability" — OpenRouter's API is already reachable from RU; only its payment path is constrained, which hosting location does not fix.

**Проверить:**

- VLESS/REALITY block status on YOUR relay's path: test the live xray profiles from the prod RU node now — if VLESS is being TSPU-blocked, Topology A's HTTP egress is already failing and the case for D strengthens. (RKN reportedly blocked VLESS late 2025 — VERIFY for your specific relay/IP.)
- Telegram MTProto reachability from a bare RU IP vs via MTProxy under the 2026 throttling regime — confirm with the operator whether ingest still needs the MTProxy/SOCKS5 helper or whether direct DC connect now works (do NOT assume bare-RU-IP works just because you're in RU; the consumer-app throttling may or may not hit MTProto).
- Foreign-node payability + RKN-survivability: pick a specific provider that is (a) payable from RU (MIR/RUB or crypto — Hetzner and many Western hosts now refuse RU), and (b) whose IP ranges are not RKN-blocked outbound-irrelevant but matter if you ever expose anything inbound. RUVDS-class RU-owned-foreign-DC providers are the pragmatic candidates — confirm current 2026 terms with the vendor.
- Telegram account-IP policy: confirm whether the 1-2 shared sessions can tolerate a foreign DC IP at all, or whether they must stay on the RU/residential-adjacent IP. A single ban is a multi-day, no-DR outage for a solo op — get the operator's risk tolerance in writing before moving ingest.
- Legal/152-FZ boundary for the foreign node (lawyer): the foreign worker in C/D must provably never receive RAW RU-citizen PDn (Telegram bodies/@handles) — confirm with counsel that 'fetch open foreign non-PDn sources + receive only pre-pseudonymized text' keeps the foreign node outside the localization obligation, and that the RU↔foreign link's contents are classified before they leave RU.
- OpenRouter billing continuity: confirm how the existing OPENROUTER_API_KEY is being topped up (crypto-USDC ~5% fee or foreign instrument) and that it survives the 2026-05-11 RU-region payment block long-term — this is a payment/compliance question, NOT solved by hosting location.
- GigaChat embeddings latency from any foreign compute: if you go C/D, measure the added border RTT on the embedding hot path (embeddings are GigaChat-only, 2560d, on every post) and confirm it's acceptable — keep embeddings on the RU node if not.

---

### Приложение C. Хостинг и оплата из РФ (2026)

_Уверенность: **medium**_

## TL;DR

The owner's hypothesis — "pick a foreign host reachable+payable from RU, drop the xray/VPN egress layer" — is **partially viable but cannot be the *primary* persistence tier**, and the naive version (one foreign node holds everything) is the **worst of both worlds**: it likely violates 152-FZ localization AND inherits new, volatile risks (RU inbound throttling, sanctioned-host contamination, payment fragility). The defensible architecture is a **split**: a RU-resident PDn-primary DB (localization-compliant) + an optional foreign egress/compute node that handles only de-identified data and outbound fetching. Whether you even need the foreign node depends on whether you can keep the existing xray layer working — egress simplification is a *convenience* win, not a *compliance* win.

---

## PART A — STABLE STRUCTURE (slow-changing law & architecture)

### A1. 152-FZ localization is about the *primary write*, not all storage
- **ст.18 ч.5 (the "localization" rule, often cited as 18.5):** when **collecting** PDn of RU citizens (incl. via internet), the operator must **record, systematize, accumulate, store, update, extract** using a **database located in the RU**. The **primary collection→storage must land in a RU DB first.** [consultant.ru 18, comply.ru, omjur.ru]
- **Localization ≠ cross-border transfer.** These are two separate regimes. Per Roskomnadzor (24.03.2025) and Mincifry (12.05.2025) clarifications: once PDn has *first* been collected into a RU DB, **ст.18 does not by itself forbid sending a copy abroad** (cross-border transfer is then governed by ст.12, which needs its own notification/grounds). So: **RU-primary + foreign replica/secondary is a recognized pattern.** [comply.ru]
- **Applies regardless of where the company is registered / where it hosts** — the trigger is "targeting RU citizens." Enforcement against purely-foreign operators is real (RKN fined Take-Two and IAB for localization in 2025). A самозанятый/RU-registered operator is squarely in scope. [vc.ru/legal, riverstart.ru]

### A2. De-identified ("обезличенные") data exits the 152-FZ regime
- Properly **обезличенные** data — where you cannot, *without additional information*, attribute it to a subject — **falls outside 152-FZ**, therefore outside localization. The commonly-cited compliant pattern is exactly the owner's: **RU storage for PDn-primary; a foreign DR/compute site may hold only обезличенные/pseudonymized data.** [b-152.ru, wcr-consulting.com]
- **Caveats that make this non-trivial for *this* product:**
  - Roskomnadzor приказ № 140 (19.06.2025, in force from ~01.09.2025; further обезличивание rules land Q4 2026) sets **formal methods/requirements** for обезличивание, and operators may be obliged to feed обезличенные datasets into a state GIS on Mincifry request. So "de-identify" is a **regulated process, not just dropping a column.** [e-office24.ru, sec.ussc.ru]
  - **Pseudonymization (reversible mapping kept in RU) is the realistic engineering target**, not true irreversible anonymization — a trends product needs author identity, entity links, and Neo4j NEL, which are inherently re-identifying. If the foreign node holds @handles, message bodies that name people, or a reversible key, regulators can treat it as PDn-abroad. **VERIFY this column-by-column with a RU privacy lawyer.**

### A3. Is public Telegram channel content even PDn?
- **Do not assume "public = free of 152-FZ."** RU practice: data in open sources (VK, Telegram, etc.) is **NOT automatically "общедоступные ПДн"** — that status requires the subject's *separate written consent for distribution* (ст.10.1, since 01.03.2021). Author @handles + message bodies that identify natural persons can be PDn even though publicly visible. [garant.ru, selectel.ru blog]
- **Mitigant:** much of a *trends* corpus is about organizations/products/topics, not identifiable individuals; channel-level metadata and aggregated trends are low-risk. The risk concentrates in: author handles, bodies quoting/naming people, faces in media. **This is a data-classification exercise, not an all-or-nothing call.**

### A4. The defensible target architecture (stable)
```
RU-resident, 152-FZ-certified tier (PRIMARY of record for any PDn)
  - Postgres (posts incl. author handle / raw bodies / media refs) -> УЗ-1 segment
  - "ingest landing" writes here FIRST (satisfies localization primary-write)
        |  (pseudonymize: strip/または tokenize @handles, names; keep map in RU)
        v
Foreign egress/compute node (OPTIONAL) — only обезличенные/pseudonymized data
  - outbound fetching: Telegram DCs, Medium, Google News RSS, OpenRouter
  - embeddings/LLM orchestration over de-identified text
  - results written back to RU primary
```
- This **keeps localization on the RU side** and uses the foreign node purely as an **egress + compute proxy** — which is exactly what the xray sidecar does today, just relocated into the host's own network position.
- **Key insight:** the foreign node **replaces the xray egress hop, not the database.** It does NOT let you "drop a tier"; it lets you drop the *VPN tunneling* in favor of *native egress from a friendly-jurisdiction IP.*

---

## PART B — VOLATILE FACTS (re-verify at purchase time; these move monthly)

### B1. The payment-from-RU problem (the real bottleneck)
- **Hyperscalers / Western clouds REFUSE RU and are blocked inbound:** Hetzner terminated all RU-postal-address contracts (since late 2023); AWS/GCP/Azure/DO/OVH do not onboard RU cards. Separately, **RKN is now throttling/blocking Hetzner, DigitalOcean, OVH, Cloudflare inbound from RU** under the "grounding/приземление" law — so even reaching their panels/APIs from a RU IP is unreliable. **Both directions are broken for the majors.** [tass, interfax, news.ycombinator, therecord]
- **What actually works = RU-facing resellers of foreign capacity.** Russian providers (4VPS, AdminVPS, VDSina, HOSTKEY, UFO.Hosting, Fornex, etc.) sell VPS in friendly/EU locations (Amsterdam, Frankfurt, Almaty/KZ, Belgrade/RS, Yerevan/AM, Istanbul/TR, UAE, HK) and **bill in RUB via Мир / СБП / YooMoney** — these are reported "accessible without VPN, accept RU payment." [dtf, vc.ru, hostkey.ru]
- **Crypto / alt-payment:** OpenRouter and many foreign hosts accept USDC/USDT/crypto (OpenRouter: ~5% crypto fee, ~5.5% card fee; supports cards/AliPay/USDC; Pay-with-Moon style gateways exist). Crypto is the fallback when no RU rail exists, but adds AML/exchange friction for a самозанятый. [openrouter pricing/faq, paywithmoon]
- **YooKassa is your INBOUND collection rail (customers→you); it is NOT how you pay foreign hosts.** Don't conflate the two. For paying a foreign host you'll use a RU-reseller's RUB invoice, or crypto.

### B2. ⚠️ SANCTIONS/REPUTATION LANDMINE — aeza
- **aeza / aeza.net is OFAC-designated (US Treasury, 01.07.2025) as a bulletproof-hosting provider for ransomware/infostealer crews; follow-on designation of its UK rebrand "Hypercore Ltd." on 19.11.2025.** Despite being a top "RU-payable foreign VPS," **aeza is disqualifying for a legitimate SaaS** — IP-reputation, payment-processor, and OpenRouter/OpenAI account-ban risk, plus it signals "bulletproof neighborhood." **Avoid aeza and any reseller fronting aeza capacity.** [home.treasury.gov sb0185/sb0319, darkreading, trmlabs, thehackernews]
- **Generalize this:** the RU-payable-foreign-VPS market overlaps heavily with abuse/BPH hosts. **Egress reputation matters** — OpenRouter/foreign newsrooms may block "dirty" ranges, recreating the very reachability problem you're trying to solve. Prefer providers with clean IP reputation in KZ/RS/AM/TR/UAE/HK.

### B3. RU-resident 152-FZ clouds (for the PDn-primary tier) — viable today
- **Cloud.ru (already used for S3), Yandex Cloud, Selectel, VK Cloud, MWS, Nubes** all hold ФСТЭК certification up to **УЗ-1** + ФСБ; you place PDn in their **certified segment** and need no separate cert of your own. [selectel.ru/152fz, yandex.cloud/solutions/152-fz, cloudindex.ru]
- **Managed data services exist:** Selectel and Yandex offer **Managed PostgreSQL + Managed Redis** (Selectel also MySQL/Timescale/Kafka); good for solo-operability (no DB babysitting). **Qdrant/Neo4j are generally self-managed VMs** on these clouds — VERIFY current managed-offering status per provider. [selectel docs, yandex.cloud]
- **Pragmatic move:** you already use Cloud.ru S3, so a **Cloud.ru or Yandex/Selectel УЗ-1 Postgres** as the localization tier minimizes new vendor surface.

### B4. Fine exposure (volatile — law text changing through 2026-2027)
- **Localization breach (КоАП ст.13.11 ч.8):** legal entity 30k–6M RUB; **repeat up to 18M RUB.** Since 28.12.2025 (ФЗ-508) these cases moved back to **мировые судьи.** Separately, **data-leak оборотные штрафы up to 3% of revenue / 500M RUB** apply if PDn leaks. [data-sec.ru, consultant 13.11, vitvet.com]
- **NB on legal form:** owner is currently самозанятый with **no legal entity registered**. As an individual operator the КоАП exposure differs from the юрлицо bands above, and самозанятый status fits poorly with "PDn оператор of a multi-tenant SaaS." **VERIFY operator-registration & RKN-notification obligations with a lawyer BEFORE scaling** — this is a bigger near-term risk than host selection.

---

## SELECTION CRITERIA MATRIX (score 1–5; ⚠ = volatile, re-verify)

| Criterion | RU 152-FZ cloud (Cloud.ru / Yandex / Selectel) | Foreign via RU-reseller (KZ/RS/AM/TR — clean IP) | Direct Western cloud (Hetzner/DO/AWS) | aeza & BPH-type RU-payable foreign |
|---|---|---|---|---|
| Open egress incl. Telegram DCs | 2 (needs proxy, like today) | **5** ⚠ | 5 (if you could pay) | 5 |
| RU-payable (Мир/СБП/RUB) | **5** | **4–5** ⚠ | **1** (refuse RU) ⚠ | 4 ⚠ |
| RU-reachable inbound (panel/API/your API for customers) | **5** | 4 ⚠ | **1** (RKN throttling) ⚠ | 3 ⚠ |
| 152-FZ localization region available | **5** (УЗ-1 certified) | **1** (foreign = not localization tier) | 1 | 1 |
| GPU availability | 3 ⚠ | 3 ⚠ | 5 | 3 |
| Managed Postgres/Redis | **4** (Selectel/Yandex) ⚠ | 1–2 (self-manage) | 4 | 1 |
| Price | 3 | **4** ⚠ | 4 | 5 (cheap, toxic) |
| Solo-operability | **4** (managed) | 3 | 2 (can't pay/reach) | 2 |
| Sanctions / reputation safety | **5** | 4 (IF clean IP, not aeza-fronted) ⚠ | 5 | **0** (OFAC-listed) |
| **Role in target arch** | **PDn-PRIMARY (localization tier)** | **Egress/compute over de-identified data only** | **Excluded** | **Excluded** |

### Shortlist (with caveats — do NOT single-vendor commit)
1. **PDn-primary / localization tier → Cloud.ru *or* Yandex Cloud *or* Selectel, УЗ-1 segment, Managed Postgres.** Prefer Cloud.ru (already your S3 vendor) to limit vendor sprawl; Selectel if you want the broadest managed-DB menu. ⚠ Verify each still offers УЗ-1 + the specific managed services you need.
2. **Egress/compute tier (optional) → a clean-reputation VPS in a friendly jurisdiction, bought via a RU-reseller billing in RUB** (Almaty/KZ or Belgrade/RS favored for latency+tolerance). This node does outbound Telegram/Medium/OpenRouter and processes ONLY pseudonymized text. ⚠ Vet the IP range reputation; **explicitly exclude aeza/BPH-linked stock.**
3. **Keep the xray/MTProxy layer as a fallback,** at least through migration. The foreign egress node and the xray sidecar are *substitutes for the same job*; don't tear out xray until the foreign node is proven on Telegram + OpenRouter + blocked newsrooms.
4. **LLM/payments:** OpenRouter already works (key configured) — fund it via crypto/USDC or a working RU rail; GigaChat/Wormsoft/Polza stay RU-native. Customer billing via YooKassa is unaffected by host choice.

### What the hypothesis gets RIGHT vs WRONG
- **RIGHT:** a foreign node in a friendly jurisdiction can reach Telegram/Medium/OpenRouter natively and can *functionally* replace the xray *egress* hop. Egress *simplification* is achievable.
- **WRONG / dangerous:** "drop the egress layer entirely by moving everything abroad." Moving the **DB** abroad (a) breaks 152-FZ localization for any RU-citizen PDn in the corpus, and (b) trades a known, controllable risk (your own xray) for **uncontrollable** ones (RKN inbound blocking of foreign ranges, sanctioned-host contamination, payment cutoff). The egress layer's *complexity* moves; it does not *disappear* — and the localization tier must stay in RU.


**Рекомендация (направление):** Do NOT adopt the "foreign-only, drop egress layer" plan. Adopt a split: keep a RU-resident, 152-FZ/УЗ-1 certified PDn-primary database (Cloud.ru — already your S3 vendor — or Yandex/Selectel with Managed Postgres) so the primary write of any RU-citizen personal data lands in RU first (ст.18 ч.5). Optionally add ONE clean-reputation foreign VPS in a friendly jurisdiction (KZ/RS/AM), bought via a RU-reseller billing in RUB, that acts ONLY as an egress+compute proxy over de-identified/pseudonymized data (Telegram, Medium, OpenRouter) — this can replace the xray *egress hop*, but NOT the database tier and NOT the xray fallback until proven. Hard-exclude aeza and any bulletproof/OFAC-linked stock. Treat every vendor/sanctions/payment fact here as expiring: re-verify host reachability, RU-payment rail, and IP reputation at purchase time, and do a column-by-column PDn classification with a RU privacy lawyer before deciding what may leave RU.

**Проверить:**

- RU privacy lawyer: column-by-column PDn classification of the Telegram corpus (author @handles, bodies naming individuals, faces in media) — confirm what is PDn and whether pseudonymization (reversible map kept in RU) is sufficient to send a copy abroad, or whether ст.12 cross-border-transfer notification is required.
- Lawyer/accountant: operator legal form — самозанятый/НПД has no legal entity registered; confirm RKN оператор-ПДн registration/notification obligations and КоАП exposure bands BEFORE scaling a multi-tenant SaaS; самозанятый may be an inadequate vehicle for a PDn operator.
- Compliance status of Roskomnadzor приказ № 140 (19.06.2025) обезличивание methods and the Q4-2026 обезличенные-data rules incl. any obligation to feed datasets into the Mincifry state GIS — confirm the de-identification process you implement is formally compliant, not ad-hoc.
- At purchase time: re-verify each candidate RU cloud (Cloud.ru/Yandex/Selectel/VK) still holds ФСТЭК УЗ-1 certification AND offers the specific managed services (Postgres, Redis; Qdrant/Neo4j likely self-managed) you need.
- At purchase time: verify the chosen friendly-jurisdiction VPS reseller's actual IP-range reputation (not aeza/BPH-fronted), that it is reachable inbound from RU, that its egress is NOT blocked by OpenRouter/Telegram/target newsrooms, and that the RUB/Мир/СБП payment rail currently works (these flip frequently).
- OpenRouter funding path from RU in 2026: confirm the current working method (USDC/crypto vs any RU card rail), fees, and that the account/key is not at risk from a 'dirty' egress IP.
- Current КоАП ст.13.11 ч.8 localization-fine amounts and procedure after ФЗ-508 (28.12.2025) moved cases to мировые судьи — and current оборотные-штраф (up to 3% / 500M RUB) leak-fine wording, as these are still changing through 2026-2027.

---

