# RSI — рекурсивное самоулучшение детектора сигналов

Контур, который улучшает **сам детектор**, а не только знание. Все изменения детектора
проходят через **человека-гейта** — рекурсия разорвана оператором. Реализовано как Фаза 0
(сбор субстрата) + четыре контура (A/B/C/D) + связка B→A.

Философия: петли **предлагают**, человек **решает**. Ни одно изменение порога или слияние
графа не применяется автоматически — только после approve через MCP.

---

## Фаза 0 — субстрат

Append-only логи, на которых учатся петли. Поведение пайплайна не меняют.

| Таблица | Что | Пишется |
|---|---|---|
| `weak_signal_snapshots` | снимок каждого weak-кандидата на прогоне кластеризации (score/confidence/burst/diversity + значения порогов + threshold_version), включая **подавленные** weak-гейтом | `worker/services/semantic_clustering.py` (хук в `_signal_results`) |
| `relevance_decisions` | лог reject'ов Relevance Filter (score, reasoning, model, excerpt) | `worker/tasks/enrichment_task.py` (`_log_relevance_decision`) |

`judge_verdict` (JSONB) на снимке заполняет Контур B.

---

## Контур A — ретроспективная петля

`worker/services/retrospective.py`. Крон `40 4... `→ см. таблицу кронов.

1. Берёт зрелые снимки (≥30 дней) **и** судённые-но-незрелые (ранний сигнал, B→A).
2. Размечает исход: `vindicated` (вырос в trend/emerging/stable, поднял burst/score, **или** уверенный вердикт судьи) vs `faded`.
3. Оптимизатор `propose_threshold`: порог-кандидат = низкий перцентиль значений у оправдавшихся (удерживает ~90%). **Гайард net-пользы**: предлагает только если `vindicated_recovered + faded_cut_delta > 0` (без no-op).
4. Пишет в `threshold_proposals` (pending). Устаревшие pending авто-`superseded`.
5. **MCP-гейт**: `approve_threshold_change` пишет новое значение в `workspaces.extra->cluster_analysis` (для weak-гейтов) или `relevance_weights->threshold` (для порога релевантности) — читается на следующем прогоне, **без редеплоя**. Новые снимки получают новый `threshold_version` → петля меряет, помогло ли.

Approvable пороги: `weak_signal_min_score/confidence/source_diversity/source_count`, `relevance_threshold`.

---

## Контур B — кросс-семейный novelty-judge

`worker/chains/novelty_judge_chain.py`, `worker/services/novelty_judge.py`.

Primary-экстракция идёт на Gemma+GigaChat — у связки системная слепота к тому, что вне её
распределения, а слабые сигналы живут именно там. Судья **другой семьи** (DeepSeek-v4-pro
через wormsoft → polza fallback) независимо судит малый набор weak-снимков. Где детектор
сказал weak, а судья — high novelty (`underrated`), это пойманная слепота.

ВАЖНО: DeepSeek-v4-pro — reasoning-модель, нужен `max_tokens ≥ 1500` (иначе reasoning съедает
бюджет → пустой контент). Вердикт пишется в `weak_signal_snapshots.judge_verdict`. Метрика
`frontier_novelty_judge_total{verdict}`. MCP `list_underrated_signals`.

### B→A — судья кормит петлю
Уверенный `underrated` (out_of_distribution + novelty ≥ 0.7) засчитывается петлёй как
`vindicated` **до** роста burst → петля предлагает подстройку на недели раньше. Рационале
честно помечает, сколько лейблов от предсказания судьи, а не от факта.

---

## Контур C — аудит тихих false-negative

`worker/services/relevance_audit.py`. Relevance Filter режет на входе молча. Здесь:
- метрики отклонений и подтверждённых false-negative → гейджи `frontier_relevance_audit`;
- MCP `list_relevance_audit_sample` (топ по score = ближе к порогу = вероятнее ошибка) + `mark_relevance_audit` (вердикт человека);
- при достаточной доле подтверждённых FN — авто-предложение **понизить порог релевантности** через тот же гейт (`threshold_proposals`, key=`relevance_threshold`).

---

## Контур D — здоровье графа + entity-resolution

`worker/services/graph_maintenance.py`, `worker/integrations/neo4j_client.py`.

NEL копит дубли сущностей, GraphRAG деградирует молча. Решение в два слоя:

**Строковая нормализация (дубли по форме):** `upsert_concepts` теперь MERGE по `norm =
apoc.text.clean(name)` (ловит регистр/пробелы/пунктуацию). **UNIQUE-constraint `(norm,
workspace_id)` ОБЯЗАТЕЛЕН** — иначе параллельные транзакции создают дубли (MERGE race).
Канонический узел хранит display-имя + `aliases`.

**Метрики и слияние накопленного:** крон считает `frontier_graph_health` (concept_count,
orphan, duplicate_clusters, edge_density) — только метрики (безопасно). Слияние накопленных
дублей — операторский job `run_graph_resolution` (apply=True, мутация графа). MCP
`get_graph_health` для инспекции.

### Контур D+ — семантический entity-resolution
`worker/services/entity_resolution.py`, `worker/chains/entity_equivalence_chain.py`. Строковая
нормализация не ловит `HMI ↔ Human-Machine Interface`. Конвейер:
1. **Кандидаты по акронимам** (acronym_of: первые буквы значимых слов).
2. **Фильтр со-встречаемости** (RELATED_TO ≥ 2) — режет шум (95k → ~180), точность резко вверх.
3. **LLM-судья другой семьи** (DeepSeek) подтверждает эквивалентность (отсекает `ИИ↔ИИ-инструменты`).
4. **Дедуп по акрониму** — одно предложение на акроним (LLM/ИИ/MCP), без перекрытий.
5. → `entity_merge_proposals` (pending) → MCP `approve_entity_merge` сливает узлы графа.

Не ловит синонимы без акронима (`autonomous vehicle ↔ self-driving car`) — это embedding-сигнал (будущий шаг).

---

## Ночные кроны (admin scheduler, UTC)

| Крон | Джоба | Контур |
|---|---|---|
| `35 3` | semantic clustering | пайплайн (пишет снимки Фазы 0) |
| `20 */8` | signal analysis | пайплайн |
| `10 4` | retrospective review | A |
| `40 4` | novelty judge | B |
| `0 5` | relevance audit | C |
| `20 5` | graph maintenance (метрики, без слияния) | D |
| `40 5` | entity resolution | D+ |

Слияния графа (`run_graph_resolution`) — только вручную оператором.

---

## MCP-инструменты (гейты)

- A/C: `list_threshold_proposals`, `approve_threshold_change`, `reject_threshold_change`
- B: `list_underrated_signals`
- C: `list_relevance_audit_sample`, `mark_relevance_audit`
- D: `get_graph_health`
- D+: `list_entity_merge_proposals`, `approve_entity_merge`, `reject_entity_merge`

## Наблюдаемость

Метрики: `frontier_graph_health`, `frontier_relevance_audit`, `frontier_novelty_judge_total`.
Дашборд `grafana/dashboards/frontier-rsi.json`. Алерты (`prometheus/alerts.yml`, группа
`frontier_rsi`): `FrontierAdminDown` (critical — admin держит весь планировщик),
`FrontierGraphDuplicateClustersRising`, `FrontierNoveltyJudgeFailing`.

## Эксплуатация

- **Деплой кода = пересборка образа** (`server-build-stack.sh`), не rsync+restart — код запечён в образ. admin-образ обязан иметь все deps worker.services.* (включая neo4j).
- **Модели для судьи** — через wormsoft/polza (DeepSeek), НЕ GigaChat (смысл в другой семье).
- Подробности и грабли — `docs/ops-server-troubleshooting.md`, память проекта.
