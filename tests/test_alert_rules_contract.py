"""
Контракт правил алертов, не зависящий от партиции по workspace.

Соседний `test_alerts_workspace_coverage.py` стережёт полноту покрытия воркспейсов.
Этот файл — про другой, более широкий класс: **правило загружено, валидно и не может
сработать никогда**. За 2026-08-04 он проявился трижды в разных обличьях:

  * `FrontierS3QuotaCritical` сравнивал два вектора с разным набором меток —
    пар не находилось, выражение молча давало пустоту при истинном условии;
  * `FrontierGraphDuplicateClustersRising` и `FrontierNoveltyJudgeFailing` висят
    на метриках, которые объявлены, но не отдают ни одного сэмпла: значения
    выставляются в дочернем процессе планировщика с собственным реестром;
  * опечатка в имени метрики оставляет выражение синтаксически целым и вечно пустым.

Общий корень один: **имя метрики в `expr` ничем не связано с тем, кто эту метрику
производит.** Поэтому главная проверка здесь — сквозная: каждое имя, на которое
опирается правило, обязано производиться либо `shared/metrics.py`, либо
textfile-экспортёром в `scripts/*.sh`, либо быть явно объявлено внешним.

Плюс зеркальный класс, найденный тем же днём: **правило, которое срабатывает
каждый день на штатном поведении.** `FrontierOpenRouterModelQuarantineBurst` был
firing 6 суток из 7 на суточном сбросе квоты бесплатного тарифа OpenRouter, при
нулевом вреде за то же окно. Алерт, который кричит на норме, обучает не смотреть
на алерты — то есть даёт ровно тот же результат, что молчащий, только дороже.
Против возврата — `test_no_rule_pages_on_bare_openrouter_quarantine`.

Разбор статический: yaml для правил, ast для экспортёра, регулярки для shell.
Ни импорта проекта, ни сети.
"""

from __future__ import annotations

import ast
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]
ALERTS_YML = REPO_ROOT / "prometheus" / "alerts.yml"
METRICS_MODULE = REPO_ROOT / "shared" / "metrics.py"
SCRIPTS_DIR = REPO_ROOT / "scripts"
FRESHNESS_EXPORTER = SCRIPTS_DIR / "export-analysis-freshness.sh"

_METRIC_FACTORIES: frozenset[str] = frozenset(
    {"Counter", "Gauge", "Histogram", "Summary", "Info", "Enum"}
)

# Функции и ключевые слова PromQL: не метрики, хотя выглядят как идентификаторы.
_PROMQL_KEYWORDS: frozenset[str] = frozenset(
    {
        "sum", "rate", "increase", "avg", "max", "min", "count", "time", "absent",
        "absent_over_time", "vector", "scalar", "by", "without", "on", "ignoring",
        "group_left", "group_right", "changes", "delta", "idelta", "irate", "resets",
        "histogram_quantile", "clamp_max", "clamp_min", "topk", "bottomk", "quantile",
        "count_over_time", "max_over_time", "min_over_time", "avg_over_time",
        "sum_over_time", "last_over_time", "present_over_time", "stddev_over_time",
        "predict_linear", "deriv", "abs", "ceil", "floor", "round", "sqrt", "exp", "ln",
        "and", "or", "unless", "offset", "label_replace", "label_join", "stddev",
        "stdvar", "bool", "atan2", "sgn", "timestamp", "day_of_week", "hour",
    }
)

# Метрики, которые производит НЕ этот репозиторий. Список явный и короткий
# намеренно: он же — реестр внешних зависимостей наблюдаемости. Появление новой
# строки здесь обязано быть осознанным, иначе опечатка в имени собственной метрики
# «оправдается» тем, что она якобы внешняя.
EXTERNAL_METRICS: dict[str, str] = {
    "up": "Prometheus, состояние scrape-таргета",
    "node_filesystem_avail_bytes": "node-exporter",
    "node_filesystem_size_bytes": "node-exporter",
}

# Имена меток, которые встречаются в `by (...)` / `on (...)` и не являются метриками.
# Таблицы, тишина в которых ожидаема, а не сломана: trend_clusters упирается в
# структурные ворота у низкопоточных воркспейсов (развилка 29), card_feedback пуста,
# потому что редакторская петля не запускалась ни разу. Будящие правила свежести
# обязаны их исключать — иначе висят firing неделями на известном состоянии.
DEFERRED_FRESHNESS_TABLES: frozenset[str] = frozenset({"trend_clusters", "card_feedback"})

_GROUPING_CLAUSE = re.compile(
    r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\(([^()]*)\)"
)
_LABEL_MATCHER_BLOCK = re.compile(r"\{[^{}]*\}")
_IDENTIFIER = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")
_DURATION_SUFFIX = re.compile(r"^\d+[smhdwy]$")


@lru_cache(maxsize=1)
def _alerts_document() -> dict[str, Any]:
    loaded = yaml.safe_load(ALERTS_YML.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


@lru_cache(maxsize=1)
def _rules() -> tuple[dict[str, Any], ...]:
    found: list[dict[str, Any]] = []
    for group in _alerts_document().get("groups") or []:
        if not isinstance(group, dict):
            continue
        for raw in group.get("rules") or []:
            if isinstance(raw, dict):
                found.append({**raw, "_group": group.get("name")})
    return tuple(found)


def _rule(name: str) -> dict[str, Any] | None:
    for rule in _rules():
        if rule.get("alert") == name:
            return rule
    return None


def _referenced_metrics(expr: str) -> frozenset[str]:
    """Имена серий, на которые опирается выражение.

    Из текста последовательно вычищается всё, что идентификатором не является,
    но им выглядит: содержимое `{...}` (имена и значения меток), списки меток
    в `by (...)`/`on (...)`, литералы длительности.
    """
    text = _LABEL_MATCHER_BLOCK.sub(" ", expr)
    text = _GROUPING_CLAUSE.sub(" ", text)
    names: set[str] = set()
    for candidate in _IDENTIFIER.findall(text):
        if candidate in _PROMQL_KEYWORDS or _DURATION_SUFFIX.match(candidate):
            continue
        names.add(candidate)
    return frozenset(names)


@lru_cache(maxsize=1)
def _declared_in_exporter() -> frozenset[str]:
    """Имена, объявленные в shared/metrics.py.

    Разбор ast'ом, а не импортом: prometheus_client в тест-образе может
    отсутствовать, а импорт зарегистрировал бы все метрики в глобальном реестре.
    Для Counter добавляется и суффикс `_total`: prometheus_client дописывает его
    сам, и в `expr` метрика встречается именно так.
    """
    tree = ast.parse(METRICS_MODULE.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        factory = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if factory not in _METRIC_FACTORIES:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            names.add(first.value)
            if factory == "Counter":
                names.add(f"{first.value}_total")
    return frozenset(names)


@lru_cache(maxsize=1)
def _emitted_by_shell_exporters() -> frozenset[str]:
    """Имена, которые печатают textfile-экспортёры в scripts/*.sh.

    Это второй легитимный производитель серий: node-exporter отдаёт содержимое
    prometheus/textfile как обычные метрики. Не учитывать его — значит требовать
    объявления в Python того, что Python никогда не увидит.
    """
    names: set[str] = set()
    for path in SCRIPTS_DIR.glob("*.sh"):
        text = path.read_text(encoding="utf-8", errors="replace")
        names.update(re.findall(r"\b(frontier_[a-z0-9_]+)", text))
    return frozenset(names)


# ── Самопроверка извлекателей ────────────────────────────────────────────────


def test_extraction_is_not_vacuous() -> None:
    assert len(_rules()) >= 40, f"alerts.yml parsed into {len(_rules())} rules"
    assert len(_declared_in_exporter()) >= 20, "ast walk over shared/metrics.py is broken"
    assert len(_emitted_by_shell_exporters()) >= 10, "shell exporter scan is broken"
    sample = _referenced_metrics('sum by (service, reason) (increase(frontier_x_total[15m])) >= 5')
    assert sample == {"frontier_x_total"}, (
        f"the expression parser is broken: got {sorted(sample)} instead of "
        "{'frontier_x_total'} — label names or functions are leaking through, and "
        "the coverage test below would be meaningless"
    )


# ── Главный инвариант: у каждой метрики правила есть производитель ───────────


def test_every_alert_metric_has_a_producer() -> None:
    """
    Имя в `expr` ничем не связано с тем, кто метрику публикует, — поэтому связь
    проверяется здесь.

    Ловит: опечатку в имени (выражение остаётся валидным и вечно пустым);
    переименование метрики в экспортёре без правки правила; правило, написанное
    под метрику, которую только собирались добавить. Во всех трёх случаях алерт
    выглядит рабочим и не может сработать никогда.
    """
    producers = _declared_in_exporter() | _emitted_by_shell_exporters() | set(EXTERNAL_METRICS)
    problems: list[str] = []
    for rule in _rules():
        expr = str(rule.get("expr") or "")
        for metric in sorted(_referenced_metrics(expr) - producers):
            problems.append(f"{rule.get('alert')}: no producer for series {metric!r}")
    assert not problems, "\n  ".join(
        [
            "alert rules depend on series that nothing publishes "
            "(the expression stays valid and selects nothing — the alert can never fire):",
            *problems,
            "",
            "Declare the metric in shared/metrics.py, emit it from a scripts/*.sh "
            "textfile exporter, or — if it genuinely comes from outside this repo — "
            "add it to EXTERNAL_METRICS with a source.",
        ]
    )


def test_wormsoft_soft_cap_burst_is_non_paging_diagnostic() -> None:
    """A bare local soft-cap burst must not be presented as an outage page."""
    rule = _rule("FrontierWormsoftLocalThrottleBurst")
    labels = rule.get("labels") or {}

    assert labels.get("severity") == "info"
    assert labels.get("notify") == "never"


def test_wormsoft_throttle_outage_mismatch_uses_real_reasons_and_zero_fallback() -> None:
    """The mismatch remains evaluable when no outage fallback series exists."""
    rule = _rule("FrontierLocalThrottleVsOutageMismatch")
    expr = str(rule.get("expr") or "")

    assert ".*_(timeout|connection|5xx|unavailable)" in expr

    credit_reason_exclusion = (
        'reason!~"wormsoft_credit_soft_cap|wormsoft_credit_hard_cap|'
        'wormsoft_credit_read_failed|wormsoft_credit_pricing_unknown"'
    )
    assert expr.count(credit_reason_exclusion) == 2

    assert re.search(r"\bor\s+on\s*\(\s*service\s*\)", expr)
    assert re.search(
        r"0\s*\*\s*sum\s+by\s*\(\s*service\s*\).*"
        r"frontier_llm_throttle_events_total",
        expr,
        flags=re.DOTALL,
    )

    accounting_failure = _rule("FrontierWormsoftCreditAccountingFailure")
    assert "frontier_wormsoft_credit_accounting_errors_total" in str(
        accounting_failure.get("expr") or ""
    )
    assert (accounting_failure.get("labels") or {}).get("severity") == "warning"


def test_wormsoft_credit_utilization_alerts_use_fresh_account_window() -> None:
    """Credit pages use the shared account window and reject stale process gauges."""
    high = _rule("FrontierWormsoftCreditUtilizationHigh")
    critical = _rule("FrontierWormsoftCreditUtilizationCritical")

    for rule in (high, critical):
        expr = str(rule.get("expr") or "")
        assert "frontier_wormsoft_credit_utilization_ratio" in expr
        assert "frontier_wormsoft_credit_window_refresh_timestamp_seconds" in expr
        assert "execution_role" not in expr
        assert "frontier_llm_throttle_events_total" not in expr

    assert (high.get("labels") or {}).get("severity") == "warning"
    assert (critical.get("labels") or {}).get("severity") == "critical"
    assert "frontier_wormsoft_credit_soft_cap_ratio" in str(high.get("expr") or "")
    assert "frontier_wormsoft_credit_hard_cap_ratio" in str(high.get("expr") or "")
    assert "frontier_wormsoft_credit_hard_cap_ratio" in str(
        critical.get("expr") or ""
    )


def test_external_metrics_registry_is_used() -> None:
    """Мёртвая строка в реестре внешних метрик разрешает опечатку в своей."""
    referenced: set[str] = set()
    for rule in _rules():
        referenced |= _referenced_metrics(str(rule.get("expr") or ""))
    stale = sorted(set(EXTERNAL_METRICS) - referenced)
    assert not stale, (
        f"EXTERNAL_METRICS lists series no rule references any more: {stale}. "
        "Remove them — otherwise a typo in one of our own metric names could be "
        "'justified' by a stale allowlist entry."
    )


# ── Зеркальный класс: правило, кричащее на норме ─────────────────────────────


def test_no_rule_pages_on_bare_openrouter_quarantine() -> None:
    """
    Регрессия пункта 56 реестра.

    `FrontierOpenRouterModelQuarantineBurst` стрелял на ФАКТ карантина
    (`sum(frontier_openrouter_model_quarantine) >= 3`). Измерение за 7 суток:
    14 из 16 бесплатных моделей уходят в карантин КАЖДЫЕ сутки с
    `in_quarantine_until`, равным ближайшей полуночи UTC, и пустым `last_error` —
    это исчерпание суточной квоты, которое снимается само. Правило было firing
    6 суток из 7 по 3-11 часов, а picker за то же окно не пропустил ни одного
    запроса по причине карантина.

    Алерт, который будит каждый день на норме, обучает не смотреть на алерты —
    результат тот же, что у молчащего. Поэтому карантин разрешено использовать
    только как УТОЧНЕНИЕ к признаку реального вреда (пропуски picker'а),
    а не как самостоятельное условие побудки.
    """
    problems: list[str] = []
    for rule in _rules():
        expr = str(rule.get("expr") or "")
        if "frontier_openrouter_model_quarantine" not in expr:
            continue
        labels = rule.get("labels") if isinstance(rule.get("labels"), dict) else {}
        wakes_somebody = bool(labels.get("notify")) or labels.get("severity") in {
            "warning",
            "critical",
        }
        if wakes_somebody and "frontier_openrouter_picker_skip_total" not in expr:
            problems.append(
                f"{rule.get('alert')}: pages on the quarantine gauge alone. Quarantine "
                "of the free pool happens nightly by design (daily quota reset) and "
                "caused zero picker skips over the measured week."
            )
    assert not problems, "\n  ".join(
        ["alert rules that would fire nightly on expected behaviour:", *problems]
    )


# ── Наблюдаемость самих петель ───────────────────────────────────────────────


def test_alert_triage_liveness_rule_exists_and_survives_a_missing_series() -> None:
    """
    Пункт 18: петля разбора алертов умерла два дня подряд и не сообщила об этом ничем.

    Правило обязано (а) существовать, (б) содержать `absent(...)`. Без второго оно
    молчит именно в худшем случае: если экспортёр не написал серию вообще,
    `time() - <нет серии>` даёт пустой вектор, и алерт зелен ровно тогда, когда
    всё сломано. Тот же дефект уже ловили у метрик свежести аналитики.
    """
    rule = _rule("FrontierAlertTriageStale")
    assert rule is not None, (
        "FrontierAlertTriageStale is missing. The daily alert-triage loop has no other "
        "sign of life: on 2026-08-03 and 2026-08-04 it died twice in a row "
        "(0xC000013A) and nothing reported it."
    )
    expr = str(rule.get("expr") or "")
    assert "frontier_alert_triage_last_digest_timestamp_seconds" in expr
    assert "absent(" in expr, (
        "FrontierAlertTriageStale must handle a missing series with absent(): without "
        "it the rule is green precisely when the exporter stopped writing."
    )


def test_alert_triage_threshold_exceeds_a_full_day() -> None:
    """
    Порог обязан быть больше суток, иначе правило стреляет каждую ночь по построению.

    Петля ходит раз в сутки в 09:15, поэтому в здоровом режиме возраст последнего
    дайджеста доходит почти до 24 часов перед очередным прогоном. Порог 24ч дал бы
    ежедневное ложное срабатывание — ровно ту болезнь, от которой этим же заходом
    лечили карантинный алерт OpenRouter.
    """
    rule = _rule("FrontierAlertTriageStale")
    assert rule is not None
    thresholds = [int(value) for value in re.findall(r">\s*(\d+)", str(rule.get("expr") or ""))]
    assert thresholds, "no numeric threshold found in FrontierAlertTriageStale"
    assert min(thresholds) > 86400, (
        f"FrontierAlertTriageStale fires at {min(thresholds)}s. The loop runs once a day, "
        "so anything at or below 86400s false-alarms every night before the next run."
    )


# ── Свежесть аналитики: селектор по `table` против экспортёра ────────────────


@lru_cache(maxsize=1)
def _exporter_tables() -> frozenset[str]:
    """Таблицы, по которым экспортёр реально печатает серии."""
    text = FRESHNESS_EXPORTER.read_text(encoding="utf-8", errors="replace")
    block = re.search(r"tables AS \(\s*SELECT \* FROM \(VALUES(.*?)\)\s*AS t\(tbl\)", text, re.S)
    assert block, (
        "cannot locate the table registry inside scripts/export-analysis-freshness.sh — "
        "the check below would pass vacuously"
    )
    return frozenset(re.findall(r"'([a-z_]+)'", block.group(1)))


def test_freshness_rules_only_select_tables_the_exporter_emits() -> None:
    """
    Селектор `table=~"..."` в правилах свежести обязан называть только те таблицы,
    которые экспортёр действительно печатает.

    Опечатка или таблица, выброшенная из экспортёра, оставляют правило валидным
    и вечно пустым: `max by (workspace, table)` по несуществующей серии не считается.
    """
    assert len(_exporter_tables()) >= 4, (
        f"the exporter declares {sorted(_exporter_tables())} — extractor broken"
    )
    problems: list[str] = []
    for rule in _rules():
        expr = str(rule.get("expr") or "")
        if "frontier_analysis_last_update_timestamp_seconds" not in expr:
            continue
        for pattern in re.findall(r'table\s*=~?\s*"([^"]*)"', expr):
            for table in (part.strip() for part in pattern.split("|") if part.strip()):
                if table not in _exporter_tables():
                    problems.append(
                        f"{rule.get('alert')}: selects table {table!r}, which "
                        f"scripts/export-analysis-freshness.sh never emits "
                        f"(it emits {sorted(_exporter_tables())})"
                    )
    assert not problems, "\n  ".join(
        ["freshness rules select tables that produce no series:", *problems]
    )


def test_trend_cluster_silence_is_observed_but_does_not_page() -> None:
    """
    Тишина в trend_clusters — не отказ, а следствие структурных ворот при 94-97%
    синглтонов у низкопоточных воркспейсов (развилка 29 в AUDIT-2026-08-04).

    Наблюдать её надо: у `design` последний стабильный тренд датирован 14.07.2026,
    и метрика свежести всё это время показывала 2-3 часа, потому что считалась по
    другим таблицам. Но будить по ней нельзя, пока владелец не решил, снижать ли
    минимумы: постоянно firing правило с notify — та же болезнь, что у карантинного
    алерта OpenRouter, только с другого конца.
    """
    rule = _rule("FrontierTrendClustersStale")
    assert rule is not None, (
        "FrontierTrendClustersStale is missing: trend silence would again be invisible, "
        "which is exactly how 21 days without a single trend in `design` went unnoticed."
    )
    labels = rule.get("labels") if isinstance(rule.get("labels"), dict) else {}
    assert labels.get("severity") == "info", (
        f"severity={labels.get('severity')!r}: trend silence is a pending decision, "
        "not a failure. Promote it only together with decision 29."
    )
    # `notify: never` — не то же самое, что отсутствие метки `notify`, и первая
    # редакция этого правила ошиблась именно здесь. В alertmanager.yml в blackhole
    # уходят ровно два маршрута (alertname="FrontierWatchdog" и notify="never"),
    # а всё прочее подхватывает catch-all `telegram-admin`. Правило без метки
    # доставлялось в Telegram и повторялось каждые 6 часов; поймано
    # `amtool config routes test` на рабочем конфиге, а не чтением YAML.
    assert labels.get("notify") == "never", (
        f"notify={labels.get('notify')!r}. Omitting the label does NOT keep the alert "
        "quiet: alertmanager routes everything that is not notify=\"never\" (or the "
        "watchdog) to the telegram-admin catch-all, so this would sit in Telegram "
        "repeating every 6h about a known, deferred condition."
    )

    # Требование ПОЛОЖИТЕЛЬНОЕ, а не «в тексте нет слова trend_clusters».
    # Мутационный прогон 2026-08-04: если селектор `table=~"..."` просто убрать,
    # правило начинает выбирать ВСЕ таблицы, включая trend_clusters, — а проверка
    # на отсутствие подстроки при этом зеленеет, потому что подстроки и правда нет.
    # Поэтому селектор обязан присутствовать и обязан быть перечислением.
    for paging in ("FrontierAnalysisStale", "FrontierAnalysisStaleCritical"):
        paging_rule = _rule(paging)
        assert paging_rule is not None, f"{paging} disappeared from alerts.yml"
        expr = str(paging_rule.get("expr") or "")
        selectors = re.findall(r'table\s*=~\s*"([^"]*)"', expr)
        assert selectors, (
            f"{paging} does not restrict the `table` label at all, so it selects every "
            "table the exporter emits — including trend_clusters, where silence is a "
            "known deferred condition (decision 29), and card_feedback, where zero rows "
            "are expected. It would sit permanently firing in Telegram."
        )
        selected = {
            table.strip()
            for pattern in selectors
            for table in pattern.split("|")
            if table.strip()
        }
        overlap = sorted(selected & DEFERRED_FRESHNESS_TABLES)
        assert not overlap, (
            f"{paging} pages on {overlap}, whose silence is expected rather than broken. "
            "Observe them with FrontierTrendClustersStale (info, no notify) instead."
        )


def test_no_rule_groups_own_metrics_by_the_reserved_job_label() -> None:
    """`job` принадлежит Prometheus, а не приложению.

    Метка `job` проставляется скрейпом; одноимённая метка приложения при
    honor_labels=false молча уезжает в `exported_job`. Значит выражение
    `sum by (job) (frontier_...)` группирует не по тому, что имел в виду автор,
    а по имени скрейп-задачи — все джобы схлопываются в один ряд `job="admin"`,
    их отказы складываются между собой, а `{{ $labels.job }}` в тексте
    уведомления печатает «admin».

    Найдено 06.08.2026 замером: `sum by (job) (increase(
    frontier_admin_job_runs_total{outcome=~"failed|timeout"}[49h]))` на живом
    Prometheus вернул единственный ряд `{job="admin"}`, хотя на `/metrics`
    сервиса лежало девять рядов с разными именами джобов.

    Свои метки размечаются `job_name` — это имя уже принято в проекте
    (`frontier_admin_manual_job_*`, белый список меток в
    admin/backend/services/telegram_alerts.py).
    """
    offenders: list[str] = []
    for rule in _rules():
        expr = str(rule.get("expr") or "")
        if "frontier_" not in expr:
            continue
        for group_by in re.findall(r"by\s*\(([^)]*)\)", expr):
            labels = {label.strip() for label in group_by.split(",")}
            if "job" in labels:
                offenders.append(f"{rule.get('alert')}: by ({group_by.strip()})")
    assert not offenders, (
        "правила группируют собственные метрики по зарезервированной метке `job` "
        f"(нужен `job_name`): {offenders}"
    )


def test_every_scrape_target_is_covered_by_a_down_rule() -> None:
    """У каждой скрейп-цели обязано быть правило, ловящее её падение.

    Заведено 06.08.2026. Повод: `mcp-gateway` — единственная поверхность,
    которой пользуется человек (Claude Code и Desktop ходят только на 8102,
    REST 8100 закрыт на loopback), — не был ни в scrape_configs, ни в одном
    правиле `up == 0`. Его отказ обнаруживался тем, что кто-то ткнул.

    Проверка идёт от prometheus.yml к alerts.yml: добавил таргет — обязан
    накрыть его правилом, иначе новый сервис молча окажется вне наблюдения
    ровно так же.
    """
    import yaml as _yaml

    scrape_path = REPO_ROOT / "prometheus" / "prometheus.yml"
    scrape = _yaml.safe_load(scrape_path.read_text(encoding="utf-8"))
    targets = {
        str(job.get("job_name"))
        for job in scrape.get("scrape_configs") or []
        if job.get("job_name")
    }
    assert len(targets) >= 8, f"разобрано целей: {sorted(targets)} — сломался разбор"

    covered: set[str] = set()
    for rule in _rules():
        expr = str(rule.get("expr") or "")
        if "up{" not in expr or "== 0" not in expr:
            continue
        for pattern in re.findall(r'job\s*=~?\s*"([^"]*)"', expr):
            covered.update(part.strip() for part in pattern.split("|") if part.strip())

    uncovered = sorted(targets - covered)
    assert not uncovered, (
        f"скрейп-цели без правила на падение: {uncovered}. Цель, которую никто "
        "не проверяет на up == 0, отказывает молча."
    )
