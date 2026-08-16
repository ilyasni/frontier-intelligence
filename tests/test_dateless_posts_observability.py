"""
Наблюдаемость пропавшей даты публикации (пункт 75 реестра).

Класс отказа: пост, у которого не разобралась `published_at`, сохраняется в БД,
считается принятым (`source_runs.status='success'`, `fetched_count` и
`emitted_count` растут, поиск его находит) — и при этом навсегда исключён из
аналитики. Выборка кластеризации требует `p.published_at IS NOT NULL` наравне с
`embedding_status='done'` и порогом релевантности
(`worker/services/semantic_clustering.py::_fetch_posts`), то есть материал не
«редко проходит», а не участвует ни в кластерах, ни в трендах, ни в
emerging-сигналах.

Замер по всей базе 16.08.2026: 1235 постов из 337 735. Разложение показывает,
почему разбивки по воркспейсу было мало: web — 1199 из 1262 (95%), rss — 36 из
223 899 (0.02%), api и telegram — ноль. У `disruption` доля 0.39% и тонет в
знаменателе, а внутри неё тринадцать источников со 100% пропусков.

Проверки ниже держат три инварианта, и каждый из них уже ломался в проекте
в других обличьях:

  1. Метрика адресная (по источнику), а не только по воркспейсу — иначе отказ
     невидим ровно там, где он живёт.
  2. Экспортёр печатает ряд `prior` ВСЕГДА, включая ноль. Потеря этой гарантии
     глушит алерт именно на новом источнике — на единственном случае, ради
     которого он написан. Тот же класс, что «отсутствие данных обязано быть
     нулём, а не отсутствием ряда» у метрик свежести аналитики.
  3. У правила существует вход, при котором оно тревожит, И вход, при котором
     оно молчит. Правило приёмки принято в проекте для метрик качества
     (tests/test_cluster_quality_metrics.py). Здесь оно означает конкретное:
     порог на ФАКТ высокой доли молчать не умеет вовсе — перемотка по реальным
     данным за 45 суток даёт firing 46 суток из 46.

Плюс пин на сам предикат кластеризации: развилка «COALESCE(published_at,
created_at) вместо жёсткого исключения» не решена владельцем, и её тихая правка
обязана упасть, а не проехать.

Разбор статический: yaml для правил и промтул-кейсов, регулярки для shell,
чтение исходника для предиката. Ни импорта проекта, ни сети, ни БД.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]
ALERTS_YML = REPO_ROOT / "prometheus" / "alerts.yml"
ALERTS_TEST_YML = REPO_ROOT / "prometheus" / "alerts.test.yml"
EXPORTER = REPO_ROOT / "scripts" / "export-analysis-freshness.sh"
VERIFY_SCRIPT = REPO_ROOT / "scripts" / "verify-alert-rules.sh"
CLUSTERING = REPO_ROOT / "worker" / "services" / "semantic_clustering.py"

ALERT_NAME = "FrontierSourceDatelessRatioJumped"
RATIO_METRIC = "frontier_source_dateless_ratio"
DENOMINATOR_METRIC = "frontier_source_posts_in_window"

# Окна метрики. `7d` — недавнее, `prior` — вся история источника до его начала.
RECENT_WINDOW = "7d"
PRIOR_WINDOW = "prior"


# ── Разбор ───────────────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _exporter_text() -> str:
    return EXPORTER.read_text(encoding="utf-8", errors="replace")


@lru_cache(maxsize=1)
def _alerts_document() -> dict[str, Any]:
    loaded = yaml.safe_load(ALERTS_YML.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


@lru_cache(maxsize=1)
def _rules() -> tuple[dict[str, Any], ...]:
    found: list[dict[str, Any]] = []
    for group in _alerts_document().get("groups") or []:
        if isinstance(group, dict):
            found.extend(raw for raw in (group.get("rules") or []) if isinstance(raw, dict))
    return tuple(found)


def _rule(name: str) -> dict[str, Any] | None:
    for rule in _rules():
        if rule.get("alert") == name:
            return rule
    return None


@lru_cache(maxsize=1)
def _rule_names() -> frozenset[str]:
    return frozenset(str(rule.get("alert")) for rule in _rules() if rule.get("alert"))


@lru_cache(maxsize=1)
def _promtool_document() -> dict[str, Any]:
    loaded = yaml.safe_load(ALERTS_TEST_YML.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


@lru_cache(maxsize=1)
def _promtool_cases() -> tuple[dict[str, Any], ...]:
    """Плоский список утверждений `alert_rule_test` из всех прогонов."""
    found: list[dict[str, Any]] = []
    for test in _promtool_document().get("tests") or []:
        if not isinstance(test, dict):
            continue
        for case in test.get("alert_rule_test") or []:
            if isinstance(case, dict):
                found.append({**case, "_input": test.get("input_series") or []})
    return tuple(found)


@lru_cache(maxsize=1)
def _promtool_input_metrics() -> frozenset[str]:
    """Имена серий, которые промтул-кейсы подают на вход."""
    names: set[str] = set()
    for test in _promtool_document().get("tests") or []:
        if not isinstance(test, dict):
            continue
        for entry in test.get("input_series") or []:
            if not isinstance(entry, dict):
                continue
            match = re.match(r"\s*([a-zA-Z_:][a-zA-Z0-9_:]*)", str(entry.get("series") or ""))
            if match:
                names.add(match.group(1))
    return frozenset(names)


@lru_cache(maxsize=1)
def _exporter_metric_names() -> frozenset[str]:
    return frozenset(re.findall(r"\b(frontier_[a-z0-9_]+)", _exporter_text()))


# ── Самопроверка извлекателей ────────────────────────────────────────────────
# Сломанный разбор превращает всё ниже в тавтологию на пустых множествах —
# ровно тот класс отказа, ради которого файл и написан.


def test_extraction_is_not_vacuous() -> None:
    for path in (ALERTS_YML, ALERTS_TEST_YML, EXPORTER, VERIFY_SCRIPT, CLUSTERING):
        assert path.is_file(), f"missing required file: {path}"
    assert len(_rules()) >= 40, f"alerts.yml parsed into {len(_rules())} rules"
    assert len(_promtool_cases()) >= 3, (
        f"alerts.test.yml parsed into {len(_promtool_cases())} assertions — "
        "the extractor is broken and the polarity check below would pass vacuously"
    )
    assert len(_exporter_metric_names()) >= 8, (
        f"the exporter scan found {sorted(_exporter_metric_names())} — extractor broken"
    )


# ── 1. Предикат, из-за которого материал исчезает ────────────────────────────


def test_clustering_selection_still_excludes_dateless_posts() -> None:
    """
    Пин на текущее поведение `_fetch_posts`, а не на желаемое.

    Развилка «брать COALESCE(published_at, created_at) вместо жёсткого
    исключения» открыта и решается владельцем: она меняет семантику окна
    кластеризации для всей системы (velocity и change points по таким постам
    считались бы от момента обнаружения, а не публикации).

    Замер на живых данных 16.08.2026, сколько постов добавилось бы в окно:
    auto_hmi 42 -> 61 (+45%), design 886 -> 897, disruption 27 130 -> 27 342,
    ai_trends +3, остальные ноль. После потолка выборки у disruption доезжают
    только 18 из 212 — потолок 2000 бьёт раньше окна.

    Пока решения нет, правка предиката обязана быть ГРОМКОЙ: наблюдаемость,
    которую держит этот файл, описывает материал, исключённый ЭТИМ условием.
    Поменяли предикат — обязаны пересобрать и метрику, и текст алерта, иначе
    они начнут описывать состояние, которого больше нет.
    """
    source = CLUSTERING.read_text(encoding="utf-8")
    marker = '"p.published_at IS NOT NULL"'
    assert marker in source, (
        f"{CLUSTERING.name}::_fetch_posts больше не содержит {marker}. Если предикат "
        "изменён осознанно (развилка пункта 75 закрыта владельцем) — обнови этот тест, "
        "долю в docs/TODO-UNFINISHED.md и текст FrontierSourceDatelessRatioJumped: "
        "алерт объясняет дежурному, что материал не попадёт в тренды, и это перестанет "
        "быть правдой."
    )


# ── 2. Метрика: адресная и с обоими окнами ───────────────────────────────────


def test_exporter_emits_the_per_source_ratio_and_its_denominator() -> None:
    """
    Доля без знаменателя рядом — не измерение.

    У источника с одним постом доля 1.00 и у источника с четырьмя сотнями доля
    1.00 выглядят одинаково, а значат разное. Знаменатель — единственный способ
    отличить «нечего мерить» от «плохо».
    """
    text = _exporter_text()
    for metric in (RATIO_METRIC, DENOMINATOR_METRIC):
        assert f"{metric}{{" in text, (
            f"scripts/export-analysis-freshness.sh больше не печатает {metric}. "
            "Правило FrontierSourceDatelessRatioJumped останется валидным и вечно "
            "пустым."
        )
    assert 'source_id=\\"$sid\\"' in text, (
        "метрика перестала быть адресной: без метки source_id отказ снова тонет в "
        "знаменателе воркспейса (у disruption 1129 пропусков из 285 879 — 0.39%, "
        "при тринадцати источниках со 100%)"
    )


def test_exporter_emits_both_windows() -> None:
    """Без второго окна сравнивать не с чем, и правило вырождается в порог на факт."""
    text = _exporter_text()
    for window in (RECENT_WINDOW, PRIOR_WINDOW):
        assert f'window="{window}"' in text, (
            f"экспортёр не печатает окно {window!r}. Правило сравнивает недавнее "
            "состояние с историей источника; без одного из окон вычитание даёт пустой "
            "вектор, и алерт молчит навсегда."
        )


def test_prior_window_is_emitted_unconditionally() -> None:
    """
    Главный инвариант экспортёра, и он несимметричный.

    `prior` печатается ВСЕГДА, включая ноль у источника без истории. `7d`,
    наоборот, печатается только при непустом знаменателе — доля от нуля постов
    не ноль, а отсутствие измерения.

    Если «оптимизировать» вывод и пропускать prior у новых источников, правило
    замолчит именно на них: вычитание из пустого вектора даёт пустоту. А новый
    источник, рождённый без дат, — это и есть разбираемый случай
    (`auto_web_ieee_spectrum_autonomous`, включён 05.08.2026, 33 поста подряд
    без `published_at`, все прогоны `success`).

    Проверка структурная: строка с prior обязана лежать ВНЕ условного блока,
    который стережёт `7d`.
    """
    lines = _exporter_text().splitlines()

    def _line_of(pattern: str) -> int:
        hits = [index for index, line in enumerate(lines) if pattern in line]
        assert len(hits) == 1, (
            f"ожидалась ровно одна строка с {pattern!r}, найдено {len(hits)} — "
            "структурная проверка ниже стала недостоверной"
        )
        return hits[0]

    prior_line = _line_of(f'{RATIO_METRIC}{{%s,window="{PRIOR_WINDOW}"}}')
    recent_line = _line_of(f'{RATIO_METRIC}{{%s,window="{RECENT_WINDOW}"}}')
    guard_line = _line_of('if [ "${recent_n:-0}" -gt 0 ]; then')

    assert prior_line < guard_line, (
        "ряд prior печатается внутри (или после) условия на непустой знаменатель "
        f"окна {RECENT_WINDOW}: prior на строке {prior_line + 1}, условие на "
        f"{guard_line + 1}. У нового источника знаменатель 7d бывает пуст ровно в тот "
        "момент, когда prior нужнее всего, а без пары для вычитания алерт молчит."
    )
    assert guard_line < recent_line, (
        f"ряд окна {RECENT_WINDOW} перестал быть защищён проверкой знаменателя "
        "(строка условия должна идти перед ним). Доля от нуля постов, напечатанная "
        "нулём, читается на графике как «источник здоров», хотя он просто молчит."
    )


def test_exporter_covers_every_predicate_of_the_clustering_selection() -> None:
    """
    Симметрия: у выборки три предиката, и спрашивать надо про каждый.

    Дата публикации оказалась главной по объёму (1235 постов), но не
    единственной. `relevance_score IS NULL` выключает пост так же навсегда —
    49 постов по всей базе, все с `embedding_status` error (42) или pending (7),
    то есть осадок упавшего обогащения. Отсутствие строки в `indexing_status`
    сейчас даёт ноль, и ноль этот обязан печататься: серия, которая просто
    исчезла из вывода, неотличима от «проверять перестали».

    Отрицательный результат замера тоже зафиксирован: `embedding_status` НЕ
    является самостоятельными воротами — релевантных постов без эмбеддинга ноль
    за 30 суток, а весь `dropped` (20 161) совпадает с низкой релевантностью.
    Поэтому отдельной причины по нему здесь нет намеренно, и её появление должно
    сопровождаться новым замером, а не догадкой.
    """
    text = _exporter_text()
    for reason in ("no_published_at", "no_relevance_score", "no_indexing_row"):
        assert f'reason="{reason}"' in text, (
            f"экспортёр не печатает причину {reason!r}. Предикат выборки "
            "`_fetch_posts`, по которому пост исчезает из аналитики, обязан иметь "
            "свой ряд — иначе следующий такой отказ снова будет найден вручную."
        )


# ── 3. Правило: тревожит на скачке, молчит на известном состоянии ────────────


def test_alert_rule_exists_and_reads_both_windows() -> None:
    rule = _rule(ALERT_NAME)
    assert rule is not None, (
        f"{ALERT_NAME} исчезло из alerts.yml. Пропавшая дата публикации снова стала "
        "полностью невидимой: source_runs пишет success, а материал не попадает в "
        "аналитику вовсе."
    )
    expr = str(rule.get("expr") or "")
    for window in (RECENT_WINDOW, PRIOR_WINDOW):
        assert f'window="{window}"' in expr, f"{ALERT_NAME}: в выражении нет окна {window!r}"
    assert DENOMINATOR_METRIC in expr, (
        f"{ALERT_NAME}: нет условия на знаменатель. Источник с единственным постом без "
        "даты даёт долю 1.00 и разбудил бы дежурного на одном событии."
    )


def test_alert_compares_windows_instead_of_thresholding_the_bare_ratio() -> None:
    """
    Требование ПОЛОЖИТЕЛЬНОЕ: в выражении обязано быть вычитание одного окна из другого.

    Проверка «в тексте нет порога на факт» зеленела бы на правиле
    `frontier_source_dateless_ratio{window="7d"} > 0.5` — порог там есть, а
    сравнения с прошлым нет. Между тем именно эта редакция и не имеет входа,
    при котором молчит: перемотка условия по реальным данным за 45 суток даёт
    firing 46 суток из 46 и 116 источнико-суток, потому что восемнадцать
    источников держат долю 1.00 месяцами. Это открытая развилка пункта 75, а не
    поломка, и будить по ней нельзя — ровно болезнь пункта 56 (карантин
    OpenRouter, firing 6 суток из 7 на штатном сбросе квоты).

    С вычитанием то же условие на тех же данных: 18 суток из 46, 70
    источнико-суток, ноль на момент внедрения.
    """
    rule = _rule(ALERT_NAME)
    assert rule is not None
    expr = " ".join(str(rule.get("expr") or "").split())
    subtraction = re.search(
        rf'{RATIO_METRIC}\{{window="{RECENT_WINDOW}"\}}\s*-\s*ignoring\(window\)\s*'
        rf'{RATIO_METRIC}\{{window="{PRIOR_WINDOW}"\}}',
        expr,
    )
    assert subtraction, (
        f"{ALERT_NAME} больше не вычитает историю источника из недавнего окна "
        f"(выражение: {expr!r}). Без этого правило тревожит на состоянии, а не на "
        "событии, и молчать не умеет вовсе."
    )
    assert "and on(source_id)" in expr, (
        f"{ALERT_NAME}: вычитание с `ignoring(window)` снимает метку window, поэтому "
        "остальные условия обязаны присоединяться через `and on(source_id)`. Иначе "
        "наборы меток левой и правой части не совпадут, пересечение будет пустым, и "
        "правило останется валидным и вечно молчащим — класс FrontierS3QuotaCritical."
    )


# ── 4. Правило приёмки: вход, на котором тревожит, и вход, на котором молчит ─


def test_promtool_cases_cover_both_polarities_of_the_new_rule() -> None:
    """
    То самое правило приёмки, принятое в проекте для метрик качества.

    Одного полюса мало в обе стороны: кейс только с firing пропускает правило,
    которое кричит на всём подряд; кейс только с тишиной пропускает правило,
    которое не может сработать никогда. Оба дефекта уже случались здесь.
    """
    ours = [case for case in _promtool_cases() if case.get("alertname") == ALERT_NAME]
    assert ours, (
        f"в prometheus/alerts.test.yml нет ни одного утверждения про {ALERT_NAME}. "
        "Правило не проверено вычислением — только формой."
    )
    firing = [case for case in ours if case.get("exp_alerts")]
    silent = [case for case in ours if not case.get("exp_alerts")]
    assert firing, (
        f"{ALERT_NAME}: нет кейса, в котором правило ТРЕВОЖИТ. Проверка, у которой нет "
        "такого входа, зелена и при полностью мёртвом выражении."
    )
    assert silent, (
        f"{ALERT_NAME}: нет кейса, в котором правило МОЛЧИТ. Именно этого входа не "
        "существует у порога на факт — он был бы firing 46 суток из 46."
    )


def test_promtool_silent_cases_are_not_empty_inputs() -> None:
    """
    Тишина на пустом входе ничего не доказывает.

    Правило молчит и тогда, когда серий просто нет; такой кейс зелен при любом
    выражении, включая заведомо сломанное. Поэтому у каждого «молчит» на входе
    обязаны быть ряды.
    """
    problems: list[str] = []
    for case in _promtool_cases():
        if case.get("alertname") != ALERT_NAME or case.get("exp_alerts"):
            continue
        if not case.get("_input"):
            problems.append(f"eval_time={case.get('eval_time')!r}: пустой input_series")
    assert not problems, "\n  ".join(
        [f"{ALERT_NAME}: кейсы «молчит», доказывающие тишину отсутствием данных:", *problems]
    )


def test_promtool_cases_reference_real_rules_and_real_series() -> None:
    """
    Кейс про несуществующее правило или про серию, которую никто не публикует, —
    зелёная тавтология.

    `promtool test rules` на утверждение об отсутствии алерта у несуществующего
    имени не ругается: алертов нет, ожидалось ноль, всё сходится.
    """
    unknown_rules = sorted(
        {
            str(case.get("alertname"))
            for case in _promtool_cases()
            if case.get("alertname") and str(case.get("alertname")) not in _rule_names()
        }
    )
    assert not unknown_rules, (
        f"alerts.test.yml утверждает про правила, которых нет в alerts.yml: {unknown_rules}"
    )
    unknown_series = sorted(_promtool_input_metrics() - _exporter_metric_names())
    assert not unknown_series, (
        f"alerts.test.yml подаёт на вход серии, которых экспортёр не печатает: "
        f"{unknown_series}. Тест проверял бы правило на данных, которых в проде не "
        "бывает."
    )


def test_promtool_suite_loads_the_real_rule_file() -> None:
    """Кейсы обязаны гонять НАСТОЯЩИЙ alerts.yml, а не копию рядом."""
    files = _promtool_document().get("rule_files")
    assert files == ["alerts.yml"], (
        f"rule_files={files!r}: юнит-тесты правил обязаны загружать prometheus/alerts.yml — "
        "иначе они проверяют текст, который в Prometheus не попадает."
    )


def test_verify_script_actually_runs_the_promtool_suite() -> None:
    """
    Набор кейсов, который никто не запускает, — документация, а не проверка.

    pytest промтул не вызывает: его нет в тест-образе. Значит исполняемая
    половина живёт в скрипте, и она обязана ссылаться и на `test rules`, и на
    файл кейсов — иначе останется только `check rules`, то есть снова проверка
    формы.
    """
    text = VERIFY_SCRIPT.read_text(encoding="utf-8", errors="replace")
    for marker in ("promtool check rules", "promtool test rules", "alerts.test.yml"):
        assert marker in text, (
            f"scripts/verify-alert-rules.sh не содержит {marker!r} — исполняемая проверка "
            "правил неполна"
        )
