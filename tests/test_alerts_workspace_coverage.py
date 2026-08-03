"""
Контракт между config/workspaces.yml и порогами FrontierNoNewPosts в prometheus/alerts.yml.

Инцидент 2026-08-03. Workspace `auto_hmi` добавили в config/workspaces.yml и НЕ добавили
ни в одно из четырёх выражений FrontierNoNewPosts / FrontierNoNewPostsCritical. Два из них
партиционируют по `workspace!~"..."` (высокообъёмные, 6ч warn / 12ч crit), два — по
`workspace=~"..."` (низкообъёмные, 24ч / 36ч). Слаг, отсутствующий в перечислении, попадает
в НЕГИРОВАННУЮ ветку по умолчанию: он молча наследует самые жёсткие пороги и ложно
срабатывает в первую же тихую ночь. Ничего не падало и не логировалось — алерт просто
выстрелил бы.

Класс отказа тот же, что в ops_llm_truncation_silent_fallback: конфиг разъехался, а сигнала
о расхождении нет ни одного. Поэтому проверки ниже держат три инварианта:

  1. Каждый активный workspace обслуживается КАЖДЫМ семейством правил, которое
     партиционирует по label `workspace` (exhaustive).
  2. Он попадает ровно в ОДИН tier — не в два (двойной алерт) и не в ноль (тишина).
  3. Правила одного tier выбирают одно и то же множество workspace: четыре выражения не
     имеют права разъезжаться между собой.

Плюс явный реестр EXPECTED_VOLUME_TIER: седьмой workspace обязан упасть громко и в одном
месте, потому что структурные проверки его поймать НЕ могут — негированная ветка
`workspace!~"..."` по построению является catch-all и молча принимает любой новый слаг.

Ревизия 2026-08-03 (три дыры, найденные мутационным прогоном по этому же файлу):

  4. Label `tier` — это ПРОКСИ, а не сам порог. Проверок «слаг лежит в нужной регулярке»
     недостаточно: правку `> 86400` → `> 600` в низкообъёмной ветке (тот же ложный алерт
     auto_hmi, только пришедший через число, а не через слаг), перестановку порогов между
     tier и инверсию `>` → `<` не видел ни один assert. Теперь пороги, оператор сравнения
     и порядок warn/critical пришпилены явно: EXPECTED_TIER_THRESHOLD_SECONDS плюс два
     структурных инварианта (низкообъёмный порог мягче высокообъёмного, critical выше warn).
  5. `workspace="a|b"` — НЕ перечисление. В Prometheus `=`/`!=` сравнивают с литералом
     целиком, вместе с трубами: `=~` → `=` выключает низкообъёмные правила полностью
     (ни одна серия не совпадёт), `!~` → `!=` наоборот включает в высокообъёмную ветку
     вообще всех. Модель матчера теперь различает regex- и exact-операторы, а
     перечисление через `|` обязано быть regex-матчером.
  6. Присутствие ключа ≠ пригодное значение. `for: 720h` присутствует — и алерт не может
     сработать 30 суток; опечатка в имени метрики оставляет выражение синтаксически целым
     и вечно пустым. Значения `for`, `expr`, `summary` теперь валидируются, а имя метрики
     под матчером workspace пришпилено реестром EXPECTED_PARTITION_METRIC.

Правила ищутся по структуре выражения (наличие матчера на label `workspace`), а не по
номерам строк и не по списку имён: любое новое правило с перечислением workspace в регулярке
попадает в проверки автоматически.

Файлы читаются как есть, без импорта кода проекта: тест не зависит ни от заглушек
tests/conftest.py, ни от наличия pydantic/bs4/boto3 в тест-образе.
"""

from __future__ import annotations

import re
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any, NamedTuple

import pytest
import yaml

pytestmark = pytest.mark.unit

# Корень репозитория — относительно файла теста: прогон идёт и на хосте, и внутри образа,
# где дерево смонтировано в /src.
REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACES_YML = REPO_ROOT / "config" / "workspaces.yml"
ALERTS_YML = REPO_ROOT / "prometheus" / "alerts.yml"

# Ключи, без которых правило деградирует молча: без `for` алерт дёргается на одиночном
# скрейпе, без `severity` его некуда роутить, без `summary` уведомление приходит пустым.
REQUIRED_TOP_LEVEL_KEYS: tuple[str, ...] = ("alert", "expr", "for")
KNOWN_SEVERITIES: frozenset[str] = frozenset({"info", "warning", "critical"})

# Границы вменяемости `for`. scrape_interval и evaluation_interval — 15s
# (prometheus/prometheus.yml), поэтому дебаунс меньше минуты — это 4 скрейпа, дребезг;
# дебаунс больше двух часов означает, что правило не может сработать в пределах окна,
# на которое оно рассчитано (`for: 720h` — присутствует и бесполезен).
# Осознанно нужен другой дебаунс — меняется здесь, и это видно в диффе.
MIN_FOR_SECONDS = 60
MAX_FOR_SECONDS = 2 * 3600

# Единственное место, где объявляется объёмный tier workspace.
# Добавили workspace в config/workspaces.yml — обязаны добавить строку сюда И слаг
# в выражения FrontierNoNewPosts/...Critical. Иначе тесты ниже краснеют.
EXPECTED_VOLUME_TIER: dict[str, str] = {
    "disruption": "high_volume",
    "ai_products_media": "high_volume",
    "ai_trends": "low_volume",
    "ai_research": "low_volume",
    "design": "low_volume",
    "auto_hmi": "low_volume",
}

# Сами пороги, которые label `tier` только ОБОЗНАЧАЕТ. Ключ — (имя алерта, tier),
# значение — правая часть сравнения в секундах. Меняется вместе с alerts.yml и
# осознанно: 6ч/12ч для высокообъёмных, 24ч/36ч для низкообъёмных.
EXPECTED_TIER_THRESHOLD_SECONDS: dict[tuple[str, str], int] = {
    ("FrontierNoNewPosts", "high_volume"): 21600,  # 6ч
    ("FrontierNoNewPostsCritical", "high_volume"): 43200,  # 12ч
    ("FrontierNoNewPosts", "low_volume"): 86400,  # 24ч
    ("FrontierNoNewPostsCritical", "low_volume"): 129600,  # 36ч
}

# Порядок строгости tier: чем больше число, тем МЯГЧЕ должен быть порог (больше секунд
# тишины до срабатывания). Смысл всего разделения: низкообъёмный workspace штатно молчит
# ночью, поэтому его порог обязан быть строго больше высокообъёмного.
TIER_LOOSENESS_ORDER: dict[str, int] = {"high_volume": 0, "low_volume": 1}

# Серия, на которой держится партиция. Опечатка в имени оставляет выражение
# синтаксически целым и вечно пустым — матчер по workspace при этом цел, поэтому
# все проверки покрытия продолжают «видеть» правило.
EXPECTED_PARTITION_METRIC: dict[str, str] = {
    "FrontierNoNewPosts": "frontier_last_post_age_seconds",
    "FrontierNoNewPostsCritical": "frontier_last_post_age_seconds",
}

# `\bworkspace` не даст зацепить cross_workspace и подобные лейблы: `_` — словесный символ,
# границы слова перед `workspace` внутри `cross_workspace` нет.
_WORKSPACE_MATCHER = re.compile(r'\bworkspace\s*(=~|!~|!=|=)\s*"([^"]*)"')
_SLUG = re.compile(r"^[a-z][a-z0-9_]*$")

# Метрика, к селектору которой прицеплен матчер по workspace.
_WORKSPACE_SELECTOR = re.compile(
    r'([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{[^{}]*\bworkspace\s*(?:=~|!~|!=|=)\s*"'
)
# Правая часть сравнения: `... ) > 21600`.
_COMPARISON = re.compile(r"(>=|<=|==|!=|>|<)\s*([0-9]+(?:\.[0-9]+)?)\s*$")

# Regex-операторы Prometheus сравнивают с ЯКОРНОЙ регуляркой, exact-операторы — со строкой
# целиком. Поэтому `workspace="a|b"` не перечисление, а литерал с трубой внутри: ни одна
# реальная серия такого значения не несёт, правило молча выбирает пустое множество.
_REGEX_OPS: frozenset[str] = frozenset({"=~", "!~"})
_EXACT_OPS: frozenset[str] = frozenset({"=", "!="})
_NEGATED_OPS: frozenset[str] = frozenset({"!~", "!="})


class WorkspaceMatcher(NamedTuple):
    """Один матчер по label `workspace`.

    `pattern` — то, что реально стоит в кавычках; `alternatives` — разбиение по `|`,
    осмысленное ТОЛЬКО для regex-операторов. Для `=`/`!=` труба — обычный символ
    внутри литерала, и разбивать по ней нельзя (см. `_matcher_selection`).
    """

    operator: str
    pattern: str
    alternatives: tuple[str, ...]


class AlertRule(NamedTuple):
    """Одно правило alerts.yml плюс разобранные матчеры по label `workspace`."""

    group: str
    position: int
    name: str
    expr: str
    for_raw: Any
    summary: Any
    tier: str | None
    severity: str | None
    labels: tuple[str, ...]
    annotations: tuple[str, ...]
    keys: tuple[str, ...]
    matchers: tuple[WorkspaceMatcher, ...]
    selector_metrics: tuple[str, ...]
    comparison: str | None
    threshold: float | None

    @property
    def where(self) -> str:
        return f"{self.group}/{self.name}#{self.position}"


# ── Разбор конфигов ──────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _workspaces_document() -> dict[str, Any]:
    loaded = yaml.safe_load(WORKSPACES_YML.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


@lru_cache(maxsize=1)
def _workspace_entries() -> tuple[tuple[str, bool], ...]:
    """(id, is_active) для каждого workspace из config/workspaces.yml."""
    entries: list[tuple[str, bool]] = []
    for raw in _workspaces_document().get("workspaces") or []:
        if not isinstance(raw, dict) or not raw.get("id"):
            continue
        entries.append((str(raw["id"]), bool(raw.get("is_active", True))))
    return tuple(entries)


@lru_cache(maxsize=1)
def _active_workspaces() -> frozenset[str]:
    """Только активные: выключенный workspace не пишет frontier_last_post_age_seconds."""
    return frozenset(ws_id for ws_id, active in _workspace_entries() if active)


@lru_cache(maxsize=1)
def _all_workspaces() -> frozenset[str]:
    return frozenset(ws_id for ws_id, _ in _workspace_entries())


@lru_cache(maxsize=1)
def _alerts_document() -> dict[str, Any]:
    loaded = yaml.safe_load(ALERTS_YML.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _extract_workspace_matchers(expr: str) -> tuple[WorkspaceMatcher, ...]:
    """Матчеры на label `workspace`, в порядке появления в выражении."""
    found: list[WorkspaceMatcher] = []
    for operator, pattern in _WORKSPACE_MATCHER.findall(expr):
        alternatives = tuple(part.strip() for part in pattern.split("|") if part.strip())
        found.append(
            WorkspaceMatcher(operator=operator, pattern=pattern, alternatives=alternatives)
        )
    return tuple(found)


def _parse_duration_seconds(raw: Any) -> int | None:
    """Длительность Prometheus (`15m`, `1h30m`) в секундах. None — не разбирается."""
    if not isinstance(raw, str) or not raw:
        return None
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800, "y": 31536000}
    total = 0
    position = 0
    for match in re.finditer(r"(\d+)([smhdwy])", raw):
        if match.start() != position:
            return None
        total += int(match.group(1)) * units[match.group(2)]
        position = match.end()
    return total if position == len(raw) else None


@lru_cache(maxsize=1)
def _alert_rules() -> tuple[AlertRule, ...]:
    rules: list[AlertRule] = []
    for group in _alerts_document().get("groups") or []:
        if not isinstance(group, dict):
            continue
        group_name = str(group.get("name") or "?")
        for position, raw in enumerate(group.get("rules") or []):
            if not isinstance(raw, dict):
                continue
            labels = raw.get("labels") if isinstance(raw.get("labels"), dict) else {}
            annotations = (
                raw.get("annotations") if isinstance(raw.get("annotations"), dict) else {}
            )
            expr = str(raw.get("expr") or "")
            tier = labels.get("tier")
            severity = labels.get("severity")
            comparison = _COMPARISON.search(expr.strip())
            rules.append(
                AlertRule(
                    group=group_name,
                    position=position,
                    name=str(raw.get("alert") or raw.get("record") or "?"),
                    expr=expr,
                    for_raw=raw.get("for"),
                    summary=annotations.get("summary"),
                    tier=None if tier is None else str(tier),
                    severity=None if severity is None else str(severity),
                    labels=tuple(str(key) for key in labels),
                    annotations=tuple(str(key) for key in annotations),
                    keys=tuple(str(key) for key in raw),
                    matchers=_extract_workspace_matchers(expr),
                    selector_metrics=tuple(_WORKSPACE_SELECTOR.findall(expr)),
                    comparison=None if comparison is None else comparison.group(1),
                    threshold=None if comparison is None else float(comparison.group(2)),
                )
            )
    return tuple(rules)


@lru_cache(maxsize=1)
def _partitioning_rules() -> tuple[AlertRule, ...]:
    """Правила, чьё выражение фильтрует по label `workspace`."""
    return tuple(rule for rule in _alert_rules() if rule.matchers)


@lru_cache(maxsize=1)
def _partition_families() -> tuple[tuple[str, tuple[AlertRule, ...]], ...]:
    """
    Семейства правил, которые делят workspace между собой: одно имя алерта, два и более
    выражений с матчером по workspace. Именно они обязаны покрывать все workspace целиком.

    Правило-одиночка с матчером (например точечный алерт под один workspace) семейством
    не является и в проверку полноты не попадает — но его слаги всё равно валидируются.
    """
    grouped: dict[str, list[AlertRule]] = defaultdict(list)
    for rule in _partitioning_rules():
        grouped[rule.name].append(rule)
    return tuple(
        (name, tuple(rules)) for name, rules in sorted(grouped.items()) if len(rules) >= 2
    )


def _matcher_selection(matcher: WorkspaceMatcher, universe: frozenset[str]) -> frozenset[str]:
    """Какие workspace выбирает матчер — по семантике Prometheus, а не по «похожести».

    Ключевое различие: `=~`/`!~` матчат ЯКОРНОЙ регуляркой, поэтому `a|b` — перечисление.
    `=`/`!=` сравнивают значение лейбла со строкой ЦЕЛИКОМ, вместе с трубами, поэтому
    `workspace="a|b"` не выбирает ни `a`, ни `b` — а `workspace!="a|b"` не исключает
    ни одного из них. Уравнивать эти операторы нельзя: `=~` → `=` глушит правило
    полностью, `!~` → `!=` наоборот распространяет его на все workspace сразу.
    """
    if matcher.operator in _REGEX_OPS:
        listed = frozenset(matcher.alternatives)
    else:
        listed = universe & frozenset({matcher.pattern})
    return (universe - listed) if matcher.operator in _NEGATED_OPS else (universe & listed)


def _rule_selection(rule: AlertRule, universe: frozenset[str]) -> frozenset[str]:
    selected = universe
    for matcher in rule.matchers:
        selected &= _matcher_selection(matcher, universe)
    return selected


@lru_cache(maxsize=1)
def _enumerated_slugs() -> frozenset[str]:
    """Все слаги, перечисленные в регулярках по workspace во всём файле."""
    return frozenset(
        slug
        for rule in _partitioning_rules()
        for matcher in rule.matchers
        for slug in matcher.alternatives
    )


def _observed_tiers(workspace: str) -> frozenset[str]:
    universe = _active_workspaces()
    return frozenset(
        rule.tier or "<no tier label>"
        for _, rules in _partition_families()
        for rule in rules
        if workspace in _rule_selection(rule, universe)
    )


# ── Самопроверка извлекателей ────────────────────────────────────────────────
# Без неё сломанный парсер превращает все проверки ниже в тавтологию на пустых множествах:
# ровно тот сценарий, ради которого этот файл и написан.


def test_config_files_exist() -> None:
    for path in (WORKSPACES_YML, ALERTS_YML):
        assert path.is_file(), f"missing required file: {path}"


def test_extraction_is_not_vacuous() -> None:
    assert len(_workspace_entries()) >= 5, (
        f"config/workspaces.yml parsed into {len(_workspace_entries())} workspaces — "
        "the extractor is broken, every assertion below would pass vacuously"
    )
    assert len(_alert_rules()) >= 40, (
        f"prometheus/alerts.yml parsed into {len(_alert_rules())} rules — extractor broken"
    )
    assert len(_partitioning_rules()) >= 4, (
        "expected at least the four FrontierNoNewPosts expressions to filter on the "
        f"`workspace` label, found {len(_partitioning_rules())}"
    )
    families = {name for name, _ in _partition_families()}
    assert {"FrontierNoNewPosts", "FrontierNoNewPostsCritical"} <= families, (
        "the volume-tier partition families disappeared from alerts.yml "
        f"(discovered families: {sorted(families)}). If the alerts were renamed, re-check "
        "that every workspace is still covered by both thresholds, then update this anchor."
    )
    tiers = {rule.tier for rule in _partitioning_rules()}
    assert len(tiers) >= 2, f"expected at least two distinct volume tiers, found {tiers}"


# ── 3. Валидность alerts.yml и обязательные поля правил ──────────────────────


def test_alerts_yaml_is_valid_mapping() -> None:
    """Сломанный YAML Prometheus просто не загрузит — правила замолчат все разом."""
    document = _alerts_document()
    assert document, "prometheus/alerts.yml is empty or not a YAML mapping"
    groups = document.get("groups")
    assert isinstance(groups, list) and groups, "alerts.yml has no `groups` list"
    for index, group in enumerate(groups):
        assert isinstance(group, dict), f"groups[{index}] is not a mapping"
        assert group.get("name"), f"groups[{index}] has no name"
        assert isinstance(group.get("rules"), list) and group["rules"], (
            f"group {group.get('name')!r} has no rules"
        )


def test_every_rule_has_the_house_required_keys() -> None:
    """
    alert / expr / for / labels.severity / annotations.summary — ключ И пригодное значение.

    Пропущенный `for` — алерт стреляет с одного скрейпа; пропущенный `severity` — правило
    не матчится роутом alertmanager и уходит в никуда; пустой `summary` — уведомление
    без содержания. Все три деградируют молча.

    Присутствия ключа мало: `alert: ""` или `expr: ""` — тоже мёртвое правило, а ключ
    на месте. Поэтому проверяются значения.
    """
    problems: list[str] = []
    for rule in _alert_rules():
        missing = [key for key in REQUIRED_TOP_LEVEL_KEYS if not rule.keys.count(key)]
        if missing:
            problems.append(f"{rule.where}: missing top-level keys {missing}")
        if not rule.name or rule.name == "?":
            problems.append(f"{rule.where}: `alert` is empty")
        if not rule.expr.strip():
            problems.append(f"{rule.where}: `expr` is empty")
        if "severity" not in rule.labels:
            problems.append(f"{rule.where}: labels.severity is missing")
        elif rule.severity not in KNOWN_SEVERITIES:
            problems.append(
                f"{rule.where}: labels.severity={rule.severity!r} is not one of "
                f"{sorted(KNOWN_SEVERITIES)} — alertmanager would not route it"
            )
        if "summary" not in rule.annotations:
            problems.append(f"{rule.where}: annotations.summary is missing")
        elif not (isinstance(rule.summary, str) and rule.summary.strip()):
            problems.append(f"{rule.where}: annotations.summary={rule.summary!r} is empty")
    assert not problems, "alerts.yml rules violate the required shape:\n  " + "\n  ".join(
        problems
    )


def test_every_rule_for_duration_is_usable() -> None:
    """
    `for` обязан быть разбираемой длительностью в разумных границах.

    Наличие ключа ничего не гарантирует: `for: 720h` присутствует — и правило не может
    сработать 30 суток, ровно та же тихая смерть, что и полностью отсутствующий `for`,
    только с другой стороны. Нижняя граница — дребезг на одиночных скрейпах
    (scrape_interval 15s), верхняя — дебаунс, переживающий само окно наблюдения.
    """
    problems: list[str] = []
    for rule in _alert_rules():
        seconds = _parse_duration_seconds(rule.for_raw)
        if seconds is None:
            problems.append(
                f"{rule.where}: `for: {rule.for_raw!r}` is not a Prometheus duration"
            )
        elif not (MIN_FOR_SECONDS <= seconds <= MAX_FOR_SECONDS):
            problems.append(
                f"{rule.where}: `for: {rule.for_raw}` = {seconds}s is outside "
                f"[{MIN_FOR_SECONDS}s, {MAX_FOR_SECONDS}s]. Below the floor the alert "
                "flaps on single scrapes; above the ceiling it silently cannot fire. "
                "If the debounce is deliberate, move the bound in this file."
            )
    assert not problems, "\n  ".join(["unusable `for` durations:", *problems])


# ── 1. Полнота: каждый workspace обслуживается каждым семейством ─────────────


def test_workspace_matchers_are_plain_enumerations() -> None:
    """
    Разбор партиции держится на том, что регулярка — это список слагов через `|`.
    Любой метасимвол (`.*`, группы, якоря) делает вывод о покрытии недостоверным,
    поэтому такой матчер обязан упасть, а не быть тихо пропущенным.
    """
    problems: list[str] = []
    for rule in _partitioning_rules():
        for matcher in rule.matchers:
            operator = matcher.operator
            if not matcher.alternatives:
                problems.append(f"{rule.where}: empty `workspace{operator}\"\"` matcher")
            for slug in matcher.alternatives:
                if not _SLUG.match(slug):
                    problems.append(
                        f"{rule.where}: `workspace{operator}` alternative {slug!r} is not a "
                        "plain slug; tier coverage can no longer be verified statically"
                    )
    assert not problems, "\n  ".join(["non-enumerable workspace matchers:", *problems])


def test_enumerations_use_a_regex_operator_not_exact_match() -> None:
    """
    `workspace="a|b"` — не перечисление, а литерал с трубой внутри.

    Prometheus сравнивает `=`/`!=` со значением лейбла ЦЕЛИКОМ: ни одна серия не несёт
    значение `"design|ai_research|ai_trends|auto_hmi"`, поэтому `=~` → `=` выключает
    низкообъёмные правила полностью (оба порога исчезают молча), а `!~` → `!=` наоборот
    делает высокообъёмное условие истинным для ВСЕХ workspace — и низкообъёмные начинают
    получать ещё и 6ч/12ч поверх своих. Обе правки не трогают ни одного слага, поэтому
    проверка «альтернатива похожа на слаг» их не видит.
    """
    problems: list[str] = []
    for rule in _partitioning_rules():
        for matcher in rule.matchers:
            if matcher.operator in _EXACT_OPS and "|" in matcher.pattern:
                problems.append(
                    f"{rule.where}: `workspace{matcher.operator}\"{matcher.pattern}\"` is an "
                    f"exact-match comparison against the literal string, pipes included — "
                    f"use {'!~' if matcher.operator == '!=' else '=~'} for an alternation"
                )
            elif matcher.operator in _EXACT_OPS and not _SLUG.match(matcher.pattern):
                problems.append(
                    f"{rule.where}: `workspace{matcher.operator}\"{matcher.pattern}\"` compares "
                    "against a literal that is not a workspace slug"
                )
    assert not problems, "\n  ".join(
        ["workspace alternations written with an exact-match operator:", *problems]
    )


def test_every_enumerated_slug_is_a_real_workspace() -> None:
    """
    Опечатка или переименование слага в регулярке — та же тихая деградация с другой стороны:
    workspace выпадает из своей ветки и наследует чужие пороги.
    """
    unknown = sorted(_enumerated_slugs() - _all_workspaces())
    assert not unknown, (
        f"alerts.yml enumerates workspaces that config/workspaces.yml does not define: "
        f"{unknown} (typo, or the workspace was renamed/removed). "
        f"Known ids: {sorted(_all_workspaces())}"
    )


def test_every_workspace_is_covered_by_every_partitioning_family() -> None:
    """
    Полнота. Для каждого семейства правил, которое партиционирует по `workspace`, каждый
    активный workspace обязан выбираться хотя бы одним выражением семейства.

    Ловит: слаг убрали из `=~`-ветки (низкообъёмной) — workspace перестал матчиться
    низкими порогами, а высокие его тоже не берут, потому что `!~` его исключает.
    Алерт по нему не сработает никогда.
    """
    universe = _active_workspaces()
    problems: list[str] = []
    for family, rules in _partition_families():
        for workspace in sorted(universe):
            hits = [rule for rule in rules if workspace in _rule_selection(rule, universe)]
            if not hits:
                problems.append(
                    f"{family}: workspace {workspace!r} is selected by NONE of the "
                    f"{len(rules)} expressions — it has no threshold at all. "
                    f"Add it to the tier enumeration (expressions: "
                    f"{[rule.where for rule in rules]})"
                )
    assert not problems, "\n  ".join(["workspaces left without an alert threshold:", *problems])


# ── 2. Дизъюнктность: ровно один tier ────────────────────────────────────────


def test_workspace_tier_partition_is_disjoint() -> None:
    """
    Один workspace — один tier. Слаг, попавший и в `!~`-, и в `=~`-список, матчится обоими
    выражениями сразу: два инстанса одного алерта с разными порогами и разной severity.
    """
    universe = _active_workspaces()
    problems: list[str] = []
    for family, rules in _partition_families():
        for workspace in sorted(universe):
            hits = [rule for rule in rules if workspace in _rule_selection(rule, universe)]
            if len(hits) > 1:
                problems.append(
                    f"{family}: workspace {workspace!r} is selected by {len(hits)} "
                    f"expressions at once — tiers {[rule.tier for rule in hits]} "
                    f"({[rule.where for rule in hits]}). It would double-alert."
                )
    for workspace in sorted(universe):
        tiers = _observed_tiers(workspace)
        if len(tiers) > 1:
            problems.append(
                f"workspace {workspace!r} lands in more than one volume tier: {sorted(tiers)}"
            )
    assert not problems, "\n  ".join(["workspace tier partition is not disjoint:", *problems])


def test_partitioning_rules_declare_a_tier_label() -> None:
    """Без label `tier` инстансы правил-близнецов неразличимы в alertmanager и в дежурстве."""
    untiered = [rule.where for _, rules in _partition_families() for rule in rules if not rule.tier]
    assert not untiered, f"workspace-partitioning rules without a `tier` label: {untiered}"


def test_rules_of_the_same_tier_select_the_same_workspaces() -> None:
    """
    Четыре выражения обязаны разъезжаться только порогами, не составом workspace.
    Warn и critical одного tier, обслуживающие разные наборы, — это дыра, в которой
    workspace получает предупреждение и не получает critical (или наоборот).
    """
    universe = _active_workspaces()
    by_tier: dict[str, list[tuple[str, frozenset[str]]]] = defaultdict(list)
    for _, rules in _partition_families():
        for rule in rules:
            by_tier[rule.tier or "<no tier label>"].append(
                (rule.where, _rule_selection(rule, universe))
            )
    problems: list[str] = []
    for tier, selections in sorted(by_tier.items()):
        distinct = {selection for _, selection in selections}
        if len(distinct) > 1:
            detail = ", ".join(
                f"{where} -> {sorted(selection)}" for where, selection in selections
            )
            problems.append(f"tier {tier!r} is inconsistent across expressions: {detail}")
    assert not problems, "\n  ".join(["tier membership drifted between rules:", *problems])


# ── Пороги: то, ЧТО label `tier` только обозначает ───────────────────────────
# Всё выше проверяет состав workspace внутри `{...}`. Ни один assert не смотрел на
# правую часть сравнения — а именно она и есть алерт. Дальше проверяется она.


def test_partitioning_rules_alert_on_the_declared_metric() -> None:
    """
    Имя серии под матчером workspace пришпилено реестром.

    Опечатка в имени метрики (`..._seconds` → `..._second`) не ломает ни YAML, ни матчер:
    выражение остаётся синтаксически целым, `_partitioning_rules()` продолжает его находить,
    покрытие и дизъюнктность по-прежнему «сходятся» — но серии не существует, выражение
    вечно пустое, и оба порога tier исчезают, не оставив следа.
    """
    problems: list[str] = []
    for family, rules in _partition_families():
        expected = EXPECTED_PARTITION_METRIC.get(family)
        if expected is None:
            problems.append(
                f"{family}: partitions workspaces but declares no metric in "
                "EXPECTED_PARTITION_METRIC. Add it, so a renamed series cannot pass silently."
            )
            continue
        for rule in rules:
            if rule.selector_metrics != (expected,):
                problems.append(
                    f"{rule.where}: workspace matcher sits on "
                    f"{list(rule.selector_metrics) or 'no metric'}, expected [{expected!r}]"
                )
    stale = sorted(set(EXPECTED_PARTITION_METRIC) - {name for name, _ in _partition_families()})
    assert not stale, f"EXPECTED_PARTITION_METRIC lists families that no longer exist: {stale}"
    assert not problems, "\n  ".join(["partitioning rules alert on the wrong series:", *problems])


def test_partitioning_thresholds_match_the_declared_tiers() -> None:
    """
    Порог и оператор сравнения каждого правила — явный реестр, не «как-нибудь».

    Ловит правку числа вместо слага: `> 86400` → `> 600` в низкообъёмной ветке — это
    ровно тот ложный алерт auto_hmi, ради которого файл написан, только пришедший
    с другой стороны. И инверсию `>` → `<`: алерт на устаревание данных, срабатывающий
    когда данные СВЕЖИЕ, и вечно молчащий, когда ingest встал.
    """
    problems: list[str] = []
    seen: set[tuple[str, str]] = set()
    for family, rules in _partition_families():
        for rule in rules:
            key = (family, rule.tier or "<no tier label>")
            seen.add(key)
            expected = EXPECTED_TIER_THRESHOLD_SECONDS.get(key)
            if expected is None:
                problems.append(
                    f"{rule.where}: no declared threshold for {key}. Add it to "
                    "EXPECTED_TIER_THRESHOLD_SECONDS so the number cannot drift unnoticed."
                )
                continue
            if rule.comparison != ">":
                problems.append(
                    f"{rule.where}: compares with {rule.comparison!r}, expected '>' — a "
                    "staleness alert must fire when the age EXCEEDS the threshold"
                )
            if rule.threshold != float(expected):
                problems.append(
                    f"{rule.where}: threshold is {rule.threshold} s, declared {expected} s "
                    f"for tier {rule.tier!r}"
                )
    stale = sorted(set(EXPECTED_TIER_THRESHOLD_SECONDS) - seen)
    assert not stale, (
        f"EXPECTED_TIER_THRESHOLD_SECONDS declares (alert, tier) pairs that alerts.yml no "
        f"longer contains: {stale}"
    )
    assert not problems, "\n  ".join(["declared thresholds disagree with alerts.yml:", *problems])


def test_low_volume_thresholds_are_looser_than_high_volume() -> None:
    """
    Смысл всего разделения на tier: низкообъёмный workspace штатно молчит ночью.

    Если пороги переставить местами, каждое отдельное правило останется «правильным»,
    label `tier` — на месте, состав workspace — на месте, а означать tier будут ровно
    противоположное. Проверяется отношение между числами, а не сами числа.
    """
    problems: list[str] = []
    for family, rules in _partition_families():
        by_tier = {rule.tier: rule for rule in rules if rule.tier}
        ordered = sorted(
            (tier for tier in by_tier if tier in TIER_LOOSENESS_ORDER),
            key=lambda tier: TIER_LOOSENESS_ORDER[tier],
        )
        for stricter, looser in zip(ordered, ordered[1:]):
            strict_rule, loose_rule = by_tier[stricter], by_tier[looser]
            if strict_rule.threshold is None or loose_rule.threshold is None:
                problems.append(f"{family}: cannot read thresholds of {stricter}/{looser}")
            elif not loose_rule.threshold > strict_rule.threshold:
                problems.append(
                    f"{family}: tier {looser!r} ({loose_rule.threshold} s) must be strictly "
                    f"looser than {stricter!r} ({strict_rule.threshold} s) — otherwise the "
                    "low-volume workspaces false-alarm on their first quiet night, which is "
                    "the incident this file exists for"
                )
    assert not problems, "\n  ".join(["volume tiers are inverted:", *problems])


def test_critical_threshold_is_above_the_warning_of_the_same_tier() -> None:
    """Critical, стреляющий раньше собственного warning, — сломанная эскалация."""
    by_tier: dict[str, dict[str, AlertRule]] = defaultdict(dict)
    for _, rules in _partition_families():
        for rule in rules:
            if rule.tier and rule.severity:
                by_tier[rule.tier][rule.severity] = rule
    problems: list[str] = []
    for tier, by_severity in sorted(by_tier.items()):
        warning, critical = by_severity.get("warning"), by_severity.get("critical")
        if warning is None or critical is None:
            problems.append(
                f"tier {tier!r} has no warning/critical pair: {sorted(by_severity)} — "
                "escalation cannot be verified"
            )
        elif warning.threshold is None or critical.threshold is None:
            problems.append(f"tier {tier!r}: thresholds are not statically readable")
        elif not critical.threshold > warning.threshold:
            problems.append(
                f"tier {tier!r}: critical fires at {critical.threshold} s, warning at "
                f"{warning.threshold} s — critical would fire first (or together)"
            )
    assert not problems, "\n  ".join(["broken warn -> critical escalation:", *problems])


# ── Реестр: седьмой workspace обязан упасть громко и в одном месте ───────────


def test_expected_tier_registry_matches_workspaces_yml() -> None:
    """
    Структурные проверки выше НЕ ловят добавление нового workspace: ветка
    `workspace!~"..."` — catch-all, любой незнакомый слаг она принимает молча и выдаёт ему
    самые жёсткие пороги (6ч/12ч). Ровно так проехал auto_hmi.

    Поэтому объёмный tier объявляется явно. Новый workspace обязан быть внесён сюда
    осознанно — это и есть «одно место», где добавление седьмого workspace падает громко.
    """
    declared = frozenset(EXPECTED_VOLUME_TIER)
    active = _active_workspaces()
    undeclared = sorted(active - declared)
    stale = sorted(declared - _all_workspaces())
    assert not undeclared, (
        f"workspaces without a declared volume tier: {undeclared}. A new workspace defaults "
        "into the negated (high-volume) branch of FrontierNoNewPosts and would false-alarm on "
        "its first quiet night. Decide the tier, add it to EXPECTED_VOLUME_TIER here, and add "
        "the slug to all four FrontierNoNewPosts/FrontierNoNewPostsCritical expressions."
    )
    assert not stale, (
        f"EXPECTED_VOLUME_TIER lists workspaces absent from config/workspaces.yml: {stale}"
    )
    unknown_tiers = sorted(
        {tier for tier in EXPECTED_VOLUME_TIER.values()}
        - {rule.tier for rule in _partitioning_rules() if rule.tier}
    )
    assert not unknown_tiers, (
        f"EXPECTED_VOLUME_TIER uses tier names that no alert rule declares: {unknown_tiers}"
    )


def test_declared_tier_matches_alerts_yml() -> None:
    """Объявленный tier обязан совпадать с тем, что реально выбирают выражения."""
    problems: list[str] = []
    for workspace in sorted(_active_workspaces()):
        expected = EXPECTED_VOLUME_TIER.get(workspace)
        if expected is None:
            continue  # покрыто тестом реестра выше
        observed = _observed_tiers(workspace)
        if observed != {expected}:
            problems.append(
                f"workspace {workspace!r}: declared {expected!r}, but alerts.yml puts it in "
                f"{sorted(observed) or 'no tier at all'}"
            )
    assert not problems, "\n  ".join(
        ["alerts.yml thresholds disagree with the declared volume tiers:", *problems]
    )
