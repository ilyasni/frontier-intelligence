"""
Контракт config/sources.yml и config/workspaces.yml — проверка БЕЗ импорта ingest.*.

Почему без импорта. `ingest/sources/base.py` тянет `bs4`, `ingest/sources/telegram_source.py` —
ещё и telethon с boto3; ни того, ни другого в тест-образе (admin) нет. Поэтому оба модуля
читаются как текст и разбираются через `ast` — тем же приёмом, каким
`tests/test_settings_contract.py` разбирает `shared/config.py`.

Ключевое следствие приёма: ожидания ниже не выписаны руками, а ВЫВЕДЕНЫ из кода. Имя блока
`filters` и набор его ключей берутся из тела `StructuredSource.matches_filters`; список
обязательных полей источника — из тела `bootstrap_sources_from_config`; список допустимых
`source_type` — из `shared/source_definitions.py`. Переименуют ключ в коде — ожидание
переедет вместе с ним, и красным станет ровно тот конфиг, который отстал.

Что здесь ловится. Все четыре отказа — молчаливые: ни один не бросает исключение в рантайме,
ни один не виден в логах, и существующий прогон их не замечал.

(a) Блок `filters` у telegram-источника МЁРТВ. `matches_filters` определён в
    `StructuredSource` (ingest/sources/base.py) и вызывается ровно из одного места —
    `StructuredSource.fetch`. `TelegramSource` наследуется от `AbstractSource` и пишет свой
    `fetch()` сам, мимо этого пути. Записанный, но не работающий фильтр хуже отсутствующего:
    следующий читатель уверен, что поток отфильтрован, и отсекает только LLM ниже по трубе.

(b) Источник со ссылкой на несуществующий workspace бутстрапится «в никуда»: строка в
    `sources` появится, а качать он будет в пространство, которого нет ни в одном отчёте.

(c) Два источника с одним `id` молча схлопываются: bootstrap делает
    `INSERT ... ON CONFLICT (id) DO UPDATE`, второй затирает первый. В ответе ручки —
    два «bootstrapped» id, в БД — одна строка.

(d) Ключ внутри `filters` должен совпадать с читаемым побуквенно. Опечатка (или ключ,
    придуманный по аналогии) не ломает ничего: `filters.get("...")` вернёт None,
    `or []` подставит пустой список, фильтр выключится. Получается нефильтрованный поток,
    который в конфиге выглядит отфильтрованным.
"""

from __future__ import annotations

import ast
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

# Корень ищем относительно файла теста: прогон идёт в контейнере с маунтом в /src,
# абсолютный путь разработчика тут не годится.
REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCES_YML = REPO_ROOT / "config" / "sources.yml"
WORKSPACES_YML = REPO_ROOT / "config" / "workspaces.yml"
INGEST_BASE_PY = REPO_ROOT / "ingest" / "sources" / "base.py"
TELEGRAM_SOURCE_PY = REPO_ROOT / "ingest" / "sources" / "telegram_source.py"
BOOTSTRAP_PY = REPO_ROOT / "admin" / "backend" / "services" / "bootstrap_configs.py"
SOURCE_DEFINITIONS_PY = REPO_ROOT / "shared" / "source_definitions.py"

ALL_SOURCE_FILES = (
    SOURCES_YML,
    WORKSPACES_YML,
    INGEST_BASE_PY,
    TELEGRAM_SOURCE_PY,
    BOOTSTRAP_PY,
    SOURCE_DEFINITIONS_PY,
)

# ── Реестр известного дефекта (a) ────────────────────────────────────────────
# Девять telegram-источников, у которых блок filters УЖЕ записан и УЖЕ не работает.
# Четыре из них включены и качают прямо сейчас: tg_ru_nami_russia, tg_ru_yandex_auto,
# tg_ru_sber_auto, tg_ru_rbc_auto — их include/exclude_keywords и lang_allow не
# применяются ни разу. Реестр существует не чтобы узаконить это, а чтобы:
#   * НОВЫЙ telegram-источник с filters стал красным (test_no_new_telegram_source_...);
#   * реестр не сгнил — вычистили блок, но забыли строку здесь, тоже красное
#     (test_telegram_dead_filters_ledger_is_current).
# Правильная починка — удалить блоки из YAML (или научить TelegramSource фильтровать,
# тогда красным станет test_telegram_filters_are_still_dead и реестр надо снести целиком).
TELEGRAM_DEAD_FILTERS_LEDGER: frozenset[str] = frozenset(
    {
        "tg_ru_autoruonline",
        "tg_ru_autostatis",
        "tg_ru_russianev",
        "tg_ru_avtovaz_official",
        "tg_ru_nami_russia",
        "tg_ru_yandex_auto",
        "tg_ru_sber_auto",
        "tg_ru_izvestia_auto",
        "tg_ru_rbc_auto",
    }
)

# ── Ожидаемое состояние батча auto_* (задача 6) ──────────────────────────────
# Значения сверены с файлом 2026-08-03.
#
# РАСХОЖДЕНИЕ УСТРАНЕНО 2026-08-03. Раньше здесь стояло `arxiv: False`, и рядом было
# написано, что в БД источник уже true, а в YAML false — то есть ближайший
# bootstrap_sources_from_config молча выключил бы его обратно, потому что UPSERT идёт
# с `is_enabled = EXCLUDED.is_enabled` (admin/backend/services/bootstrap_configs.py:
# YAML всегда побеждает). Тест покраснел ровно в тот момент, когда YAML догнал БД, —
# это и был момент осознанной правки. Теперь обе стороны говорят true.
#
# Правило на будущее: включение источника — это ДВА действия, PATCH .../toggle и правка
# YAML. Одного PATCH недостаточно, он живёт только до следующего bootstrap.
AUTO_BATCH_EXPECTED_ENABLED: dict[str, bool] = {
    # включён первым в раскате auto_hmi; в БД тоже true
    "auto_rss_arxiv_cs_hc_automotive": True,
    "auto_rss_techcrunch_transportation": False,
    "auto_rss_insideevs_ux": False,
    "auto_web_ieee_spectrum_autonomous": False,
    "auto_web_automotiveworld_sdv": False,
    "auto_tg_ru_atom": False,
    "auto_tg_ru_rusautomobile": False,
    "auto_tg_ru_avtovoz": False,
    "auto_tg_ru_atelega": False,
    "auto_tg_ru_mashinatory": False,
}

# `bool(src.get("is_enabled", True))` — bootstrap_configs.py: отсутствие ключа = включён.
IS_ENABLED_DEFAULT = True


# ── Загрузка YAML ────────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _sources() -> tuple[dict[str, Any], ...]:
    """Список источников как есть. Именно список: дубли id в нём сохраняются."""
    data = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8")) or {}
    return tuple(data.get("sources") or [])


@lru_cache(maxsize=1)
def _workspace_ids() -> frozenset[str]:
    data = yaml.safe_load(WORKSPACES_YML.read_text(encoding="utf-8")) or {}
    return frozenset(str(ws["id"]) for ws in (data.get("workspaces") or []))


def _source_id(src: dict[str, Any]) -> str:
    return str(src.get("id", "<без id>"))


def _extra(src: dict[str, Any]) -> dict[str, Any]:
    extra = src.get("extra")
    return extra if isinstance(extra, dict) else {}


def _is_enabled(src: dict[str, Any]) -> bool:
    return bool(src.get("is_enabled", IS_ENABLED_DEFAULT))


# ── Разбор кода через ast ────────────────────────────────────────────────────


def _named_function(tree: ast.Module, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _method(
    tree: ast.Module, class_name: str, method_name: str
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for stmt in node.body:
            if (
                isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef))
                and stmt.name == method_name
            ):
                return stmt
    return None


def _dot_get_calls(node: ast.AST) -> list[tuple[ast.expr, str]]:
    """Вызовы вида `<expr>.get("литерал")` внутри узла: пары (владелец, ключ)."""
    found: list[tuple[ast.expr, str]] = []
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call) or not sub.args:
            continue
        func = sub.func
        if not isinstance(func, ast.Attribute) or func.attr != "get":
            continue
        first = sub.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            found.append((func.value, first.value))
    return found


def _is_self_config(expr: ast.expr) -> bool:
    return (
        isinstance(expr, ast.Attribute)
        and expr.attr == "config"
        and isinstance(expr.value, ast.Name)
        and expr.value.id == "self"
    )


@lru_cache(maxsize=1)
def _filters_contract() -> tuple[str, frozenset[str]]:
    """
    Настоящий контракт фильтра: (имя блока в config, ключи внутри блока).

    Читается из тела `StructuredSource.matches_filters`: `self.config.get("<блок>")`
    даёт имя блока, все остальные `.get("...")` в том же теле — его ключи.
    """
    tree = ast.parse(INGEST_BASE_PY.read_text(encoding="utf-8"))
    func = _method(tree, "StructuredSource", "matches_filters")
    assert func is not None, "StructuredSource.matches_filters не найден в ingest/sources/base.py"
    blocks: set[str] = set()
    keys: set[str] = set()
    for owner, key in _dot_get_calls(func):
        if _is_self_config(owner):
            blocks.add(key)
        else:
            keys.add(key)
    assert len(blocks) == 1, f"ожидался ровно один self.config.get(...) в matches_filters: {blocks}"
    return blocks.pop(), frozenset(keys)


@lru_cache(maxsize=1)
def _bootstrap_source_keys() -> tuple[frozenset[str], frozenset[str]]:
    """
    Что bootstrap_sources_from_config читает у записи источника.

    Возвращает (обязательные, все читаемые):
      * обязательные — обращения `src["ключ"]`; отсутствие такого ключа = KeyError,
        и бутстрап падает на середине списка, оставив часть источников незалитой;
      * все читаемые — плюс `src.get("ключ")`; любой ключ вне этого множества
        бутстрап не видит вообще, он молча пропадает по дороге в БД.
    """
    tree = ast.parse(BOOTSTRAP_PY.read_text(encoding="utf-8"))
    func = _named_function(tree, "bootstrap_sources_from_config")
    assert func is not None, "bootstrap_sources_from_config не найден"
    required: set[str] = set()
    for sub in ast.walk(func):
        if not isinstance(sub, ast.Subscript):
            continue
        owner, key = sub.value, sub.slice
        if isinstance(owner, ast.Name) and owner.id == "src":
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                required.add(key.value)
    optional = {
        key
        for owner, key in _dot_get_calls(func)
        if isinstance(owner, ast.Name) and owner.id == "src"
    }
    return frozenset(required), frozenset(required | optional)


def _module_level_value(tree: ast.Module, name: str) -> ast.expr | None:
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == name:
                return node.value
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return node.value
    return None


@lru_cache(maxsize=1)
def _source_type_contract() -> tuple[frozenset[str], frozenset[str]]:
    """(канонические source_type, ключи RSS_PRESETS) из shared/source_definitions.py."""
    tree = ast.parse(SOURCE_DEFINITIONS_PY.read_text(encoding="utf-8"))
    literals: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            value = node.value
            if (
                isinstance(target, ast.Name)
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            ):
                literals[target.id] = value.value
    canonical_node = _module_level_value(tree, "CANONICAL_SOURCE_TYPES")
    assert isinstance(canonical_node, ast.Tuple), "CANONICAL_SOURCE_TYPES не найден как кортеж"
    canonical = {
        literals[el.id]
        for el in canonical_node.elts
        if isinstance(el, ast.Name) and el.id in literals
    }
    presets_node = _module_level_value(tree, "RSS_PRESETS")
    assert isinstance(presets_node, ast.Dict), "RSS_PRESETS не найден как словарь"
    presets = {
        key.value
        for key in presets_node.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }
    return frozenset(canonical), frozenset(presets)


def _sources_with_filters() -> list[tuple[dict[str, Any], dict[str, Any]]]:
    block, _ = _filters_contract()
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for src in _sources():
        raw = _extra(src).get(block)
        if raw is not None:
            pairs.append((src, raw if isinstance(raw, dict) else {"<не словарь>": raw}))
    return pairs


# ── Самопроверка извлекателей ────────────────────────────────────────────────
# Без неё всё, что ниже, зелено на пустых множествах: сломанный парсер превратил бы
# каждую проверку в тавтологию «пусто не нарушает ничего».


def test_source_files_exist() -> None:
    for path in ALL_SOURCE_FILES:
        assert path.is_file(), f"нет обязательного файла: {path}"


def test_extractors_are_not_vacuous() -> None:
    total = len(_sources())
    assert total > 100, f"в sources.yml разобрано подозрительно мало записей: {total}"
    assert len(_workspace_ids()) >= 5, f"воркспейсов разобрано мало: {sorted(_workspace_ids())}"

    block, keys = _filters_contract()
    assert block == "filters", f"блок фильтра в matches_filters называется {block!r}, не 'filters'"
    assert len(keys) >= 3, f"ключей фильтра извлечено мало: {sorted(keys)}"

    required, readable = _bootstrap_source_keys()
    assert required, "не извлечено ни одного обязательного ключа bootstrap"
    assert required <= readable and len(readable) > len(required)

    canonical, presets = _source_type_contract()
    assert len(canonical) >= 4, f"канонических source_type извлечено мало: {sorted(canonical)}"
    assert len(presets) > 10, f"RSS_PRESETS извлечён подозрительно маленьким: {len(presets)}"

    assert _sources_with_filters(), "ни одного источника с блоком filters — извлекатель сломан"
    assert len(AUTO_BATCH_EXPECTED_ENABLED) == 10


# ── (c) Уникальность id ──────────────────────────────────────────────────────


def test_source_ids_are_unique() -> None:
    """
    Дубль id схлопывается в UPSERT-е бутстрапа: вторая запись затирает первую.

    Отказ полностью бесшумный — ручка отвечает «bootstrapped: [id, id]», в БД одна строка
    с настройками того, кто в файле ниже.
    """
    counts = Counter(_source_id(src) for src in _sources())
    duplicates = {source_id: n for source_id, n in counts.items() if n > 1}
    assert not duplicates, (
        f"дублирующиеся id источников (bootstrap UPSERT молча оставит по одной записи): "
        f"{duplicates}"
    )


# ── (b) Ссылка на workspace ──────────────────────────────────────────────────


def test_every_source_points_at_an_existing_workspace() -> None:
    known = _workspace_ids()
    orphans = {
        _source_id(src): src.get("workspace_id")
        for src in _sources()
        if str(src.get("workspace_id")) not in known
    }
    assert not orphans, (
        f"источники ссылаются на workspace, которого нет в config/workspaces.yml "
        f"(будут качать в никуда): {orphans}. Известные: {sorted(known)}"
    )


# ── (a) Мёртвые фильтры у telegram ───────────────────────────────────────────


def _telegram_ids_with_filters() -> set[str]:
    return {
        _source_id(src)
        for src, _ in _sources_with_filters()
        if str(src.get("source_type")) == "telegram"
    }


def test_telegram_filters_are_still_dead() -> None:
    """
    Предпосылка реестра: TelegramSource действительно не проходит через matches_filters.

    Проверяется по коду, а не на веру: `TelegramSource` не наследник `StructuredSource`,
    и во всём модуле нет ни одного упоминания `matches_filters`. Если это перестанет быть
    правдой — фильтры у telegram заработают, и TELEGRAM_DEAD_FILTERS_LEDGER надо удалить
    вместе с этим тестом.
    """
    text = TELEGRAM_SOURCE_PY.read_text(encoding="utf-8")
    tree = ast.parse(text)
    bases: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "TelegramSource":
            bases = [b.id for b in node.bases if isinstance(b, ast.Name)]
            break
    assert bases, "класс TelegramSource не найден в ingest/sources/telegram_source.py"
    assert "StructuredSource" not in bases, (
        f"TelegramSource теперь наследует {bases} — фильтры у telegram могли ожить. "
        "Перепроверь и снеси TELEGRAM_DEAD_FILTERS_LEDGER, если так."
    )
    assert "matches_filters" not in text, (
        "в telegram_source.py появился matches_filters — фильтры у telegram больше не мёртвые, "
        "TELEGRAM_DEAD_FILTERS_LEDGER устарел"
    )


def test_no_new_telegram_source_carries_a_filters_block() -> None:
    """
    Главный сторож (a): новый telegram-источник с блоком filters — красное.

    Блок не применяется никогда, но в конфиге читается как «поток отфильтрован».
    """
    block, _ = _filters_contract()
    offenders = _telegram_ids_with_filters()
    new_offenders = sorted(offenders - TELEGRAM_DEAD_FILTERS_LEDGER)
    assert not new_offenders, (
        f"у telegram-источников появился блок {block!r}, который никогда не применится "
        f"(matches_filters живёт в StructuredSource, TelegramSource туда не заходит): "
        f"{new_offenders}. Убери блок — отсекать всё равно будет только LLM ниже по трубе."
    )


def test_telegram_dead_filters_ledger_is_current() -> None:
    """Реестр не должен гнить: вычистили блок — вычисти и строку в реестре."""
    offenders = _telegram_ids_with_filters()
    stale = sorted(TELEGRAM_DEAD_FILTERS_LEDGER - offenders)
    assert not stale, (
        f"в TELEGRAM_DEAD_FILTERS_LEDGER остались id, у которых блока filters больше нет: "
        f"{stale}. Удали их из реестра — иначе он перестанет описывать реальность."
    )


def test_auto_hmi_telegram_batch_carries_no_filters() -> None:
    """
    Батч auto_hmi заведён с явным намерением: у telegram-записей filters нет (комментарий
    в sources.yml это фиксирует). Здесь — без всякого реестра, жёстко.
    """
    block, _ = _filters_contract()
    offenders = sorted(
        _source_id(src)
        for src, _ in _sources_with_filters()
        if str(src.get("source_type")) == "telegram"
        and _source_id(src) in AUTO_BATCH_EXPECTED_ENABLED
    )
    assert not offenders, (
        f"telegram-записи батча auto_hmi получили блок {block!r}, который не применяется: "
        f"{offenders}"
    )


# ── (d) Ключи и значения внутри filters ──────────────────────────────────────


def test_filter_blocks_use_only_keys_matches_filters_reads() -> None:
    """
    Ключ, которого matches_filters не читает, не ломает ничего и не фильтрует ничего.

    Набор допустимых ключей извлекается из тела метода, а не выписан здесь: переименуют
    ключ в коде — красным станет конфиг, который отстал.
    """
    block, allowed = _filters_contract()
    unknown: dict[str, list[str]] = {}
    for src, filters in _sources_with_filters():
        extra_keys = sorted(str(key) for key in filters if str(key) not in allowed)
        if extra_keys:
            unknown[_source_id(src)] = extra_keys
    assert not unknown, (
        f"ключи в блоке {block!r}, которых StructuredSource.matches_filters не читает "
        f"(фильтр молча выключен): {unknown}. Читаются только: {sorted(allowed)}"
    )


def test_filter_values_are_lists_of_non_empty_strings() -> None:
    """
    Скаляр вместо списка — самый тихий из отказов этого блока.

    matches_filters итерирует значение: `include_keywords: ai` (строка вместо списка)
    развернётся в ['a', 'i'], и в выборку пройдёт почти всё — при том, что конфиг
    выглядит строго отфильтрованным.
    """
    block, _ = _filters_contract()
    bad: dict[str, list[str]] = {}
    for src, filters in _sources_with_filters():
        problems: list[str] = []
        for key, value in filters.items():
            if not isinstance(value, list):
                problems.append(f"{key}: {type(value).__name__} вместо list ({value!r})")
                continue
            for item in value:
                if not isinstance(item, str) or not item.strip():
                    problems.append(f"{key}: элемент {item!r} — не непустая строка")
        if problems:
            bad[_source_id(src)] = problems
    assert not bad, f"значения в блоке {block!r} должны быть списками непустых строк: {bad}"


def test_arxiv_auto_source_filters_sit_under_the_key_that_is_read() -> None:
    """
    Точечная проверка (d) для arXiv из батча auto_hmi — единственного источника, у которого
    отбор темы держится на include_keywords, а не на LLM ниже по трубе.

    Здесь важно не «фильтр есть», а «фильтр лежит там, откуда его читают»: блок `filters`
    внутри `extra`, ключ — тот самый, что называет matches_filters.
    """
    block, allowed = _filters_contract()
    include_key = "include_keywords"
    assert include_key in allowed, (
        f"matches_filters больше не читает {include_key!r} (читает {sorted(allowed)}) — "
        "проверка ниже потеряла смысл, поправь её вместе с конфигом"
    )
    source_id = "auto_rss_arxiv_cs_hc_automotive"
    matches = [src for src in _sources() if _source_id(src) == source_id]
    assert len(matches) == 1, f"ожидалась ровно одна запись {source_id}, найдено {len(matches)}"
    src = matches[0]

    assert block not in src, (
        f"{source_id}: блок {block!r} лежит на верхнем уровне записи. Бутстрап отдаёт в "
        f"нормализацию только src['extra'], верхнеуровневый блок теряется целиком."
    )
    filters = _extra(src).get(block)
    assert isinstance(filters, dict), f"{source_id}: нет extra.{block} ({filters!r})"
    keywords = filters.get(include_key)
    assert isinstance(keywords, list) and keywords, (
        f"{source_id}: extra.{block}.{include_key} — непустой список, а не {keywords!r}. "
        "Иначе поток arXiv идёт целиком, выглядя отфильтрованным."
    )
    assert all(isinstance(word, str) and word.strip() for word in keywords)


# ── (5) Поля, которые нужны бутстрапу ────────────────────────────────────────


def test_every_source_has_the_keys_bootstrap_requires() -> None:
    """
    Обязательные ключи выведены из `src["..."]` в bootstrap_sources_from_config.

    Отсутствие любого — KeyError на середине цикла: часть источников уже вставлена,
    коммита нет, ответ ручки — 500 без указания виновника.
    """
    required, _ = _bootstrap_source_keys()
    missing: dict[str, list[str]] = {}
    for src in _sources():
        gaps = sorted(key for key in required if key not in src)
        if gaps:
            missing[_source_id(src)] = gaps
    assert not missing, (
        f"источники без обязательных для бутстрапа ключей {sorted(required)}: {missing}"
    )


def test_no_source_carries_a_key_bootstrap_never_reads() -> None:
    """
    Ключ верхнего уровня, которого бутстрап не читает, теряется по дороге в БД молча.

    Сюда же попадёт `filters:`, положенный рядом с `extra` вместо места внутри него.
    """
    _, readable = _bootstrap_source_keys()
    ignored: dict[str, list[str]] = {}
    for src in _sources():
        strays = sorted(str(key) for key in src if str(key) not in readable)
        if strays:
            ignored[_source_id(src)] = strays
    assert not ignored, (
        f"ключи верхнего уровня, которые bootstrap_sources_from_config не читает "
        f"(молча пропадут): {ignored}. Читаются: {sorted(readable)}"
    )


def test_source_types_are_canonical() -> None:
    canonical, _ = _source_type_contract()
    bad = {
        _source_id(src): src.get("source_type")
        for src in _sources()
        if str(src.get("source_type")) not in canonical
    }
    assert not bad, (
        f"source_type вне CANONICAL_SOURCE_TYPES {sorted(canonical)}: {bad}. "
        "'habr' — устаревший алиас, пиши 'rss'."
    )


def test_type_specific_required_fields_are_present() -> None:
    """
    validate_source_payload бросает ValueError: telegram без tg_channel, rss/web/api без url.

    Для rss url может подставиться из RSS_PRESETS по имени пресета — этот путь учтён.
    """
    _, presets = _source_type_contract()
    problems: dict[str, str] = {}
    for src in _sources():
        source_type = str(src.get("source_type"))
        source_id = _source_id(src)
        if source_type == "telegram":
            if not src.get("tg_channel"):
                problems[source_id] = "telegram без tg_channel"
        elif source_type in {"rss", "web", "api"}:
            preset = str(_extra(src).get("preset") or "")
            preset_gives_url = source_type == "rss" and preset in presets
            if not src.get("url") and not preset_gives_url:
                problems[source_id] = f"{source_type} без url и без пресета с url"
    assert not problems, f"источники не пройдут validate_source_payload: {problems}"


# ── (6) Состояние батча auto_* ───────────────────────────────────────────────


def test_auto_batch_membership_is_exactly_ten_sources() -> None:
    """Добавили одиннадцатый auto_* — состав батча изменился осознанно, а не мимоходом."""
    found = sorted(
        _source_id(src) for src in _sources() if _source_id(src).startswith("auto_")
    )
    assert found == sorted(AUTO_BATCH_EXPECTED_ENABLED), (
        f"состав батча auto_* разошёлся с ожидаемым.\n"
        f"в файле:  {found}\n"
        f"ожидалось: {sorted(AUTO_BATCH_EXPECTED_ENABLED)}"
    )


def test_auto_batch_enabled_flags_match_the_recorded_state() -> None:
    """
    Зафиксированное состояние батча: девять выключены, arXiv включён (и в YAML, и в БД).

    Красное здесь означает одно из двух — либо флаг перевернули случайно (и тогда его надо
    вернуть), либо перевернули осознанно (и тогда надо поправить AUTO_BATCH_EXPECTED_ENABLED).
        Включение источника — два действия: PATCH /toggle и правка YAML. Тест ловит
        случай, когда сделали только первое, — до ближайшего bootstrap.
    """
    actual = {
        _source_id(src): _is_enabled(src)
        for src in _sources()
        if _source_id(src) in AUTO_BATCH_EXPECTED_ENABLED
    }
    drift = {
        source_id: {"в файле": actual.get(source_id), "ожидалось": expected}
        for source_id, expected in AUTO_BATCH_EXPECTED_ENABLED.items()
        if actual.get(source_id) != expected
    }
    assert not drift, f"is_enabled батча auto_* разошёлся с зафиксированным состоянием: {drift}"
