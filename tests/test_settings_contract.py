"""
Контракт env-алиасов Settings — проверка БЕЗ импорта shared.config.

Зачем отдельный файл и почему через ast.

tests/conftest.py подменяет пакет `pydantic_settings` заглушкой (`_BaseSettings` —
обычный класс на три строки). Заглушка нужна: pydantic_settings нет в тест-образах,
без неё падает сам импорт `shared.config`. Но следствие — под pytest класс
`Settings` НЕ является pydantic-моделью: у него нет `model_fields`, объявления
`Field(..., alias="X")` не обрабатываются, валидация не выполняется.

Практический риск: опечатка в алиасе (например `OWN_STAKE_ENABLD` вместо
`OWN_STAKE_ENABLED`) проходит весь прогон зелёной. Переменная окружения при этом
молча не привязывается, и фича живёт на дефолте — ровно тот класс отказа, что уже
случался в проекте (см. ops_llm_truncation_silent_fallback: молчаливый фолбэк).

Поэтому здесь `shared.config` не импортируется вообще: файл разбирается как текст
через `ast`. Результат не зависит ни от заглушки, ни от наличия pydantic_settings.
conftest.py при этом не трогаем — он обслуживает весь остальной прогон.
"""

from __future__ import annotations

import ast
import re
from functools import lru_cache
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

# Корень репозитория ищем относительно файла теста, а не по абсолютному пути:
# тесты гоняются и на хосте, и внутри образа (маунт в произвольную точку).
REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PY = REPO_ROOT / "shared" / "config.py"
COMPOSE_YML = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE = REPO_ROOT / ".env.example"

# Сервисы compose, которые поднимают НАШ Python и читают Settings.
# gpt2giga-proxy / xray / paddleocr / crawl4ai собираются здесь же, но это чужие
# рантаймы со своим словарём переменных — их env к Settings отношения не имеет.
SETTINGS_SERVICES: tuple[str, ...] = ("admin", "ingest", "mcp", "mcp-gateway", "worker")

# Где искать прямые чтения os.environ (env-ключи в обход Settings — легальный приём).
ENV_SCAN_DIRS: tuple[str, ...] = (
    "shared",
    "worker",
    "ingest",
    "admin",
    "mcp",
    "services",
    "scripts",
)

# Переменные, которые никто в репозитории не читает: их потребляют сторонние
# библиотеки и сам Docker. Список намеренно крошечный — всё остальное обязано
# быть либо алиасом Settings, либо явным os.environ-чтением.
EXTERNAL_RUNTIME_ENV: frozenset[str] = frozenset(
    {
        "HOSTNAME",  # подставляет Docker; читается только как имя контейнера в логах
        "XDG_CACHE_HOME",  # корень кэша для fastembed / huggingface
    }
)

# Алиасы задачи B: имена фиксируем побуквенно.
OWN_STAKE_ALIASES: tuple[str, ...] = (
    "QDRANT_OWN_CORPUS_COLLECTION",
    "OWN_STAKE_ENABLED",
    "OWN_STAKE_TOP_K",
    "OWN_STAKE_HIGH",
    "RELEVANCE_HIGH",
)

# Префиксы фич, документированных в .env.example.
DOCUMENTED_PREFIXES: tuple[str, ...] = (
    "OWN_STAKE",
    "QDRANT_OWN",
    "RELEVANCE_HIGH",
    "MISSING_SIGNALS",
    "SEARXNG",
)

# Строка вида `KEY=value`, в том числе закомментированная (`# KEY=value`).
_ENV_LINE = re.compile(r"^[\s#]*([A-Z][A-Z0-9_]*)=")


def _alias_strings(node: ast.expr) -> list[str]:
    """Вытащить строковые литералы из alias=... / validation_alias=AliasChoices(...)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Call):  # AliasChoices("A", "B")
        return [
            a.value
            for a in node.args
            if isinstance(a, ast.Constant) and isinstance(a.value, str)
        ]
    return []


def _is_class_var(annotation: ast.expr) -> bool:
    """ClassVar[...] — не поле настроек."""
    target = annotation.value if isinstance(annotation, ast.Subscript) else annotation
    return (getattr(target, "attr", None) or getattr(target, "id", None)) == "ClassVar"


@lru_cache(maxsize=1)
def _settings_fields() -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Поля класса Settings: (имя поля, объявленные алиасы). Разбор строго через ast."""
    tree = ast.parse(CONFIG_PY.read_text(encoding="utf-8"))
    fields: list[tuple[str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "Settings":
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
                continue
            name = stmt.target.id
            if name.startswith("_") or _is_class_var(stmt.annotation):
                continue
            aliases: list[str] = []
            if isinstance(stmt.value, ast.Call):
                func = stmt.value.func
                func_name = getattr(func, "attr", None) or getattr(func, "id", None)
                if func_name == "Field":
                    for kw in stmt.value.keywords:
                        if kw.arg in ("alias", "validation_alias"):
                            aliases.extend(_alias_strings(kw.value))
            fields.append((name, tuple(aliases)))
    return tuple(fields)


@lru_cache(maxsize=1)
def _settings_aliases() -> frozenset[str]:
    return frozenset(alias for _, aliases in _settings_fields() for alias in aliases)


@lru_cache(maxsize=1)
def _compose_environment() -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Ключи блоков environment каждого сервиса docker-compose.yml."""
    data = yaml.safe_load(COMPOSE_YML.read_text(encoding="utf-8")) or {}
    result: list[tuple[str, tuple[str, ...]]] = []
    for service, body in (data.get("services") or {}).items():
        env = (body or {}).get("environment")
        if isinstance(env, dict):
            keys = tuple(str(k) for k in env)
        elif isinstance(env, list):  # список "KEY=value"
            keys = tuple(str(item).split("=", 1)[0].strip() for item in env)
        else:
            keys = ()
        result.append((service, keys))
    return tuple(result)


@lru_cache(maxsize=1)
def _os_environ_reads() -> frozenset[str]:
    """Ключи, читаемые напрямую: os.environ["K"], os.environ.get("K"), os.getenv("K")."""
    keys: set[str] = set()
    for directory in ENV_SCAN_DIRS:
        root = REPO_ROOT / directory
        if not root.is_dir():
            continue
        for path in root.rglob("*.py"):
            if any(part in {".venv", "__pycache__", "node_modules"} for part in path.parts):
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Subscript):
                    owner = node.value
                    key = node.slice
                    if isinstance(owner, ast.Attribute) and owner.attr == "environ":
                        if isinstance(key, ast.Constant) and isinstance(key.value, str):
                            keys.add(key.value)
                elif isinstance(node, ast.Call) and node.args:
                    func = node.func
                    name = getattr(func, "attr", None) or getattr(func, "id", None)
                    is_read = name == "getenv"
                    if name == "get" and isinstance(func, ast.Attribute):
                        base = func.value
                        is_read = isinstance(base, ast.Attribute) and base.attr == "environ"
                    first = node.args[0]
                    if is_read and isinstance(first, ast.Constant) and isinstance(first.value, str):
                        keys.add(first.value)
    return frozenset(keys)


@lru_cache(maxsize=1)
def _documented_env_example_keys() -> frozenset[str]:
    """Ключи фич из .env.example (в т.ч. закомментированные и упомянутые в пояснениях)."""
    keys: set[str] = set()
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        match = _ENV_LINE.match(line)
        if match and match.group(1).startswith(DOCUMENTED_PREFIXES):
            keys.add(match.group(1))
    return frozenset(keys)


# ── Самопроверка извлекателя ─────────────────────────────────────────────────
# Без неё остальные тесты «зелёные» на пустом множестве: сломанный парсер молча
# превратил бы все проверки ниже в тавтологию.


def test_source_files_exist() -> None:
    for path in (CONFIG_PY, COMPOSE_YML, ENV_EXAMPLE):
        assert path.is_file(), f"missing required file: {path}"


def test_alias_extraction_is_not_vacuous() -> None:
    fields = _settings_fields()
    assert len(fields) > 200, f"expected 200+ Settings fields, ast found {len(fields)}"
    assert len(_settings_aliases()) > 200, "alias extraction returned an implausibly small set"
    assert len(_os_environ_reads()) > 10, "os.environ scan returned an implausibly small set"
    assert _documented_env_example_keys(), ".env.example feature-key extraction returned nothing"


def test_every_settings_field_declares_an_alias() -> None:
    """Поле без alias не привяжется к ожидаемой переменной окружения."""
    without = sorted(name for name, aliases in _settings_fields() if not aliases)
    assert not without, f"Settings fields without an explicit alias: {without}"


# ── Контракт ─────────────────────────────────────────────────────────────────


def test_no_duplicate_aliases() -> None:
    """Два поля с одним алиасом — одно из них молча не получит своё значение."""
    owner: dict[str, str] = {}
    collisions: dict[str, list[str]] = {}
    for name, aliases in _settings_fields():
        for alias in aliases:
            if alias in owner:
                collisions.setdefault(alias, [owner[alias]]).append(name)
            else:
                owner[alias] = name
    assert not collisions, f"alias declared by more than one field: {collisions}"


def test_settings_services_still_exist_in_compose() -> None:
    """Переименовали сервис — тест ниже перестал бы что-либо проверять."""
    present = {service for service, _ in _compose_environment()}
    missing = sorted(set(SETTINGS_SERVICES) - present)
    assert not missing, f"SETTINGS_SERVICES lists services absent from compose: {missing}"


def test_compose_env_keys_are_backed_by_config_or_explicit_read() -> None:
    """
    Каждый env-ключ наших сервисов должен быть либо алиасом Settings, либо
    явным os.environ-чтением, либо переменной чужого рантайма.

    Ловит опечатку с обеих сторон: в compose (WOKER_BATCH_SIZE) и в config.py
    (alias="OWN_STAKE_ENABLD") — второй случай «отвязывает» ключ compose, и он
    перестаёт находиться в множестве алиасов.
    """
    aliases = _settings_aliases()
    reads = _os_environ_reads()
    orphans: dict[str, list[str]] = {}
    for service, keys in _compose_environment():
        if service not in SETTINGS_SERVICES:
            continue
        unknown = [
            key
            for key in keys
            if key not in aliases and key not in reads and key not in EXTERNAL_RUNTIME_ENV
        ]
        if unknown:
            orphans[service] = sorted(unknown)
    assert not orphans, (
        "compose environment keys that no Settings alias and no os.environ read backs "
        f"(typo, or a field alias was renamed): {orphans}"
    )


def test_documented_feature_keys_exist_in_config() -> None:
    """own_stake / missing_signals / searxng: документация не должна расходиться с кодом."""
    documented = _documented_env_example_keys()
    aliases = _settings_aliases()
    missing = sorted(documented - aliases)
    assert not missing, (
        f".env.example documents keys that no Settings field declares: {missing}"
    )


def test_own_stake_aliases_present_by_exact_name() -> None:
    aliases = _settings_aliases()
    missing = [alias for alias in OWN_STAKE_ALIASES if alias not in aliases]
    assert not missing, f"own_stake aliases missing from shared/config.py: {missing}"
