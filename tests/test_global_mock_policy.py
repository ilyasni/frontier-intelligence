"""
Гард: в conftest не появляются глобальные заглушки опасных драйверов.

Парсит литералы в sys.modules.setdefault("...", ...) в tests/conftest.py.
"""
from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.stub_policy import FORBIDDEN_ROOT_MODULES, GLOBAL_MAGICMOCK_STUB_MODULES

# Файлы, где допустимо упоминание sys.modules.* в тексте (докстринги/комменты).
_ALLOW_TEXT_MENTION = frozenset(
    {
        Path("tests/stub_policy.py").as_posix(),
        Path("tests/test_global_mock_policy.py").as_posix(),
    }
)

_SYS_MODULES_BRACKET_ASSIGN = re.compile(
    r'sys\.modules\s*\[\s*["\'][a-zA-Z0-9_.]+["\']\s*\]\s*='
)


def _iter_project_py_files(root: Path) -> Iterator[tuple[Path, str]]:
    """(path, rel_posix) для .py под root, с исключением мусора и old_docs."""
    for path in root.rglob("*.py"):
        rel = path.relative_to(root)
        rel_s = rel.as_posix()
        parts = set(rel.parts)
        if (
            "docs" in parts
            or ".venv" in parts
            or ".git" in parts
            or "old_docs" in parts
            or "__pycache__" in parts
        ):
            continue
        yield path, rel_s


@pytest.mark.unit
def test_conftest_setdefault_targets_not_forbidden_drivers() -> None:
    """Корень имени модуля не из FORBIDDEN_ROOT_MODULES (neo4j, …)."""
    conftest = Path(__file__).resolve().parent / "conftest.py"
    text = conftest.read_text(encoding="utf-8")
    pattern = re.compile(r'sys\.modules\.setdefault\(\s*["\']([a-zA-Z0-9_.]+)["\']')
    found = pattern.findall(text)
    assert found, "expected sys.modules.setdefault(...) entries in conftest.py"
    for mod in found:
        root = mod.split(".", 1)[0]
        assert root not in FORBIDDEN_ROOT_MODULES, (
            f"Нельзя глобально подменять {mod!r}: добавь в tests/stub_policy.FORBIDDEN_ROOT_MODULES "
            f"и используй реальный драйвер в integration-тестах или локальный mock (monkeypatch)."
        )


@pytest.mark.unit
def test_conftest_imports_shared_stub_module_list() -> None:
    """conftest тянет список глобальных MagicMock из stub_policy (один источник правды)."""
    conftest = Path(__file__).resolve().parent / "conftest.py"
    text = conftest.read_text(encoding="utf-8")
    assert "from tests.stub_policy import GLOBAL_MAGICMOCK_STUB_MODULES" in text
    assert "for _mod in GLOBAL_MAGICMOCK_STUB_MODULES:" in text


@pytest.mark.unit
def test_global_magicmock_stub_list_unique_sorted_contract() -> None:
    """Нет дубликатов; запрещённые корни не попали в список plain MagicMock."""
    seen: set[str] = set()
    dups: list[str] = []
    for m in GLOBAL_MAGICMOCK_STUB_MODULES:
        if m in seen:
            dups.append(m)
        seen.add(m)
    assert not dups, f"Дубликаты в GLOBAL_MAGICMOCK_STUB_MODULES: {dups}"
    for m in GLOBAL_MAGICMOCK_STUB_MODULES:
        root = m.split(".", 1)[0]
        assert root not in FORBIDDEN_ROOT_MODULES, (
            f"{m!r} в GLOBAL_MAGICMOCK_STUB_MODULES конфликтует с FORBIDDEN_ROOT_MODULES"
        )


@pytest.mark.unit
def test_repo_no_sys_modules_setdefault_outside_conftest() -> None:
    """Глобальная подмена модулей только в tests/conftest.py."""
    root = Path(__file__).resolve().parents[1]
    offenders: list[str] = []
    for path, rel_s in _iter_project_py_files(root):
        if rel_s in _ALLOW_TEXT_MENTION:
            continue
        if path.name == "conftest.py" and path.parent.name == "tests":
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if "sys.modules.setdefault" in text:
            offenders.append(rel_s)
    assert not offenders, (
        "sys.modules.setdefault только в tests/conftest.py; иначе вынеси в conftest или "
        f"используй pytest monkeypatch в конкретном тесте. Найдено: {offenders}"
    )


@pytest.mark.unit
def test_repo_no_sys_modules_bracket_assign_outside_conftest() -> None:
    """sys.modules['pkg'] = … только в tests/conftest.py (обход setdefault)."""
    root = Path(__file__).resolve().parents[1]
    offenders: list[str] = []
    for path, rel_s in _iter_project_py_files(root):
        if rel_s in _ALLOW_TEXT_MENTION:
            continue
        if path.name == "conftest.py" and path.parent.name == "tests":
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if _SYS_MODULES_BRACKET_ASSIGN.search(line):
                offenders.append(f"{rel_s}: {stripped[:80]}")
                break
    assert not offenders, (
        "Присваивание sys.modules['…'] = … только в tests/conftest.py; "
        f"иначе monkeypatch. Найдено: {offenders}"
    )
