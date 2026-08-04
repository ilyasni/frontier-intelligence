"""
Контракт на ops-скрипты в `scripts/`: то, что деградирует молча и обнаруживается
в момент, когда инструмент понадобился.

Заведён 2026-08-04 по пункту 44 реестра. Разбор показал, что дефект был не один,
а два, и второй опаснее первого:

  1. Админка закрыта авторизацией (всё под `/api/` кроме health, login и вебхука
     Alertmanager отдаёт 401), а три скрипта ходили туда без credential.
  2. `server-reprocess-window.ps1` при этом печатал `reprocess ok N/N` независимо
     от 401, 500 и таймаута: строка была `ssh ... | Out-Null`, а в PowerShell 5.1
     ненулевой код возврата нативного exe НЕ бросает исключение даже под
     `$ErrorActionPreference = 'Stop'`.

Одна авторизация вылечила бы симптом и оставила механизм молчания на месте —
поэтому проверки ниже держат ОБА инварианта, а не только наличие заголовка.
Проверка, которая ищет лишь credential, на прежней версии скрипта прошла бы зелёной.

Плюс два класса отказа, уже стоившие времени на этом проекте и относящиеся ко всем
файлам каталога, а не к конкретному скрипту:

  * CRLF в `scripts/*.sh`. Bash падает на `set -o pipefail` с `\r` в конце строки,
    а локально файл выглядит нормально.
  * `.ps1` с кириллицей без BOM. PowerShell 5.1 читает такой файл как ANSI, байт
    из кириллицы становится «умной кавычкой» и рвёт строку. Так `sync-pull.ps1`
    не запускался вообще.

Разбор статический: тесты не импортируют скрипты и не ходят по сети.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"

# Публичные эндпоинты админки (admin/backend/main.py::_path_needs_auth).
# Скрипт, который трогает только их, credential не требует.
PUBLIC_ADMIN_PATHS: tuple[str, ...] = (
    "/api/health",
    "/api/auth/login",
    "/api/monitoring/alertmanager/webhook",
)

# Любой файл в scripts/, обращающийся к защищённому /api/ админки, обязан быть здесь.
# Реестр, а не только структурная проверка: новый скрипт не должен уметь тихо
# проехать мимо контракта, а «умное» автоопределение способа авторизации на трёх
# разных языках даёт больше ложных зелёных, чем пользы.
#
# Значение — маркеры, по которым видно, что скрипт (а) авторизуется и (б) не
# проглатывает отказ. Оба обязаны присутствовать.
ADMIN_API_CALLERS: dict[str, dict[str, tuple[str, ...]]] = {
    "reprocess-window.sh": {
        # `--config -` вместо `-u`: пароль уходит curl на stdin и не попадает в argv,
        # то есть не виден в `ps` и не оседает в истории.
        "auth": ("--config -", "ADMIN_PASSWORD"),
        # Маркеры намеренно точечные. Мутационный прогон 2026-08-04: замена
        # fail-closed-ветки на `admin_password=""` не покраснела, потому что голое
        # `exit 78` встречается в файле ещё и в ветке нечитаемого .env — маркер
        # находился, а защиты уже не было.
        "failure_is_loud": (
            "ADMIN_PASSWORD is empty or absent",
            'if [ "$bad" -gt 0 ]; then',
            "exit 1",
        ),
    },
    "server-reprocess-window.ps1": {
        "auth": ("reprocess-window.sh",),  # авторизуется серверная половина
        "failure_is_loud": ("Assert-LastExitCode", "$LASTEXITCODE -ne 0", "throw"),
    },
    "reprocess_done_for_sparse.py": {
        "auth": ("basic_auth_header",),
        "failure_is_loud": ("raise SystemExit", "failed += 1"),
    },
    "server_apply_vision_chain_policy.py": {
        "auth": ("basic_auth_header",),
        # urllib.request.urlopen сам бросает HTTPError на 4xx/5xx — отдельного
        # разбора кода не нужно, достаточно не глушить его широким except.
        "failure_is_loud": ("urlopen",),
    },
}

# Скрипты, которым авторизация не нужна: трогают только публичные пути.
PUBLIC_ONLY_CALLERS: frozenset[str] = frozenset({"server-pipeline-checks.sh"})

_ADMIN_API = re.compile(r"8101(?:/|\"|')?[^\s\"']*|/api/[a-z0-9_/{}$-]+", re.IGNORECASE)
_ADMIN_PORT = re.compile(r"\b8101\b")
_API_PATH = re.compile(r"/api/[a-zA-Z0-9_./{}$-]*")


@lru_cache(maxsize=1)
def _script_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            path
            for path in SCRIPTS_DIR.iterdir()
            if path.is_file() and path.suffix in {".sh", ".ps1", ".py"}
        )
    )


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _touches_protected_admin_api(path: Path) -> bool:
    """Скрипт обращается к защищённому пути админки."""
    text = _text(path)
    if not _ADMIN_PORT.search(text) and "ADMIN_API_BASE" not in text:
        return False
    paths = set(_API_PATH.findall(text))
    protected = {
        found
        for found in paths
        if not any(found.startswith(public) for public in PUBLIC_ADMIN_PATHS)
    }
    return bool(protected)


# ── Самопроверка извлекателей ────────────────────────────────────────────────
# Сломанный обходчик каталога превратил бы все проверки ниже в тавтологию
# на пустом множестве — ровно тот класс, ради которого файл и написан.


def test_scripts_directory_is_discovered() -> None:
    assert SCRIPTS_DIR.is_dir(), f"missing {SCRIPTS_DIR}"
    assert len(_script_files()) >= 20, (
        f"scripts/ parsed into {len(_script_files())} files — the walker is broken"
    )


def test_admin_api_detector_is_not_vacuous() -> None:
    """Детектор обязан находить хотя бы известных вызывающих."""
    found = {path.name for path in _script_files() if _touches_protected_admin_api(path)}
    assert found, (
        "no script was detected as calling the protected admin API, although "
        f"{sorted(ADMIN_API_CALLERS)} are known to do so — the detector is broken"
    )


# ── 1. Кодировки: два класса отказа, уже стоившие времени ────────────────────


def test_shell_scripts_use_lf_line_endings() -> None:
    """
    CRLF в `.sh` — не косметика. Bash разбирает `set -o pipefail\\r` как имя опции
    с возвратом каретки и падает на первой строке, а `git diff` и редактор
    показывают файл нормальным.
    """
    offenders = [
        path.name
        for path in _script_files()
        if path.suffix == ".sh" and b"\r" in path.read_bytes()
    ]
    assert not offenders, (
        f"shell scripts with CR bytes: {offenders}. They fail on the server at "
        "`set -o pipefail` while looking fine locally. Re-save with LF endings."
    )


def test_powershell_scripts_with_cyrillic_carry_a_bom() -> None:
    """
    PowerShell 5.1 читает BOM-less файл как ANSI. Байт 0x94 из кириллицы или
    типографики превращается в закрывающую «умную кавычку», парсер принимает её
    за конец строки, и скрипт не запускается вообще (так жил sync-pull.ps1).

    Требование адресное: чистый ASCII в BOM не нуждается, и заставлять его —
    лишний шум в диффах.
    """
    bom = b"\xef\xbb\xbf"
    offenders: list[str] = []
    for path in _script_files():
        if path.suffix != ".ps1":
            continue
        raw = path.read_bytes()
        try:
            raw.decode("ascii")
        except UnicodeDecodeError:
            if not raw.startswith(bom):
                offenders.append(path.name)
    assert not offenders, (
        f"non-ASCII .ps1 files without a UTF-8 BOM: {offenders}. PowerShell 5.1 will "
        "read them as ANSI and break on the first Cyrillic byte. Re-save as UTF-8 "
        "with BOM (see scripts/server-reprocess-window.ps1 for the shape)."
    )


# ── 2. Доступ к админке: авторизация И громкий отказ ─────────────────────────


def test_every_protected_admin_caller_is_registered() -> None:
    """
    Новый скрипт не должен уметь тихо проехать мимо контракта.

    Структурные проверки ниже разбирают только зарегистрированные файлы, поэтому
    незарегистрированный вызывающий не проверялся бы вовсе — та же дыра
    «catch-all принимает молча», что ловил EXPECTED_VOLUME_TIER в соседнем файле.
    """
    detected = {path.name for path in _script_files() if _touches_protected_admin_api(path)}
    unregistered = sorted(detected - set(ADMIN_API_CALLERS) - PUBLIC_ONLY_CALLERS)
    assert not unregistered, (
        f"scripts calling the protected admin API without an entry in "
        f"ADMIN_API_CALLERS: {unregistered}. Every /api/ path except "
        f"{list(PUBLIC_ADMIN_PATHS)} answers 401 — without credentials the script "
        "does nothing. Add the credential mechanism, then register it here."
    )


def test_registry_has_no_stale_entries() -> None:
    """Запись, пережившая свой скрипт, разрешает то, чего нет."""
    names = {path.name for path in _script_files()}
    stale = sorted((set(ADMIN_API_CALLERS) | PUBLIC_ONLY_CALLERS) - names)
    assert not stale, f"registry references scripts that no longer exist: {stale}"


def test_admin_api_callers_authenticate() -> None:
    problems: list[str] = []
    for name, markers in sorted(ADMIN_API_CALLERS.items()):
        path = SCRIPTS_DIR / name
        if not path.is_file():
            continue  # покрыто тестом устаревших записей
        text = _text(path)
        missing = [marker for marker in markers["auth"] if marker not in text]
        if missing:
            problems.append(f"{name}: no sign of authentication, missing {missing}")
    assert not problems, "\n  ".join(
        ["scripts calling the closed admin API without credentials:", *problems]
    )


def test_admin_api_callers_fail_loudly() -> None:
    """
    Главный инвариант файла, и единственный, который не проходил на прежней версии.

    Скрипт обязан не только авторизоваться, но и УПАСТЬ, когда вызов не удался.
    `server-reprocess-window.ps1` до правки авторизацию бы не прошёл, но и после
    добавления заголовка продолжал бы печатать «reprocess ok N/N» при 500: результат
    уходил в Out-Null, а `$LASTEXITCODE` никто не смотрел.
    """
    problems: list[str] = []
    for name, markers in sorted(ADMIN_API_CALLERS.items()):
        path = SCRIPTS_DIR / name
        if not path.is_file():
            continue
        text = _text(path)
        missing = [marker for marker in markers["failure_is_loud"] if marker not in text]
        if missing:
            problems.append(f"{name}: failure would pass unnoticed, missing {missing}")
    assert not problems, "\n  ".join(
        ["scripts that swallow admin API failures:", *problems]
    )


def test_powershell_ssh_results_are_checked() -> None:
    """
    Точечно про PowerShell, потому что здесь молчание — свойство языка, а не недосмотр.

    В PS 5.1 нативный exe с ненулевым кодом не бросает исключение и под
    `$ErrorActionPreference = 'Stop'`. Значит каждый вызов `ssh` обязан
    сопровождаться явной проверкой `$LASTEXITCODE`, а перенаправление результата
    в `Out-Null` без такой проверки — тот самый дефект.
    """
    problems: list[str] = []
    for path in _script_files():
        if path.suffix != ".ps1":
            continue
        lines = _text(path).splitlines()
        ssh_lines = [
            index
            for index, line in enumerate(lines)
            if re.search(r"(?<![\w-])ssh\s", line) and not line.strip().startswith("#")
        ]
        if not ssh_lines:
            continue
        body = "\n".join(lines)
        if "$LASTEXITCODE" not in body:
            problems.append(
                f"{path.name}: calls ssh on {len(ssh_lines)} line(s) and never inspects "
                "$LASTEXITCODE"
            )
        for index in ssh_lines:
            if "Out-Null" in lines[index] and "$LASTEXITCODE" not in "\n".join(
                lines[index : index + 3]
            ):
                problems.append(
                    f"{path.name}:{index + 1}: ssh result goes to Out-Null with no exit "
                    "code check within the next two lines — a 401 would print success"
                )
    assert not problems, "\n  ".join(
        ["PowerShell scripts that ignore remote command failures:", *problems]
    )
