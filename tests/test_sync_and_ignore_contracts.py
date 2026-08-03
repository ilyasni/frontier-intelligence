"""
Контракт .rsync-exclude и .gitignore — защита секретов от синхронизации и коммита.

ПОЧЕМУ ЭТОТ ФАЙЛ СУЩЕСТВУЕТ (два реальных отказа, 2026-08-03)

  (a) .rsync-exclude защищал `searxng/settings.yml` ТОЧНЫМ ПУТЁМ. Ручной снапшот
      `searxng/settings.yml.bak-before-engines-20260803` с тем же живым secret_key
      под правило не подпадал. `sync-push --delete` с рабочей станции снёс три
      таких серверных точки отката. С тем же успехом он мог утащить снапшот в
      обратную сторону — в репозиторий.
  (b) .gitignore имел ровно ту же дыру: `git add .` унёс бы боевой ключ в историю.

Оба отказа молчаливые: ни rsync, ни git о них не сообщают. Заметно становится
только по факту — когда точка отката нужна, а её нет.

ДВЕ ПРОТИВОПОЛОЖНЫЕ СЕМАНТИКИ — ГЛАВНОЕ, ЧТО ЗДЕСЬ ПРОВЕРЯЕТСЯ

  rsync (--exclude-from):  решает ПЕРВОЕ совпавшее правило (first-match-wins).
      Отмена исключения — правило `+ pattern` (плюс и ПРОБЕЛ), и оно обязано
      стоять ВЫШЕ широкого шаблона, который иначе поглотил бы файл. Строка
      `!pattern` не отменяет НИЧЕГО: rsync читает её как обычный шаблон с именем
      «!pattern» и молча ни с чем не сопоставляет. Отсюда ловушка — правка
      `+ .env.example` → `!.env.example` снимает защиту без единого сообщения.

  .gitignore:  решает ПОСЛЕДНЕЕ совпавшее правило (last-match-wins).
      Отмена — префикс `!`, и он обязан стоять НИЖЕ широкого шаблона. Плюс
      жёсткое ограничение: файл нельзя вернуть `!`-правилом, если исключён его
      РОДИТЕЛЬСКИЙ каталог — git внутрь просто не спускается.

  Порядки обратные: один и тот же набор строк в двух файлах даёт разный
  результат. Поэтому матчеры ниже реализованы раздельно, а тест
  `test_match_orders_are_opposite` доказывает, что они действительно разные —
  без него обе реализации могли бы деградировать в одну (неверную), и весь
  остальной файл превратился бы в зелёную тавтологию.

Ни rsync, ни git здесь не вызываются: в тест-образе (admin) их нет, а тесты
обязаны быть офлайновыми. Матчинг реализован прямо в этом файле.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

# Корень репозитория ищем относительно файла теста, а не по абсолютному пути:
# тесты гоняются и на хосте, и внутри образа (маунт в произвольную точку).
REPO_ROOT = Path(__file__).resolve().parents[1]
RSYNC_EXCLUDE = REPO_ROOT / ".rsync-exclude"
GITIGNORE = REPO_ROOT / ".gitignore"


# ── Трансляция glob → regex (общая для обоих синтаксисов) ────────────────────
# `*` не переходит через `/`, `**` переходит, `/**/` допускает ноль каталогов.

_GLOB_TOKEN = re.compile(r"/\*\*/|\*\*|\*|\?|\[[^\]]*\]")


def _glob_body(pattern: str) -> str:
    """Тело регулярного выражения для одного glob-шаблона (без якорей)."""
    parts: list[str] = []
    pos = 0
    for match in _GLOB_TOKEN.finditer(pattern):
        parts.append(re.escape(pattern[pos : match.start()]))
        token = match.group(0)
        if token == "/**/":
            parts.append("/(?:.*/)?")
        elif token == "**":
            parts.append(".*")
        elif token == "*":
            parts.append("[^/]*")
        elif token == "?":
            parts.append("[^/]")
        else:  # символьный класс [abc] / [!abc]
            inner = token[1:-1]
            if inner.startswith("!"):
                inner = "^" + inner[1:]
            parts.append("[" + inner + "]")
        pos = match.end()
    parts.append(re.escape(pattern[pos:]))
    return "".join(parts)


@dataclass(frozen=True)
class Rule:
    """Одно правило файла-фильтра."""

    lineno: int
    raw: str
    pattern: str
    dir_only: bool
    # rsync: правило начиналось с «+ ». gitignore: правило начиналось с «!».
    reinclude: bool
    regex: re.Pattern[str]

    def matches(self, path: str, is_dir: bool) -> bool:
        if self.dir_only and not is_dir:
            return False
        return self.regex.match(path) is not None


def _compile_rule(
    lineno: int,
    raw: str,
    pattern: str,
    reinclude: bool,
    *,
    anchored_by_slash: bool,
) -> Rule:
    """
    anchored_by_slash=True (gitignore): шаблон со слэшем внутри привязан к корню.
    anchored_by_slash=False (rsync): без ведущего «/» шаблон сопоставляется
    с ХВОСТОМ пути, слэш внутри якоря не даёт.
    """
    dir_only = pattern.endswith("/")
    body = pattern[:-1] if dir_only else pattern
    rooted = body.startswith("/")
    if rooted:
        body = body[1:]
    if anchored_by_slash and "/" in body:
        rooted = True
    prefix = "^" if rooted else "(?:^|.*/)"
    return Rule(
        lineno=lineno,
        raw=raw,
        pattern=pattern,
        dir_only=dir_only,
        reinclude=reinclude,
        regex=re.compile(prefix + _glob_body(body) + r"\Z"),
    )


# ── Разбор файлов ────────────────────────────────────────────────────────────


def parse_rsync_rules(text: str) -> list[Rule]:
    """
    Правила rsync-фильтра. `+ ` — включение, `- ` и голый шаблон — исключение.

    Строка `!pattern` НАМЕРЕННО не считается отрицанием: разбирается как обычный
    шаблон с именем «!pattern», ровно как это делает rsync. Именно из-за такого
    молчаливого поведения существует test_rsync_exclude_has_no_bang_lines.
    """
    rules: list[Rule] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        reinclude = False
        pattern = line
        if line[:2] in ("+ ", "- "):
            reinclude = line.startswith("+ ")
            pattern = line[2:].strip()
        if not pattern:
            continue
        rules.append(
            _compile_rule(lineno, raw, pattern, reinclude, anchored_by_slash=False)
        )
    return rules


def parse_gitignore_rules(text: str) -> list[Rule]:
    """Правила .gitignore. `!` — возврат файла обратно в индексируемые."""
    rules: list[Rule] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        reinclude = line.startswith("!")
        pattern = line[1:] if reinclude else line
        if not pattern:
            continue
        rules.append(
            _compile_rule(lineno, raw, pattern, reinclude, anchored_by_slash=True)
        )
    return rules


def _path_levels(path: str) -> list[tuple[str, bool]]:
    """
    Путь по уровням: ('docs', True) → ('docs/ops', True) → ('docs/ops/x.md', False).

    Уровни нужны обоим матчерам: и rsync, и git не спускаются внутрь каталога,
    исключённого целиком, поэтому решение может приниматься на любом уровне.
    """
    explicit_dir = path.endswith("/")
    parts = [part for part in path.strip("/").split("/") if part]
    levels: list[tuple[str, bool]] = []
    for idx in range(1, len(parts) + 1):
        levels.append(("/".join(parts[:idx]), idx < len(parts) or explicit_dir))
    return levels


# ── Матчеры ──────────────────────────────────────────────────────────────────


def rsync_excluded(rules: list[Rule], path: str) -> tuple[bool, Rule | None]:
    """rsync: ПЕРВОЕ совпавшее правило решает судьбу пути."""
    for level, is_dir in _path_levels(path):
        for rule in rules:
            if not rule.matches(level, is_dir):
                continue
            if rule.reinclude:
                break  # уровень явно включён — правила ниже не смотрим, идём глубже
            return True, rule
    return False, None


def gitignore_ignored(rules: list[Rule], path: str) -> tuple[bool, Rule | None]:
    """gitignore: ПОСЛЕДНЕЕ совпавшее правило решает; родителя `!` не вернуть."""
    for level, is_dir in _path_levels(path):
        decisive: Rule | None = None
        for rule in rules:
            if rule.matches(level, is_dir):
                decisive = rule
        if decisive is not None and not decisive.reinclude:
            return True, decisive
    return False, None


@lru_cache(maxsize=1)
def _rsync_rules() -> tuple[Rule, ...]:
    return tuple(parse_rsync_rules(RSYNC_EXCLUDE.read_text(encoding="utf-8")))


@lru_cache(maxsize=1)
def _gitignore_rules() -> tuple[Rule, ...]:
    return tuple(parse_gitignore_rules(GITIGNORE.read_text(encoding="utf-8")))


def _is_excluded(path: str) -> tuple[bool, Rule | None]:
    return rsync_excluded(list(_rsync_rules()), path)


def _is_ignored(path: str) -> tuple[bool, Rule | None]:
    return gitignore_ignored(list(_gitignore_rules()), path)


def _describe(rule: Rule | None) -> str:
    return "no rule matched" if rule is None else f"line {rule.lineno}: {rule.raw.strip()!r}"


# ── Самопроверка матчеров ────────────────────────────────────────────────────
# Без этих трёх тестов остальной файл может быть зелёным на сломанной
# реализации: матчер, который «исключает всё», пройдёт половину проверок,
# а матчер, который не исключает ничего, — вторую.


def test_ignore_files_exist() -> None:
    for path in (RSYNC_EXCLUDE, GITIGNORE):
        assert path.is_file(), f"missing required file: {path}"


def test_rule_parsing_is_not_vacuous() -> None:
    rsync_rules = _rsync_rules()
    git_rules = _gitignore_rules()
    assert len(rsync_rules) > 40, f".rsync-exclude parsed into only {len(rsync_rules)} rules"
    assert len(git_rules) > 40, f".gitignore parsed into only {len(git_rules)} rules"
    plus_rules = [rule for rule in rsync_rules if rule.reinclude]
    assert len(plus_rules) >= 3, (
        f".rsync-exclude should carry at least 3 '+ ' rules, parsed {len(plus_rules)}"
    )
    bang_rules = [rule for rule in git_rules if rule.reinclude]
    assert len(bang_rules) >= 3, (
        f".gitignore should carry at least 3 '!' rules, parsed {len(bang_rules)}"
    )
    # Комментарии не должны попадать в правила: иначе «# .env» защищал бы .env.
    assert not [rule for rule in rsync_rules + git_rules if rule.pattern.startswith("#")]


def test_match_orders_are_opposite() -> None:
    """
    rsync = first-match-wins, gitignore = last-match-wins. Порядки обратные.

    Один и тот же порядок строк даёт в двух файлах ПРОТИВОПОЛОЖНЫЙ результат —
    это и есть содержание теста. Если кто-то унифицирует матчеры «чтобы не
    дублировать код», тест покраснеет.
    """
    plus_above = parse_rsync_rules("+ keep.txt\n*.txt\n")
    plus_below = parse_rsync_rules("*.txt\n+ keep.txt\n")
    assert rsync_excluded(plus_above, "keep.txt")[0] is False, "rsync: '+ ' above must win"
    assert rsync_excluded(plus_below, "keep.txt")[0] is True, (
        "rsync: '+ ' placed below the broad pattern must NOT rescue the file"
    )

    bang_below = parse_gitignore_rules("*.txt\n!keep.txt\n")
    bang_above = parse_gitignore_rules("!keep.txt\n*.txt\n")
    assert gitignore_ignored(bang_below, "keep.txt")[0] is False, "gitignore: '!' below must win"
    assert gitignore_ignored(bang_above, "keep.txt")[0] is True, (
        "gitignore: '!' placed above the broad pattern must NOT rescue the file"
    )

    # gitignore: исключённый родитель не возвращается `!`-правилом.
    parent_excluded = parse_gitignore_rules("secrets/\n!secrets/keep.txt\n")
    assert gitignore_ignored(parent_excluded, "secrets/keep.txt")[0] is True, (
        "gitignore: a file under an excluded directory cannot be re-included"
    )


def test_rsync_treats_bang_line_as_a_silent_no_op() -> None:
    """
    Ловушка из шапки .rsync-exclude: `!pattern` в rsync — не отрицание.

    Тест фиксирует именно молчаливость: файл остаётся исключённым, хотя автор
    правки был уверен, что вернул его.
    """
    with_bang = parse_rsync_rules("!keep.txt\n*.txt\n")
    excluded, rule = rsync_excluded(with_bang, "keep.txt")
    assert excluded is True, (
        "'!keep.txt' must NOT behave as a negation in rsync syntax; "
        "if this fails, the matcher no longer reproduces the real trap"
    )
    assert rule is not None and rule.pattern == "*.txt", _describe(rule)


# ── .rsync-exclude: что обязано не уезжать и не приезжать ────────────────────

RSYNC_MUST_EXCLUDE: tuple[str, ...] = (
    ".env",
    "sessions/anything.session",
    "private.key",
    "x.pem",
    "searxng/settings.yml",
    # Инцидент (a): ручной снапшот с тем же живым secret_key.
    "searxng/settings.yml.bak-20260803",
    "backups/x",
    "runtime/x",
    # Серверные артефакты: их локально нет, --delete снёс бы историю.
    "docs/ops/alert-digests/2026-08-01.md",
    "prometheus/textfile/x.prom",
)

RSYNC_MUST_KEEP: tuple[str, ...] = (
    ".env.example",
    ".env.balanced.example",
    "searxng/settings.example.yml",
    "docker-compose.yml",
    "shared/config.py",
)


@pytest.mark.parametrize("path", RSYNC_MUST_EXCLUDE)
def test_rsync_exclude_covers_secrets_and_server_only_paths(path: str) -> None:
    excluded, rule = _is_excluded(path)
    assert excluded, (
        f"{path!r} is NOT excluded by .rsync-exclude: a --delete push would carry it "
        f"across (or wipe the server copy). Decision: {_describe(rule)}"
    )


@pytest.mark.parametrize("path", RSYNC_MUST_KEEP)
def test_rsync_exclude_keeps_repository_files(path: str) -> None:
    excluded, rule = _is_excluded(path)
    assert not excluded, (
        f"{path!r} IS excluded by .rsync-exclude but must be synced. "
        f"Excluded by {_describe(rule)}"
    )


def test_rsync_exclude_has_no_bang_lines() -> None:
    """
    Выбранный инвариант: строк, начинающихся с `!`, в .rsync-exclude быть не должно.

    Почему именно так, а не «каждая `!` продублирована `+ `-правилом»: сейчас в
    файле НЕТ ни одной `!`-строки, все три отмены записаны через `+ ` — то есть
    файл уже корректен, и запрет `!` целиком является самым узким и самым
    дешёвым в проверке инвариантом. Разрешить `!` рядом с `+ ` значило бы
    оставить в файле строку, которая выглядит как защита, а не делает ничего:
    следующий редактор скопирует её как образец. Проще запретить форму.
    """
    offenders = [
        f"line {lineno}: {raw.strip()!r}"
        for lineno, raw in enumerate(
            RSYNC_EXCLUDE.read_text(encoding="utf-8").splitlines(), start=1
        )
        if raw.strip().startswith("!")
    ]
    assert not offenders, (
        "rsync has no '!' negation: such a line is a silent no-op (a literal pattern "
        "named '!...'). Use '+ pattern' placed ABOVE the broad rule. Offenders: "
        f"{offenders}"
    )


def test_rsync_plus_rules_are_placed_above_the_rules_they_override() -> None:
    """
    `+ `-правило ниже широкого шаблона бесполезно — первое совпадение уже решило.

    Проверяем не порядок строк, а результат: каждый литеральный путь из `+ `
    действительно доезжает.
    """
    literal_includes = [
        rule
        for rule in _rsync_rules()
        if rule.reinclude and not set(rule.pattern) & set("*?[")
    ]
    assert literal_includes, "no literal '+ ' rules found — the check would be vacuous"
    dead: list[str] = []
    for rule in literal_includes:
        excluded, decider = _is_excluded(rule.pattern)
        if excluded:
            dead.append(
                f"'+ {rule.pattern}' (line {rule.lineno}) is dead — swallowed by "
                f"{_describe(decider)}"
            )
    assert not dead, "misplaced '+ ' rules in .rsync-exclude: " + "; ".join(dead)


# ── .gitignore: что обязано не попасть в историю ─────────────────────────────

GIT_MUST_IGNORE: tuple[str, ...] = (
    ".env",
    "searxng/settings.yml",
    # Инцидент (b): точный путь выше снапшот не закрывал, secret_key тот же.
    "searxng/settings.yml.bak-before-engines-20260803",
    "sessions/x.session",
    "backups/x",
    "runtime/x",
)

GIT_MUST_KEEP: tuple[str, ...] = (
    ".env.example",
    "searxng/settings.example.yml",
)


@pytest.mark.parametrize("path", GIT_MUST_IGNORE)
def test_gitignore_covers_secrets(path: str) -> None:
    ignored, rule = _is_ignored(path)
    assert ignored, (
        f"{path!r} is NOT ignored by .gitignore: 'git add .' would commit it. "
        f"Decision: {_describe(rule)}"
    )


@pytest.mark.parametrize("path", GIT_MUST_KEEP)
def test_gitignore_keeps_examples_tracked(path: str) -> None:
    ignored, rule = _is_ignored(path)
    assert not ignored, (
        f"{path!r} IS ignored by .gitignore but must stay tracked "
        f"(it is the template for the secret file). Ignored by {_describe(rule)}"
    )


# ── Оба файла сразу ──────────────────────────────────────────────────────────

SECRET_BEARING_PATHS: tuple[str, ...] = (
    ".env",
    "searxng/settings.yml",
    "searxng/settings.yml.bak-before-engines-20260803",
    "searxng/settings.yml.bak",
    "sessions/prod.session",
)


@pytest.mark.parametrize("path", SECRET_BEARING_PATHS)
def test_secret_paths_are_closed_in_both_files(path: str) -> None:
    """
    Дыра была одна и та же в двух файлах — значит и проверять надо парой.

    Закрыть только .gitignore недостаточно: `sync-push --delete` утаскивает файл
    мимо git вообще. Закрыть только .rsync-exclude недостаточно: `git add .`
    коммитит его мимо rsync.
    """
    excluded, rsync_rule = _is_excluded(path)
    ignored, git_rule = _is_ignored(path)
    holes: list[str] = []
    if not excluded:
        holes.append(f".rsync-exclude does not exclude it ({_describe(rsync_rule)})")
    if not ignored:
        holes.append(f".gitignore does not ignore it ({_describe(git_rule)})")
    assert not holes, f"secret-bearing path {path!r}: " + "; ".join(holes)
