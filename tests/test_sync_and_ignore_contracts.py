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


# ── Ожидания: ОДИН список на оба файла ───────────────────────────────────────
#
# Ревизия 2026-08-03. До неё сторона .gitignore была строго короче стороны rsync
# (6 путей «закрыть» против 10 и 2 «сохранить» против 5), и дыра пряталась ровно
# в этой разнице: удаление строки `!.env.balanced.example` из .gitignore не роняло
# ни одного из 34 тестов — шаблон просто тихо переставал отслеживаться. Тем же
# способом, каким тихо не отслеживался `searxng/settings.yml.bak-*` до инцидента (b).
#
# Поэтому ожидание теперь ОДНО на оба файла: разъехаться нечему, списка два, а не
# четыре. Каждый секрет проверяется в обоих файлах, каждый шаблон — тоже в обоих.
# Асимметрия допускается только явная, через DELIBERATE_ASYMMETRY ниже.

CLOSED_IN_BOTH: tuple[str, ...] = (
    # Секреты окружения.
    ".env",
    ".env.production",
    # Telethon-сессии = доступ к аккаунту без пароля.
    "sessions/prod.session",
    "telethon.session",
    # Ключевой материал.
    "private.key",
    "x.pem",
    "id_rsa",
    # Инциденты (a) и (b): боевой secret_key и ручные снапшоты того же файла.
    "searxng/settings.yml",
    "searxng/settings.yml.bak",
    "searxng/settings.yml.bak-before-engines-20260803",
    # Состояние, писанное на сервере: локально его нет, --delete стёр бы историю,
    # а `git add .` затащил бы root-owned мусор в индекс.
    "backups/x",
    "runtime/x",
    "prometheus/textfile/x.prom",
    # Данные docker-томов (в отличие от схемы и миграций рядом — см. KEPT_IN_BOTH).
    "storage/postgres/data/base/1",
    "prometheus/data/chunks_head/000001",
)

KEPT_IN_BOTH: tuple[str, ...] = (
    # Шаблоны секретных файлов: если они перестают доезжать или отслеживаться,
    # разворачивать стек не из чего, а заметно это только при развёртывании.
    ".env.example",
    ".env.balanced.example",
    "searxng/settings.example.yml",
    # Конфиги, которыми стек описан.
    "docker-compose.yml",
    "prometheus/alerts.yml",
    "config/workspaces.yml",
    "config/sources.yml",
    # Схема рядом с исключёнными данными тома — та самая граница, ради которой в
    # обоих файлах стоят точечные `storage/**/data/`, а не `storage/`.
    "storage/postgres/init.sql",
    "shared/config.py",
)

# Единственные допустимые расхождения между файлами. Всё, что не здесь, обязано
# вести себя одинаково; всё, что здесь, — объяснено и зафиксировано.
DELIBERATE_ASYMMETRY: tuple[tuple[str, bool, bool, str], ...] = (
    (
        "tmp/.gitkeep",
        True,
        False,
        "заглушка каталога: git обязан её отслеживать (`tmp/*` плюс `!tmp/.gitkeep`), "
        "иначе tmp/ не создастся при клоне; rsync каталог tmp/ не возит целиком, "
        "и возить пустышку незачем",
    ),
    (
        ".git/config",
        True,
        False,
        "git живёт ТОЛЬКО на сервере: без `.git/` в .rsync-exclude push с рабочей "
        "станции затирал бы серверный чекаут вместе с историей. У .gitignore аналога "
        "нет и быть не может — git не индексирует собственный служебный каталог",
    ),
)


@pytest.mark.parametrize("path", CLOSED_IN_BOTH)
def test_path_is_closed_in_both_files(path: str) -> None:
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
        holes.append(
            f".rsync-exclude does not exclude it ({_describe(rsync_rule)}) — a --delete "
            "push would carry it across, or wipe the server copy"
        )
    if not ignored:
        holes.append(
            f".gitignore does not ignore it ({_describe(git_rule)}) — 'git add .' would "
            "commit it"
        )
    assert not holes, f"path {path!r} must be closed in both files: " + "; ".join(holes)


@pytest.mark.parametrize("path", KEPT_IN_BOTH)
def test_path_survives_in_both_files(path: str) -> None:
    """
    Зеркало предыдущего теста — та половина, которой не было.

    Потерять файл можно двумя способами, и оба молчаливые: он перестаёт доезжать
    до сервера (rsync) или перестаёт отслеживаться (git). Второй хуже: локально
    файл на месте, в репозитории его нет, и узнаётся это на чистом клоне.
    """
    excluded, rsync_rule = _is_excluded(path)
    ignored, git_rule = _is_ignored(path)
    losses: list[str] = []
    if excluded:
        losses.append(f".rsync-exclude excludes it ({_describe(rsync_rule)}) — it never syncs")
    if ignored:
        losses.append(f".gitignore ignores it ({_describe(git_rule)}) — it is not tracked")
    assert not losses, f"path {path!r} must survive in both files: " + "; ".join(losses)


def test_kept_paths_name_files_that_actually_exist() -> None:
    """
    KEPT_IN_BOTH обязан сторожить реальные файлы, а не призраки.

    Список выше — единственное место, где сказано «этот файл терять нельзя». Если
    файл переименовали, а строку не поправили, тест `test_path_survives_in_both_files`
    останется зелёным на несуществующем пути: ни один фильтр не исключает то, чего нет.
    Здесь проверка смотрит в рабочее дерево, а не в свой же литерал.
    """
    missing = [path for path in KEPT_IN_BOTH if not (REPO_ROOT / path).exists()]
    assert not missing, (
        f"KEPT_IN_BOTH guards paths that do not exist in the working tree: {missing}. "
        "Either the file was renamed (update the list AND both ignore files) or it was "
        "deleted (drop the line) — until then the guard watches nothing."
    )


@pytest.mark.parametrize(("path", "rsync_blocks", "git_blocks", "why"), DELIBERATE_ASYMMETRY)
def test_documented_asymmetries_still_hold(
    path: str, rsync_blocks: bool, git_blocks: bool, why: str
) -> None:
    """Расхождение между файлами допустимо только объяснённое — и оно тоже фиксируется."""
    excluded, rsync_rule = _is_excluded(path)
    ignored, git_rule = _is_ignored(path)
    assert excluded is rsync_blocks, (
        f"{path!r}: .rsync-exclude {'must' if rsync_blocks else 'must NOT'} exclude it "
        f"({why}). Decision: {_describe(rsync_rule)}"
    )
    assert ignored is git_blocks, (
        f"{path!r}: .gitignore {'must' if git_blocks else 'must NOT'} ignore it "
        f"({why}). Decision: {_describe(git_rule)}"
    )


# ── .rsync-exclude: инварианты, у которых нет git-двойника ───────────────────


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


def test_gitignore_negations_are_not_buried_under_an_excluded_parent() -> None:
    """
    Git-двойник предыдущей проверки: `!`-правило, чей РОДИТЕЛЬСКИЙ каталог исключён
    целиком, мертво — git внутрь такого каталога просто не спускается, и файл
    остаётся неотслеживаемым, хотя строка о возврате в файле стоит.

    Как и в rsync-варианте, проверяется не порядок строк, а результат: каждый
    литеральный путь из `!`-правила действительно отслеживается.
    """
    literal_negations = [
        rule
        for rule in _gitignore_rules()
        if rule.reinclude and not set(rule.pattern) & set("*?[")
    ]
    assert literal_negations, "no literal '!' rules found — the check would be vacuous"
    dead: list[str] = []
    for rule in literal_negations:
        ignored, decider = _is_ignored(rule.pattern)
        if ignored:
            dead.append(
                f"'!{rule.pattern}' (line {rule.lineno}) is dead — still ignored by "
                f"{_describe(decider)}"
            )
    assert not dead, "dead '!' rules in .gitignore: " + "; ".join(dead)


# ── Закрыто в rsync, но ВЕДЁТСЯ в git ────────────────────────────────────────
#
# Разведено 06.08.2026. Дневные дайджесты алертов стояли в CLOSED_IN_BOTH рядом
# с `backups/` и `runtime/` под общей причиной «состояние, писанное на сервере».
# Причина верна только наполовину: `--delete` их правда сотрёт, поэтому исключение
# в .rsync-exclude обязано остаться. Но второй половины — «`git add .` затащил бы
# root-owned мусор» — здесь нет: это 80 КБ markdown'а, написанного человеком,
# по одному файлу в сутки, и они никогда не переписываются.
#
# Цена прежней трактовки измерена сверкой 06.08.2026: закрытые в обоих файлах,
# они существовали ровно в одном экземпляре — на сервере. При этом пункт 18
# реестра (петля разбора алертов падает через раз) доказывается ПРОПУЩЕННЫМИ
# днями — 27.07, 31.07, 03.08, 04.08, — то есть вся его доказательная база жила
# без единой копии.
CLOSED_IN_RSYNC_TRACKED_IN_GIT: tuple[str, ...] = (
    "docs/ops/alert-digests/2026-08-01.md",
    "docs/ops/rollout-baselines/wave1-auto_hmi-design_ux-2026-08-05T053006Z.txt",
)


@pytest.mark.parametrize("path", CLOSED_IN_RSYNC_TRACKED_IN_GIT)
def test_server_written_history_is_excluded_from_rsync_but_kept_in_git(path: str) -> None:
    """Асимметрия здесь намеренная, и обе её половины обязаны держаться.

    Половина первая: файл ОБЯЗАН быть исключён из rsync. Локально его нет, и
    `sync-push --delete` без исключения снесёт историю — ровно так 03.08.2026
    погиб `prometheus/textfile`.

    Половина вторая: файл обязан НЕ быть в .gitignore. Иначе история снова
    останется в одном экземпляре, а её потеря обнаружится в тот момент, когда
    понадобится доказать, какие дни петля пропустила.
    """
    excluded, rsync_rule = _is_excluded(path)
    ignored, git_rule = _is_ignored(path)

    assert excluded, (
        f"{path!r} не исключён из rsync ({_describe(rsync_rule)}) — `sync-push --delete` "
        "сотрёт серверную историю, которой локально нет"
    )
    assert not ignored, (
        f"{path!r} снова закрыт в .gitignore ({_describe(git_rule)}). Это возвращает "
        "состояние, при котором история существует в единственном экземпляре на сервере."
    )
