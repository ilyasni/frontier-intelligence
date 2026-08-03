"""Индексация личного корпуса автора в отдельную Qdrant-коллекцию (ТЗ B, ось `own_stake`).

Коллекция намеренно живёт вне общего поиска: без `workspace_id`, без sparse-ветки,
`_build_payload_filter` к ней не применяется, `hybrid_search` и `get_frontier_brief`
в неё не ходят. Имя коллекции всегда версионированное
(`qdrant_collection_name_for_embedding`), иначе смена модели эмбеддингов молча
оставила бы own_stake считаться по коллекции с другой размерностью.

Рекомендуемый первый запуск — всегда `--dry-run`: он обходит корпус, считает чанки
и печатает разбивку по `doc_kind`, не обращаясь ни к эмбеддеру, ни к Qdrant, и вообще
не требует ни `.env`, ни сети.

    python scripts/index_own_corpus.py --dry-run --report-json out.json

Боевой прогон (корпус лежит на рабочей станции, Qdrant слушает loopback на сервере —
нужны ДВА туннеля, а не один: без gpt2giga-proxy первый же `embed()` упадёт):

    ssh -NT -L 16333:127.0.0.1:6333 -L 18090:127.0.0.1:8090 frontier-intelligence &
    QDRANT_URL=http://127.0.0.1:16333 python scripts/index_own_corpus.py --tier 1

Пути по умолчанию — рабочая станция автора (`D:\\telegramChanel`, `D:\\Workspace`).
Переопределяются флагами `--channel-root`, `--workspace-root`, `--roots-file`;
захардкоженных путей за пределами этого документированного дефолта нет.

Жёсткие исключения (`secrets/`, `.env`, credentials, `.git`, `node_modules`,
`vendor/dist/build`, `*-raw.json`) выражены кодом, а не комментарием: каталоги
режутся при обходе, а на каждый уцелевший файл дополнительно срабатывает
`assert_not_secret_path` — если путь под `secrets/` вообще доедет до отбора,
скрипт отказывается работать и выходит с ненулевым кодом.
"""

from __future__ import annotations

import argparse
import asyncio
import fnmatch
import hashlib
import io
import json
import logging
import os
import re
import sys
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePath, PurePosixPath
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.embedding_models import (  # noqa: E402
    embedding_input_text,
    get_embedding_model_spec,
    qdrant_collection_name_for_embedding,
)

logger = logging.getLogger("index_own_corpus")

# ─────────────────────────── контракты, которые нельзя менять молча ────────────

#: Единственное легальное значение purpose для этого скрипта. У `EmbeddingsGigaR`
#: асимметричные префиксы (`search_query: ` / `search_document: `), и перепутанный
#: purpose НЕ бросает исключения нигде по цепочке — ни в `embedding_input_text`,
#: ни в `LLMRouterClient.embed`, ни в адаптере GigaChat. Наружу выходят тихо
#: неверные, внешне правдоподобные числа. Проверка ниже — единственное место,
#: где эта ошибка ловится.
EMBED_PURPOSE: str = "document"

#: Дефолт на случай пустого QDRANT_OWN_CORPUS_COLLECTION: с пустой базой
#: `qdrant_collection_name_for_embedding` подставит "frontier_docs", то есть
#: личный корпус уехал бы в общий индекс.
DEFAULT_OWN_CORPUS_COLLECTION: str = "own_corpus"

#: Порог из ТЗ («40–60 чанков в лучшем случае» — риск 1 раздела B). Ниже него
#: own_stake шумный: одно случайное совпадение даёт высокий балл по теме,
#: где у автора ничего нет.
SPEC_STABILITY_FLOOR_CHUNKS: int = 60

# ─────────────────────────────── правила нарезки ──────────────────────────────

CHUNK_TARGET_CHARS: int = 1000
CHUNK_MAX_CHARS: int = 1200
CHUNK_MERGE_MAX_CHARS: int = 1500
CHUNK_MIN_CHARS: int = 200
CHUNK_DROP_CHARS: int = 80
STANDALONE_BULLET_CHARS: int = 400
SUBHEADING_FLUSH_CHARS: int = 800

# ───────────────────────────── жёсткие исключения ─────────────────────────────

INDEXABLE_SUFFIXES: frozenset[str] = frozenset({".md", ".mdx"})

#: Каталоги, в которые обход не заходит вообще. Плюс общее правило: любой каталог,
#: имя которого начинается с точки (`.git`, `.github`, `.claude`, `.cursor`,
#: `.venv`, `.next`, `.cache`, …).
EXCLUDED_DIR_NAMES: frozenset[str] = frozenset(
    {
        "node_modules",
        "vendor",
        "dist",
        "build",
        "out",
        "target",
        "coverage",
        "site-packages",
        "python_embeded",
        "__pycache__",
        "venv",
        "secrets",
        "storage",
        "tmp",
        "_install",
        "uploads",
        "_checkpoints",
        "old_env",
        "old_site_va_home",
        "tmp_migration",
        "tmp-seed",
    }
)

#: Каталоги, само появление которых в отобранном пути — аварийная ситуация.
SECRET_DIR_NAMES: frozenset[str] = frozenset({"secrets"})

#: Матчится по имени файла в нижнем регистре. `.json` / `.sql` / `.log` не прошли бы
#: и по расширениям, но список оставлен как второй контур: `docs/SECRETS.md` —
#: настоящий `.md`, и его спасает только именной глоб.
EXCLUDED_FILE_GLOBS: tuple[str, ...] = (
    ".env",
    ".env.*",
    "*.env",
    "*.env.example",
    "*credential*",
    "*secret*",
    "*password*",
    "*token*",
    "*.pem",
    "*.key",
    "*.session",
    "id_rsa*",
    "*-raw.json",
    "*.json",
    "*.sql",
    "*.log",
)

#: Дешёвая проверка первых 4 КБ на случай, когда секрет спрятан внутри `.md`.
SECRET_CONTENT_RE = re.compile(
    r"BEGIN [A-Z ]*PRIVATE KEY"
    r"|api[_-]?key\s*[:=]\s*\S{16,}"
    r"|Bearer\s+[A-Za-z0-9._-]{20,}",
    re.IGNORECASE,
)
SECRET_CONTENT_PROBE_CHARS: int = 4096

REPO_README_STEMS: frozenset[str] = frozenset({"readme", "claude", "agents", "architecture"})

_VERSION_SUFFIX_RE = re.compile(r"^(?P<base>.+?)-v(?P<num>\d+)$", re.IGNORECASE)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?\u2026])\s+")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
_FENCE_RE = re.compile(r"^\s*(```|~~~)")
_LIST_ITEM_RE = re.compile(r"^(\s*)([-*+]|\d+[.)])\s+")
_TABLE_SEP_RE = re.compile(r"^\s*\|?[\s:|-]+\|[\s:|-]*$")


class SecretsPathRefused(RuntimeError):
    """Путь под `secrets/` дошёл до отбора файлов. Работа прекращается."""


# ────────────────────────────── описание корпуса ──────────────────────────────


@dataclass(frozen=True)
class CorpusSource:
    """Один корень корпуса.

    `exclude` — POSIX-глобы относительно корня, матчатся регистронезависимо;
    `*` внутри глоба перекрывает и разделители (fnmatch, не pathlib.match).
    """

    slug: str
    root: Path
    default_doc_kind: str
    default_tier: int
    exclude: tuple[str, ...] = ()
    channel_layout: bool = False


@dataclass(frozen=True)
class CorpusFile:
    source_slug: str
    path: Path
    corpus_path: str
    doc_kind: str
    tier: int


@dataclass(frozen=True)
class Chunk:
    corpus_path: str
    chunk_idx: int
    doc_kind: str
    tier: int
    source_slug: str
    heading: str
    text: str

    @property
    def chars(self) -> int:
        return len(self.text)

    @property
    def point_id(self) -> str:
        return own_corpus_point_id(self.corpus_path, self.chunk_idx)

    def payload(self, indexed_at: str) -> dict[str, Any]:
        """Payload точки.

        Контракт ТЗ — первые четыре ключа. Остальные добавлены сверху и ничего
        не ломают: `tier` даёт калибровку на первом тире без переиндексации,
        `project` и `heading` — объяснение, почему own_stake сработал
        (при max-агрегации argmax и есть объяснение).

        `workspace_id` здесь нет и быть не должно — инвариант 2.
        """
        return {
            "doc_kind": self.doc_kind,
            "path": self.corpus_path,
            "chunk_idx": self.chunk_idx,
            "indexed_at": indexed_at,
            "tier": self.tier,
            "project": self.source_slug,
            "heading": self.heading,
            "chars": self.chars,
        }


@dataclass
class SkipCounters:
    by_reason: dict[str, int] = field(default_factory=dict)

    def add(self, reason: str) -> None:
        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1

    @property
    def total(self) -> int:
        return sum(self.by_reason.values())


DEFAULT_CHANNEL_ROOT = Path(r"D:\telegramChanel")
DEFAULT_WORKSPACE_ROOT = Path(r"D:\Workspace")

#: (каталог внутри workspace-root, doc_kind по умолчанию, tier по умолчанию, исключения).
#: Отбор — «написано этим человеком». Сюда не входят ComfyUI / LoRA /
#: StableDiffusion (чужие чекауты), `docs/books` и `docs/chatgpt` (пересказы чужих
#: книг — на max-агрегации они заставили бы own_stake срабатывать на GTD и
#: «Дилемме инноватора»), архив `old_docs …ilyasni-telegram-assistant.git`
#: (свой, но вытесненный дубль) и `docs/research` (вывод deep-research, не автор).
DEFAULT_REPO_SOURCES: tuple[tuple[str, str, int, tuple[str, ...]], ...] = (
    (
        "frontier-intelligence",
        "repo_doc",
        2,
        ("docs/chatgpt/*", "docs/old_docs ilyasni-telegram-assistant.git/*"),
    ),
    ("personal-intelligence", "repo_doc", 2, ("docs/research/*",)),
    ("new_vitabrava", "repo_doc", 2, ()),
    ("ecomm-home", "repo_doc", 2, ("old_site_va_home/*",)),
    ("MTProxy", "repo_doc", 2, ()),
    ("pve_orchestrator", "repo_doc", 2, ("secrets/*",)),
    ("Visualist", "repo_doc", 2, ()),
    ("Prototipe", "repo_doc", 2, ()),
    ("GSAP projects", "repo_doc", 2, ("_checkpoints/*",)),
    ("leads-toolkit", "repo_doc", 2, ("docs/books/*", "_install/*")),
    ("MRFlowers", "repo_doc", 2, ("02-references/*",)),
    ("ITMO", "repo_doc", 2, ()),
    (
        "\u041b\u0435\u043a\u0446\u0438\u0438 \u043e \u0418\u0418 \u0434\u043b\u044f "
        "\u043f\u0440\u043e\u0434\u0432\u0438\u043d\u0443\u0442\u044b\u0445 "
        "\u0434\u0438\u0437\u0430\u0439\u043d\u0435\u0440\u043e\u0432",
        "lecture",
        2,
        ("*/uploads/*",),
    ),
)

#: Префикс имени файла рецензии в ITMO (кириллица вынесена в escape, чтобы файл
#: оставался читаемым в любой кодировке консоли).
_REVIEW_FILENAME_PREFIX = "\u0420\u0435\u0446\u0435\u043d\u0437\u0438\u044f"


def default_sources(channel_root: Path, workspace_root: Path) -> list[CorpusSource]:
    """Документированный дефолт: рабочее пространство канала + репозитории автора."""
    sources = [
        CorpusSource(
            slug="channel",
            root=channel_root,
            default_doc_kind="channel_analytics",
            default_tier=1,
            exclude=("secrets/*",),
            channel_layout=True,
        )
    ]
    for name, doc_kind, tier, exclude in DEFAULT_REPO_SOURCES:
        sources.append(
            CorpusSource(
                slug=name,
                root=workspace_root / name,
                default_doc_kind=doc_kind,
                default_tier=tier,
                exclude=exclude,
            )
        )
    return sources


def sources_from_file(path: Path) -> list[CorpusSource]:
    """Полная замена дефолтного инвентаря из JSON-файла.

    Формат: список объектов `{slug, root, doc_kind, tier, exclude, channel_layout}`.
    """
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("roots-file must contain a JSON list of source objects")
    sources: list[CorpusSource] = []
    for item in raw:
        sources.append(
            CorpusSource(
                slug=str(item["slug"]),
                root=Path(str(item["root"])),
                default_doc_kind=str(item.get("doc_kind") or "repo_doc"),
                default_tier=int(item.get("tier") or 2),
                exclude=tuple(str(value) for value in (item.get("exclude") or ())),
                channel_layout=bool(item.get("channel_layout")),
            )
        )
    return sources


# ─────────────────────────────── фильтры путей ────────────────────────────────


#: Оба разделителя сразу. Ни один из предикатов ниже не имеет права зависеть от того,
#: на какой ОС запущен интерпретатор.
_PATH_SEPARATOR_RE = re.compile(r"[\\/]+")


def to_posix_path(value: PurePath | str) -> str:
    """Путь с любыми разделителями -> строка с `/`, независимо от платформы."""
    raw = os.fspath(value) if isinstance(value, (str, os.PathLike)) else str(value)
    return _PATH_SEPARATOR_RE.sub("/", raw)


def normalized_path_parts(value: PurePath | str) -> tuple[str, ...]:
    """Компоненты пути, разобранные по ОБОИМ разделителям и приведённые к нижнему регистру.

    `PurePath.parts` разбирает путь по правилам той ОС, где запущен интерпретатор:
    под POSIX строка `D:\\telegramChanel\\secrets\\credentials.md` — ОДИН неделимый
    компонент, и проверка «есть ли в пути каталог secrets» молча отвечает «нет».
    Предохранитель, который тихо выключается при смене платформы, — это не
    предохранитель, поэтому разбор здесь свой, а не платформенный.
    """
    chunks = to_posix_path(value).split("/")
    return tuple(part for part in (chunk.strip().lower() for chunk in chunks) if part)


def path_leaf(value: PurePath | str) -> str:
    """Последний компонент пути в нижнем регистре ("" для пустого значения).

    Нужен там, где предикат по контракту принимает ИМЯ, но может получить целый путь:
    имя должно проверяться одинаково, пришло оно как `id_rsa`, `repo/id_rsa`
    или `repo\\id_rsa`.
    """
    parts = normalized_path_parts(value)
    return parts[-1] if parts else ""


def is_secret_path(path: PurePath | str) -> bool:
    """True, если хоть один компонент пути — каталог секретов.

    Регистронезависимо и независимо от разделителя: `Secrets\\x.md` и `secrets/x.md`
    отвергаются одинаково и под Windows, и под POSIX.
    """
    return any(part in SECRET_DIR_NAMES for part in normalized_path_parts(path))


def assert_not_secret_path(path: PurePath | str) -> None:
    """Аварийный стоп: путь под `secrets/` не должен доходить до отбора файлов.

    Не `assert`: под `python -O` инструкция assert вырезается, и единственный
    предохранитель исчез бы ровно там, где он нужен.
    """
    if is_secret_path(path):
        raise SecretsPathRefused(
            f"refusing to index a path under a secrets directory: {path}"
        )


def is_excluded_dir(name: PurePath | str) -> bool:
    """Каталоги, в которые обход не заходит."""
    normalized = path_leaf(name)
    if not normalized:
        return False
    if normalized.startswith("."):
        return True
    return normalized in EXCLUDED_DIR_NAMES


def is_excluded_filename(name: PurePath | str) -> bool:
    """Именные глобы — второй контур поверх allowlist расширений."""
    normalized = path_leaf(name)
    if not normalized:
        return False
    return any(fnmatch.fnmatch(normalized, pattern) for pattern in EXCLUDED_FILE_GLOBS)


def is_indexable_suffix(name: PurePath | str) -> bool:
    return PurePosixPath(path_leaf(name)).suffix in INDEXABLE_SUFFIXES


def matches_any_glob(relative_posix: PurePath | str, patterns: Sequence[str]) -> bool:
    normalized = to_posix_path(relative_posix).lower()
    return any(
        fnmatch.fnmatch(normalized, to_posix_path(pattern).lower()) for pattern in patterns
    )


def looks_like_secret_content(text: str) -> bool:
    return bool(SECRET_CONTENT_RE.search(text[:SECRET_CONTENT_PROBE_CHARS]))


def prune_version_families(paths: Sequence[Path]) -> tuple[list[Path], list[Path]]:
    """Из семейств `name`, `name-v2`, `name-v3` оставить только старшую версию.

    Возвращает (оставленные, отброшенные). Семейством считается только группа,
    в которой есть хотя бы один файл с суффиксом `-vN`: иначе near-дубли
    надувают счётчик чанков и перекашивают max-агрегацию.
    """
    families: dict[tuple[str, str], list[tuple[int, Path]]] = {}
    for path in paths:
        stem = path.stem
        match = _VERSION_SUFFIX_RE.match(stem)
        if match:
            base = match.group("base").lower()
            version = int(match.group("num"))
        else:
            base = stem.lower()
            version = 1
        families.setdefault((str(path.parent).lower(), base), []).append((version, path))

    kept: list[Path] = []
    dropped: list[Path] = []
    for members in families.values():
        if len(members) == 1:
            kept.append(members[0][1])
            continue
        best_version = max(version for version, _ in members)
        winner_taken = False
        for version, path in sorted(members, key=lambda item: (item[0], str(item[1]))):
            if version == best_version and not winner_taken:
                winner_taken = True
                kept.append(path)
            else:
                dropped.append(path)
    return sorted(kept, key=str), sorted(dropped, key=str)


# ─────────────────────────── doc_kind и обход дерева ──────────────────────────


def resolve_channel_doc_kind(relative: PurePosixPath) -> tuple[str, int] | None:
    """Раскладка рабочего пространства канала (источники a, b, c из инвентаря)."""
    parts = relative.parts
    text = relative.as_posix().lower()
    if text == "content/inbox.md":
        return ("inbox", 1)
    if text == "content/ideas.md":
        return ("idea", 1)
    if text.startswith("content/published/"):
        return ("published", 1)
    if text.startswith("content/drafts/"):
        return ("draft", 1)
    if text.startswith("channel/"):
        return ("author_profile", 1)
    if text.startswith("analytics/"):
        return ("channel_analytics", 1)
    if len(parts) == 1:
        return ("repo_readme", 1)
    return None


def resolve_doc_kind(source: CorpusSource, relative: PurePosixPath) -> tuple[str, int] | None:
    if source.channel_layout:
        return resolve_channel_doc_kind(relative)
    parts = relative.parts
    if len(parts) == 1 and Path(parts[0]).stem.strip().lower() in REPO_README_STEMS:
        return ("repo_readme", 1)
    if relative.name.startswith(_REVIEW_FILENAME_PREFIX):
        return ("review", source.default_tier)
    return (source.default_doc_kind, source.default_tier)


def walk_source(source: CorpusSource, skips: SkipCounters) -> Iterator[Path]:
    """Обход одного корня с вырезанием каталогов на месте.

    `os.walk` с правкой `dirnames` — единственный способ НЕ спускаться в
    `node_modules` (4 000+ файлов) и в `secrets/`, а не отфильтровывать их потом.
    """
    if not source.root.exists():
        logger.warning("corpus_root_missing slug=%s root=%s", source.slug, source.root)
        return
    for dirpath, dirnames, filenames in os.walk(source.root):
        pruned = [name for name in dirnames if is_excluded_dir(name)]
        for _ in pruned:
            skips.add("dir_pruned")
        dirnames[:] = sorted(name for name in dirnames if not is_excluded_dir(name))
        current = Path(dirpath)
        for filename in sorted(filenames):
            path = current / filename
            if not is_indexable_suffix(filename):
                skips.add("suffix_not_indexable")
                continue
            if is_excluded_filename(filename):
                skips.add("filename_glob")
                continue
            # Второй контур: сюда путь под secrets/ попасть уже не может —
            # если попал, это баг обхода, и работать дальше нельзя.
            assert_not_secret_path(path)
            yield path


def collect_files(
    sources: Sequence[CorpusSource],
    *,
    skips: SkipCounters,
) -> list[CorpusFile]:
    collected: list[CorpusFile] = []
    for source in sources:
        candidates: list[Path] = []
        for path in walk_source(source, skips):
            try:
                relative = PurePosixPath(path.relative_to(source.root).as_posix())
            except ValueError:  # pragma: no cover - защита от подмены root на лету
                skips.add("outside_root")
                continue
            if matches_any_glob(relative.as_posix(), source.exclude):
                skips.add("source_exclude_glob")
                continue
            if resolve_doc_kind(source, relative) is None:
                skips.add("not_a_confirmed_source")
                continue
            candidates.append(path)

        kept, dropped = prune_version_families(candidates)
        for _ in dropped:
            skips.add("older_version_family")

        for path in kept:
            relative = PurePosixPath(path.relative_to(source.root).as_posix())
            resolved = resolve_doc_kind(source, relative)
            if resolved is None:  # pragma: no cover - отфильтровано выше
                continue
            doc_kind, tier = resolved
            collected.append(
                CorpusFile(
                    source_slug=source.slug,
                    path=path,
                    corpus_path=f"{source.slug}/{relative.as_posix()}",
                    doc_kind=doc_kind,
                    tier=tier,
                )
            )
    return collected


def read_corpus_file(entry: CorpusFile, skips: SkipCounters) -> str | None:
    try:
        text = entry.path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            text = entry.path.read_text(encoding="utf-8-sig")
        except (UnicodeDecodeError, OSError) as exc:
            logger.warning("corpus_file_unreadable path=%s error=%s", entry.corpus_path, exc)
            skips.add("unreadable")
            return None
    except OSError as exc:
        logger.warning("corpus_file_unreadable path=%s error=%s", entry.corpus_path, exc)
        skips.add("unreadable")
        return None
    if looks_like_secret_content(text):
        logger.warning("corpus_file_looks_like_secret path=%s", entry.corpus_path)
        skips.add("secret_content")
        return None
    return text


# ──────────────────────────────── нарезка ─────────────────────────────────────


@dataclass(frozen=True)
class Block:
    kind: str
    text: str
    level: int = 0


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))


def parse_blocks(text: str) -> list[Block]:
    """Markdown -> блоки. Кодовый фенс и таблица атомарны, буллет тащит продолжения."""
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks: list[Block] = []
    index = 0
    total = len(lines)

    while index < total:
        line = lines[index]
        if not line.strip():
            index += 1
            continue

        fence = _FENCE_RE.match(line)
        if fence:
            marker = fence.group(1)
            buffer = [line]
            index += 1
            while index < total:
                buffer.append(lines[index])
                if lines[index].strip().startswith(marker):
                    index += 1
                    break
                index += 1
            blocks.append(Block(kind="code", text="\n".join(buffer)))
            continue

        heading = _HEADING_RE.match(line)
        if heading:
            blocks.append(
                Block(kind="heading", text=heading.group(2).strip(), level=len(heading.group(1)))
            )
            index += 1
            continue

        if "|" in line and index + 1 < total and _TABLE_SEP_RE.match(lines[index + 1]):
            buffer = []
            while index < total and "|" in lines[index] and lines[index].strip():
                buffer.append(lines[index])
                index += 1
            blocks.append(Block(kind="table", text="\n".join(buffer)))
            continue

        marker_match = _LIST_ITEM_RE.match(line)
        if marker_match:
            indent = len(marker_match.group(1))
            buffer = [line]
            index += 1
            while index < total:
                current = lines[index]
                if not current.strip():
                    lookahead = index
                    while lookahead < total and not lines[lookahead].strip():
                        lookahead += 1
                    if lookahead < total and _indent_of(lines[lookahead]) > indent:
                        buffer.extend(lines[index:lookahead])
                        index = lookahead
                        continue
                    break
                if _HEADING_RE.match(current) or _FENCE_RE.match(current):
                    break
                nested = _LIST_ITEM_RE.match(current)
                if nested and len(nested.group(1)) <= indent:
                    break
                buffer.append(current)
                index += 1
            blocks.append(Block(kind="list_item", text="\n".join(buffer).rstrip()))
            continue

        buffer = []
        while index < total and lines[index].strip():
            current = lines[index]
            if (
                _HEADING_RE.match(current)
                or _FENCE_RE.match(current)
                or _LIST_ITEM_RE.match(current)
            ):
                break
            buffer.append(current)
            index += 1
        if buffer:
            blocks.append(Block(kind="paragraph", text="\n".join(buffer).rstrip()))
        else:  # pragma: no cover - гарантия продвижения курсора
            index += 1
    return blocks


def split_oversized(
    text: str,
    *,
    target: int = CHUNK_TARGET_CHARS,
    hard_cap: int = CHUNK_MAX_CHARS,
) -> list[str]:
    """Разрезать слишком длинный блок по границам предложений.

    Ничего не усекается и не выбрасывается: длинный файл — это проблема количества
    чанков, а не усечения, а усечение молча удалило бы авторский текст.
    """
    if len(text) <= hard_cap:
        return [text]

    units = [unit for unit in _SENTENCE_SPLIT_RE.split(text) if unit]
    if len(units) == 1:
        units = [unit for unit in text.split("\n") if unit.strip()] or [text]

    pieces: list[str] = []
    buffer = ""
    for unit in units:
        if len(unit) > hard_cap:
            if buffer:
                pieces.append(buffer)
                buffer = ""
            for start in range(0, len(unit), target):
                pieces.append(unit[start : start + target])
            continue
        candidate = f"{buffer} {unit}".strip() if buffer else unit
        if len(candidate) > hard_cap:
            pieces.append(buffer)
            buffer = unit
        elif len(candidate) >= target:
            pieces.append(candidate)
            buffer = ""
        else:
            buffer = candidate
    if buffer:
        pieces.append(buffer)
    return pieces or [text]


def _breadcrumb(trail: dict[int, str]) -> str:
    return " > ".join(trail[level] for level in sorted(trail) if trail.get(level))


def chunk_markdown(text: str) -> list[tuple[str, str]]:
    """Markdown -> список (breadcrumb, body).

    Правила: H1/H2 всегда закрывают чанк, H3+ — только если в буфере уже больше
    800 символов (иначе короткие подразделы дают 200-символьные огрызки);
    буллет от 400 символов становится отдельным чанком; жадная упаковка до 1000
    с потолком 1200; чанк короче 200 склеивается назад (с допуском до 1500),
    короче 80 без предшественника — выбрасывается. Перекрытия нет намеренно:
    при max-агрегации оно не даёт выигрыша в полноте, только раздувает точки.
    """
    blocks = parse_blocks(text)
    trail: dict[int, str] = {}
    buffer: list[str] = []
    buffer_len = 0
    buffer_heading = ""
    staged: list[tuple[str, str]] = []

    def flush() -> None:
        nonlocal buffer, buffer_len, buffer_heading
        if buffer:
            staged.append((buffer_heading, "\n\n".join(buffer).strip()))
        buffer = []
        buffer_len = 0
        buffer_heading = _breadcrumb(trail)

    def append(body: str) -> None:
        nonlocal buffer, buffer_len, buffer_heading
        body = body.strip()
        if not body:
            return
        if not buffer:
            buffer_heading = _breadcrumb(trail)
        separator = 2 if buffer else 0
        if buffer and buffer_len + separator + len(body) > CHUNK_MAX_CHARS:
            flush()
            buffer_heading = _breadcrumb(trail)
            separator = 0
        buffer.append(body)
        buffer_len += separator + len(body)
        if buffer_len >= CHUNK_TARGET_CHARS:
            flush()

    for block in blocks:
        if block.kind == "heading":
            if block.level <= 2:
                flush()
            elif block.level == 3 and buffer_len > SUBHEADING_FLUSH_CHARS:
                flush()
            if block.level <= 3:
                for level in list(trail):
                    if level >= block.level:
                        trail.pop(level, None)
                trail[block.level] = block.text
                if not buffer:
                    buffer_heading = _breadcrumb(trail)
            else:
                append(f"{'#' * block.level} {block.text}")
            continue

        if block.kind == "list_item" and len(block.text) >= STANDALONE_BULLET_CHARS:
            flush()
            heading = _breadcrumb(trail)
            for piece in split_oversized(block.text):
                staged.append((heading, piece.strip()))
            buffer_heading = heading
            continue

        if len(block.text) > CHUNK_MAX_CHARS:
            flush()
            heading = _breadcrumb(trail)
            for piece in split_oversized(block.text):
                staged.append((heading, piece.strip()))
            buffer_heading = heading
            continue

        append(block.text)

    flush()

    merged: list[list[str]] = []
    for heading, body in staged:
        if not body:
            continue
        if (
            len(body) < CHUNK_MIN_CHARS
            and merged
            and len(merged[-1][1]) + 2 + len(body) <= CHUNK_MERGE_MAX_CHARS
        ):
            merged[-1][1] = f"{merged[-1][1]}\n\n{body}"
            continue
        merged.append([heading, body])

    return [
        (heading, body)
        for heading, body in merged
        if len(body) >= CHUNK_DROP_CHARS
    ]


def chunk_corpus_file(entry: CorpusFile, text: str) -> list[Chunk]:
    chunks: list[Chunk] = []
    for index, (heading, body) in enumerate(chunk_markdown(text)):
        rendered = f"{heading}\n\n{body}" if heading else body
        chunks.append(
            Chunk(
                corpus_path=entry.corpus_path,
                chunk_idx=index,
                doc_kind=entry.doc_kind,
                tier=entry.tier,
                source_slug=entry.source_slug,
                heading=heading,
                text=rendered,
            )
        )
    return chunks


# ──────────────────────────── идентичность точек ──────────────────────────────


def own_corpus_point_id(corpus_path: str, chunk_idx: int) -> str:
    """Идемпотентность: повторный прогон перезаписывает точку, а не дублирует.

    Стабилен, пока стабильно КОЛИЧЕСТВО чанков в файле. Как только файл вырос,
    индексы сдвигаются и старые точки остаются висеть — поэтому перед upsert'ом
    чанков файла его прежние точки удаляются фильтром по `path`.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"own:{corpus_path}:{chunk_idx}"))


# ─────────────────────────── проверки перед эмбеддингом ───────────────────────


def assert_document_purpose(purpose: str, model: str) -> None:
    """Единственное место, где ловится перепутанный purpose.

    Проверяются две разные вещи: (1) значение равно "document"; (2) для модели
    с асимметричными префиксами префикс реально применился — иначе purpose был бы
    формально верным, но не дал бы эффекта (например, при опечатке в имени модели).
    """
    if purpose != EMBED_PURPOSE:
        raise ValueError(
            f"own corpus must be embedded with purpose={EMBED_PURPOSE!r}, got {purpose!r}: "
            "EmbeddingsGigaR uses asymmetric prefixes and a wrong purpose raises no error, "
            "it silently produces plausible but wrong vectors"
        )
    spec = get_embedding_model_spec(model)
    if spec is None or not spec.document_prefix:
        return
    probe = embedding_input_text(model, "probe", purpose=purpose)
    if not probe.startswith(spec.document_prefix):
        raise ValueError(
            f"document prefix {spec.document_prefix!r} was not applied for model {model!r}; "
            "refusing to index with an unprefixed document vector"
        )


def resolve_collection_name(base: str, model: str) -> str:
    normalized = str(base or "").strip() or DEFAULT_OWN_CORPUS_COLLECTION
    return qdrant_collection_name_for_embedding(normalized, model)


# ───────────────────────────────── отчёты ─────────────────────────────────────


def build_plan_report(
    chunks_by_file: Sequence[tuple[CorpusFile, list[Chunk]]],
    *,
    skips: SkipCounters,
    sources: Sequence[CorpusSource],
    collection: str | None,
) -> dict[str, Any]:
    by_doc_kind: dict[str, dict[str, int]] = {}
    by_tier: dict[str, dict[str, int]] = {}
    by_source: dict[str, dict[str, int]] = {}
    total_chunks = 0
    total_chars = 0

    for entry, chunks in chunks_by_file:
        chars = sum(chunk.chars for chunk in chunks)
        total_chunks += len(chunks)
        total_chars += chars
        for bucket, key in (
            (by_doc_kind, entry.doc_kind),
            (by_tier, f"tier_{entry.tier}"),
            (by_source, entry.source_slug),
        ):
            slot = bucket.setdefault(key, {"files": 0, "chunks": 0, "chars": 0})
            slot["files"] += 1
            slot["chunks"] += len(chunks)
            slot["chars"] += chars

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "collection": collection,
        "roots": [
            {"slug": source.slug, "root": str(source.root), "exists": source.root.exists()}
            for source in sources
        ],
        "totals": {
            "files": len(chunks_by_file),
            "chunks": total_chunks,
            "chars": total_chars,
        },
        "by_doc_kind": dict(sorted(by_doc_kind.items())),
        "by_tier": dict(sorted(by_tier.items())),
        "by_source": dict(sorted(by_source.items())),
        "skipped": {"total": skips.total, "by_reason": dict(sorted(skips.by_reason.items()))},
        "spec_stability_floor_chunks": SPEC_STABILITY_FLOOR_CHUNKS,
        "clears_stability_floor": total_chunks >= SPEC_STABILITY_FLOOR_CHUNKS,
    }


def log_plan_report(report: dict[str, Any]) -> None:
    totals = report["totals"]
    logger.info(
        "plan_totals files=%s chunks=%s chars=%s collection=%s",
        totals["files"],
        totals["chunks"],
        totals["chars"],
        report.get("collection") or "-",
    )
    for title, bucket in (
        ("doc_kind", report["by_doc_kind"]),
        ("tier", report["by_tier"]),
        ("source", report["by_source"]),
    ):
        for key, values in bucket.items():
            logger.info(
                "plan_by_%s key=%s files=%s chunks=%s chars=%s",
                title,
                key,
                values["files"],
                values["chunks"],
                values["chars"],
            )
    for reason, count in report["skipped"]["by_reason"].items():
        logger.info("plan_skipped reason=%s count=%s", reason, count)
    logger.info(
        "plan_stability_floor floor=%s chunks=%s clears=%s",
        report["spec_stability_floor_chunks"],
        totals["chunks"],
        report["clears_stability_floor"],
    )


# ───────────────────────────────── индексация ─────────────────────────────────


def _load_state(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("state_file_unreadable path=%s error=%s", path, exc)
        return {}
    done = raw.get("done") if isinstance(raw, dict) else None
    return {str(key): str(value) for key, value in (done or {}).items()}


def _save_state(path: Path | None, done: dict[str, str]) -> None:
    if path is None:
        return
    payload = {"updated_at": datetime.now(UTC).isoformat(), "done": done}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _file_signature(chunks: Sequence[Chunk]) -> str:
    """Стабильный между процессами отпечаток нарезки файла.

    Именно sha256, а не `hash()`: встроенный хеш строк рандомизируется на каждый
    процесс (PYTHONHASHSEED), и resume-файл после перезапуска стал бы бесполезен.
    """
    digest = hashlib.sha256()
    for chunk in chunks:
        digest.update(f"{chunk.corpus_path}\x00{chunk.chunk_idx}\x00".encode())
        digest.update(chunk.text.encode("utf-8"))
        digest.update(b"\x00")
    return digest.hexdigest()


async def embed_chunk_text(
    router: Any,
    text: str,
    *,
    model: str,
    purpose: str = EMBED_PURPOSE,
    expected_dim: int | None = None,
) -> list[float]:
    """Один вызов эмбеддера с проверкой purpose ПЕРЕД обращением к провайдеру.

    Проверка стоит на каждом вызове, а не один раз на старте: подмена purpose
    где-то в середине прогона иначе прошла бы незамеченной — и, что важнее,
    падение происходит до того, как потрачен токен.
    """
    assert_document_purpose(purpose, model)
    vector = await router.embed(text, purpose=purpose)
    if expected_dim is not None and len(vector) != expected_dim:
        raise RuntimeError(
            f"embedding dimension mismatch: got {len(vector)}, expected {expected_dim}"
        )
    return list(vector)


async def index_corpus(
    chunks_by_file: Sequence[tuple[CorpusFile, list[Chunk]]],
    *,
    model: str,
    expected_dim: int | None,
    expected_collection: str,
    concurrency: int,
    state_path: Path | None,
) -> dict[str, Any]:
    """Реальный прогон: эмбеддинги через LLMRouterClient, запись через QdrantFrontierClient.

    Запись идёт `upsert_own_corpus_chunk`, а не собственным `create_collection`:
    иначе размерность, distance, HNSW и payload-индексы описаны в двух местах и
    однажды разъедутся. Скрипт добавляет к этому только уборку осиротевших точек.

    Импорты тяжёлых зависимостей отложены: `--dry-run` не должен требовать ни
    qdrant-client, ни настроек, ни сети.
    """
    from qdrant_client.models import FieldCondition, FilterSelector, MatchValue, Range
    from qdrant_client.models import Filter as QdrantFilter

    from worker.integrations.qdrant_client import QdrantFrontierClient
    from worker.llm_router_client import LLMRouterClient

    done = _load_state(state_path)
    indexed_at = datetime.now(UTC).isoformat()

    router = LLMRouterClient(service_name="index_own_corpus")
    qdrant = QdrantFrontierClient()
    collection = str(qdrant.own_corpus_collection)
    semaphore = asyncio.Semaphore(max(1, concurrency))

    upserted = 0
    files_done = 0
    files_skipped = 0

    async def embed_chunk(chunk: Chunk) -> list[float]:
        async with semaphore:
            return await embed_chunk_text(
                router,
                chunk.text,
                model=model,
                purpose=EMBED_PURPOSE,
                expected_dim=expected_dim,
            )

    try:
        # Без Redis роутинг откатывается на статическую политику, и имя коллекции
        # может молча разъехаться с боевым. Тогда корпус уедет туда, куда own_stake
        # не смотрит, и это не даст ни одного исключения — только пустую вторую ось.
        if collection != expected_collection:
            raise RuntimeError(
                f"own corpus collection mismatch: client resolved {collection!r}, "
                f"this run expected {expected_collection!r}"
            )
        logger.info("own_corpus_collection collection=%s", collection)

        for entry, chunks in chunks_by_file:
            if not chunks:
                continue
            signature = _file_signature(chunks)
            if done.get(entry.corpus_path) == signature:
                files_skipped += 1
                continue

            vectors = await asyncio.gather(*(embed_chunk(chunk) for chunk in chunks))

            for chunk, vector in zip(chunks, vectors):
                await qdrant.upsert_own_corpus_chunk(
                    path=chunk.corpus_path,
                    chunk_idx=chunk.chunk_idx,
                    dense_vector=vector,
                    doc_kind=chunk.doc_kind,
                    indexed_at=indexed_at,
                    extra_payload={
                        "tier": chunk.tier,
                        "project": chunk.source_slug,
                        "heading": chunk.heading,
                        "chars": chunk.chars,
                    },
                )

            # Уборка хвоста ПОСЛЕ записи: point_id устойчив только пока устойчиво
            # КОЛИЧЕСТВО чанков в файле. Файл сократился — старшие индексы остались
            # висеть, и критерий «повторный прогон не меняет N» проходит на первом
            # ре-ране и тихо ломается на первой правке. Порядок «сначала записать,
            # потом убрать» выбран специально: файл ни на мгновение не пропадает
            # из индекса.
            await qdrant.client.delete(
                collection_name=collection,
                points_selector=FilterSelector(
                    filter=QdrantFilter(
                        must=[
                            FieldCondition(
                                key="path",
                                match=MatchValue(value=entry.corpus_path),
                            ),
                            FieldCondition(
                                key="chunk_idx",
                                range=Range(gte=len(chunks)),
                            ),
                        ]
                    )
                ),
                wait=True,
            )

            upserted += len(chunks)
            files_done += 1
            done[entry.corpus_path] = signature
            _save_state(state_path, done)
            logger.info(
                "own_corpus_file_indexed path=%s chunks=%s doc_kind=%s tier=%s",
                entry.corpus_path,
                len(chunks),
                entry.doc_kind,
                entry.tier,
            )
    finally:
        await router.close()
        await qdrant.close()

    return {
        "collection": collection,
        "files_indexed": files_done,
        "files_skipped_resume": files_skipped,
        "points_upserted": upserted,
        "indexed_at": indexed_at,
    }


# ─────────────────────────────────── CLI ──────────────────────────────────────


def configure_logging(verbose: bool) -> None:
    """Прогресс идёт в stderr через logging.

    Поток оборачивается с errors="replace": пути корпуса содержат кириллицу,
    а консоль Windows живёт в cp1251 — иначе первый же лог падает
    с UnicodeEncodeError.
    """
    buffer = getattr(sys.stderr, "buffer", None)
    if buffer is not None:
        stream: Any = io.TextIOWrapper(
            buffer,
            encoding=(getattr(sys.stderr, "encoding", None) or "utf-8"),
            errors="replace",
            line_buffering=True,
        )
    else:  # pragma: no cover - stderr подменён (pytest capture)
        stream = sys.stderr
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.handlers[:] = [handler]
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.propagate = False


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Index the author's own corpus into a separate Qdrant collection "
            "(own_stake axis). Run with --dry-run first."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be indexed (files, chunks, per-doc_kind breakdown) "
        "without embedding or writing anything; recommended first invocation",
    )
    parser.add_argument(
        "--channel-root",
        type=Path,
        default=Path(os.environ.get("OWN_CORPUS_CHANNEL_ROOT") or DEFAULT_CHANNEL_ROOT),
        help="channel workspace root (default: the documented author workstation path)",
    )
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path(os.environ.get("OWN_CORPUS_WORKSPACE_ROOT") or DEFAULT_WORKSPACE_ROOT),
        help="parent directory holding the author's repositories",
    )
    parser.add_argument(
        "--roots-file",
        type=Path,
        default=None,
        help="JSON list of {slug, root, doc_kind, tier, exclude} replacing the default inventory",
    )
    parser.add_argument(
        "--tier",
        choices=("1", "2", "all"),
        default="all",
        help="index only tier 1 (channel voice + repo READMEs), only tier 2, or everything",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="restrict to these source slugs (repeatable)",
    )
    parser.add_argument(
        "--doc-kind",
        action="append",
        default=None,
        help="restrict to these doc_kind values (repeatable)",
    )
    parser.add_argument(
        "--exclude-glob",
        action="append",
        default=None,
        help="extra exclusion glob matched against '<slug>/<relative path>' (repeatable)",
    )
    parser.add_argument(
        "--collection",
        default=os.environ.get("QDRANT_OWN_CORPUS_COLLECTION") or DEFAULT_OWN_CORPUS_COLLECTION,
        help="base collection name; the versioned suffix is appended automatically",
    )
    parser.add_argument(
        "--expect-collection",
        default=None,
        help="refuse to run unless the resolved collection name equals this value",
    )
    parser.add_argument("--concurrency", type=int, default=4, help="parallel embed calls")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="resume file: already-indexed files are skipped on a re-run",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="write the plan/run report as UTF-8 JSON (safe on a cp1251 console)",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def select_sources(args: argparse.Namespace) -> list[CorpusSource]:
    if args.roots_file is not None:
        sources = sources_from_file(args.roots_file)
    else:
        sources = default_sources(args.channel_root, args.workspace_root)
    if args.source:
        wanted = {str(value).strip().lower() for value in args.source}
        sources = [source for source in sources if source.slug.lower() in wanted]
    return sources


PlannedFiles = list[tuple[CorpusFile, list[Chunk]]]


def build_plan(
    args: argparse.Namespace,
) -> tuple[PlannedFiles, SkipCounters, list[CorpusSource]]:
    sources = select_sources(args)
    skips = SkipCounters()
    files = collect_files(sources, skips=skips)

    extra_globs = tuple(args.exclude_glob or ())
    wanted_kinds = {str(value).strip() for value in (args.doc_kind or [])} or None

    planned: list[tuple[CorpusFile, list[Chunk]]] = []
    for entry in files:
        if args.tier != "all" and entry.tier != int(args.tier):
            skips.add("tier_filter")
            continue
        if wanted_kinds is not None and entry.doc_kind not in wanted_kinds:
            skips.add("doc_kind_filter")
            continue
        if extra_globs and matches_any_glob(entry.corpus_path, extra_globs):
            skips.add("cli_exclude_glob")
            continue
        text = read_corpus_file(entry, skips)
        if text is None:
            continue
        chunks = chunk_corpus_file(entry, text)
        if not chunks:
            skips.add("no_chunks")
            continue
        planned.append((entry, chunks))
    return planned, skips, sources


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    collection: str | None = None
    model = ""
    expected_dim: int | None = None
    if args.dry_run:
        # Имя коллекции показываем и в dry-run — это самая дешёвая проверка того,
        # что корпус поедет туда, куда смотрит own_stake. Модель берём из env,
        # а не из Settings: dry-run не должен требовать DATABASE_URL.
        collection = resolve_collection_name(
            args.collection,
            os.environ.get("GIGACHAT_EMBEDDINGS_MODEL") or "EmbeddingsGigaR",
        )
    else:
        from shared.config import get_settings

        settings = get_settings()
        model = str(settings.gigachat_embeddings_model or "").strip()
        collection = resolve_collection_name(args.collection, model)
        spec = get_embedding_model_spec(model)
        expected_dim = spec.dim if spec else None
        if args.expect_collection and collection != args.expect_collection:
            logger.error(
                "collection_name_mismatch resolved=%s expected=%s",
                collection,
                args.expect_collection,
            )
            return 3
        # Ассерт до первого обхода: падать надо до того, как потрачены токены.
        assert_document_purpose(EMBED_PURPOSE, model)

    try:
        planned, skips, sources = build_plan(args)
    except SecretsPathRefused as exc:
        logger.error("secrets_path_refused %s", exc)
        return 2

    report = build_plan_report(planned, skips=skips, sources=sources, collection=collection)
    log_plan_report(report)

    if args.dry_run:
        report["dry_run"] = True
        if args.report_json is not None:
            args.report_json.write_text(
                json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        logger.info("dry_run_complete nothing_embedded=1 nothing_written=1")
        return 0

    if collection is None:  # pragma: no cover - недостижимо, но не через assert
        logger.error("collection_name_unresolved")
        return 3
    try:
        result = asyncio.run(
            index_corpus(
                planned,
                model=model,
                expected_dim=expected_dim,
                expected_collection=collection,
                concurrency=int(args.concurrency),
                state_path=args.state_file,
            )
        )
    except SecretsPathRefused as exc:  # pragma: no cover - вторая линия обороны
        logger.error("secrets_path_refused %s", exc)
        return 2

    report["dry_run"] = False
    report["run"] = result
    logger.info(
        "index_complete collection=%s files=%s points=%s",
        result["collection"],
        result["files_indexed"],
        result["points_upserted"],
    )
    if args.report_json is not None:
        args.report_json.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
