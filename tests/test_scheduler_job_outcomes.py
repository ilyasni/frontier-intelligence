"""
Каждая зарегистрированная джоба планировщика обязана оставлять исход.

Класс дефекта, против которого стоит этот файл: **джоба исполняется, падает и не
оставляет в метриках ничего.** Замер 17.08.2026 — из 16 зарегистрированных джоб исход
отдавали ровно 8. Восемь тяжёлых пишут его внутри `_run_job_subprocess`, по одному разу
на воркспейс; восемь лёгких (балансы провайдеров, каталог и ключ OpenRouter, здоровье
xray, срочные тренды) исполняются прямо в event loop, мимо субпроцесса, и потому не
отмечались вовсе. Их падение улетало в APScheduler, попадало в лог и на этом кончалось.

Отдельно важно, ПОЧЕМУ это не требовало новых правил: `FrontierAdminJobFailing` и
`…Daily` фильтруют по `outcome`, а не по имени джобы, то есть покрывали бы эти восемь
с самого начала — считать было нечего. Правило было валидно и бессильно одновременно,
ровно как в `test_alert_rules_contract`.

Проверок три, и вторая важнее первой:
  * каждая зарегистрированная джоба покрыта ровно одним механизмом;
  * имя в `@_records_run_outcome("…")` СОВПАДАЕТ с `id=` в `add_job`. Расхождение
    молча приписало бы исход чужой джобе: метрика есть, непустая, и врёт;
  * ни одна джоба не покрыта обоими механизмами сразу — иначе один прогон считался бы
    дважды, и порог «≥2 отказа за 49ч» срабатывал бы с одного отказа.

Разбор статический (ast), без импорта проекта и без сети: модуль тянет БД и сервисы,
а проверяемое утверждение — о тексте, а не о поведении.
"""

from __future__ import annotations

import ast
from functools import lru_cache
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEDULER_PY = REPO_ROOT / "admin" / "backend" / "scheduler.py"

_DECORATOR = "_records_run_outcome"
_PER_WORKSPACE_CONST = "_PER_WORKSPACE_OUTCOME_JOBS"


@lru_cache(maxsize=1)
def _tree() -> ast.Module:
    return ast.parse(SCHEDULER_PY.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _registered_jobs() -> dict[str, str]:
    """{id из add_job: имя вызываемого}. Пусто = разбор сломался, а не джоб нет."""
    jobs: dict[str, str] = {}
    for node in ast.walk(_tree()):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "add_job"):
            continue
        job_id = next(
            (
                kw.value.value
                for kw in node.keywords
                if kw.arg == "id" and isinstance(kw.value, ast.Constant)
            ),
            None,
        )
        callable_name = None
        if node.args and isinstance(node.args[0], ast.Name):
            callable_name = node.args[0].id
        if job_id and callable_name:
            jobs[job_id] = callable_name
    return jobs


@lru_cache(maxsize=1)
def _per_workspace_jobs() -> frozenset[str]:
    for node in ast.walk(_tree()):
        # AnnAssign, а не только Assign: константа объявлена с аннотацией
        # (`_PER_WORKSPACE_OUTCOME_JOBS: frozenset[str] = …`), и разбор, знающий лишь
        # Assign, тихо возвращал пустое множество — тогда ВСЕ восемь тяжёлых джоб
        # выглядели бы непокрытыми, и проверка кричала бы на здоровом коде.
        if isinstance(node, ast.AnnAssign):
            targets = [node.target.id] if isinstance(node.target, ast.Name) else []
        elif isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        else:
            continue
        if _PER_WORKSPACE_CONST not in targets:
            continue
        for sub in ast.walk(node.value):
            if isinstance(sub, ast.Set):
                return frozenset(
                    e.value for e in sub.elts if isinstance(e, ast.Constant)
                )
    return frozenset()


@lru_cache(maxsize=1)
def _decorated_jobs() -> dict[str, str]:
    """{имя функции: job_name из декоратора}."""
    found: dict[str, str] = {}
    for node in ast.walk(_tree()):
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        for dec in node.decorator_list:
            if not isinstance(dec, ast.Call):
                continue
            name = dec.func.id if isinstance(dec.func, ast.Name) else None
            if name != _DECORATOR:
                continue
            if dec.args and isinstance(dec.args[0], ast.Constant):
                found[node.name] = dec.args[0].value
    return found


def test_parsing_actually_found_jobs() -> None:
    """Тест про тест: пустой разбор сделал бы все проверки ниже зелёными на пустоте."""
    assert len(_registered_jobs()) >= 16, (
        f"разобрано {len(_registered_jobs())} add_job — разбор сломался, "
        "остальные проверки в этом файле ничего не значат"
    )
    assert _per_workspace_jobs(), f"{_PER_WORKSPACE_CONST} не разобран"
    assert _decorated_jobs(), f"декоратор {_DECORATOR} не найден ни на одной функции"


def test_every_registered_job_reports_an_outcome() -> None:
    per_workspace = _per_workspace_jobs()
    decorated = _decorated_jobs()
    silent = [
        f"{job_id} (вызывает {fn})"
        for job_id, fn in sorted(_registered_jobs().items())
        if job_id not in per_workspace and fn not in decorated
    ]
    assert not silent, (
        "джобы исполняются, но не оставляют исхода в frontier_admin_job_runs_total, "
        "поэтому их падение не увидит ни один алерт: " + "; ".join(silent)
    )


def test_decorator_job_name_matches_registration_id() -> None:
    """Имя в декораторе — то же, что `id=`. Иначе исход припишется чужой джобе."""
    decorated = _decorated_jobs()
    mismatched = [
        f"{fn}: декоратор пишет '{decorated[fn]}', а зарегистрирована как '{job_id}'"
        for job_id, fn in sorted(_registered_jobs().items())
        if fn in decorated and decorated[fn] != job_id
    ]
    assert not mismatched, "; ".join(mismatched)


def test_no_job_is_counted_twice() -> None:
    """Оба механизма сразу дали бы двойной счёт одного прогона."""
    per_workspace = _per_workspace_jobs()
    decorated = _decorated_jobs()
    doubled = [
        job_id
        for job_id, fn in sorted(_registered_jobs().items())
        if job_id in per_workspace and fn in decorated
    ]
    assert not doubled, f"исход считается дважды: {doubled}"


def test_per_workspace_list_has_no_phantoms() -> None:
    """Имя в списке, которого нет среди зарегистрированных, — след переименования."""
    phantom = sorted(_per_workspace_jobs() - set(_registered_jobs()))
    assert not phantom, (
        f"{_PER_WORKSPACE_CONST} называет незарегистрированные джобы: {phantom} — "
        "после переименования такая строка молча оправдывает отсутствие исхода"
    )
