"""
Счётчики собственных стадий конвейера и наблюдаемость Redis-стримов (заход 8).

Три семейства, заведённые 2026-08-05, и у каждого свой класс молчания, который
оно закрывает:

  * `frontier_pipeline_stage_total` — у конвейера не было НИ ОДНОГО счётчика
    собственных стадий. Статус писался только в PostgreSQL, то есть СОСТОЯНИЕ
    можно было посчитать задним числом, а ПОТОКА не существовало: ни `rate()`,
    ни доли дропа, ни всплеска ошибок. На 190 977 `done` приходилось 122 600
    `dropped`, и 29 из 32 ошибок за всю историю случились за последние сутки —
    ни один дашборд этого не показывал.
  * `frontier_redis_dlq_length` — DLQ объявлена в коде с апреля 2026, ключей
    в Redis нет ни одного, и «пусто» было неотличимо от «механизм сломан».
    Поэтому ноль обязан ПЕЧАТАТЬСЯ, а не подразумеваться отсутствием серии.
  * `frontier_redis_stream_delivery_gap` / `_groups` — потеря при тримминге и
    осиротевший продюсер невыразимы через `lag`/`pending`: там, где они
    интересны, обе величины равны нулю по построению (группы нет — отставать
    нечему; записи срезаны — отстающих больше не существует). Ровно та тишина,
    которой сопровождался Redis-OOM 31.07.2026.

Тесты работают с реальным `prometheus_client`, если он доступен, и читают
значения через `REGISTRY.get_sample_value` — то есть проверяют экспозицию,
а не факт вызова сеттера.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

prometheus_client = pytest.importorskip("prometheus_client")

from prometheus_client import REGISTRY  # noqa: E402

from shared import metrics  # noqa: E402
from shared.redis_streams import delivery_gap  # noqa: E402


def _sample(name: str, labels: dict[str, str]) -> float:
    value = REGISTRY.get_sample_value(name, labels)
    return 0.0 if value is None else float(value)


# ── frontier_pipeline_stage_total ────────────────────────────────────────────


def test_pipeline_stage_counter_increments_only_the_matching_label_set() -> None:
    """Соседние комбинации меток не должны шевелиться.

    Проверяется приращение, а не абсолют: реестр глобальный и переживает другие
    тесты в том же процессе.
    """
    labels_done = {
        "service": "worker",
        "stage": "index",
        "workspace": "disruption",
        "outcome": "done",
    }
    labels_dropped = {**labels_done, "outcome": "dropped"}
    labels_other_ws = {**labels_done, "workspace": "design"}

    before_done = _sample("frontier_pipeline_stage_total", labels_done)
    before_dropped = _sample("frontier_pipeline_stage_total", labels_dropped)
    before_other = _sample("frontier_pipeline_stage_total", labels_other_ws)

    metrics.note_pipeline_stage("worker", "index", "done", "disruption")

    assert _sample("frontier_pipeline_stage_total", labels_done) == before_done + 1
    assert _sample("frontier_pipeline_stage_total", labels_dropped) == before_dropped
    assert _sample("frontier_pipeline_stage_total", labels_other_ws) == before_other


def test_pipeline_stage_counter_supports_bulk_increment() -> None:
    """ingest публикует пачку и отчитывается одним числом, а не в цикле."""
    labels = {
        "service": "ingest",
        "stage": "ingest",
        "workspace": "ai_trends",
        "outcome": "published",
    }
    before = _sample("frontier_pipeline_stage_total", labels)
    metrics.note_pipeline_stage("ingest", "ingest", "published", "ai_trends", 17)
    assert _sample("frontier_pipeline_stage_total", labels) == before + 17


def test_pipeline_stage_zero_count_creates_no_series() -> None:
    """Нулевой инкремент не должен плодить серию-пустышку.

    У ingest `publish_failed` в норме всегда ноль; если бы каждый прогон создавал
    серию, разрез по воркспейсам заполнился бы нулями и `rate()` по ним стал бы
    отвечать «всё хорошо» вместо «ничего не измерялось».
    """
    labels = {
        "service": "ingest",
        "stage": "ingest",
        "workspace": "workspace_that_never_fails",
        "outcome": "publish_failed",
    }
    metrics.note_pipeline_stage("ingest", "ingest", "publish_failed", labels["workspace"], 0)
    assert REGISTRY.get_sample_value("frontier_pipeline_stage_total", labels) is None


def test_pipeline_stage_blank_workspace_becomes_unknown() -> None:
    """Пустая метка выглядела бы в выдаче отдельным воркспейсом с именем «»."""
    labels = {
        "service": "worker",
        "stage": "index",
        "workspace": "unknown",
        "outcome": "error",
    }
    before = _sample("frontier_pipeline_stage_total", labels)
    metrics.note_pipeline_stage("worker", "index", "error", "")
    assert _sample("frontier_pipeline_stage_total", labels) == before + 1


# ── frontier_admin_job_runs_total ────────────────────────────────────────────


def test_admin_job_outcomes_are_counted_separately() -> None:
    """Провал прогона обязан оставлять след.

    При исключении дочерний процесс пишет JSON в stderr и возвращает 1, то есть
    перепубликовывать нечего — без этого счётчика провальные прогоны не
    оставляли в метриках вообще ничего, а именно ради них всё и делалось.
    """
    ok = {"service": "admin", "job": "run_novelty_judge", "outcome": "ok"}
    failed = {**ok, "outcome": "failed"}
    before_ok = _sample("frontier_admin_job_runs_total", ok)
    before_failed = _sample("frontier_admin_job_runs_total", failed)

    metrics.note_admin_job_run("run_novelty_judge", "failed")

    assert _sample("frontier_admin_job_runs_total", failed) == before_failed + 1
    assert _sample("frontier_admin_job_runs_total", ok) == before_ok


# ── DLQ и здоровье стримов ───────────────────────────────────────────────────


def test_missing_dlq_key_still_publishes_a_zero() -> None:
    """Главный кейс пункта 12.

    Ключа `stream:posts:parsed:dlq` в Redis нет вовсе. Если снапшот с нулевой
    длиной не создаёт серию, `FrontierDlqNotEmpty` не сможет отличить «poison
    не случался» от «DLQ вообще не наблюдается», и правило будет вечно зелёным
    по обеим причинам сразу.
    """
    snapshot = {
        "streams": [],
        "dlq": [{"stream": "stream:posts:parsed:dlq", "length": 0}],
        "health": [],
    }
    metrics.set_redis_stream_metrics("admin", snapshot)
    value = REGISTRY.get_sample_value(
        "frontier_redis_dlq_length",
        {"service": "admin", "stream": "stream:posts:parsed:dlq"},
    )
    assert value == 0.0, "серия обязана существовать со значением 0, а не отсутствовать"


def test_non_empty_dlq_is_reported_as_is() -> None:
    snapshot = {
        "streams": [],
        "dlq": [{"stream": "stream:posts:vision:dlq", "length": 3}],
        "health": [],
    }
    metrics.set_redis_stream_metrics("admin", snapshot)
    assert (
        REGISTRY.get_sample_value(
            "frontier_redis_dlq_length",
            {"service": "admin", "stream": "stream:posts:vision:dlq"},
        )
        == 3.0
    )


def test_orphan_stream_is_visible_as_zero_groups() -> None:
    """Продюсер без единого подписчика.

    `stream:posts:enriched` набрал 47 313 записей при длине 10 004 — то есть
    37 309 событий вытеснено триммингом непрочитанными. `lag` и `pending` при
    этом отсутствуют: групп нет, отставать нечему.
    """
    snapshot = {
        "streams": [],
        "dlq": [],
        "health": [
            {
                "stream": "stream:posts:enriched",
                "length": 10004,
                "entries_added": 47313,
                "groups": 0,
                "gaps": [],
            }
        ],
    }
    metrics.set_redis_stream_metrics("admin", snapshot)
    labels = {"service": "admin", "stream": "stream:posts:enriched"}
    assert REGISTRY.get_sample_value("frontier_redis_stream_groups", labels) == 0.0
    assert (
        REGISTRY.get_sample_value("frontier_redis_stream_entries_added", labels) == 47313.0
    )


def test_delivery_gap_is_published_per_group() -> None:
    snapshot = {
        "streams": [],
        "dlq": [],
        "health": [
            {
                "stream": "stream:posts:parsed",
                "length": 10000,
                "entries_added": 69375,
                "groups": 1,
                "gaps": [{"group": "enrichment_workers", "delivery_gap": True}],
            }
        ],
    }
    metrics.set_redis_stream_metrics("admin", snapshot)
    assert (
        REGISTRY.get_sample_value(
            "frontier_redis_stream_delivery_gap",
            {
                "service": "admin",
                "stream": "stream:posts:parsed",
                "group": "enrichment_workers",
            },
        )
        == 1.0
    )


# ── Сам инвариант потери ─────────────────────────────────────────────────────


# ── Счётчик объявлен ≠ счётчик вызывается ────────────────────────────────────
# Мутационный прогон 2026-08-05 поймал дыру в тестах выше: они проверяют
# ПОМОЩНИК, а не то, что его кто-то зовёт. Удаление вызова из
# `_update_indexing_status` и из обработчика провала джоба оставляло всё зелёным.
#
# Это буквально дефект пункта 24 реестра, воспроизведённый в тестах к его же
# починке: метрика объявлена, выглядит рабочей, значений не отдаёт. Поэтому
# ниже — тесты на РЕАЛЬНЫЕ точки вызова.


class _FakeSession:
    """Минимальная замена AsyncSession: execute/commit ничего не делают."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def execute(self, *args, **kwargs):
        return None

    async def commit(self):
        return None


async def test_update_indexing_status_actually_counts_the_stage() -> None:
    """Вызов внутри рабочего кода, а не сам счётчик."""
    from types import SimpleNamespace

    from worker.tasks.enrichment_task import EnrichmentTask

    task = EnrichmentTask.__new__(EnrichmentTask)
    task.Session = _FakeSession
    task.settings = SimpleNamespace()

    labels = {
        "service": "worker",
        "stage": "index",
        "workspace": "disruption",
        "outcome": "dropped",
    }
    before = _sample("frontier_pipeline_stage_total", labels)
    await task._update_indexing_status(
        "post-1", "dropped", qdrant_id="", graph_status="skipped", workspace_id="disruption"
    )
    assert _sample("frontier_pipeline_stage_total", labels) == before + 1, (
        "_update_indexing_status не инкрементировал счётчик стадии — метрика "
        "объявлена, но её никто не вызывает, то есть ровно пункт 24 реестра"
    )


async def test_emit_to_stream_actually_counts_published_and_failed() -> None:
    """Вход конвейера считается на реальном пути ingest."""
    from ingest.sources.base import AbstractSource

    class _Redis:
        def __init__(self) -> None:
            self.calls = 0

        async def xadd(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("redis is full")
            return "1-0"

    # AbstractSource объявляет абстрактный fetch(); подставляем минимальный
    # конкретный подкласс — нас интересует только emit_to_stream базового класса.
    class _Source(AbstractSource):
        async def fetch(self):  # pragma: no cover - не вызывается
            return []

    source = _Source.__new__(_Source)
    source.redis = _Redis()
    source.stream_name = "stream:posts:parsed"
    source.workspace_id = "ai_research"
    source.source_id = "test-source"

    class _Event:
        external_id = "42"

        def model_dump(self, **kwargs):
            return {"external_id": "42"}

    published = {
        "service": "ingest",
        "stage": "ingest",
        "workspace": "ai_research",
        "outcome": "published",
    }
    failed = {**published, "outcome": "publish_failed"}
    before_published = _sample("frontier_pipeline_stage_total", published)
    before_failed = _sample("frontier_pipeline_stage_total", failed)

    pushed = await source.emit_to_stream([_Event(), _Event(), _Event()])

    assert pushed == 2
    assert _sample("frontier_pipeline_stage_total", published) == before_published + 2
    assert _sample("frontier_pipeline_stage_total", failed) == before_failed + 1


async def test_failed_job_subprocess_actually_counts_the_failure(monkeypatch) -> None:
    """Провал ребёнка обязан отмечаться ДО того, как поднимется исключение.

    Кросс-платформенно: код возврата 1, а не сигнал, — `signal.Signals(9)`
    на Windows не существует, и тест на SIGKILL там падает по платформе.
    """
    import asyncio

    from admin.backend import scheduler as scheduler_module

    class _FakeProc:
        returncode = 1

        async def communicate(self):
            return b"", b'{"status":"error"}'

    async def _fake_exec(*args, **kwargs):
        return _FakeProc()

    monkeypatch.setattr(scheduler_module.asyncio, "create_subprocess_exec", _fake_exec)

    labels = {"service": "admin", "job": "run_graph_maintenance", "outcome": "failed"}
    before = _sample("frontier_admin_job_runs_total", labels)

    with pytest.raises(RuntimeError):
        await scheduler_module._run_job_subprocess("run_graph_maintenance", "disruption")

    assert _sample("frontier_admin_job_runs_total", labels) == before + 1, (
        "провал дочернего процесса не отмечен в метриках: ребёнок пишет JSON "
        "в stderr и возвращает 1, перепубликовывать нечего — без этого счётчика "
        "провальный прогон не оставляет следа вообще"
    )
    await asyncio.sleep(0)


async def test_successful_job_subprocess_republishes_child_metrics(monkeypatch) -> None:
    """Ради чего пункт 24 и делался: значения ребёнка доезжают до экспозиции родителя."""
    import json as _json

    from admin.backend import scheduler as scheduler_module

    payload = {
        "status": "ok",
        "workspace_id": None,
        "results": [
            {"workspace_id": "disruption", "judged": 5, "underrated": 2, "failed": 3},
            {"workspace_id": "design", "judged": 1, "underrated": 0, "failed": 0},
        ],
    }

    class _FakeProc:
        returncode = 0

        async def communicate(self):
            return _json.dumps(payload).encode(), b""

    async def _fake_exec(*args, **kwargs):
        return _FakeProc()

    monkeypatch.setattr(scheduler_module.asyncio, "create_subprocess_exec", _fake_exec)

    failed_labels = {"service": "admin", "verdict": "failed"}
    underrated_labels = {"service": "admin", "verdict": "underrated"}
    confirmed_labels = {"service": "admin", "verdict": "confirmed_weak"}
    before_failed = _sample("frontier_novelty_judge_total", failed_labels)
    before_underrated = _sample("frontier_novelty_judge_total", underrated_labels)
    before_confirmed = _sample("frontier_novelty_judge_total", confirmed_labels)

    await scheduler_module._run_job_subprocess("run_novelty_judge", None)

    # 3 + 0 провалов, 2 + 0 недооценённых, confirmed_weak = (5-2) + (1-0) = 4.
    assert _sample("frontier_novelty_judge_total", failed_labels) == before_failed + 3
    assert _sample("frontier_novelty_judge_total", underrated_labels) == before_underrated + 2
    assert _sample("frontier_novelty_judge_total", confirmed_labels) == before_confirmed + 4


async def test_graph_health_is_republished_with_workspace_labels(monkeypatch) -> None:
    """Метка `workspace` берётся из разбивки `results`, а не теряется на `__all__`."""
    import json as _json

    from admin.backend import scheduler as scheduler_module

    payload = {
        "status": "ok",
        "workspace_id": None,
        "results": [
            {
                "workspace_id": "disruption",
                "health": {"duplicate_clusters": 137, "concept_count": 758337},
            }
        ],
    }

    class _FakeProc:
        returncode = 0

        async def communicate(self):
            return _json.dumps(payload).encode(), b""

    monkeypatch.setattr(
        scheduler_module.asyncio,
        "create_subprocess_exec",
        lambda *a, **k: _as_awaitable(_FakeProc()),
    )

    await scheduler_module._run_job_subprocess("run_graph_maintenance", None)

    assert (
        REGISTRY.get_sample_value(
            "frontier_graph_health",
            {"service": "admin", "workspace": "disruption", "metric": "duplicate_clusters"},
        )
        == 137.0
    )


async def _as_awaitable(value):
    return value


@pytest.mark.parametrize(
    ("last_delivered", "first_entry", "expected", "why"),
    [
        ("100-0", "200-0", True, "группа не дошла до самой старой уцелевшей записи"),
        ("300-0", "200-0", False, "группа впереди первой записи — всё прочитано"),
        ("200-0", "200-0", False, "ровно на границе потери ещё нет"),
        ("200-5", "200-7", True, "сравнение обязано учитывать порядковый номер, не только время"),
        ("0-0", "200-0", False, "новая группа ничего не читала — это не потеря"),
        ("", "200-0", False, "неразбираемый id не повод объявлять потерю"),
        ("100-0", "", False, "пустой стрим: first-entry отсутствует"),
        ("garbage", "200-0", False, "мусор вместо id"),
    ],
)
def test_delivery_gap_invariant(
    last_delivered: str, first_entry: str, expected: bool, why: str
) -> None:
    """`lag` и `pending` этот класс потери не видят — проверяем сам предикат.

    Отдельно важен кейс `0-0`: у только что созданной группы `last-delivered-id`
    равен нулю, и наивное сравнение объявило бы потерю на каждом новом
    консьюмере, то есть правило стало бы шумом с первого дня.
    """
    assert delivery_gap(last_delivered, first_entry) is expected, why
