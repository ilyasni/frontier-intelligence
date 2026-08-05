"""
Имя консьюмера и уборка мёртвых (пункты 37 и 53 реестра).

Оба дефекта копили мусор в корне, а не разово:

  * уборка написана только в двух потребителях из четырёх, и корреляция прямая —
    где она есть, консьюмер один; где нет, к 05.08.2026 накопилось 85 и 14;
  * имя генерировалось на старт процесса (`f"{service}-{uuid4().hex[:8]}"`),
    поэтому каждый рестарт оставлял мёртвую запись навсегда: у `crawl4ai`
    84 рестарта за 45 суток дали ровно 85 записей вместе с текущей.

Главный инвариант формулируется как «имя переживает ПЕРЕСОЗДАНИЕ контейнера»,
а не «имя одинаково при одинаковом HOSTNAME»: вторая формулировка проходит и на
нерабочем варианте, потому что HOSTNAME равен id контейнера и меняется как раз
на `--force-recreate`.
"""

from __future__ import annotations

import pytest

from shared.stream_consumers import (
    DEAD_CONSUMER_IDLE_MS,
    cleanup_dead_consumers,
    consumer_name,
)

pytestmark = pytest.mark.unit


class _FakeRedis:
    def __init__(self, consumers, fail_info=False, fail_delete=False):
        self._consumers = consumers
        self.deleted: list[str] = []
        self._fail_info = fail_info
        self._fail_delete = fail_delete

    async def xinfo_consumers(self, stream, group):
        if self._fail_info:
            raise RuntimeError("redis is down")
        return self._consumers

    async def xdel_consumer(self, stream, group, name):
        if self._fail_delete:
            raise RuntimeError("delete failed")
        self.deleted.append(name)


# ── Имя ──────────────────────────────────────────────────────────────────────


def test_consumer_name_survives_container_recreate(monkeypatch) -> None:
    """Главный инвариант: пересоздание контейнера не меняет имя.

    Пересоздание меняет HOSTNAME (он равен короткому id контейнера) и pid.
    Прежняя реализация меняла имя на каждом старте процесса — отсюда 85 записей
    в группе при одном живом потребителе.
    """
    monkeypatch.delenv("FRONTIER_CONSUMER_INSTANCE", raising=False)

    monkeypatch.setenv("HOSTNAME", "3f2a91bc4d7e")
    before = consumer_name("crawl4ai")
    monkeypatch.setenv("HOSTNAME", "c1d8e5a04b19")  # другой контейнер
    after = consumer_name("crawl4ai")

    assert before == after == "crawl4ai-1", (
        "имя изменилось вместе с HOSTNAME — значит оно снова привязано к "
        "контейнеру и призраки будут копиться с каждого --force-recreate"
    )


def test_consumer_name_distinguishes_services_in_one_container(monkeypatch) -> None:
    """В контейнере worker живут три консьюмера, и их надо различать.

    Общая переменная с готовым именем сделала бы разбор «кто именно завис»
    невозможным, поэтому из окружения приходит только номер экземпляра.
    """
    monkeypatch.delenv("FRONTIER_CONSUMER_INSTANCE", raising=False)
    names = {consumer_name(s) for s in ("worker", "vision", "reindex")}
    assert names == {"worker-1", "vision-1", "reindex-1"}


def test_consumer_instance_can_be_overridden(monkeypatch) -> None:
    """Задел на реплики: масштабирование не должно требовать правки кода."""
    monkeypatch.setenv("FRONTIER_CONSUMER_INSTANCE", "2")
    assert consumer_name("worker") == "worker-2"


# ── Уборка ───────────────────────────────────────────────────────────────────


async def test_cleanup_removes_only_idle_consumers_with_zero_pending() -> None:
    """Оба условия обязательны, и второе — про потерю данных.

    Консьюмер с ненулевым pending держит неподтверждённые сообщения. Удалить
    его значит осиротить их в PEL, то есть превратить уборку мусора в потерю.
    """
    redis = _FakeRedis(
        [
            {"name": "alive", "idle": 1_000, "pending": 0},
            {"name": "ghost", "idle": DEAD_CONSUMER_IDLE_MS + 1, "pending": 0},
            {"name": "idle-but-holding", "idle": DEAD_CONSUMER_IDLE_MS + 1, "pending": 3},
            {"name": "myself", "idle": DEAD_CONSUMER_IDLE_MS + 1, "pending": 0},
        ]
    )
    removed = await cleanup_dead_consumers(redis, "stream:posts:crawl", "grp", keep="myself")

    assert removed == 1
    assert redis.deleted == ["ghost"], (
        "удалено не то: живой, держащий сообщения и собственная запись обязаны "
        f"остаться, фактически удалено {redis.deleted}"
    )


async def test_cleanup_never_raises_when_redis_misbehaves() -> None:
    """Уборка — вспомогательная работа и не имеет права уронить цикл потребителя."""
    assert await cleanup_dead_consumers(_FakeRedis([], fail_info=True), "s", "g") == 0

    redis = _FakeRedis(
        [{"name": "ghost", "idle": DEAD_CONSUMER_IDLE_MS + 1, "pending": 0}],
        fail_delete=True,
    )
    assert await cleanup_dead_consumers(redis, "s", "g") == 0


async def test_cleanup_survives_garbage_in_consumer_fields() -> None:
    """Redis отдаёт плоский вывод, и версии различаются составом полей."""
    redis = _FakeRedis(
        [
            {"name": "broken", "idle": "не число", "pending": 0},
            {"name": "", "idle": DEAD_CONSUMER_IDLE_MS + 1, "pending": 0},
            {"name": "ghost", "idle": DEAD_CONSUMER_IDLE_MS + 1, "pending": 0},
        ]
    )
    assert await cleanup_dead_consumers(redis, "s", "g") == 1
    assert redis.deleted == ["ghost"]


# ── Все четыре потребителя обязаны убирать за собой ──────────────────────────


def test_every_stream_consumer_wires_the_shared_cleanup() -> None:
    """Структурная проверка вместо четырёх почти одинаковых интеграционных.

    Ловит ровно тот перекос, из-за которого пункт 37 и возник: реализация была
    в двух файлах из четырёх, и никто не замечал, пока в группах не накопилось
    99 записей.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    consumers = {
        "worker/tasks/enrichment_task.py",
        "worker/tasks/vision_task.py",
        "worker/tasks/reindex_task.py",
        "crawl4ai/crawl4ai_service.py",
    }
    problems = []
    for rel in sorted(consumers):
        text = (root / rel).read_text(encoding="utf-8")
        if "cleanup_dead_consumers" not in text:
            problems.append(f"{rel}: не убирает мёртвых консьюмеров")
        if "consumer_name(" not in text:
            problems.append(f"{rel}: имя консьюмера не из shared.stream_consumers")
        # Проверка адресная: `uuid4()` в этих файлах законно используется для
        # trace_id, и широкий поиск по подстроке краснел бы на здоровом коде.
        # Смотрим именно на строку присваивания CONSUMER.
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("CONSUMER") and "=" in stripped and "uuid" in stripped:
                problems.append(f"{rel}: имя снова генерируется на старт процесса — {stripped}")
    assert not problems, "\n  ".join(["потребители Redis-стримов разъехались:", *problems])
