"""Имя консьюмера и уборка мёртвых — один раз на все четыре потребителя.

Пункты 37 и 53 реестра. До 2026-08-05 обе вещи были устроены так, что мусор
копился в корне:

  * Уборка мёртвых консьюмеров написана только в `enrichment_task` и
    `vision_task`. У `reindex_task` и `crawl4ai_service` её нет вовсе, и
    корреляция прямая: где уборка есть — один консьюмер, где нет — 85 и 14.
  * Имя консьюмера генерировалось на старт процесса, поэтому КАЖДЫЙ рестарт
    оставлял запись навсегда. У `crawl4ai` 84 рестарта за 45 суток — отсюда
    ровно 85 записей вместе с текущей.

Про имя отдельно, потому что очевидное решение здесь не работает. Взять
`HOSTNAME` мало: у этих сервисов в compose нет ни `hostname:`, ни
`container_name`, поэтому внутри контейнера HOSTNAME равен короткому id
контейнера. Он переживает `restart`, но МЕНЯЕТСЯ на каждом `--force-recreate`
и на каждой пересборке — а их в маршруте по нескольку на заход. То есть
«стабильное имя из HOSTNAME» продержалось бы до ближайшего деплоя.

Поэтому имя берётся из явной переменной окружения, а HOSTNAME остаётся лишь
последним запасным вариантом. Инвариант, который должен держать тест:
имя переживает пересоздание контейнера, а не только рестарт процесса.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# Консьюмер считается мёртвым, если простаивает дольше этого и НИЧЕГО не держит.
# Оба условия обязательны: консьюмер с ненулевым pending держит неподтверждённые
# сообщения, и его удаление осиротило бы их в PEL — то есть превратило бы уборку
# мусора в потерю данных.
DEAD_CONSUMER_IDLE_MS = 3_600_000


def consumer_name(service: str) -> str:
    """Стабильное имя консьюмера: `<сервис>-<номер экземпляра>`.

    Номер берётся из `FRONTIER_CONSUMER_INSTANCE` и по умолчанию равен `1`,
    то есть в compose ничего объявлять не нужно, пока реплика одна. Захотите
    масштабировать потребителя — задайте разные номера репликам.

    Почему НЕ `HOSTNAME`, хотя он напрашивается: у этих сервисов в compose нет
    ни `hostname:`, ни `container_name`, поэтому внутри контейнера HOSTNAME
    равен короткому id контейнера. Он переживает `restart`, но МЕНЯЕТСЯ на
    каждом `--force-recreate` и на каждой пересборке — то есть «стабильное имя
    из HOSTNAME» продержалось бы до ближайшего деплоя, а их в маршруте
    по нескольку на заход.

    Почему не одна общая переменная с готовым именем: в контейнере `worker`
    живут ТРИ разных консьюмера (enrichment, vision, reindex). Общее имя
    сделало бы метку `service` бессмысленной, а разбор «кто именно завис» —
    невозможным. Имя собирается из аргумента, а из окружения приходит только
    номер экземпляра.
    """
    instance = os.environ.get("FRONTIER_CONSUMER_INSTANCE", "").strip() or "1"
    return f"{service}-{instance}"


async def cleanup_dead_consumers(redis, stream: str, group: str, keep: str = "") -> int:
    """Удалить мёртвых консьюмеров группы. Возвращает число удалённых.

    Никогда не поднимает исключение: уборка — вспомогательная работа, и её сбой
    не имеет права уронить потребительский цикл.
    """
    removed = 0
    try:
        consumers = await redis.xinfo_consumers(stream, group)
    except Exception as exc:
        logger.warning("consumer cleanup: xinfo failed stream=%s: %s", stream, exc)
        return 0

    for consumer in consumers or []:
        name = consumer.get("name", "")
        if not name or name == keep:
            continue
        try:
            idle_ms = int(consumer.get("idle", 0))
            pending = int(consumer.get("pending", 0))
        except (TypeError, ValueError):
            continue
        if idle_ms <= DEAD_CONSUMER_IDLE_MS or pending != 0:
            continue
        try:
            await redis.xdel_consumer(stream, group, name)
        except Exception as exc:
            logger.warning("consumer cleanup: delete failed %s: %s", name, exc)
            continue
        removed += 1
        logger.info(
            "deleted dead consumer stream=%s group=%s name=%s idle=%ds",
            stream,
            group,
            name,
            idle_ms // 1000,
        )
    return removed
