from __future__ import annotations

import time

import redis.asyncio as aioredis

from shared.reindex import GROUP_POSTS_REINDEX, STREAM_POSTS_REINDEX

DEFAULT_STREAM_GROUPS: tuple[tuple[str, str], ...] = (
    ("stream:posts:parsed", "enrichment_workers"),
    ("stream:posts:vision", "vision_workers"),
    ("stream:posts:crawl", "crawl4ai_workers"),
    (STREAM_POSTS_REINDEX, GROUP_POSTS_REINDEX),
)

# DLQ, куда enrichment и vision складывают poison-сообщения. Значения совпадают
# с дефолтами shared/config.py (INDEXING_DLQ_STREAM / VISION_DLQ_STREAM);
# держим их здесь литералами, потому что этот модуль сознательно не тянет
# Settings — он импортируется и из admin, и из воркеров.
DEFAULT_DLQ_STREAMS: tuple[str, ...] = (
    "stream:posts:parsed:dlq",
    "stream:posts:vision:dlq",
)

# Маска для перечисления ВСЕХ живых стримов, а не только четырёх известных.
# Нужна детектору осиротевших продюсеров: стрим, в который пишут и который никто
# не читает, по определению отсутствует в списке ожидаемых пар.
STREAM_KEY_PATTERN = "stream:*"


def _message_age_seconds(message_id: str) -> float:
    try:
        ts_ms = int(str(message_id).split("-", 1)[0])
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, time.time() - ts_ms / 1000.0)


def _parse_stream_id(message_id: str) -> tuple[int, int] | None:
    """`1785888000123-4` → (1785888000123, 4). None, если это не id."""
    raw = str(message_id or "").strip()
    if not raw or "-" not in raw:
        return None
    left, _, right = raw.partition("-")
    try:
        return int(left), int(right)
    except (TypeError, ValueError):
        return None


def delivery_gap(last_delivered_id: str, first_entry_id: str) -> bool:
    """Были ли записи вытеснены триммингом ДО того, как группа их прочитала.

    Инвариант здорового стрима: группа успела дойти как минимум до самой старой
    записи, которая ещё лежит в стриме. Если `last-delivered-id` СТАРШЕ, чем
    `first-entry-id`, значит MAXLEN срезал промежуток между ними, и эти сообщения
    не будут доставлены никогда.

    Отдельная метрика нужна потому, что `lag` и `pending` при этом остаются
    нулевыми: с точки зрения Redis группа не отстаёт — отстающих записей больше
    не существует. Ровно та тишина, которой сопровождался Redis-OOM 31.07.2026.
    """
    delivered = _parse_stream_id(last_delivered_id)
    first = _parse_stream_id(first_entry_id)
    if delivered is None or first is None:
        return False
    # `0-0` — группа не прочитала ещё ничего. Это не потеря, а новый консьюмер.
    if delivered == (0, 0):
        return False
    return delivered < first


async def _collect_dlq_lengths(client, dlq_streams: tuple[str, ...]) -> list[dict]:
    """Длина каждой DLQ. Несуществующий ключ даёт НОЛЬ, а не отсутствие записи.

    Это весь смысл блока. До 2026-08-05 DLQ не наблюдалась вовсе: ключей в Redis
    нет (ни одного poison-сообщения так и не случилось), и «пусто» было
    неотличимо от «механизм сломан и никогда ничего не запишет». Серия со
    значением 0 отвечает на этот вопрос однозначно.
    """
    items: list[dict] = []
    for stream in dlq_streams:
        try:
            length = int(await client.xlen(stream))
        except Exception:
            # Ключа нет — XLEN отдаёт 0; сюда попадаем только при реальной
            # ошибке связи, и тогда честнее показать 0 с пометкой, чем
            # промолчать: молчание уже один раз стоило инцидента.
            length = 0
        items.append({"stream": stream, "length": length})
    return items


async def _collect_stream_health(client, expected_groups: dict[str, str]) -> list[dict]:
    """Перечислить ВСЕ живые стримы и оценить их по двум инвариантам.

    1. `groups` — сколько consumer-групп подписано. Ноль при растущем
       `entries-added` означает продюсера, пишущего в пустоту: события
       вытесняются триммингом, не будучи прочитанными никем, и ни `lag`,
       ни `pending` этого не показывают (групп нет — нечему отставать).
    2. `delivery_gap` — были ли записи срезаны MAXLEN до доставки (см.
       `delivery_gap()`).
    """
    items: list[dict] = []
    try:
        keys = [key async for key in client.scan_iter(match=STREAM_KEY_PATTERN, count=100)]
    except Exception:
        return items

    for stream in sorted(keys):
        try:
            info = await client.xinfo_stream(stream)
        except Exception:
            continue
        first_entry = info.get("first-entry") or []
        first_entry_id = str(first_entry[0]) if first_entry else ""
        try:
            groups_info = await client.xinfo_groups(stream)
        except Exception:
            groups_info = []

        gaps = []
        for group_info in groups_info or []:
            gaps.append(
                {
                    "group": str(group_info.get("name") or ""),
                    "delivery_gap": delivery_gap(
                        str(group_info.get("last-delivered-id") or ""), first_entry_id
                    ),
                }
            )

        items.append(
            {
                "stream": stream,
                "length": int(info.get("length") or 0),
                "entries_added": int(info.get("entries-added") or 0),
                "groups": len(groups_info or []),
                "expected_group": expected_groups.get(stream, ""),
                "gaps": gaps,
            }
        )
    return items


async def collect_redis_stream_snapshot(
    redis_url: str,
    *,
    stream_groups: tuple[tuple[str, str], ...] = DEFAULT_STREAM_GROUPS,
    dlq_streams: tuple[str, ...] = DEFAULT_DLQ_STREAMS,
) -> dict:
    client = aioredis.from_url(redis_url, decode_responses=True)
    try:
        streams = []
        for stream, expected_group in stream_groups:
            try:
                groups_info = await client.xinfo_groups(stream)
            except Exception:
                groups_info = []

            matching_group = next(
                (
                    group_info
                    for group_info in groups_info
                    if group_info.get("name") == expected_group
                ),
                None,
            )
            lag = int((matching_group or {}).get("lag") or 0)
            pending = int((matching_group or {}).get("pending") or 0)
            last_delivered_id = str((matching_group or {}).get("last-delivered-id") or "")

            oldest_pending_age_seconds = 0.0
            pending_items = []
            try:
                if pending > 0:
                    pending_items = await client.xpending_range(
                        stream,
                        expected_group,
                        "-",
                        "+",
                        1,
                    )
            except Exception:
                pending_items = []
            if pending_items:
                oldest_pending_age_seconds = _message_age_seconds(
                    str(pending_items[0].get("message_id") or "")
                )

            consumers = []
            try:
                consumers_info = await client.xinfo_consumers(stream, expected_group)
            except Exception:
                consumers_info = []
            for consumer in consumers_info or []:
                consumers.append(
                    {
                        "name": str(consumer.get("name") or ""),
                        "pending": int(consumer.get("pending") or 0),
                        "idle_seconds": round(float(consumer.get("idle") or 0) / 1000.0, 3),
                    }
                )

            streams.append(
                {
                    "stream": stream,
                    "group": expected_group,
                    "lag": lag,
                    "pending": pending,
                    "oldest_pending_age_seconds": round(oldest_pending_age_seconds, 3),
                    "last_delivered_id": last_delivered_id,
                    "consumers": consumers,
                }
            )
        return {
            "streams": streams,
            "dlq": await _collect_dlq_lengths(client, dlq_streams),
            "health": await _collect_stream_health(
                client, {stream: group for stream, group in stream_groups}
            ),
        }
    finally:
        await client.aclose()
