"""Чтение `workspaces.cross_workspace_bridges` и расширение набора воркспейсов.

Зачем модуль. Поле `cross_workspace_bridges` заполнено у пяти воркспейсов из шести,
у него три пути записи (bootstrap из YAML, PATCH из админки, редактор в UI) — и до
сих пор ни одного чтения ради поведения: значение только попадало в ответ
`list_workspaces`/`get_workspace_overview`. Модель-клиент читала обещание поведения,
которого нет.

Здесь ровно чтение и разрешение набора. Два правила, из-за которых модуль отдельный:

1. **Источник — БД, а не YAML.** `config/workspaces.yml` запечён в образ (mcp/Dockerfile
   делает `COPY config/`), а мосты правятся через админку в рантайме. Читать YAML значит
   читать снимок на момент сборки.
2. **Неизвестный мост не роняет запрос.** Мост может указывать на воркспейс, которого
   уже нет: в БД строка осталась, в allowlist его нет. Такой мост логируется и
   пропускается — падать из-за чужой опечатки поиск не должен.

Атрибуция происхождения — обязанность вызывающего кода: набор воркспейсов расширяется
здесь, а вот пометить каждый результат воркспейсом, откуда он приехал, обязаны
`search_frontier` и `get_frontier_brief`. Без этого `disruption` начинает молча получать
сигналы `ai_trends`, и отличить своё от чужого нечем.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from mcp.guards import get_workspace_allowlist
from shared.db import get_engine

logger = logging.getLogger(__name__)

# Потолок на добавленные мостами воркспейсы. У `disruption` мостов четыре, и каждый
# в брифе — это полный `get_workspace_overview` (pipeline_stats + sources_health +
# clusters, три-четыре запроса в PG на штуку). Потолок держит стоимость одного вызова
# ограниченной сверху вне зависимости от того, что напишут в редакторе мостов.
MAX_BRIDGED_WORKSPACES = 8


async def _fetch_bridge_rows(workspace_ids: list[str]) -> list[dict[str, Any]]:
    """Сырые строки `(id, cross_workspace_bridges)` из БД.

    Отдельной функцией, чтобы её можно было подменить в тестах: вся остальная логика
    разрешения — чистая и проверяется без PostgreSQL.
    """
    if not workspace_ids:
        return []
    async with AsyncSession(get_engine()) as session:
        result = await session.execute(
            text(
                """
                SELECT id, cross_workspace_bridges
                FROM workspaces
                WHERE id = ANY(:ids)
                """
            ),
            {"ids": list(workspace_ids)},
        )
        return [dict(row) for row in result.mappings().all()]


def normalize_bridge_value(raw: object) -> list[str]:
    """Привести значение колонки к списку слагов.

    Колонка jsonb, но по трём путям записи в неё попадает не только список: bootstrap
    кладёт список из YAML, PATCH из админки — то, что пришло в теле. Драйвер отдаёт
    либо list, либо строку (если значение записали как json-текст). Обрабатываются оба,
    плюс строка через запятую — форма, которую даёт наивная ручная правка.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        text_value = raw.strip()
        if not text_value:
            return []
        try:
            parsed = json.loads(text_value)
        except ValueError:
            parsed = [chunk for chunk in text_value.split(",")]
        return normalize_bridge_value(parsed)
    if isinstance(raw, dict):
        # Форма {"targets": [...]} встречается в черновиках редактора.
        return normalize_bridge_value(raw.get("targets") or raw.get("workspaces"))
    if isinstance(raw, (list, tuple, set)):
        items = [str(item).strip() for item in raw]
        return [item for item in items if item]
    return []


async def resolve_bridge_workspaces(
    workspaces: list[str],
    *,
    include_bridges: bool,
) -> tuple[list[str], dict[str, Any]]:
    """Набор воркспейсов для запроса плюс метаданные о том, как он получился.

    Возвращает `(effective_ids, meta)`.

    `include_bridges=False` — тождественная операция: набор равен запрошенному, в БД
    не ходим вообще. Это и есть инвариант «поведение существующих клиентов не меняется»:
    ни одного лишнего запроса, ни одного лишнего ключа на пути по умолчанию.

    `include_bridges=True` — к запрошенному набору добавляются объявленные мосты, в том
    же порядке, в котором они лежат в колонке. Запрошенные воркспейсы всегда идут первыми
    и никогда не вытесняются потолком.

    Сбой чтения БД деградирует до запрошенного набора с `degraded=True` в meta, а не в
    500: мосты — это расширение выдачи, а не её условие.
    """
    requested: list[str] = []
    for value in workspaces:
        slug = str(value or "").strip()
        if slug and slug not in requested:
            requested.append(slug)

    meta: dict[str, Any] = {
        "include_bridges": bool(include_bridges),
        "requested": list(requested),
        "bridged": [],
        "skipped_unknown": [],
        "bridges_by_workspace": {},
        "degraded": False,
    }
    if not include_bridges or not requested:
        return requested, meta

    try:
        rows = await _fetch_bridge_rows(requested)
    except Exception:
        logger.exception("cross_workspace_bridges read failed workspaces=%s", requested)
        meta["degraded"] = True
        return requested, meta

    allowlist = get_workspace_allowlist()
    effective = list(requested)
    bridged: list[str] = []
    skipped: list[str] = []
    by_workspace: dict[str, list[str]] = {}

    row_by_id = {str(row.get("id")): row for row in rows}
    for source in requested:
        row = row_by_id.get(source)
        declared = normalize_bridge_value((row or {}).get("cross_workspace_bridges"))
        by_workspace[source] = declared
        for target in declared:
            if target == source or target in effective:
                continue
            if target not in allowlist:
                # Мост указывает на воркспейс, которого нет в allowlist: строка в БД
                # пережила удаление/переименование. Это не повод ронять запрос.
                if target not in skipped:
                    skipped.append(target)
                logger.warning(
                    "cross_workspace_bridge unknown_target source=%s target=%s",
                    source[:64],
                    target[:64],
                )
                continue
            if len(bridged) >= MAX_BRIDGED_WORKSPACES:
                logger.warning(
                    "cross_workspace_bridge cap_reached source=%s target=%s cap=%d",
                    source[:64],
                    target[:64],
                    MAX_BRIDGED_WORKSPACES,
                )
                break
            effective.append(target)
            bridged.append(target)

    meta["bridged"] = bridged
    meta["skipped_unknown"] = skipped
    meta["bridges_by_workspace"] = by_workspace
    return effective, meta
