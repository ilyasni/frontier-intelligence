"""Редакторский контур: экспорт карточек в inbox и разметка выбора автора.

export_inbox_cards собирает топ-N сигналов через существующие observability-инструменты
(вызов в процессе, без HTTP-хопа к самому себе), минтит batch_id и отдаёт готовый markdown.
Файл он не пишет и не может: стек живёт на сервере, а content/inbox.md — на рабочей станции;
дописывает локальный агент, у которого под рукой скил голоса. Поэтому поле question уходит
пустым — вопрос формулируется там, где есть чем его проверить.

record_card_feedback принимает ту же пятёрку обратно: одна строка на карточку, ровно одна
с verdict='chosen'. Общий reason батча объясняет, почему остальные карточки прошли мимо, —
поэтому он ложится только на строки verdict='passed'. У chosen-строки reason пустой, если
автор не написал отдельного объяснения выбора в поле карточки: писать туда «остальные —
пересказ чужого релиза» значит выдать это за характеристику выбранной карточки, а таблица
append-only и переписать строку нечем.

Радиус поражения на запись — одна таблица card_feedback: ни пороги, ни Neo4j этот инструмент
не трогает. Из workspaces делается один SELECT 1 на существование воркспейса (иначе FK роняет
вставку пятисотым, и ручная разметка теряется). Калибровка порогов по накопленной разметке —
отдельное задание через существующий гейт approve_threshold_change, а не отсюда.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from mcp.guards import assert_known_workspace
from shared.db import get_engine

from .observability import (
    ClusterListRequest,
    EmergingSignalListRequest,
    MissingSignalListRequest,
    list_clusters,
    list_emerging_signals,
    list_missing_signals,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Совпадает с CHECK (entity_kind IN ...) в 20260803_card_feedback.sql.
ENTITY_KINDS = {"post", "semantic", "trend", "emerging", "missing"}

# Куда локальный агент дописывает markdown (относительно D:\telegramChanel).
INBOX_TARGET_PATH = "content/inbox.md"

# Колонка БД, из которой у карточки этого вида взялась бы релевантность. None — колонки нет.
# Проверено по storage/postgres/init.sql: relevance живёт только в semantic_clusters
# .avg_relevance, posts.relevance_score и signal_time_series.avg_relevance. Ни emerging_signals
# (signal_score / confidence / velocity_score), ни trend_clusters (burst_score / coherence /
# novelty), ни missing_signals (gap_score / *_frequency) релевантности не хранят, а перечисленные
# метрики — другая шкала и другой смысл. Поэтому relevance_at_pick у этих карточек NULL, и это
# заявлено в ответе явно, а не подменено ближайшим числом.
RELEVANCE_COLUMN_BY_SOURCE: dict[str, str | None] = {
    "emerging": None,
    "trend": None,
    "missing": None,
}

# «Колонки в таблице-источнике нет» — это НЕ «числа нет нигде». Для этих видов карточек
# релевантность посчитана в signal_time_series.avg_relevance: init.sql ограничивает entity_kind
# значениями ('semantic','trend','emerging'), а worker/services/semantic_clustering.py
# (_persist_signal_outputs → _series_rows_for_posts) пишет туда entity_id = id строки
# emerging_signals / trend_clusters, то есть ровно тот id, который уходит в карточку.
# У 'missing' такой строки нет и быть не может — missing_signals не проходит по CHECK.
# Путать эти два случая нельзя: во втором числа правда нет, в первом оно лежит рядом.
SOURCES_WITH_SERIES_RELEVANCE = frozenset({"emerging", "trend"})

# Диагностику пустой выдачи отдаёт только list_missing_signals (ключ last_run, появляется
# исключительно при пустом signals). list_emerging_signals и list_clusters ничего похожего не
# возвращают, и собственный запрос к cluster_runs отсюда был бы цифрой, которую инструмент-
# источник не подтверждает, — поэтому для них честно пишем, что различить нечем.
SOURCES_WITH_RUN_DIAGNOSTIC = frozenset({"missing"})

# list_card_feedback отдаёт два набора счётчиков с ОДИНАКОВЫМИ ключами — по странице и по
# всей таблице. Перепутать их значит ошибиться ровно в том числе, ради которого их считают,
# поэтому что есть что, сказано в самом ответе, а не только в docstring.
COUNTS_NOTE = (
    "count и axes_filled описывают ТОЛЬКО возвращённую страницу (limit). "
    "total и axes_filled_total — всю таблицу под тем же фильтром по workspace. "
    "Гейт спеки C («60 пар с обоими числами») читается по axes_filled_total.both: "
    "при limit по умолчанию 50 и максимуме 200 страница до 60 может просто не дотянуться. "
    "truncated=true означает, что строк в таблице больше, чем показано."
)


class CardInput(BaseModel):
    """Снимок одной показанной карточки. Та же форма, что отдаёт export_inbox_cards."""

    entity_kind: str  # 'post' | 'semantic' | 'trend' | 'emerging' | 'missing'
    entity_id: str
    title: str | None = None
    metric_name: str | None = None
    metric_value: float | None = None
    relevance: float | None = None
    # Задача B построена, но её числа живут на ДОКУМЕНТАХ: search_frontier и search_balanced
    # кладут own_stake и stake_quadrant ключами верхнего уровня каждого хита. Оттуда карточка
    # приезжает с обоими значениями и перекладывать ничего не надо. У карточек
    # export_inbox_cards (строки emerging_signals / trend_clusters / missing_signals) их нет —
    # см. _axes_availability, там сказано почему.
    own_stake: float | None = None
    stake_quadrant: str | None = None
    question: str | None = None
    # Персональный reason строки. Общий reason батча (RecordCardFeedbackRequest.reason)
    # объясняет, почему карточку прошли мимо, и на chosen-строку не ставится; если автору
    # есть что сказать именно про выбранную карточку — это пишется сюда.
    reason: str | None = None


class RecordCardFeedbackRequest(BaseModel):
    workspace: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    cards: list[CardInput] = Field(min_length=2, max_length=10)
    # None — половина сквозного батча без выбранной карточки. Вызов принимает ровно
    # один workspace, а недельная пятёрка может прийти из двух; требование «в каждом
    # вызове свой chosen» давало бы две chosen-строки на один batch_id, а таблица
    # append-only и исправить их потом нечем. Ровно одна chosen на батч в целом
    # держится уникальным индексом uq_card_feedback_batch_chosen.
    chosen_entity_id: str | None = None
    # Общий reason батча: «почему остальные прошли мимо». Ложится только на строки
    # verdict='passed' — см. модульный docstring.
    reason: str | None = None
    reviewed_by: str = "author"


class ListCardFeedbackRequest(BaseModel):
    workspace: str | None = None
    limit: int = Field(default=50, ge=1, le=200)


class ExportInboxCardsRequest(BaseModel):
    workspace: str = Field(min_length=1)
    limit: int = Field(default=5, ge=2, le=10)  # верхняя граница = cards[2..10] в записи
    source: Literal["emerging", "trend", "missing"] = "emerging"
    days_back: int = Field(default=14, ge=1, le=180)


def _feedback_row_id(batch_id: str, entity_kind: str, entity_id: str) -> str:
    """Детерминированный PK из ключа UNIQUE: повтор батча даёт тот же id, а не новую строку."""
    digest = hashlib.sha1(f"{batch_id}|{entity_kind}|{entity_id}".encode("utf-8")).hexdigest()
    return f"cardfb:{digest[:16]}"


def _mint_batch_id() -> str:
    """Новый batch_id на каждый экспорт — единственная связка экспорта с разметкой."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"cards-{stamp}-{uuid.uuid4().hex[:8]}"


def _snapshot_dict(raw: object) -> dict:
    """card_snapshot из БД в dict (asyncpg отдаёт jsonb уже разобранным, str — страховка)."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("card_feedback snapshot_not_json len=%d", len(raw))
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _metric_from_row(row: dict, columns: tuple[str, ...]) -> tuple[str | None, float | None]:
    """Первая непустая метрика из перечисленных колонок строки БД.

    Цифра на карточке обязана прослеживаться до колонки — ничего не вычисляем и не
    подставляем по умолчанию. Нет ни одной колонки со значением — метрики нет.
    """
    for name in columns:
        value = row.get(name)
        if value is None:
            continue
        try:
            return name, float(value)
        except (TypeError, ValueError):
            logger.warning("card_metric non_numeric column=%s type=%s", name, type(value).__name__)
    return None, None


def _build_card(
    entity_kind: str, row: dict, *, title: str | None, metric_columns: tuple[str, ...]
) -> dict:
    """Карточка в формате, который принимает record_card_feedback (без перекладывания)."""
    metric_name, metric_value = _metric_from_row(row, metric_columns)
    return {
        "entity_kind": entity_kind,
        "entity_id": str(row.get("id") or ""),
        "title": title,
        "metric_name": metric_name,
        "metric_value": metric_value,
        # Обе оси задачи C — null, и это не заглушка «пока не дописали». Колонки релевантности
        # у emerging_signals / trend_clusters / missing_signals нет (RELEVANCE_COLUMN_BY_SOURCE),
        # а own_stake задача B считает по вектору ДОКУМЕНТА к личному корпусу — у строки-агрегата
        # своего вектора нет, и мерить тут просто нечего. Подставить сюда metric_value нельзя:
        # другая шкала, и внутри одного батча она разная от карточки к карточке. Перенести число
        # с хита поиска — тоже нельзя: это замер другого объекта. record_card_feedback примет оба
        # значения от вызывающего, если автор проставит их руками; ответ экспорта говорит об этом
        # прямо (axes / axes_note) и называет, где число уже посчитано, — иначе полупустые пары
        # копятся молча, и гейт спеки C по axes_filled_total.both не набирается никогда.
        "relevance": None,
        "own_stake": None,
        "stake_quadrant": None,
        # Вопрос пишет локальный агент со скилом голоса — на сервере его генерировать нечем.
        "question": "",
        # Персональный reason карточки заполняет автор при разметке, экспорт его не знает.
        "reason": None,
    }


def _axes_availability(source: str) -> dict:
    """Какие из двух осей задачи C у карточек этого вида реально есть, а какие уйдут NULL."""
    return {
        # Имя колонки в ТАБЛИЦЕ-ИСТОЧНИКЕ карточки или None, если её там нет. None здесь не
        # значит «числа нет нигде»: у 'emerging' и 'trend' релевантность посчитана в
        # signal_time_series.avg_relevance по тому же entity_id — см. _axes_note.
        "relevance_at_pick": RELEVANCE_COLUMN_BY_SOURCE.get(source),
        # None — не «фича не готова». Задача B построена и считает own_stake косинусом вектора
        # ДОКУМЕНТА к личному корпусу; хиты search_frontier / search_balanced уносят own_stake и
        # stake_quadrant сами. Здесь же карточка — строка emerging_signals / trend_clusters /
        # missing_signals, то есть агрегат без собственного вектора: считать не от чего.
        # Значит, у карточек ЭТОГО вида числа нет, и колонки-источника у него быть не может.
        "own_stake_at_pick": None,
    }


def _axes_note(source: str) -> str:
    """Человекочитаемое «каких чисел на этих карточках не будет, почему и где они есть».

    Формулировка тут — не косметика. Пока в ней стояло «задача B не построена», автору не
    было смысла заполнять own_stake руками: axes_filled_total.both оставался нулём, гейт в
    60 пар не набирался, а пороги задачи C нечем было калибровать — при том, что на хитах
    поиска, которые автор и так читает, реальное число уже лежит. Ровно та же ошибка была и
    по релевантности: note утверждала, что числа нет в БД, хотя для 'emerging' и 'trend' оно
    адресуется прямо по entity_id карточки. Ответ «числа нет» закрывает вопрос, ответ «число
    вот здесь, и вот чего оно стоит» — оставляет автору ход.
    """
    if source in SOURCES_WITH_SERIES_RELEVANCE:
        relevance_note = (
            f"relevance_at_pick: колонки релевантности у таблицы-источника '{source}' нет, но "
            "само число в БД есть — signal_time_series.avg_relevance по ключу "
            f"(entity_kind='{source}', entity_id = тот же entity_id, что в карточке). "
            "Экспорт его не подставляет, и вот почему: это средняя relevance_score постов "
            "внутри окна signal_short_window_hours, и на один сигнал приходится НЕСКОЛЬКО "
            "строк-окон. Какое из них считать «моментом выбора» — решение автора, а не "
            "инструмента; молча взять последнее значило бы выдать выбор окна за факт. "
            "Как relevance_at_pick это число годится при двух условиях: окно зафиксировано "
            "явно (обычно последнее закрытое) и снято В МОМЕНТ разметки — каждый прогон "
            "signal-analysis переписывает серию по этому entity_id, поэтому задним числом "
            "то же самое значение уже не прочитается. Пока не проставлено руками — NULL. "
        )
    else:
        relevance_note = (
            f"relevance_at_pick: у источника '{source}' релевантности в БД нет вообще — ни "
            "колонки в таблице-источнике, ни строки в signal_time_series (её CHECK допускает "
            "только 'semantic' / 'trend' / 'emerging') — запишется NULL. "
        )
    return (
        relevance_note
        + "own_stake_at_pick: задача B построена, но её число живёт на документах — own_stake "
        "это косинус вектора документа к личному корпусу, а строка emerging_signals / "
        "trend_clusters / missing_signals — агрегат без своего вектора, мерить нечего, "
        "запишется NULL. Готовое число лежит на хитах search_frontier и search_balanced: "
        "ключи own_stake и stake_quadrant верхнего уровня хита (при OWN_STAKE_ENABLED=true; "
        "размер корпуса, пороги и degraded — в блоке own_corpus ответа). "
        "Метрика карточки на место осей не подставляется: другая шкала; число с хита поиска "
        "в карточку другого сигнала — тоже, это замер другого объекта. "
        "Чтобы пара попала в разметку: либо размечай карточки из поиска, где own_stake уже "
        "посчитан, либо проставь relevance / own_stake в этой карточке руками перед "
        "record_card_feedback — пока обе оси пустые, гейт спеки C (60 пар с обоими числами) "
        "не сдвигается, и калибровать пороги не на чем."
    )


def _within_window(value: object, cutoff: datetime) -> bool:
    """Попадает ли строка в окно days_back. Недатируемую строку молча не выбрасываем."""
    if not isinstance(value, datetime):
        return True
    moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return moment >= cutoff


def _format_metric(card: dict) -> str:
    if card.get("metric_name") is None or card.get("metric_value") is None:
        return "метрики нет в строке БД"
    return f"{card['metric_name']} {card['metric_value']:g}"


def _render_empty_reason(source: str, last_run: dict | None, rows_before_window: int) -> list[str]:
    """Почему выдача пуста: «окно узкое», «сигналов правда нет» или «анализ не отработал».

    Пустой список карточек сам по себе на этот вопрос не отвечает, а ситуации требуют разных
    действий. Диагностику прогона даёт только list_missing_signals; для остальных источников
    пишем, что различить нечем, — выдумывать своё число здесь нельзя.
    """
    if rows_before_window:
        # До фильтра строки были — прогон точно отработал, виновато окно, а не анализ.
        return [
            f"Строки у источника есть ({rows_before_window}), но все старше окна days_back. "
            "Прогон тут ни при чём — расширь days_back.",
        ]
    if source not in SOURCES_WITH_RUN_DIAGNOSTIC:
        return [
            f"Почему пусто — по этому ответу не различить: инструмент-источник для '{source}' "
            "диагностики прогона не отдаёт. Смотри get_pipeline_stats / list_sources_health.",
        ]
    if not last_run:
        return [
            "Прогонов signal-analysis в cluster_runs для этого воркспейса нет вообще — "
            "анализ ни разу не отрабатывал, пустота ничего не значит.",
        ]
    return [
        "Последний прогон signal-analysis: "
        f"status={last_run.get('status')}, started_at={last_run.get('started_at')}, "
        f"finished_at={last_run.get('finished_at')} (целиком — в поле last_run ответа).",
    ]


def _render_markdown(
    batch_id: str,
    workspace: str,
    source: str,
    cards: list[dict],
    *,
    last_run: dict | None = None,
    rows_before_window: int = 0,
) -> str:
    """Одна строка темы, одна цифра, пустой слот под вопрос. Дописывается в inbox как есть."""
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [f"## Карточки {stamp} · {workspace} · {source} · batch `{batch_id}`", ""]
    if not cards:
        lines.append("Сигналов за окно нет.")
        lines.extend(_render_empty_reason(source, last_run, rows_before_window))
        lines.append("")
        return "\n".join(lines)
    lines.append(f"_{_axes_note(source)}_")
    lines.append("")
    for index, card in enumerate(cards, start=1):
        lines.append(f"{index}. {card.get('title') or card['entity_id']} — {_format_metric(card)}")
        lines.append(f"   `{card['entity_kind']}:{card['entity_id']}`")
        lines.append("   Вопрос:")
        lines.append("")
    return "\n".join(lines)


def _hydrate_feedback_row(row: dict) -> dict:
    """Заголовок и цифру поднять из снимка наверх — строки-источника может уже не быть."""
    snapshot = _snapshot_dict(row.get("card_snapshot"))
    row["title"] = snapshot.get("title")
    row["metric_name"] = snapshot.get("metric_name")
    row["metric_value"] = snapshot.get("metric_value")
    return row


def _axes_filled(rows: list[dict]) -> dict[str, int]:
    """Сколько строк несут релевантность, own_stake и обе оси сразу."""
    return {
        "relevance_at_pick": sum(1 for row in rows if row.get("relevance_at_pick") is not None),
        "own_stake_at_pick": sum(1 for row in rows if row.get("own_stake_at_pick") is not None),
        "both": sum(
            1
            for row in rows
            if row.get("relevance_at_pick") is not None
            and row.get("own_stake_at_pick") is not None
        ),
    }


@router.post("/export_inbox_cards")
async def export_inbox_cards(req: ExportInboxCardsRequest) -> dict:
    """Топ-N сигналов в карточки + markdown для inbox. Файл на диск не пишется."""
    assert_known_workspace(req.workspace)
    workspace = req.workspace.strip()
    if not workspace:
        raise HTTPException(status_code=400, detail="workspace must not be empty")
    cutoff = datetime.now(timezone.utc) - timedelta(days=req.days_back)
    fetch_limit = min(req.limit * 3, 100)
    cards: list[dict] = []
    # Сколько строк отдал инструмент-источник ДО фильтра по окну. Если тут не ноль, а карточек
    # ноль — виновато days_back, и валить пустоту на прогон анализа было бы неправдой.
    rows: list[dict] = []

    if req.source == "emerging":
        payload = await list_emerging_signals(
            EmergingSignalListRequest(
                workspace=workspace, limit=fetch_limit, stages=["weak", "emerging"]
            )
        )
        rows = payload.get("signals") or []
        cards = [
            _build_card(
                "emerging",
                row,
                title=row.get("title"),
                metric_columns=("signal_score", "source_count"),
            )
            for row in rows
            if _within_window(row.get("detected_at"), cutoff)
        ]
    elif req.source == "trend":
        payload = await list_clusters(
            ClusterListRequest(workspace=workspace, kind="trend", limit=fetch_limit)
        )
        rows = payload.get("trends") or []
        cards = [
            _build_card(
                "trend",
                row,
                title=row.get("title_ru") or row.get("title"),
                metric_columns=("burst_score",),
            )
            for row in rows
            if _within_window(row.get("detected_at"), cutoff)
        ]
    else:
        payload = await list_missing_signals(
            MissingSignalListRequest(workspace=workspace, limit=fetch_limit)
        )
        rows = payload.get("signals") or []
        cards = [
            _build_card("missing", row, title=row.get("topic"), metric_columns=("gap_score",))
            for row in rows
            if _within_window(row.get("updated_at"), cutoff)
        ]

    rows_before_window = len(rows)
    cards = cards[: req.limit]
    # Диагностика пустой выдачи, если инструмент-источник её отдал (list_missing_signals кладёт
    # last_run только когда signals пуст). Терять её здесь — значит снова сделать «на этой неделе
    # разрывов нет» неотличимым от «анализ сломан».
    last_run = payload.get("last_run") if isinstance(payload, dict) else None
    batch_id = _mint_batch_id()
    return {
        "status": "ok",
        "batch_id": batch_id,
        "workspace": workspace,
        "source": req.source,
        "count": len(cards),
        "cards": cards,
        "markdown": _render_markdown(
            batch_id,
            workspace,
            req.source,
            cards,
            last_run=last_run,
            rows_before_window=rows_before_window,
        ),
        "target_path": INBOX_TARGET_PATH,
        # Строки до фильтра days_back: отделяет «окно узкое» от «сигналов нет».
        "rows_before_window": rows_before_window,
        # Какие числа у карточек этого вида есть, а какие уйдут в разметку NULL.
        "axes": _axes_availability(req.source),
        "axes_note": _axes_note(req.source),
        # last_run_supported отделяет «диагностика есть, прогонов нет» от «источник её не отдаёт».
        "last_run": last_run,
        "last_run_supported": req.source in SOURCES_WITH_RUN_DIAGNOSTIC,
    }


@router.post("/record_card_feedback")
async def record_card_feedback(req: RecordCardFeedbackRequest) -> dict:
    """Разметка недельной пятёрки: строка на карточку, ровно одна с verdict='chosen'.

    «Ровно одна» считается по batch_id целиком, а не внутри вызова: сквозной батч
    может прийти двумя вызовами из разных воркспейсов, и тогда второй присылает
    свою половину без chosen_entity_id. Вторая chosen-строка на тот же batch_id —
    409 и полный откат вставки (uq_card_feedback_batch_chosen).

    Общий reason батча («почему остальные прошли мимо») пишется только на строки
    verdict='passed'; у chosen остаётся её персональный card.reason или NULL.
    Воркспейс, не заведённый в таблице workspaces, отклоняется 400 до любой записи.
    """
    assert_known_workspace(req.workspace)
    workspace = req.workspace.strip()
    if not workspace:
        raise HTTPException(status_code=400, detail="workspace must not be empty")
    batch_id = req.batch_id.strip()
    if not batch_id:
        raise HTTPException(status_code=400, detail="batch_id must not be empty")
    chosen_entity_id = (req.chosen_entity_id or "").strip() or None

    seen: set[tuple[str, str]] = set()
    prepared: list[dict] = []
    chosen_hits = 0
    for card in req.cards:
        entity_kind = card.entity_kind.strip().lower()
        if entity_kind not in ENTITY_KINDS:
            raise HTTPException(
                status_code=400, detail=f"entity_kind must be one of {sorted(ENTITY_KINDS)}"
            )
        entity_id = card.entity_id.strip()
        if not entity_id:
            raise HTTPException(status_code=400, detail="entity_id must not be empty")
        if (entity_kind, entity_id) in seen:
            raise HTTPException(
                status_code=400, detail=f"duplicate card in batch: {entity_kind}:{entity_id}"
            )
        seen.add((entity_kind, entity_id))
        is_chosen = chosen_entity_id is not None and entity_id == chosen_entity_id
        chosen_hits += int(is_chosen)
        snapshot = card.model_dump()
        snapshot["entity_kind"] = entity_kind
        snapshot["entity_id"] = entity_id
        # Общий reason батча — это «почему остальные прошли мимо», и на chosen-строке он
        # означал бы ровно противоположное: SELECT reason WHERE verdict='chosen' вернул бы
        # фразу, характеризующую выбранную карточку как отбракованную. Таблица append-only,
        # поправить потом нечем, поэтому общий reason идёт только на passed. У chosen остаётся
        # персональный reason карточки, если автор его написал, иначе NULL.
        card_reason = (card.reason or "").strip() or None
        row_reason = card_reason if is_chosen else (card_reason or req.reason)
        prepared.append(
            {
                "id": _feedback_row_id(batch_id, entity_kind, entity_id),
                "ws": workspace,
                "batch_id": batch_id,
                "entity_kind": entity_kind,
                "entity_id": entity_id,
                "verdict": "chosen" if is_chosen else "passed",
                "reason": row_reason,
                "relevance": card.relevance,
                "own_stake": card.own_stake,
                "snapshot": json.dumps(snapshot, ensure_ascii=False, default=str),
                "reviewed_by": req.reviewed_by,
            }
        )
    if chosen_entity_id is not None and chosen_hits != 1:
        raise HTTPException(
            status_code=400, detail="chosen_entity_id must match exactly one card in cards"
        )

    # DO NOTHING, а не DO UPDATE: таблица append-only, первая запись батча авторитетна.
    # Повтор (ретрай агента, повторный вызов) обязан быть no-op — иначе поздний вызов
    # молча перепишет уже принятое решение человека, ради накопления которого всё и делается.
    # Передумал — новый batch_id.
    statement = text(
        """
        INSERT INTO card_feedback (
            id, workspace_id, batch_id, entity_kind, entity_id, verdict, reason,
            relevance_at_pick, own_stake_at_pick, card_snapshot, reviewed_by,
            decided_at, created_at
        ) VALUES (
            :id, :ws, :batch_id, :entity_kind, :entity_id, :verdict, :reason,
            :relevance, :own_stake, CAST(:snapshot AS jsonb), :reviewed_by,
            NOW(), NOW()
        )
        ON CONFLICT (batch_id, entity_kind, entity_id) DO NOTHING
        """
    )
    inserted = 0
    async with AsyncSession(get_engine()) as session:
        # Пред-проверка существования воркспейса в БД. assert_known_workspace валидирует по
        # config/workspaces.yml, а FK card_feedback.workspace_id ссылается на таблицу workspaces:
        # воркспейс, заведённый в конфиге, но ещё не забутстрапленный (на 2026-08-03 это auto_hmi),
        # проходит первую проверку и падает на второй. Без этого SELECT автор получает через шлюз
        # голое «Client error '500'», а набранная руками разметка пятёрки теряется без подсказки.
        # Один SELECT 1; гонку (воркспейс удалили между проверкой и вставкой) он не закрывает —
        # её по-прежнему ловит IntegrityError ниже, и подменять её этим текстом нельзя.
        known = await session.execute(
            text("SELECT 1 FROM workspaces WHERE id = :ws"), {"ws": workspace}
        )
        if known.first() is None:
            logger.warning("card_feedback workspace_not_bootstrapped workspace=%s", workspace)
            raise HTTPException(
                status_code=400,
                detail=(
                    f"workspace '{workspace}' is known to config/workspaces.yml but has no row in "
                    f"the workspaces table, so the feedback cannot be stored. Bootstrap it first: "
                    f"POST /api/workspaces/bootstrap?workspace_id={workspace} on admin, then "
                    f"re-send this batch — nothing was written."
                ),
            )
        try:
            for params in prepared:
                result = await session.execute(statement, params)
                inserted += int(result.rowcount or 0)
            await session.commit()
        except IntegrityError as exc:
            # Вся вставка откатывается в любом случае: транзакция уже аварийная, а
            # полуразмеченный батч хуже отсутствующего (таблица append-only).
            await session.rollback()
            # Пересказываем как 409 только нарушение uq_card_feedback_batch_chosen:
            # у батча уже есть другая chosen-строка (расширенный повтор батча или
            # вторая половина сквозной пятёрки, отправленная со своим chosen).
            # Ретрай того же вызова сюда не доходит — он гасится ON CONFLICT по
            # (batch_id, entity_kind, entity_id). Прочие нарушения (FK на workspaces,
            # если воркспейс удалили между пред-проверкой и вставкой; CHECK на
            # entity_kind/verdict) — другая причина, и подменять её этим текстом нельзя.
            if "uq_card_feedback_batch_chosen" not in str(getattr(exc, "orig", None) or exc):
                raise
            logger.warning(
                "card_feedback batch_already_has_chosen batch_id=%s workspace=%s",
                batch_id,
                workspace,
            )
            raise HTTPException(
                status_code=409,
                detail=(
                    f"batch {batch_id} already has a chosen card; send the remaining cards "
                    "without chosen_entity_id, or use a new batch_id to change the decision"
                ),
            ) from None
    return {
        "status": "ok",
        "batch_id": batch_id,
        "workspace_id": workspace,
        "cards_recorded": len(prepared),
        "inserted": inserted,
        "skipped_existing": len(prepared) - inserted,
        "chosen_entity_id": chosen_entity_id,
        "reviewed_by": req.reviewed_by,
    }


@router.post("/list_card_feedback")
async def list_card_feedback(req: ListCardFeedbackRequest) -> dict:
    """Накопленная разметка.

    Заголовок и цифра берутся из card_snapshot, обе оси — из relevance_at_pick /
    own_stake_at_pick: строки сигнала может уже не быть (её пересоздают каждый прогон).

    reason у chosen-строки пустой по построению (общий reason батча объясняет, почему
    остальные прошли мимо, и лежит на passed-строках). Чтобы читающий chosen-строку не
    остался без контекста, к каждой строке добавляется batch_reason — общий reason её
    батча, поднятый оконной функцией; поле явно названо «batch», а не выдано за оценку
    выбранной карточки. Окно считается по строкам, прошедшим WHERE, поэтому у сквозного
    батча с фильтром по workspace batch_reason может оказаться пустым — passed-строки лежат
    в другом воркспейсе. Это честнее, чем показать reason, которого в выборке не видно.

    В окно берутся не все passed-строки, а только те, на которых лежит именно общий reason
    батча. На записи персональный reason карточки перебивает общий (row_reason =
    card_reason or req.reason), поэтому у батча со смешанной разметкой MAX(reason) поднял бы
    фразу про ОДНУ отбракованную карточку и выдал бы её за объяснение всего батча — в том
    числе на chosen-строке. Отличить одно от другого можно по снимку: card_snapshot->>'reason'
    непустой ровно тогда, когда в строке лежит персональный reason. Такие строки из окна
    исключены. TRIM в SQL режет только пробелы, а .strip() на записи — любой пробельный
    символ; расхождение работает в безопасную сторону (строка не попадёт в кандидаты),
    подставить чужой reason оно не может.

    Счётчиков заполненности осей два, и они про разное. axes_filled — по возвращённой
    странице, axes_filled_total — по всей таблице под тем же фильтром, вторым запросом без
    LIMIT. Разделены потому, что счёт заведён под гейт спеки C («60 пар с обоими числами»),
    а страница по умолчанию 50 строк: посчитанный по ней, гейт при дефолтных настройках
    ненаблюдаем в принципе, и вызывающий об этом не узнаёт. total и truncated говорят то же
    самое про строки: сколько их всего и показаны ли все.
    """
    assert_known_workspace(req.workspace)
    clauses: list[str] = []
    filter_params: dict = {}
    if req.workspace:
        clauses.append("workspace_id = :ws")
        filter_params["ws"] = req.workspace
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    async with AsyncSession(get_engine()) as session:
        rows = await session.execute(
            text(
                f"""
                SELECT id, workspace_id, batch_id, entity_kind, entity_id, verdict, reason,
                       MAX(reason) FILTER (
                           WHERE verdict = 'passed'
                             AND NULLIF(TRIM(card_snapshot->>'reason'), '') IS NULL
                       ) OVER (PARTITION BY batch_id) AS batch_reason,
                       relevance_at_pick, own_stake_at_pick, card_snapshot, reviewed_by,
                       decided_at, created_at
                FROM card_feedback
                {where}
                ORDER BY decided_at DESC, batch_id, verdict
                LIMIT :limit
                """
            ),
            {**filter_params, "limit": req.limit},
        )
        feedback = [_hydrate_feedback_row(dict(r)) for r in rows.mappings().all()]
        # Отдельный агрегат по ВСЕЙ таблице под тем же WHERE, без LIMIT. Счётчик заведён
        # под гейт спеки C («60 пар с обоими числами»), а страница по умолчанию 50 строк
        # и максимум 200: посчитанный по странице, он до 60 не доходит при limit=50 вообще
        # никогда, и вызывающий не видит, что число усечено. Цена — один COUNT по индексу
        # idx_card_feedback_ws_decided; таблица растёт на пять строк в неделю.
        totals = (
            await session.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS rows_total,
                           COUNT(relevance_at_pick) AS relevance_at_pick,
                           COUNT(own_stake_at_pick) AS own_stake_at_pick,
                           COUNT(*) FILTER (
                               WHERE relevance_at_pick IS NOT NULL
                                 AND own_stake_at_pick IS NOT NULL
                           ) AS both
                    FROM card_feedback
                    {where}
                    """
                ),
                filter_params,
            )
        ).mappings().first()
    if totals is None:
        # Агрегат без GROUP BY на живой БД всегда отдаёт ровно одну строку, поэтому сюда
        # можно попасть только если до БД не дошли. Подставить нули нельзя: total=0 при
        # непустой странице читается как «усечения нет» — ровно то враньё, против которого
        # счётчик и заведён. null означает «не измерено», и это видно.
        logger.warning(
            "card_feedback totals_unavailable workspace=%s returned_rows=%d",
            req.workspace,
            len(feedback),
        )
        total: int | None = None
        axes_filled_total: dict[str, int] | None = None
        truncated: bool | None = None
    else:
        total = int(totals["rows_total"])
        axes_filled_total = {
            "relevance_at_pick": int(totals["relevance_at_pick"]),
            "own_stake_at_pick": int(totals["own_stake_at_pick"]),
            "both": int(totals["both"]),
        }
        truncated = total > len(feedback)
    return {
        "status": "ok",
        # Страница.
        "count": len(feedback),
        "axes_filled": _axes_filled(feedback),
        # Вся таблица под тем же фильтром. Полупустые пары копятся молча, пока карточки
        # строит только export_inbox_cards: relevance_at_pick приходит NULL (см.
        # RELEVANCE_COLUMN_BY_SOURCE), own_stake_at_pick — тоже (агрегат без вектора, см.
        # _axes_availability). У карточек, снятых с хитов search_frontier / search_balanced,
        # own_stake приезжает готовым — по ним both и растёт.
        "total": total,
        "axes_filled_total": axes_filled_total,
        "truncated": truncated,
        "counts_note": COUNTS_NOTE,
        "feedback": feedback,
    }
