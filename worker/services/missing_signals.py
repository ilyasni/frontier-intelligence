from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import get_settings
from worker.llm_router_client import LLMRouterClient
from worker.llm_json import parse_llm_json_object

from .searxng_client import SearXNGClient

logger = logging.getLogger(__name__)

# Порог «документ существенно покрывает тему».
# Раньше здесь стоял литерал 0.34 внутри _frontier_frequency. Он подбирался под
# формулу len(shared) / len(topic_tokens): для темы из трёх слов одно совпадение
# давало ровно 0.3333, то есть всё поведение фильтра держалось на зазоре 0.0067
# и на длине темы. Затем порог опустили до 0.30 против IDF-взвешенного покрытия —
# и это оказалось хуже: IDF = log((N+1)/(df+1)) зависит не только от доли df/N,
# но и от самого N, поэтому ОДНА И ТА ЖЕ пара «тема/документ» проезжала через
# порог при росте корпуса. Замер на отгруженной функции, тема
# {automotive, cockpit, telemetry} против документа {cockpit, ergonomics, study}
# при df(cockpit)=1:
#   N=16 -> 0.2741 (не в счёт)   N=64 -> 0.2943 (не в счёт)
#   N=256 -> 0.3044 (в счёт)     N=1024 -> 0.3103   N=2425 -> 0.3130 (в счёт)
# А это ровно два боевых пути: refresh читает LIMIT 64, плановый — все неархивные
# кластеры (у design 2425). Один и тот же документ считался покрытием на одном
# пути и не считался на другом.
# Сейчас вес токена — это его редкость 1 - df/N (см. _build_signal_corpus),
# функция только от доли, а не от N. У меры появились две ТОЧНЫЕ константы, не
# зависящие от размера корпуса: «один из двух равно-информативных токенов» = 1/2,
# «один из трёх» = 1/3. Порог поставлен ровно посередине этого коридора: 1/3
# никогда не считается покрытием, 1/2 считается всегда, запас с обеих сторон
# 0.067 вместо прежних 0.007. Совпадение только по общему для корпуса токену
# даёт ~0.1 и отсекается, по различающему ~0.9 и проходит.
# Это не операторская ручка, а структурная константа меры, поэтому в Settings не
# выносится: сдвинуть её осмысленно можно только вместе с формулой веса.
_MIN_TOPIC_OVERLAP = 0.40

# Насыщение frontier_presence по умолчанию: доля корпуса, при которой тема
# считается полностью покрытой. Прежний делитель 3.5 стоял в _gap_score как
# литерал и был калиброван под неограниченную сумму (см. _frontier_frequency).
# Боевое значение берётся из MISSING_SIGNALS_PRESENCE_SATURATION, здесь —
# только дефолт для прямых юнит-вызовов, чтобы функция осталась чистой.
_DEFAULT_PRESENCE_SATURATION = 0.30


def _terms(value: str) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-_]{2,}", (value or "").lower())


def _digest(value: str, prefix: str) -> str:
    return f"{prefix}:{hashlib.sha1(value.encode('utf-8')).hexdigest()[:16]}"


def _string_list(values: Any) -> list[str]:
    if isinstance(values, list):
        return [str(value).strip() for value in values if str(value).strip()]
    return []


def _signal_documents(
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for item in semantic:
        parts = [
            str(item.get("title") or ""),
            " ".join(str(value) for value in (item.get("top_concepts") or [])),
            " ".join(
                str(value)
                for value in ((item.get("explainability") or {}).get("top_terms") or [])
            ),
        ]
        tokens = set(_terms(" ".join(parts)))
        if tokens:
            documents.append({"tokens": tokens, "weight": 1.0})
    for item in [*stable, *emerging]:
        parts = [
            str(item.get("title") or ""),
            " ".join(str(value) for value in (item.get("keywords") or [])),
            " ".join(
                str(value)
                for value in ((item.get("explainability") or {}).get("top_terms") or [])
            ),
        ]
        tokens = set(_terms(" ".join(parts)))
        if tokens:
            documents.append({"tokens": tokens, "weight": 1.25 if item in stable else 1.1})
    return documents


@dataclass(frozen=True)
class _SignalCorpus:
    """Корпус сигналов workspace: документы, их суммарный вес и редкость токенов.

    Строится один раз на прогон: раньше _signal_documents пересобирался внутри
    _frontier_frequency на каждую тему (до 8 полных проходов по 2425 документам).

    token_rarity — доля корпуса, в которой токена НЕТ: 1 - df/N, всегда в [0, 1).
    Раньше здесь лежал сглаженный IDF log((N+1)/(df+1)); он зависел от N
    отдельно от доли df/N, из-за чего порог _MIN_TOPIC_OVERLAP означал разное на
    корпусе в 64 и в 2425 документов (замер — в комментарии к константе).
    """

    documents: list[dict[str, Any]]
    token_rarity: dict[str, float]
    default_rarity: float
    total_weight: float


def _build_signal_corpus(
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
) -> _SignalCorpus:
    documents = _signal_documents(semantic, stable, emerging)
    total = float(len(documents))
    document_frequency: Counter[str] = Counter()
    for item in documents:
        document_frequency.update(item["tokens"])
    # Редкость токена: 1 - df/N. Токен, встречающийся во всех документах,
    # получает ровно 0 — собственный словарь корпуса обнуляется сам, без ручного
    # стоп-листа (стоп-лист из workspaces всё равно нужен для однословных тем,
    # где такой токен — всё содержание темы).
    # Величина зависит ТОЛЬКО от доли df/N, поэтому один и тот же корпус,
    # просмотренный целиком и через LIMIT 64, даёт токенам одинаковые веса —
    # чего сглаженный IDF не давал (там оставался отдельный множитель log N).
    rarity = {
        token: 1.0 - float(df) / max(total, 1.0)
        for token, df in document_frequency.items()
    }
    return _SignalCorpus(
        documents=documents,
        token_rarity=rarity,
        # Токена нет в корпусе вообще (df = 0) — максимальная редкость. Константа,
        # а не функция N: в этом и была половина прежнего дрейфа.
        default_rarity=1.0,
        total_weight=sum(float(item["weight"]) for item in documents),
    )


def _topic_overlap_score(
    topic_tokens: set[str],
    signal_tokens: set[str],
    *,
    rarity: dict[str, float] | None = None,
    default_rarity: float = 1.0,
) -> float:
    """Доля информативности темы, покрытая документом; всегда в [0, 1].

    Вес токена — его редкость в корпусе (1 - df/N, см. _build_signal_corpus), то
    есть функция только доли, а не размера корпуса. Отсюда две точные константы,
    одинаковые при любом N: тема из k равно-информативных токенов, покрытая
    одним из них, даёт ровно 1/k. Порог _MIN_TOPIC_OVERLAP выставлен между 1/3
    и 1/2 именно поэтому.

    Без rarity вырождается в len(shared) / len(topic_tokens) — то же самое
    происходит при вырожденном корпусе, где каждый токен темы есть в каждом
    документе (суммарный вес темы равен нулю).
    """
    if not topic_tokens or not signal_tokens:
        return 0.0
    shared = topic_tokens & signal_tokens
    if not shared:
        return 0.0
    if rarity is not None:
        topic_weight = sum(rarity.get(token, default_rarity) for token in topic_tokens)
        if topic_weight > 0.0:
            shared_weight = sum(rarity.get(token, default_rarity) for token in shared)
            return min(1.0, shared_weight / topic_weight)
    return len(shared) / len(topic_tokens)


def _frontier_frequency(
    *,
    topic: str,
    query: str,
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
    stopwords: frozenset[str] = frozenset(),
    corpus: _SignalCorpus | None = None,
) -> float:
    """Взвешенная доля корпуса, существенно покрывающая тему; всегда в [0, 1].

    Раньше возвращала неограниченную сумму по всему корпусу: для design (2425
    неархивных кластеров) тема «interaction design» давала ~680, а
    external_strength ограничен 1.0 — вычитание в _gap_score было бессмысленным,
    и любая тема с суммой ≥ 1.83 отсеивалась всегда. Знаменатель — суммарный вес
    корпуса, поэтому шкала от размера корпуса не зависит.

    Что именно инвариантно (формулировка была неточной до 2026-08-03, когда
    внутри стоял IDF): и вес токена, и порог зависят только от долей df/N. Один
    и тот же состав корпуса, просмотренный целиком (плановый путь) и через
    LIMIT 64/32/32 (refresh), даёт для темы одно и то же число. Инвариантности
    «к любому корпусу любого размера» здесь нет и быть не может: если refresh
    видит другой состав сигналов, доли другие — и число обязано отличаться.
    Чего больше НЕ происходит — так это проезда одной и той же пары
    «тема/документ» через порог просто из-за роста N при фиксированном df.

    stopwords — собственный словарь workspace (см. _workspace_stopwords). Если
    после вычитания не остаётся ничего (тема целиком из словаря воркспейса — так
    выглядят все _fallback_topics), покрытие меряется по исходным токенам.
    Возврат 0.0, как было раньше, означал бы «нет покрытия» = максимальный gap,
    то есть название воркспейса выходило бы первой строкой выдачи; измеренное
    покрытие даёт для такой темы presence≈1 и gap 0 — отсев по числу, а не по
    словарю.
    """
    raw_tokens = set(_terms(f"{topic} {query}"))
    topic_tokens = (raw_tokens - stopwords) or raw_tokens
    if not topic_tokens:
        return 0.0
    signal_corpus = corpus if corpus is not None else _build_signal_corpus(semantic, stable, emerging)
    if not signal_corpus.documents:
        return 0.0
    matches = 0.0
    for item in signal_corpus.documents:
        overlap = _topic_overlap_score(
            topic_tokens,
            item["tokens"],
            rarity=signal_corpus.token_rarity,
            default_rarity=signal_corpus.default_rarity,
        )
        if overlap >= _MIN_TOPIC_OVERLAP:
            matches += float(item["weight"]) * overlap
    return round(min(1.0, matches / max(signal_corpus.total_weight, 1.0)), 4)


def _external_signal_strength(results: list[dict[str, Any]], max_results: int) -> float:
    if not results:
        return 0.0
    domains = {urlparse(item["url"]).netloc.lower() for item in results if item.get("url")}
    avg_score = sum(float(item.get("score") or 0.0) for item in results) / max(len(results), 1)
    return round(
        min(
            1.0,
            (len(results) / max(max_results, 1)) * 0.55
            + (len(domains) / max(max_results, 1)) * 0.3
            + min(avg_score / 3.0, 1.0) * 0.15,
        ),
        4,
    )


def _gap_score(
    *,
    frontier_frequency: float,
    external_strength: float,
    presence_saturation: float = _DEFAULT_PRESENCE_SATURATION,
) -> float:
    """Внешнее внимание минус собственное покрытие; обе величины в [0, 1].

    presence_saturation — доля корпуса, при которой тема считается покрытой
    полностью. Делитель 3.5 стоял здесь под старую неограниченную сумму и
    гарантировал presence = 1.0 (gap = 0) для всего, что встречалось в корпусе
    хотя бы четырежды. Параметр явный, а не через get_settings(), чтобы функция
    осталась чистой и юнит-тестируемой без окружения.
    """
    frontier_presence = min(1.0, frontier_frequency / max(presence_saturation, 1e-6))
    return round(max(0.0, external_strength - frontier_presence), 4)


def _workspace_stopwords(workspace: dict[str, Any]) -> frozenset[str]:
    """Собственный словарь workspace — искать пробел внутри своего названия бессмысленно.

    Без этого в design-воркспейсе любая тема с токеном `design` получает
    presence = 1.0, а три из трёх design_lenses (это ровно то, что генерирует
    _fallback_topics) не проходят порог никогда.
    """
    parts = [
        str(workspace.get("name") or ""),
        str(workspace.get("description") or ""),
        " ".join(_string_list(workspace.get("categories"))),
        " ".join(_string_list(workspace.get("design_lenses"))),
    ]
    # design_lenses приходят как interaction_design, а _terms допускает "_"
    # внутри токена: без замены в стоп-лист попал бы литерал interaction_design,
    # а не токены interaction/design, которые реально встречаются в заголовках.
    return frozenset(_terms(" ".join(parts).replace("_", " ")))


def _drop_self_vocabulary_topics(
    topics: list[dict[str, str]],
    stopwords: frozenset[str],
) -> tuple[list[dict[str, str]], int]:
    """Выбросить кандидатов, целиком состоящих из собственного словаря workspace.

    Применяется только к темам от LLM: тема из одного собственного словаря —
    ошибка генератора, и тратить на неё запрос к SearXNG незачем. К
    _fallback_topics НЕ применяется: он собран ровно из design_lenses +
    categories, то есть из источника стоп-листа, и отсеивался бы целиком (см.
    _generate_candidate_topics). Страховка от «тема = название воркспейса» на
    этом пути стоит в _frontier_frequency: покрытие меряется по исходным
    токенам, поэтому такая тема получает presence≈1 и отсеивается по gap_score.
    """
    if not stopwords:
        return topics, 0
    kept: list[dict[str, str]] = []
    dropped = 0
    for item in topics:
        if set(_terms(item["topic"])) - stopwords:
            kept.append(item)
            continue
        dropped += 1
        logger.info(
            "missing_signals_topic_dropped_by_stoplist topic=%s — тема целиком из "
            "собственного словаря workspace",
            item["topic"],
        )
    return kept, dropped


def _fallback_topics(
    *,
    workspace: dict[str, Any],
    topic_limit: int,
) -> list[dict[str, str]]:
    topics: list[dict[str, str]] = []
    for value in _string_list(workspace.get("design_lenses")) + _string_list(workspace.get("categories")):
        label = value.replace("_", " ").strip()
        if not label:
            continue
        topics.append(
            {
                "topic": label,
                "query": label,
                "category": label.split()[0],
                "why_expected": f"Derived from workspace profile for {workspace.get('name') or workspace.get('id')}.",
            }
        )
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in topics:
        key = item["topic"].lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= topic_limit:
            break
    return deduped


async def _generate_candidate_topics(
    *,
    workspace: dict[str, Any],
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
    stopwords: frozenset[str] = frozenset(),
) -> tuple[list[dict[str, str]], int]:
    """Кандидаты в missing signals и число тем, отброшенных стоп-листом workspace."""
    settings = get_settings()
    topic_limit = max(3, int(settings.missing_signals_topic_limit))
    signal_titles = [str(item.get("title") or "") for item in [*stable, *emerging, *semantic][:8]]
    top_concepts = [
        name
        for name, _ in Counter(
            concept
            for item in semantic[:10]
            for concept in (item.get("top_concepts") or [])
        ).most_common(12)
    ]
    payload = {
        "workspace": {
            "id": workspace.get("id"),
            "name": workspace.get("name"),
            "description": workspace.get("description"),
            "categories": _string_list(workspace.get("categories")),
            "design_lenses": _string_list(workspace.get("design_lenses")),
        },
        "current_frontier_signals": signal_titles,
        "top_concepts": top_concepts,
        "max_topics": topic_limit,
    }

    client = LLMRouterClient(service_name="worker")
    try:
        response = await client.chat(
            system=(
                "Ты аналитик missing signals. Для заданного workspace предложи темы, которые ожидаемы "
                "для мониторинга, но могут быть недопокрыты внутренними источниками. Верни только JSON."
            ),
            user=(
                "Верни JSON вида "
                '{"topics":[{"topic":"...", "query":"...", "category":"...", "why_expected":"..."}]}\n\n'
                f"{json.dumps(payload, ensure_ascii=False)}"
            ),
            task="mcp_synthesis",
            pro=True,
            # 700 не хватало: на боевом payload (10 заголовков сигналов + 12 концептов)
            # модель стабильно упиралась ровно в лимит на ~725 токенах, ответ обрывался
            # без закрывающих скобок, парсер отдавал "no JSON object in response", и
            # workspace молча уезжал на _fallback_topics. Замерено 2026-08-02.
            max_tokens=2000,
        )
        parsed = parse_llm_json_object(response.content)
    except Exception:
        logger.exception(
            "missing_signals_topic_generation_failed workspace=%s — "
            "используются шаблонные темы вместо сгенерированных",
            workspace.get("id"),
        )
        # Стоп-лист к фолбэку не применяется намеренно. Он собран из
        # design_lenses + categories, то есть ровно из источника стоп-листа:
        # с отбраковкой не оставалось ни одной темы ни в одном воркспейсе
        # (замерено 2026-08-03: 8/8, 6/6, 5/5, 5/5, 6/6, 8/8), и падение
        # LLM-генератора означало ноль запросов к SearXNG и ноль строк —
        # весь фолбэк был мёртвым кодом. Покрытие этих тем меряется в
        # _frontier_frequency, уже покрытые отсеются по gap_score.
        return _fallback_topics(workspace=workspace, topic_limit=topic_limit), 0
    finally:
        await client.close()

    raw_topics = parsed.get("topics") if isinstance(parsed, dict) else None
    topics: list[dict[str, str]] = []
    for item in raw_topics if isinstance(raw_topics, list) else []:
        if not isinstance(item, dict):
            continue
        topic = str(item.get("topic") or "").strip()
        query = str(item.get("query") or topic).strip()
        if not topic or not query:
            continue
        topics.append(
            {
                "topic": topic[:120],
                "query": query[:180],
                "category": str(item.get("category") or "").strip()[:80],
                "why_expected": str(item.get("why_expected") or "").strip()[:220],
            }
        )
    if not topics:
        return _fallback_topics(workspace=workspace, topic_limit=topic_limit), 0

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in topics:
        key = item["topic"].lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= topic_limit:
            break
    if not deduped:
        return _fallback_topics(workspace=workspace, topic_limit=topic_limit), 0
    return _drop_self_vocabulary_topics(deduped, stopwords)


async def _generate_opportunities(
    *,
    workspace: dict[str, Any],
    items: list[dict[str, Any]],
) -> dict[str, str]:
    if not items:
        return {}

    compact = [
        {
            "topic": item["topic"],
            "category": item.get("category"),
            "gap_score": item["gap_score"],
            "why_expected": item.get("why_expected"),
            "evidence": [
                {
                    "title": evidence.get("title"),
                    "url": evidence.get("url"),
                    "engine": evidence.get("engine"),
                }
                for evidence in item.get("evidence", [])[:3]
            ],
        }
        for item in items
    ]

    client = LLMRouterClient(service_name="worker")
    try:
        response = await client.chat(
            system=(
                "Ты аналитик стратегических gaps. Для каждой темы предложи короткую opportunity-формулировку "
                "в 1-2 предложениях. Верни только JSON."
            ),
            user=(
                "Верни JSON вида "
                '{"items":[{"topic":"...", "opportunity":"..."}]}\n\n'
                f"{json.dumps({'workspace': workspace, 'gaps': compact}, ensure_ascii=False)}"
            ),
            task="mcp_synthesis",
            pro=True,
            max_tokens=700,
        )
        parsed = parse_llm_json_object(response.content)
    except Exception:
        logger.exception("missing_signals_opportunity_generation_failed workspace=%s", workspace.get("id"))
        return {
            item["topic"]: (
                f"External evidence suggests growing attention around {item['topic']}; "
                f"expand frontier sources or tracking queries for this area."
            )
            for item in items
        }
    finally:
        await client.close()

    mapping: dict[str, str] = {}
    for item in parsed.get("items") if isinstance(parsed, dict) else []:
        if not isinstance(item, dict):
            continue
        topic = str(item.get("topic") or "").strip()
        opportunity = str(item.get("opportunity") or "").strip()
        if topic and opportunity:
            mapping[topic] = opportunity[:500]
    return mapping


async def _replace_missing_signals(
    session: AsyncSession,
    *,
    workspace_id: str,
    items: list[dict[str, Any]],
) -> None:
    if not items:
        # Пустой прогон раньше доходил сюда и делал DELETE без единого INSERT,
        # стирая накопленное (2026-08-03: у четырёх воркспейсов из пяти таблица
        # пуста именно поэтому). Отсутствие результата — не основание терять
        # историю; страховка продублирована на вызывающей стороне.
        logger.warning(
            "missing_signals_replace_skipped workspace=%s — пустой набор, "
            "существующие строки не тронуты",
            workspace_id,
        )
        return
    await session.execute(
        text("DELETE FROM missing_signals WHERE workspace_id = :workspace_id"),
        {"workspace_id": workspace_id},
    )
    for item in items:
        await session.execute(
            text(
                """
                INSERT INTO missing_signals (
                    id, workspace_id, topic, gap_score, opportunity,
                    searxng_frequency, frontier_frequency, evidence_urls, category,
                    created_at, updated_at
                )
                VALUES (
                    :id, :workspace_id, :topic, :gap_score, :opportunity,
                    :searxng_frequency, :frontier_frequency, CAST(:evidence_urls AS jsonb), :category,
                    NOW(), NOW()
                )
                """
            ),
            {
                "id": item["id"],
                "workspace_id": workspace_id,
                "topic": item["topic"],
                "gap_score": item["gap_score"],
                "opportunity": item.get("opportunity"),
                "searxng_frequency": item["searxng_frequency"],
                "frontier_frequency": item["frontier_frequency"],
                "evidence_urls": json.dumps(item["evidence_urls"], ensure_ascii=False),
                "category": item.get("category"),
            },
        )


async def _load_current_signal_snapshots(
    session: AsyncSession,
    *,
    workspace_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    semantic_rows = (
        await session.execute(
            text(
                """
                SELECT title, top_concepts, explainability
                FROM semantic_clusters
                WHERE workspace_id = :workspace_id
                ORDER BY detected_at DESC
                LIMIT 64
                """
            ),
            {"workspace_id": workspace_id},
        )
    ).mappings().all()
    stable_rows = (
        await session.execute(
            text(
                """
                SELECT title, keywords, explainability
                FROM trend_clusters
                WHERE workspace_id = :workspace_id
                  AND pipeline = 'stable'
                ORDER BY detected_at DESC, burst_score DESC
                LIMIT 32
                """
            ),
            {"workspace_id": workspace_id},
        )
    ).mappings().all()
    emerging_rows = (
        await session.execute(
            text(
                """
                SELECT title, keywords, explainability
                FROM emerging_signals
                WHERE workspace_id = :workspace_id
                  AND signal_stage = ANY(:stages)
                ORDER BY detected_at DESC, signal_score DESC
                LIMIT 32
                """
            ),
            {"workspace_id": workspace_id, "stages": ["weak", "emerging", "stable"]},
        )
    ).mappings().all()
    return (
        [dict(row) for row in semantic_rows],
        [dict(row) for row in stable_rows],
        [dict(row) for row in emerging_rows],
    )


def _init_counters(counters: dict[str, int] | None) -> dict[str, int]:
    """Счётчики прогона: ключи всегда есть, чтобы «нет ключа» не путалось с нулём."""
    stats = counters if counters is not None else {}
    stats.update(
        {
            "topics_generated": 0,
            "topics_dropped_by_stoplist": 0,
            "topics_dropped_by_results": 0,
            "topics_dropped_by_gap": 0,
            "searxng_errors": 0,
            "topics_kept": 0,
        }
    )
    return stats


async def run_missing_signals_analysis(
    session: AsyncSession,
    *,
    workspace_id: str | None,
    semantic: list[dict[str, Any]],
    stable: list[dict[str, Any]],
    emerging: list[dict[str, Any]],
    counters: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """Найти темы, активные снаружи и недопокрытые внутри.

    counters — необязательный словарь-выход для наблюдаемости: заполняется
    ключами topics_generated / topics_dropped_by_stoplist /
    topics_dropped_by_results / topics_dropped_by_gap / searxng_errors /
    topics_kept. Инвариант: topics_generated равен сумме остальных пяти.
    Параметр опциональный намеренно — возврат кортежа сломал бы вызывающих,
    которые считают len() от результата.
    """
    stats = _init_counters(counters)
    settings = get_settings()
    if not workspace_id or not settings.searxng_enabled or not settings.missing_signals_enabled:
        return []

    workspace = (
        await session.execute(
            text(
                """
                SELECT id, name, description, categories, design_lenses, extra
                FROM workspaces
                WHERE id = :workspace_id
                """
            ),
            {"workspace_id": workspace_id},
        )
    ).mappings().first()
    if not workspace:
        return []

    stopwords = _workspace_stopwords(dict(workspace))
    topics, dropped_by_stoplist = await _generate_candidate_topics(
        workspace=dict(workspace),
        semantic=semantic,
        stable=stable,
        emerging=emerging,
        stopwords=stopwords,
    )
    stats["topics_dropped_by_stoplist"] = dropped_by_stoplist
    stats["topics_generated"] = len(topics) + dropped_by_stoplist
    if not topics:
        # Сюда доходит либо воркспейс без design_lenses и categories (фолбэку
        # строиться не из чего), либо LLM, вернувшая одни только темы из
        # собственного словаря. И то и другое — конфигурационная проблема, её
        # должно быть видно, а не выглядеть как «ноль тем».
        logger.warning(
            "missing_signals_no_candidate_topics workspace=%s dropped_by_stoplist=%s",
            workspace_id,
            dropped_by_stoplist,
        )
        return []

    corpus = _build_signal_corpus(semantic, stable, emerging)
    presence_saturation = float(settings.missing_signals_presence_saturation)
    client = SearXNGClient(service_name="worker")
    items: list[dict[str, Any]] = []
    for topic in topics:
        try:
            results = await client.search(
                topic["query"],
                categories=settings.searxng_categories,
                language=settings.missing_signals_language,
                time_range=settings.missing_signals_time_range,
                limit=settings.searxng_max_results,
                mode="missing_signals",
            )
        except Exception:
            # Один упавший запрос не должен ронять весь воркспейс: остальные темы
            # обрабатываются, а причина видна и в логе, и в счётчике.
            logger.exception(
                "missing_signals_searxng_failed workspace=%s topic=%s",
                workspace_id,
                topic["topic"],
            )
            stats["searxng_errors"] += 1
            continue
        if len(results) < int(settings.missing_signals_min_external_results):
            stats["topics_dropped_by_results"] += 1
            continue
        frontier_frequency = _frontier_frequency(
            topic=topic["topic"],
            query=topic["query"],
            semantic=semantic,
            stable=stable,
            emerging=emerging,
            stopwords=stopwords,
            corpus=corpus,
        )
        external_strength = _external_signal_strength(results, int(settings.searxng_max_results))
        gap_score = _gap_score(
            frontier_frequency=frontier_frequency,
            external_strength=external_strength,
            presence_saturation=presence_saturation,
        )
        if gap_score < float(settings.missing_signals_min_gap_score):
            stats["topics_dropped_by_gap"] += 1
            continue
        items.append(
            {
                "id": _digest(f"{workspace_id}|{topic['topic']}", "missing"),
                "topic": topic["topic"],
                "query": topic["query"],
                "category": topic.get("category"),
                "why_expected": topic.get("why_expected"),
                "gap_score": gap_score,
                "searxng_frequency": float(len(results)),
                "frontier_frequency": frontier_frequency,
                "evidence_urls": [
                    result["url"]
                    for result in results[: int(settings.missing_signals_max_evidence_urls)]
                ],
                "evidence": results,
            }
        )

    stats["topics_kept"] = len(items)
    items.sort(key=lambda item: item["gap_score"], reverse=True)
    # Задания крутятся дочерним процессом (admin/backend/scheduler.py::_run_job_subprocess),
    # поэтому прометеевские счётчики умирают вместе с ним — числа нужны в логе и в
    # cluster_runs.summary, иначе следующая диагностика опять упрётся в «ноль без причины».
    logger.info(
        "missing_signals_run workspace=%s topics_generated=%s dropped_by_stoplist=%s "
        "dropped_by_results=%s dropped_by_gap=%s searxng_errors=%s kept=%s",
        workspace_id,
        stats["topics_generated"],
        stats["topics_dropped_by_stoplist"],
        stats["topics_dropped_by_results"],
        stats["topics_dropped_by_gap"],
        stats["searxng_errors"],
        stats["topics_kept"],
    )
    if not items:
        logger.warning(
            "missing_signals_empty_result workspace=%s — ранее записанные строки "
            "сохранены (раньше пустой прогон делал DELETE без INSERT)",
            workspace_id,
        )
        return []

    top_items = items[: int(settings.missing_signals_topic_limit)]
    opportunity_map = await _generate_opportunities(workspace=dict(workspace), items=top_items)
    for item in top_items:
        item["opportunity"] = opportunity_map.get(
            item["topic"],
            f"External evidence suggests growing attention around {item['topic']}; expand frontier coverage here.",
        )

    await _replace_missing_signals(session, workspace_id=workspace_id, items=top_items)
    return top_items


async def run_missing_signals_refresh(
    session: AsyncSession,
    *,
    workspace_id: str | None,
    counters: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    if not workspace_id:
        _init_counters(counters)
        return []
    semantic, stable, emerging = await _load_current_signal_snapshots(
        session,
        workspace_id=workspace_id,
    )
    return await run_missing_signals_analysis(
        session,
        workspace_id=workspace_id,
        semantic=semantic,
        stable=stable,
        emerging=emerging,
        counters=counters,
    )
