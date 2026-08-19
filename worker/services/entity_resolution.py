"""Контур D+ (RSI): семантический entity-resolution концептов Neo4j.

Строковая нормализация (apoc.text.clean, см. graph_maintenance) ловит регистр/пробелы/
пунктуацию, но НЕ семантику: «HMI» ↔ «Human-Machine Interface», «LLM» ↔ «Large Language
Model». Здесь — кандидаты по акронимам (дёшево, точно), затем LLM-судья другой семьи
(DeepSeek) подтверждает эквивалентность, и пара уходит предложением в entity_merge_proposals.
Слияние применяет человек через MCP-гейт. Синонимы без акронима (autonomous vehicle ↔
self-driving car) — это embedding-сигнал, отдельный шаг (v2).

acronym_of и find_acronym_candidates — чистые (тестируются без графа/LLM).
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_WORD_SPLIT = re.compile(r"[\s\-_/.]+")
# Стоп-слова не дают букву в акроним (англ. + рус. предлоги/союзы/артикли).
_ACRONYM_STOPWORDS = {
    "the", "a", "an", "of", "and", "or", "for", "to", "in", "on", "with", "by",
    "и", "в", "на", "по", "для", "с", "к", "из",
}

# Кросс-алфавитное сворачивание: кириллический акроним-узел («ЛЛМ») и его латинский
# двойник («LLM») должны сматчиться. Сворачиваем обе стороны в общий латинский вид
# и используем как fallback к точному совпадению нормы.
_CYR_TO_LAT = str.maketrans(
    {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
        "ж": "zh", "з": "z", "и": "i", "й": "i", "к": "k", "л": "l", "м": "m",
        "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
        "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    }
)


def _fold(value: str) -> str:
    """Свернуть строку в латиницу (кириллица транслитерируется, прочее проходит как есть)."""
    return (value or "").lower().translate(_CYR_TO_LAT)


def acronym_of(name: str) -> str | None:
    """Акроним из многословного имени: первые буквы значимых слов, lower. Иначе None.

    «Human-Machine Interface» → 'hmi'; «Large Language Model» → 'llm';
    «AI» (одно слово) → None; «the End of X» → 'ex' (the пропускается).
    """
    words = [w for w in _WORD_SPLIT.split((name or "").strip()) if w]
    letters: list[str] = []
    for w in words:
        if w.lower() in _ACRONYM_STOPWORDS:
            continue
        ch = w[0]
        if ch.isalpha():
            letters.append(ch.lower())
    if len(letters) < 2:
        return None
    return "".join(letters)


def find_acronym_candidates(concepts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Пары (акроним-узел, расшифровка-узел) по совпадению акронима.

    concepts: список {name, norm, mentions}. Возвращает кандидатов на эквивалентность,
    отсортированных по сумме упоминаний (сначала самые «весомые» — их слияние ценнее).
    """
    norm_index: dict[str, dict[str, Any]] = {}
    folded_index: dict[str, dict[str, Any]] = {}
    for c in concepts:
        norm = str(c.get("norm") or "")
        if norm and norm not in norm_index:
            norm_index[norm] = c
            folded_index.setdefault(_fold(norm), c)

    seen: set[tuple[str, str]] = set()
    candidates: list[dict[str, Any]] = []
    for c in concepts:
        acro = acronym_of(str(c.get("name") or ""))
        if not acro:
            continue
        # Точное совпадение нормы, затем кросс-алфавитный fallback (кириллица↔латиница).
        short = norm_index.get(acro) or folded_index.get(_fold(acro))
        if not short:
            continue
        expansion_norm = str(c.get("norm") or "")
        if short["norm"] == expansion_norm:
            continue  # акроним собственной нормы — не пара
        key = tuple(sorted((short["norm"], expansion_norm)))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {
                "acronym_name": short["name"],
                "acronym_norm": short["norm"],
                "expansion_name": c["name"],
                "expansion_norm": expansion_norm,
                "mentions": int(short.get("mentions") or 0) + int(c.get("mentions") or 0),
            }
        )
    candidates.sort(key=lambda p: p["mentions"], reverse=True)
    return candidates


async def _aclose(client: Any) -> None:
    fn = getattr(client, "close", None) or getattr(client, "aclose", None)
    if fn:
        try:
            await fn()
        except Exception:
            logger.debug("entity_resolution client close failed", exc_info=True)


async def run_entity_resolution(workspace_id: str, cap: int | None = None) -> dict[str, Any]:
    """Прогон: кандидаты-акронимы → фильтр со-встречаемости → LLM-судья → pending-предложения."""
    from sqlalchemy import text

    from shared.config import get_settings
    from worker.chains.entity_equivalence_chain import EntityEquivalenceChain
    from worker.integrations.neo4j_client import Neo4jFrontierClient
    from worker.llm_router_client import LLMRouterClient
    from worker.services.clustering_math import digest

    settings = get_settings()
    if not bool(getattr(settings, "entity_resolution_enabled", True)):
        return {"workspace_id": workspace_id, "status": "disabled", "proposals": 0}

    cap = int(cap or settings.entity_resolution_max_per_run)
    min_cooc = int(settings.entity_resolution_min_cooccurrence)
    min_conf = float(settings.entity_resolution_min_confidence)
    run_id = digest(f"{workspace_id}|entityres", "entres")

    from shared.db import get_session_factory

    neo = Neo4jFrontierClient()
    router = LLMRouterClient(service_name="worker")
    chain = EntityEquivalenceChain(
        None,
        None,
        model=settings.novelty_judge_model,
        fallback_model=settings.novelty_judge_fallback_model,
        token_budget=int(settings.novelty_judge_token_budget),
        router_client=router,
    )

    proposals = judged = candidates_n = 0
    try:
        concepts = await neo.load_concept_names(workspace_id)
        raw = find_acronym_candidates(concepts)
        cooc = await neo.cooccurrence_for_pairs(
            workspace_id, [{"a": c["acronym_norm"], "b": c["expansion_norm"]} for c in raw]
        )
        for c in raw:
            c["cooccurrence"] = cooc.get((c["acronym_norm"], c["expansion_norm"]), 0)
        ranked = sorted(
            (c for c in raw if c["cooccurrence"] >= min_cooc),
            key=lambda c: c["cooccurrence"], reverse=True,
        )
        candidates_n = len(ranked)

        async with get_session_factory()() as session:
            # Дедуп ПО АКРОНИМУ: один акроним (LLM/ИИ/MCP) даёт ОДНО предложение, без перекрытий.
            # Пропускаем акронимы с уже принятым решением (pending/approved/rejected).
            existing_acronyms = set()
            rows = await session.execute(
                text(
                    "SELECT DISTINCT norm_a FROM entity_merge_proposals "
                    "WHERE workspace_id = :ws AND status IN ('pending', 'approved', 'rejected')"
                ),
                {"ws": workspace_id},
            )
            for r in rows.mappings().all():
                existing_acronyms.add(r["norm_a"])

            seen_acronyms: set[str] = set()
            # ranked отсортирован по со-встречаемости — первый на акроним = самая частая расшифровка.
            for c in ranked:
                if judged >= cap:
                    break
                acro = c["acronym_norm"]
                if acro in existing_acronyms or acro in seen_acronyms:
                    continue
                seen_acronyms.add(acro)
                verdict = await chain.run(c["acronym_name"], c["expansion_name"])
                judged += 1
                if not verdict or not verdict["equivalent"] or verdict["confidence"] < min_conf:
                    continue
                # a = acronym, b = expansion (см. build_prompt)
                if verdict["canonical"] == "a":
                    canon_norm, canon_name = c["acronym_norm"], c["acronym_name"]
                else:
                    canon_norm, canon_name = c["expansion_norm"], c["expansion_name"]
                pid = digest(f"{workspace_id}|{c['acronym_norm']}|{c['expansion_norm']}", "entprop")
                await session.execute(
                    text(
                        """
                        INSERT INTO entity_merge_proposals (
                            id, workspace_id, norm_a, name_a, norm_b, name_b, canonical_norm,
                            canonical_name, confidence, reasoning, signal, cooccurrence,
                            judge_provider, status, run_id, created_at, updated_at
                        ) VALUES (
                            :id, :ws, :na, :nan, :nb, :nbn, :cn, :cnn, :conf, :reason, 'acronym',
                            :cooc, :prov, 'pending', :run_id, NOW(), NOW()
                        )
                        ON CONFLICT (workspace_id, norm_a, norm_b) DO NOTHING
                        """
                    ),
                    {
                        "id": pid, "ws": workspace_id,
                        "na": c["acronym_norm"], "nan": c["acronym_name"],
                        "nb": c["expansion_norm"], "nbn": c["expansion_name"],
                        "cn": canon_norm, "cnn": canon_name,
                        "conf": verdict["confidence"], "reason": verdict["reasoning"],
                        "cooc": c["cooccurrence"], "prov": verdict.get("_provider"),
                        "run_id": run_id,
                    },
                )
                proposals += 1
            await session.commit()
    finally:
        await _aclose(neo)
        await _aclose(router)

    logger.info(
        "entity_resolution workspace=%s candidates=%d judged=%d proposals=%d",
        workspace_id, candidates_n, judged, proposals,
    )
    return {
        "workspace_id": workspace_id, "status": "ok",
        "candidates": candidates_n, "judged": judged, "proposals": proposals,
    }
