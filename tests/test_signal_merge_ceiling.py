"""
Потолок предиката слияния сигналов: он опирается на признаки, равные нулю
по построению, и потому недостижим.

Заведён 07.08.2026 по пункту 74 реестра. `signals_merged = 0` у всех шести
воркспейсов за всю историю, и это не совпадение.

Кандидаты строятся как связные компоненты над семантическими кластерами
(`semantic_clustering.py:1678`), а компоненты РАЗБИВАЮТ множество: каждый кластер
попадает ровно в одну группу. Значит у любых двух кандидатов одного прогона
`doc_ids` и `semantic_cluster_ids` не пересекаются никогда.

Веса предиката (`:1310-1316`):

    doc_overlap*0.28 + semantic_overlap*0.22 + concept*0.24 + title*0.16 + temporal*0.10

Первые два слагаемых тождественно нулевые ⇒ потолок 0.50 при пороге 0.72
(у одного воркспейса 0.58).

Соседняя функция уровнем ниже, `_merge_semantic_candidates`, делает то же самое
и работает — потому что у неё есть косинус центроидов с весом 0.45, а косинус
на дизъюнктных множествах нулю не равен.

Тесты ниже фиксируют арифметику, а не текущее поведение. Если кто-то решит
«починить» это понижением порога до 0.50, второй тест покажет, что решающими
станут только concept и title, то есть слияние будет происходить по совпадению
слов в заголовке.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from worker.services import semantic_clustering as sc

pytestmark = pytest.mark.unit

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)

# Веса берём из кода, а не переписываем константами: тест обязан ломаться, когда
# формулу меняют, а не жить своей отдельной арифметикой.
_ZERO_BY_CONSTRUCTION = ("doc_overlap", "semantic_overlap")


def _candidate(idx: int, *, keywords: list[str], title: str, score: float) -> dict:
    """Кандидат, дизъюнктный со всеми остальными по документам и кластерам."""
    return {
        "signal_id": f"sig:{idx}",
        "existing_id": None,
        "workspace_id": "design",
        "doc_ids": [f"doc:{idx}:{n}" for n in range(3)],
        "semantic_cluster_ids": [f"semantic:{idx}"],
        "source_ids": [f"src:{idx}"],
        "source_count": 1,
        "keywords": keywords,
        "title": title,
        "signal_score": score,
        "first_seen_at": NOW - timedelta(hours=6),
        "last_seen_at": NOW,
        "evidence": [],
        "doc_count": 3,
    }


def _cfg(threshold: float) -> dict:
    return {
        "signal_merge_similarity_threshold": threshold,
        "signal_merge_doc_overlap_threshold": 0.5,
        "trend_cluster_max_gap_hours": 24 * 30,
    }


def test_disjoint_candidates_never_merge_at_the_configured_threshold() -> None:
    """Даже при ПОЛНОМ совпадении концептов и заголовка — если не сработала лазейка.

    Берём два кандидата с одинаковыми ключевыми словами и разными заголовками:
    concept_overlap = 1.0, title_overlap мал, temporal = 1.0. Это даёт
    0*0.28 + 0*0.22 + 1.0*0.24 + мало*0.16 + 1.0*0.10 ≈ 0.34 при пороге 0.72.
    """
    items = [
        _candidate(1, keywords=["hmi", "cockpit"], title="Первый обзор панелей", score=0.7),
        _candidate(2, keywords=["hmi", "cockpit"], title="Совсем другая формулировка", score=0.6),
    ]
    kept, merged = sc._merge_signal_candidates(items, _cfg(0.72))

    assert merged == 0, (
        "кандидаты слились, хотя по построению не пересекаются ни документами, "
        "ни семантическими кластерами — проверь, не изменились ли веса"
    )
    assert len(kept) == 2


def test_the_ceiling_is_half_because_two_terms_are_structurally_zero() -> None:
    """Арифметика потолка, а не поведение: сумма весов ненулевых членов = 0.50.

    Если однажды в предикат добавят косинус центроидов (как у соседней функции
    уровнем ниже), этот тест обязан упасть — и это будет правильно, потому что
    именно он и есть починка пункта 74.
    """
    import inspect

    source = inspect.getsource(sc._merge_signal_candidates)
    assert "centroid" not in source, (
        "в предикат слияния сигналов добавили центроид — потолок 0.50 больше не "
        "действует, пункт 74 закрыт, обнови этот тест вместе с реестром"
    )
    for term in _ZERO_BY_CONSTRUCTION:
        assert term in source, f"признак {term} исчез из формулы — разбор пункта 74 устарел"


def test_lowering_the_threshold_to_the_ceiling_merges_on_words_alone() -> None:
    """Почему «просто понизить порог» — не починка.

    При пороге 0.34 те же два кандидата сливаются, а решают это только совпадение
    ключевых слов и близость во времени: содержательного пересечения между ними
    нет вовсе. То есть понижение порога меняет не чувствительность, а СМЫСЛ
    предиката.
    """
    def _merged_at(threshold: float) -> int:
        items = [
            _candidate(1, keywords=["hmi", "cockpit"], title="Первый обзор панелей", score=0.7),
            _candidate(2, keywords=["hmi", "cockpit"], title="Совсем другая формулировка", score=0.6),
        ]
        return sc._merge_signal_candidates(items, _cfg(threshold))[1]

    # Значение similarity этой пары зажато между 0.33 и 0.35, то есть равно 0.34:
    # concept 1.0*0.24 + title 0.0*0.16 + temporal 1.0*0.10. Ровно то, что даёт
    # формула, когда два её главных члена структурно равны нулю.
    assert _merged_at(0.33) == 1, "при пороге ниже фактической similarity слияние обязано быть"
    assert _merged_at(0.35) == 0, "при пороге выше — обязано не быть"
