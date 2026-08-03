"""Тесты gap-анализа missing_signals после нормировки (правка 2026-08-03).

Прежние три теста были направленными («> 0», «> 0.3», «a > b») и гоняли корпус
из одного-двух документов. Сломан был масштаб: `_frontier_frequency` возвращала
неограниченную взвешенную сумму по всему корпусу, а `_external_signal_strength`
ограничен 1.0 — на боевом корпусе design (2425 кластеров) вычитание в
`_gap_score` было бессмысленным, и любая тема с суммой >= 1.83 отсеивалась
всегда. Направленная проверка такое увидеть не может по построению.

Здесь фиксируются: точные значения, независимость от размера корпуса, границы
[0, 1], регрессия на старом обрыве 1.83, а также инварианты пайплайна —
не стирать историю при пустом результате, не ронять воркспейс из-за одного
упавшего запроса к SearXNG и не пропускать в выдачу собственное название темы.

Второй заход (тоже 2026-08-03). Часть утверждений оказалась неразличающей —
проходила и до правки, и при любом возмущении. Правило для этого файла: каждый
тест обязан утверждать НАПРАВЛЕНИЕ с названным запасом, а не факт «число
изменилось», и обязан падать на прежнем поведении. Проверено прогоном этого
файла против старой арифметики, возвращённой под новыми именами: падают пять
тестов — про вес токена, про порог на разных размерах корпуса, про общий токен,
про дефолтный вес отсутствующего токена и про стоп-лист. Арифметика на литералах
(вычисления, не трогающие продуктовый код) из файла удалена.

Оценки релевантности во всех фикстурах — 0.4-0.5: это то, что реально отдаёт
живой SearXNG. Старые тесты использовали 1.8-2.4, то есть калибровались под
поисковик, которого не существует.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from shared.config import Settings
from worker.services import missing_signals as ms
from worker.services.missing_signals import (
    _MIN_TOPIC_OVERLAP,
    _build_signal_corpus,
    _drop_self_vocabulary_topics,
    _external_signal_strength,
    _frontier_frequency,
    _gap_score,
    _replace_missing_signals,
    _topic_overlap_score,
    _workspace_stopwords,
    run_missing_signals_analysis,
)

pytestmark = pytest.mark.unit


def _declared_default(name: str, fallback: float) -> float:
    """Дефолт настройки из shared/config.py.

    Работает и с реальным pydantic_settings, и с заглушкой BaseSettings из
    tests/conftest.py — в первом случае значение лежит в model_fields, во втором
    атрибут класса остаётся объектом FieldInfo.
    """
    fields = getattr(Settings, "model_fields", None)
    if isinstance(fields, dict) and name in fields:
        return float(fields[name].default)
    attribute = getattr(Settings, name, None)
    default = getattr(attribute, "default", attribute)
    if isinstance(default, (int, float)) and not isinstance(default, bool):
        return float(default)
    return fallback


MIN_GAP_SCORE = _declared_default("missing_signals_min_gap_score", 0.30)
PRESENCE_SATURATION = _declared_default("missing_signals_presence_saturation", 0.30)


# ── фикстуры корпуса и выдачи ────────────────────────────────────────────────


def _semantic(*token_groups: str) -> list[dict[str, Any]]:
    return [{"title": tokens, "top_concepts": [], "explainability": {}} for tokens in token_groups]


def _results(count: int, *, distinct_domains: int | None = None, score: float = 0.46) -> list[dict[str, Any]]:
    domains = count if distinct_domains is None else distinct_domains
    return [
        {
            "url": f"https://host{index % domains}.example.com/{index}",
            "title": f"result {index}",
            "engine": "bing",
            "score": score,
        }
        for index in range(count)
    ]


# ── _topic_overlap_score: вес токена и независимость от размера корпуса ──────


def test_token_weight_depends_only_on_the_document_frequency_ratio() -> None:
    """Корень дефекта корпусного дрейфа: вес токена — функция доли df/N, и только её.

    До 2026-08-03 весом был сглаженный IDF log((N+1)/(df+1)). Он раскладывается
    на log(N+1) - log(df+1), то есть при одной и той же доле df/N зависит ещё и
    от N отдельно: на этой же паре корпусов он давал 0.9163 против 1.2238.
    Именно этот остаточный множитель и проезжал через порог при росте корпуса.
    """
    quarter_of_four = _build_signal_corpus(
        _semantic("cockpit ergonomics study", "alpha one", "beta two", "gamma three"),
        [],
        [],
    )
    quarter_of_sixteen = _build_signal_corpus(
        _semantic(
            *[f"cockpit ergonomics study{index}" for index in range(4)],
            *[f"filler note{index}" for index in range(12)],
        ),
        [],
        [],
    )

    assert len(quarter_of_four.documents) == 4
    assert len(quarter_of_sixteen.documents) == 16
    # df/N = 1/4 в обоих корпусах при N, различающемся вчетверо.
    assert quarter_of_four.token_rarity["cockpit"] == pytest.approx(0.75)
    assert quarter_of_sixteen.token_rarity["cockpit"] == pytest.approx(0.75)
    # Токена нет в корпусе — вес максимальный и тоже не зависит от N.
    assert quarter_of_four.default_rarity == 1.0
    assert quarter_of_sixteen.default_rarity == 1.0


def test_same_pair_lands_the_same_side_of_the_gate_at_every_corpus_size() -> None:
    """Один и тот же документ не может считаться покрытием только из-за размера корпуса.

    Два боевых пути читают корпуса разного размера: refresh — LIMIT 64
    (_load_current_signal_snapshots), плановый — все неархивные кластеры
    (у design их 2425). Пара «тема {automotive, cockpit, telemetry} / документ
    {cockpit, ergonomics, study}» при df(cockpit)=1 на IDF-весах давала
    0.2741 / 0.2943 / 0.3044 / 0.3103 / 0.3130 при N=16/64/256/1024/2425, то есть
    при пороге 0.30 не считалась покрытием на refresh и считалась на плановом.

    Здесь доля df/N РАЗНАЯ на каждом N (1/16 … 1/2425) — в отличие от теста
    репликации ниже, который размножает корпус и все доли сохраняет, поэтому
    дрейф увидеть не может по построению.
    """
    topic = "automotive cockpit telemetry"
    scores: list[float] = []
    frequencies: list[float] = []
    ratios: list[float] = []

    for size in (16, 64, 256, 1024, 2425):
        semantic = _semantic(
            "cockpit ergonomics study",
            *[f"filler alpha{index} beta{index}" for index in range(size - 1)],
        )
        corpus = _build_signal_corpus(semantic, [], [])
        assert len(corpus.documents) == size
        ratios.append(1.0 / size)
        scores.append(
            _topic_overlap_score(
                {"automotive", "cockpit", "telemetry"},
                {"cockpit", "ergonomics", "study"},
                rarity=corpus.token_rarity,
                default_rarity=corpus.default_rarity,
            )
        )
        frequencies.append(
            _frontier_frequency(topic=topic, query=topic, semantic=semantic, stable=[], emerging=[])
        )

    # Тест обязан быть способен наблюдать дрейф: доли df/N действительно разные.
    assert len(set(ratios)) == len(ratios)
    # Вердикт порога один и тот же на всех размерах — это и есть починка.
    verdicts = {score >= _MIN_TOPIC_OVERLAP for score in scores}
    assert verdicts == {False}, dict(zip((16, 64, 256, 1024, 2425), scores))
    # Порог отстоит от всей полосы значений: «один из трёх равно-информативных
    # токенов» упирается в 1/3 сверху, а порог стоит выше с запасом.
    assert max(scores) < _MIN_TOPIC_OVERLAP - 0.05
    assert max(scores) - min(scores) < 0.02
    # Следствие, видимое наружу: покрытия нет ни на одном размере корпуса.
    # На IDF-весах при N >= 256 документ засчитывался и frontier_frequency
    # становилась ненулевой — то есть один и тот же сигнал по-разному влиял на
    # gap_score в зависимости от того, каким путём запущен анализ.
    assert frequencies == [0.0] * 5


def test_overlap_score_discounts_the_ubiquitous_token() -> None:
    """Совпадение по «design» в design-корпусе не считается покрытием.

    Это свойство из пункта 4 спеки: мера не должна быть голой
    len(shared)/len(topic_tokens), которая давала за общий токен 0.5 и
    засчитывала документ. Весовая мера оставляет от него ~0.1, а совпадение по
    различающему токену даёт ~0.9 — запас с обеих сторон от порога 0.40.
    """
    corpus = _build_signal_corpus(
        _semantic(*[f"design systems note{index}" for index in range(9)], "automotive cockpit rig"),
        [],
        [],
    )
    topic_tokens = {"design", "automotive"}

    generic_only = _topic_overlap_score(
        topic_tokens,
        {"design", "systems"},
        rarity=corpus.token_rarity,
        default_rarity=corpus.default_rarity,
    )
    discriminative = _topic_overlap_score(
        topic_tokens,
        {"automotive", "cockpit"},
        rarity=corpus.token_rarity,
        default_rarity=corpus.default_rarity,
    )

    # Голая доля токенов дала бы обоим ровно 0.5 — направленность и есть смысл теста.
    assert generic_only == pytest.approx(0.1, abs=0.001)
    assert discriminative == pytest.approx(0.9, abs=0.001)
    assert generic_only <= _MIN_TOPIC_OVERLAP - 0.25
    assert discriminative >= _MIN_TOPIC_OVERLAP + 0.4
    # Однословная тема остаётся бинарной по построению — один токен и есть всё
    # содержание темы. Этот случай закрывает стоп-лист (см. тесты ниже), не вес.
    assert (
        _topic_overlap_score({"design"}, {"design", "systems"}, rarity=corpus.token_rarity) == 1.0
    )


def test_out_of_corpus_token_counts_as_maximally_rare_not_weightless() -> None:
    """Токен, которого нет в корпусе, обязан весить максимум, а не ноль.

    Ловушка смены знака: при нулевом весе по умолчанию тема {design, telemetry}
    против документа {design, systems} даёт shared/topic = 0.1/0.1 = 1.0, то есть
    «документ покрывает тему целиком» — ровно наоборот от истины, потому что
    telemetry в корпусе нет вообще. Правильный дефолт даёт 0.09 и отсев.
    """
    rarity = {"design": 0.1, "systems": 0.1}

    score = _topic_overlap_score({"design", "telemetry"}, {"design", "systems"}, rarity=rarity)

    assert score == pytest.approx(0.1 / 1.1, abs=0.001)
    assert score < _MIN_TOPIC_OVERLAP
    assert score != 1.0


def test_overlap_score_falls_back_when_every_topic_token_is_ubiquitous() -> None:
    """Вырожденный корпус не должен давать деления на ноль.

    Токен в каждом документе весит ровно 0 — собственный словарь корпуса
    обнуляется без ручного стоп-листа. Когда так со ВСЕЙ темой, суммарный вес
    нулевой, и мера обязана откатиться на долю токенов, а не упасть.
    """
    corpus = _build_signal_corpus(_semantic("design ritual", "design ritual"), [], [])

    assert corpus.token_rarity["design"] == 0.0
    assert corpus.token_rarity["ritual"] == 0.0
    assert (
        _topic_overlap_score(
            {"design", "ritual"},
            {"design", "ritual"},
            rarity=corpus.token_rarity,
            default_rarity=corpus.default_rarity,
        )
        == 1.0
    )
    # Тот же откат при полном отсутствии весов (прямой юнит-вызов без корпуса).
    assert _topic_overlap_score({"design", "automotive"}, {"design", "systems"}) == 0.5
    assert _topic_overlap_score({"design", "automotive"}, {"weather"}) == 0.0


# ── _frontier_frequency: масштаб ─────────────────────────────────────────────


def test_frontier_frequency_is_an_exact_share_of_the_corpus() -> None:
    """Две из четырёх записей корпуса покрывают тему → ровно 0.5, а не сумма 2.0."""
    semantic = _semantic(
        "alpha beta note",
        "alpha beta memo",
        "gamma delta note",
        "epsilon zeta memo",
    )

    frequency = _frontier_frequency(
        topic="alpha beta",
        query="alpha beta",
        semantic=semantic,
        stable=[],
        emerging=[],
    )

    assert frequency == pytest.approx(0.5)


def test_frontier_frequency_is_invariant_to_replicating_the_corpus() -> None:
    """Тот же корпус, повторённый 25 раз, обязан дать то же число.

    Закрывает ровно одну ось — нормировку на суммарный вес корпуса (до неё
    величина росла линейно с числом документов и различалась в ~38 раз между
    плановым путём и refresh). Дрейф самой меры этот тест увидеть НЕ может: он
    множит корпус целиком, поэтому все доли df/N сохраняются точь-в-точь. Ось
    «доли разные» закрывает
    test_same_pair_lands_the_same_side_of_the_gate_at_every_corpus_size.
    """
    small = _semantic("alpha beta note", "alpha beta memo", "gamma delta note", "epsilon zeta memo")
    large = small * 25

    kwargs: dict[str, Any] = {"topic": "alpha beta", "query": "alpha beta", "stable": [], "emerging": []}
    assert _frontier_frequency(semantic=small, **kwargs) == pytest.approx(
        _frontier_frequency(semantic=large, **kwargs)
    )


def test_frontier_frequency_is_bounded_by_one() -> None:
    """Каждый документ покрывает тему полностью и имеет максимальный вес 1.25.

    Нормировка на len(documents) дала бы здесь 1.25 — величину, которую
    min(1.0, ...) ниже по потоку молча срезает, ломая посылку «обе стороны
    вычитания в [0, 1]». Нормировка на суммарный вес даёт ровно 1.0.
    """
    stable = [{"title": "alpha beta", "keywords": [], "explainability": {}} for _ in range(6)]

    frequency = _frontier_frequency(
        topic="alpha beta",
        query="alpha beta",
        semantic=[],
        stable=stable,
        emerging=[],
    )

    assert frequency == pytest.approx(1.0)
    assert 0.0 <= frequency <= 1.0


def test_absent_topic_scores_zero_and_zero_coverage_yields_the_maximum_gap() -> None:
    """Ноль покрытия обязан доехать до gap_score целиком, а появление документа — снизить gap.

    Направленность здесь и есть содержание: вычитание presence должно уменьшать
    gap монотонно. Тест ловит и знак (presence прибавляется вместо вычитания), и
    вырождение (presence не влияет вовсе) — при обеих поломках равенство
    «нет покрытия → gap == external_strength» или строгое неравенство ниже рвётся.
    """
    strength = _external_signal_strength(_results(5), max_results=5)
    absent_corpus = _semantic("alpha beta note", "gamma delta memo", "epsilon zeta rec")
    covered_corpus = [*absent_corpus, *_semantic("automotive hmi brief")]

    topic: dict[str, Any] = {"topic": "automotive hmi", "query": "automotive hmi"}
    absent = _frontier_frequency(**topic, semantic=absent_corpus, stable=[], emerging=[])
    covered = _frontier_frequency(**topic, semantic=covered_corpus, stable=[], emerging=[])

    def gap(frequency: float) -> float:
        return _gap_score(
            frontier_frequency=frequency,
            external_strength=strength,
            presence_saturation=PRESENCE_SATURATION,
        )

    assert absent == 0.0
    # Один покрывающий документ из четырёх — ровно четверть корпуса, не «сумма 1.0».
    assert covered == pytest.approx(0.25)
    assert gap(absent) == pytest.approx(strength)
    assert gap(covered) < strength - 0.5


def test_workspace_stoplist_lowers_measured_coverage_for_its_own_vocabulary() -> None:
    """Стоп-лист обязан СНИЖАТЬ измеренное покрытие, а не просто менять число.

    Прежний тест проверял `with_stopwords != without` на фикстуре, дававшей
    0.1 против 0.0947 — разница 0.0053, и утверждение проходило при любом
    возмущении, включая смену знака. Здесь направление зафиксировано с запасом:
    тема «design telemetry» в корпусе, где про telemetry нет ничего, засчитывалась
    как частично покрытая исключительно через собственный токен воркспейса. После
    вычитания стоп-листа покрытия не остаётся вовсе — и тема выходит в gap, как и
    должна.
    """
    semantic = _semantic(
        "design systems note0",
        "design systems note1",
        *[f"automotive cockpit rig{index}" for index in range(8)],
    )
    kwargs: dict[str, Any] = {
        "topic": "design telemetry",
        "query": "design telemetry",
        "semantic": semantic,
        "stable": [],
        "emerging": [],
    }

    without = _frontier_frequency(**kwargs)
    with_stopwords = _frontier_frequency(**kwargs, stopwords=frozenset({"design"}))

    assert without == pytest.approx(0.0889, abs=0.001)
    assert with_stopwords == 0.0
    assert without - with_stopwords >= 0.08
    assert 0.0 <= with_stopwords <= 1.0


# ── _external_signal_strength и _gap_score ───────────────────────────────────


def test_min_gap_score_sits_inside_the_domain_diversity_corridor() -> None:
    """Порог обязан лежать между «два результата с одного домена» и «с двух».

    Прежний тест фиксировал три числа и на этом останавливался — он проходил при
    любом значении MISSING_SIGNALS_MIN_GAP_SCORE, хотя весь смысл этой настройки
    в том, ГДЕ она стоит относительно этих чисел. Коридор (0.303, 0.363] —
    ровно то, что записано в .env.example и shared/config.py: ниже него фильтр
    вырождается в «SearXNG вообще ответил», выше — минимально допустимая выдача
    (MIN_EXTERNAL_RESULTS=2) не проходит никогда. Тест сцепляет константу с
    арифметикой, чтобы расхождение документации и кода падало здесь.
    """
    full = _external_signal_strength(_results(5), max_results=5)
    minimal = _external_signal_strength(_results(2), max_results=5)
    single_domain = _external_signal_strength(_results(2, distinct_domains=1), max_results=5)

    assert full == pytest.approx(0.873, abs=0.001)
    assert minimal == pytest.approx(0.363, abs=0.001)
    assert single_domain == pytest.approx(0.303, abs=0.001)
    # Направленность: разнообразие доменов обязано повышать оценку, объём — тоже.
    assert single_domain < minimal < full
    # И порог обязан стоять строго внутри коридора, а не с краю.
    assert single_domain < MIN_GAP_SCORE <= minimal


def test_min_external_results_and_min_gap_score_no_longer_contradict() -> None:
    """Минимально допустимая выдача обязана иметь шанс пройти порог.

    При старом пороге 0.35 выдача из двух результатов (external_strength 0.363)
    оставляла бюджет 3.5*(0.363-0.35) = 0.045 — меньше наименьшего ненулевого
    покрытия, то есть класс тем, пропущенный MIN_EXTERNAL_RESULTS, арифметика
    отбрасывала по построению.
    """
    minimal = _external_signal_strength(_results(2), max_results=5)
    gap = _gap_score(
        frontier_frequency=0.0,
        external_strength=minimal,
        presence_saturation=PRESENCE_SATURATION,
    )
    assert gap >= MIN_GAP_SCORE


def test_gap_score_is_monotone_and_saturates() -> None:
    strength = _external_signal_strength(_results(5), max_results=5)
    low = _gap_score(frontier_frequency=0.02, external_strength=strength, presence_saturation=PRESENCE_SATURATION)
    high = _gap_score(frontier_frequency=0.20, external_strength=strength, presence_saturation=PRESENCE_SATURATION)
    saturated = _gap_score(
        frontier_frequency=1.0, external_strength=strength, presence_saturation=PRESENCE_SATURATION
    )

    assert low > high > saturated
    assert saturated == 0.0
    assert 0.0 <= low <= 1.0


def test_regression_topic_above_the_old_1_83_cutoff_now_surfaces() -> None:
    """Регрессия на старом обрыве: `frontier_frequency >= 1.83` = «не пройдёт никогда».

    Старая арифметика: presence = min(1, 1.83/3.5) = 0.5229, gap = 0.3499 при
    практическом потолке external_strength 0.8728 — на 0.0001 ниже порога 0.35.
    Сумма 1.83 набиралась двумя-тремя совпадениями в корпусе из 64 документов;
    после нормировки это доля ~0.03, и тема выходит в выдачу.

    Два первых утверждения раньше были чистой арифметикой на литералах и не
    трогали продуктовый код вообще — они удалены. Проверять здесь имеет смысл
    только то, что считает сама функция.
    """
    semantic = _semantic(
        "alpha beta note",
        "alpha beta memo",
        *[f"unrelated topic number{index}" for index in range(62)],
    )
    frequency = _frontier_frequency(
        topic="alpha beta",
        query="alpha beta",
        semantic=semantic,
        stable=[],
        emerging=[],
    )
    gap = _gap_score(
        frontier_frequency=frequency,
        external_strength=_external_signal_strength(_results(5), max_results=5),
        presence_saturation=PRESENCE_SATURATION,
    )

    assert frequency == pytest.approx(2 / 64, abs=0.0001)
    assert gap >= MIN_GAP_SCORE


def test_saturated_topic_is_still_rejected() -> None:
    """Обратная сторона: нормировка не должна превратить фильтр в «пропускать всё»."""
    semantic = _semantic(*[f"alpha beta note{index}" for index in range(7)], *[f"other {i}" for i in range(3)])
    frequency = _frontier_frequency(
        topic="alpha beta",
        query="alpha beta",
        semantic=semantic,
        stable=[],
        emerging=[],
    )
    gap = _gap_score(
        frontier_frequency=frequency,
        external_strength=_external_signal_strength(_results(5), max_results=5),
        presence_saturation=PRESENCE_SATURATION,
    )

    assert frequency > PRESENCE_SATURATION
    assert gap == 0.0


# ── стоп-лист собственного словаря ───────────────────────────────────────────


def test_workspace_stopwords_split_design_lenses_on_underscore() -> None:
    stopwords = _workspace_stopwords(
        {
            "id": "design",
            "name": "Design",
            "description": "",
            "categories": ["visual culture"],
            "design_lenses": ["interaction_design", "service_design"],
        }
    )

    assert {"design", "interaction", "service", "visual", "culture"} <= stopwords
    # Литерал с подчёркиванием в стоп-лист попадать не должен — иначе токены
    # interaction/design из заголовков кластеров не отфильтруются.
    assert "interaction_design" not in stopwords


def test_topics_made_only_of_workspace_vocabulary_are_dropped() -> None:
    topics = [
        {"topic": "design", "query": "design"},
        {"topic": "interaction design", "query": "interaction design"},
        {"topic": "automotive HMI design", "query": "automotive HMI design trends"},
    ]

    kept, dropped = _drop_self_vocabulary_topics(topics, frozenset({"design", "interaction", "service"}))

    assert dropped == 2
    assert [item["topic"] for item in kept] == ["automotive HMI design"]


async def test_fallback_topics_survive_the_llm_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Фолбэк обязан отдавать темы, иначе падение генератора = ноль без причины.

    _fallback_topics строится ровно из design_lenses + categories, то есть из
    источника стоп-листа: пока отбраковка применялась и к нему, отсеивалось 100%
    тем во всех шести воркспейсах, и вся ветка «LLM отвалилась» была мёртвым кодом
    (ноль запросов к SearXNG, ноль строк, прогон при этом success).
    """

    class _Broken:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def chat(self, **kwargs: Any) -> SimpleNamespace:
            raise RuntimeError("llm router is down")

        async def close(self) -> None:
            return None

    monkeypatch.setattr(ms, "get_settings", _settings)
    monkeypatch.setattr(ms, "LLMRouterClient", _Broken)

    topics, dropped = await ms._generate_candidate_topics(
        workspace=_WORKSPACE,
        semantic=[],
        stable=[],
        emerging=[],
        stopwords=_workspace_stopwords(_WORKSPACE),
    )

    assert dropped == 0
    assert [item["topic"] for item in topics] == [
        "interaction design",
        "service design",
        "visual culture",
    ]


def test_topic_made_of_workspace_vocabulary_is_measured_not_zeroed() -> None:
    """Страховка на пути фолбэка: пустой после стоп-листа набор — не «нет покрытия».

    Иначе тема «design» в design-корпусе получала бы frontier_frequency 0.0 и
    максимальный gap, то есть первое место в выдаче.
    """
    semantic = _semantic(*[f"design systems note{index}" for index in range(9)], "automotive rig")

    frequency = _frontier_frequency(
        topic="design",
        query="design",
        semantic=semantic,
        stable=[],
        emerging=[],
        stopwords=frozenset({"design", "systems"}),
    )

    assert frequency >= PRESENCE_SATURATION
    assert (
        _gap_score(
            frontier_frequency=frequency,
            external_strength=_external_signal_strength(_results(5), max_results=5),
            presence_saturation=PRESENCE_SATURATION,
        )
        == 0.0
    )


# ── пайплайн: моки сессии, поиска и генераторов ──────────────────────────────


class _Result:
    def __init__(self, first: Any = None) -> None:
        self._first = first

    def mappings(self) -> "_Result":
        return self

    def first(self) -> Any:
        return self._first

    def all(self) -> list[Any]:
        return []


class _Session:
    """Мок AsyncSession: пишет тексты запросов, чтобы можно было проверить DELETE."""

    def __init__(self, workspace: dict[str, Any] | None) -> None:
        self._workspace = workspace
        self.statements: list[str] = []

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        rendered = str(statement)
        self.statements.append(rendered)
        if "FROM workspaces" in rendered:
            return _Result(first=self._workspace)
        return _Result()

    def deletes(self) -> list[str]:
        return [item for item in self.statements if "DELETE FROM missing_signals" in item]

    def inserts(self) -> list[str]:
        return [item for item in self.statements if "INSERT INTO missing_signals" in item]


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        searxng_enabled=True,
        missing_signals_enabled=True,
        missing_signals_topic_limit=8,
        searxng_categories="general",
        searxng_max_results=5,
        missing_signals_language="en",
        missing_signals_time_range="",
        missing_signals_min_external_results=2,
        missing_signals_min_gap_score=MIN_GAP_SCORE,
        missing_signals_presence_saturation=PRESENCE_SATURATION,
        missing_signals_max_evidence_urls=5,
    )


def _searxng_stub(behaviour: dict[str, Any]) -> Any:
    class _Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.queries: list[str] = []

        async def search(self, query: str, **kwargs: Any) -> list[dict[str, Any]]:
            self.queries.append(query)
            outcome = behaviour.get(query, [])
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    return _Client


def _llm_stub(payload: str) -> Any:
    class _Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def chat(self, **kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(content=payload)

        async def close(self) -> None:
            return None

    return _Client


def _patch_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    *,
    topics: list[dict[str, str]] | None = None,
    llm_payload: str | None = None,
    behaviour: dict[str, Any] | None = None,
) -> None:
    monkeypatch.setattr(ms, "get_settings", _settings)
    monkeypatch.setattr(ms, "SearXNGClient", _searxng_stub(behaviour or {}))
    if llm_payload is not None:
        monkeypatch.setattr(ms, "LLMRouterClient", _llm_stub(llm_payload))
    if topics is not None:

        async def _topics(**kwargs: Any) -> tuple[list[dict[str, str]], int]:
            return list(topics), 0

        monkeypatch.setattr(ms, "_generate_candidate_topics", _topics)

    async def _opportunities(**kwargs: Any) -> dict[str, str]:
        return {}

    monkeypatch.setattr(ms, "_generate_opportunities", _opportunities)


_WORKSPACE = {
    "id": "design",
    "name": "Design",
    "description": "",
    "categories": ["visual culture"],
    "design_lenses": ["interaction_design", "service_design"],
    "extra": {},
}


async def test_replace_missing_signals_never_deletes_on_empty_items() -> None:
    session = _Session(_WORKSPACE)

    await _replace_missing_signals(session, workspace_id="design", items=[])  # type: ignore[arg-type]

    assert session.statements == []


async def test_empty_run_preserves_existing_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Главный инвариант: пустая выдача поиска не должна стирать накопленное."""
    _patch_pipeline(
        monkeypatch,
        topics=[{"topic": "automotive hmi", "query": "automotive hmi", "category": "auto"}],
        behaviour={"automotive hmi": []},
    )
    session = _Session(_WORKSPACE)
    counters: dict[str, int] = {}

    items = await run_missing_signals_analysis(
        session,  # type: ignore[arg-type]
        workspace_id="design",
        semantic=_semantic("alpha beta note"),
        stable=[],
        emerging=[],
        counters=counters,
    )

    assert items == []
    assert session.deletes() == []
    assert counters["topics_dropped_by_results"] == 1


async def test_searxng_failure_is_counted_and_does_not_abort_the_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_pipeline(
        monkeypatch,
        topics=[
            {"topic": "broken topic", "query": "broken topic", "category": "auto"},
            {"topic": "automotive hmi", "query": "automotive hmi", "category": "auto"},
        ],
        behaviour={
            "broken topic": RuntimeError("searxng exploded"),
            "automotive hmi": _results(5),
        },
    )
    session = _Session(_WORKSPACE)
    counters: dict[str, int] = {}

    items = await run_missing_signals_analysis(
        session,  # type: ignore[arg-type]
        workspace_id="design",
        semantic=_semantic("alpha beta note"),
        stable=[],
        emerging=[],
        counters=counters,
    )

    assert counters["searxng_errors"] == 1
    assert [item["topic"] for item in items] == ["automotive hmi"]
    assert session.inserts()


async def test_counters_balance(monkeypatch: pytest.MonkeyPatch) -> None:
    """topics_generated обязан раскладываться на пять причин без остатка.

    Если инвариант не сходится — значит в цикл добавили `continue` без счётчика,
    ровно так и была построена прежняя немота.
    """
    _patch_pipeline(
        monkeypatch,
        topics=[
            {"topic": "broken topic", "query": "broken topic", "category": "auto"},
            {"topic": "thin topic", "query": "thin topic", "category": "auto"},
            {"topic": "alpha beta", "query": "alpha beta", "category": "auto"},
            {"topic": "automotive hmi", "query": "automotive hmi", "category": "auto"},
        ],
        behaviour={
            "broken topic": RuntimeError("searxng exploded"),
            "thin topic": _results(1),
            "alpha beta": _results(5),
            "automotive hmi": _results(5),
        },
    )
    session = _Session(_WORKSPACE)
    counters: dict[str, int] = {}

    await run_missing_signals_analysis(
        session,  # type: ignore[arg-type]
        workspace_id="design",
        semantic=_semantic(*[f"alpha beta note{index}" for index in range(9)], "other memo"),
        stable=[],
        emerging=[],
        counters=counters,
    )

    assert counters["topics_generated"] == 4
    assert counters["searxng_errors"] == 1
    assert counters["topics_dropped_by_results"] == 1
    assert counters["topics_dropped_by_gap"] == 1
    assert counters["topics_kept"] == 1
    assert counters["topics_generated"] == (
        counters["topics_dropped_by_stoplist"]
        + counters["topics_dropped_by_results"]
        + counters["topics_dropped_by_gap"]
        + counters["searxng_errors"]
        + counters["topics_kept"]
    )


async def test_workspace_own_name_never_reaches_the_output(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ловушка смены знака в пункте 5.

    Тема, целиком состоящая из словаря воркспейса, обязана отсеиваться на пути от
    LLM-генератора: стоп-лист опустошает topic_tokens, и до правки 2026-08-03
    `_frontier_frequency` возвращала для пустого набора 0.0 — «нет покрытия» и
    максимальный gap, то есть название воркспейса становилось первой строкой
    выдачи вместо всегда-отбрасываемой.
    """
    payload = (
        '{"topics": ['
        '{"topic": "design", "query": "design", "category": "x", "why_expected": "y"},'
        '{"topic": "interaction design", "query": "interaction design", "category": "x", "why_expected": "y"},'
        '{"topic": "automotive HMI", "query": "automotive HMI", "category": "x", "why_expected": "y"}'
        "]}"
    )
    _patch_pipeline(
        monkeypatch,
        llm_payload=payload,
        behaviour={
            "design": _results(5),
            "interaction design": _results(5),
            "automotive HMI": _results(5),
        },
    )
    session = _Session(_WORKSPACE)
    counters: dict[str, int] = {}

    items = await run_missing_signals_analysis(
        session,  # type: ignore[arg-type]
        workspace_id="design",
        semantic=_semantic(*[f"design systems note{index}" for index in range(9)], "automotive cockpit"),
        stable=[],
        emerging=[],
        counters=counters,
    )

    topics = [item["topic"] for item in items]
    assert "design" not in topics
    assert "interaction design" not in topics
    assert topics == ["automotive HMI"]
    assert counters["topics_generated"] == 3
    assert counters["topics_dropped_by_stoplist"] == 2
