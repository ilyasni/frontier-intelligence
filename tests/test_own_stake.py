"""Тесты второй оси карточки (ТЗ B): own_stake, квадранты и индексатор корпуса.

Unit-уровень: ни сети, ни Qdrant, ни Postgres, ни эмбеддера. Всё, что ходит наружу,
подменяется на месте — `QdrantFrontierClient` в `_attach_own_stake`, `self.client`
в `own_stake_scores`, роутер в `embed_chunk_text`.

Главный тест здесь — `test_invariant_one_*`: инвариант 1 («own_stake не входит
в score и не влияет на сортировку») — это не оформление, а условие существования
квадранта «низкая релевантность + высокий own_stake». Он проверяется исполняемым
сравнением выдачи с включённой и выключенной осью, а не глазами на ревью.
"""

from __future__ import annotations

import copy
import json
import uuid
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import SimpleNamespace
from typing import Any

import pytest

from mcp.tools import search_frontier as sf
from mcp.tools.search_balanced import _merge_own_corpus
from scripts import index_own_corpus as ioc
from shared.embedding_models import embedding_input_text
from shared.search_contracts import SearchRequest
from worker.integrations.qdrant_client import QdrantFrontierClient, _final_rank_score

pytestmark = pytest.mark.unit


# ─────────────────────────────── тестовые дублёры ─────────────────────────────


class _FakePoint:
    def __init__(self, score: float) -> None:
        self.score = score


class _FakeQueryResponse:
    def __init__(self, scores: list[float]) -> None:
        self.points = [_FakePoint(score) for score in scores]


class _FakeRawClient:
    """Минимальный дублёр AsyncQdrantClient для own_stake_scores."""

    def __init__(self, responses: list[list[float]] | None = None) -> None:
        self._responses = responses or []
        self.batch_calls: list[dict[str, Any]] = []

    async def query_batch_points(self, *, collection_name: str, requests: list[Any]) -> list[Any]:
        self.batch_calls.append({"collection_name": collection_name, "requests": requests})
        return [_FakeQueryResponse(scores) for scores in self._responses]


def _scorer_self(responses: list[list[float]] | None, *, corpus_exists: bool = True) -> Any:
    """Псевдо-self для несвязанного вызова QdrantFrontierClient.own_stake_scores.

    Настоящий конструктор поднимает Settings и AsyncQdrantClient; для проверки
    агрегации нужны ровно три атрибута, поэтому метод вызывается несвязанным.
    """

    async def _exists() -> bool:
        return corpus_exists

    return SimpleNamespace(
        own_corpus_collection="own_corpus__embeddingsgigar__dense_2560",
        client=_FakeRawClient(responses),
        _own_corpus_exists=_exists,
    )


class _FakeAttachClient:
    """Дублёр QdrantFrontierClient для `_attach_own_stake`."""

    own_corpus_collection = "own_corpus__embeddingsgigar__dense_2560"

    def __init__(
        self,
        *,
        vectors: dict[str, list[float]] | None = None,
        scores: dict[str, float] | None = None,
        size: int = 419,
    ) -> None:
        self._vectors = vectors or {}
        self._scores = scores or {}
        self._size = size
        self.closed = False

    async def own_corpus_size(self) -> int:
        return self._size

    async def fetch_documents(self, doc_ids: list[str]) -> list[dict[str, Any]]:
        # Как настоящий: отдаёт point-id, а не doc_id, и молча пропускает ненайденные.
        return [
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)),
                "payload": {"doc_id": doc_id, "post_id": doc_id},
                "vector": self._vectors[doc_id],
            }
            for doc_id in doc_ids
            if doc_id in self._vectors
        ]

    async def own_stake_scores(
        self,
        vectors: dict[str, list[float]],
        *,
        top_k: int,
    ) -> dict[str, float]:
        return {doc_id: self._scores.get(doc_id, 0.0) for doc_id in vectors}

    async def close(self) -> None:
        self.closed = True


class _FakeRouter:
    def __init__(self, dim: int = 4) -> None:
        self.calls: list[dict[str, Any]] = []
        self._dim = dim

    async def embed(self, text: str, *, purpose: str = "document") -> list[float]:
        self.calls.append({"text": text, "purpose": purpose})
        return [0.1] * self._dim


def _hit(doc_id: str, score: float, *, source_score: float = 0.0) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)),
        "score": score,
        "raw_score": score,
        "payload": {"post_id": doc_id, "source_id": "src", "source_score": source_score},
        "score_breakdown": {"semantic": score, "source_score": source_score, "freshness": 0.0},
    }


def _settings(*, relevance_high: float = 0.40, own_stake_high: float = 0.60, top_k: int = 3) -> Any:
    return SimpleNamespace(
        own_stake_enabled=True,
        own_stake_top_k=top_k,
        own_stake_high=own_stake_high,
        relevance_high=relevance_high,
    )


# ───────────────────────── квадранты: границы порогов ─────────────────────────

RELEVANCE_HIGH = 0.40
OWN_STAKE_HIGH = 0.60
EPS = 1e-9


def _quadrant(relevance: float, own_stake: float) -> str:
    return sf._stake_quadrant(
        relevance,
        own_stake,
        relevance_high=RELEVANCE_HIGH,
        own_stake_high=OWN_STAKE_HIGH,
    )


@pytest.mark.parametrize(
    ("relevance", "own_stake", "expected"),
    [
        # Ровно на пороге по обеим осям — «на пороге» считается высоким (>=).
        (RELEVANCE_HIGH, OWN_STAKE_HIGH, "post"),
        (RELEVANCE_HIGH, OWN_STAKE_HIGH - EPS, "run_your_own"),
        (RELEVANCE_HIGH - EPS, OWN_STAKE_HIGH, "personal_blind_spot"),
        (RELEVANCE_HIGH - EPS, OWN_STAKE_HIGH - EPS, "noise"),
        # По одной оси на пороге, по другой заведомо далеко.
        (RELEVANCE_HIGH, 0.0, "run_your_own"),
        (RELEVANCE_HIGH, 1.0, "post"),
        (0.0, OWN_STAKE_HIGH, "personal_blind_spot"),
        (1.0, OWN_STAKE_HIGH, "post"),
        # Чуть выше порога с обеих сторон.
        (RELEVANCE_HIGH + EPS, OWN_STAKE_HIGH + EPS, "post"),
        (RELEVANCE_HIGH + EPS, OWN_STAKE_HIGH - EPS, "run_your_own"),
        (RELEVANCE_HIGH - EPS, OWN_STAKE_HIGH + EPS, "personal_blind_spot"),
    ],
)
def test_quadrant_at_and_around_thresholds(
    relevance: float,
    own_stake: float,
    expected: str,
) -> None:
    assert _quadrant(relevance, own_stake) == expected


def test_quadrant_threshold_is_inclusive_on_both_axes() -> None:
    """Значение ровно на пороге относится к «высоким».

    Соглашение фиксируется тестом намеренно: разница между `>` и `>=` невидима
    на глаз и меняет квадрант ровно у тех карточек, по которым калибруются пороги.
    """
    assert _quadrant(RELEVANCE_HIGH, 0.0) == "run_your_own"
    assert _quadrant(RELEVANCE_HIGH - EPS, 0.0) == "noise"
    assert _quadrant(0.0, OWN_STAKE_HIGH) == "personal_blind_spot"
    assert _quadrant(0.0, OWN_STAKE_HIGH - EPS) == "noise"


def test_all_four_quadrants_are_reachable() -> None:
    """Все четыре имени должны быть достижимы — иначе ось вырождается в флажок."""
    produced = {
        _quadrant(0.9, 0.9),
        _quadrant(0.9, 0.1),
        _quadrant(0.1, 0.9),
        _quadrant(0.1, 0.1),
    }
    assert produced == {"post", "run_your_own", "personal_blind_spot", "noise"}


def test_low_relevance_high_stake_is_its_own_quadrant() -> None:
    """Инвариант 1 сформулирован именно про эту клетку — она обязана быть отдельной."""
    blind_spot = _quadrant(0.05, 0.95)
    assert blind_spot == "personal_blind_spot"
    assert blind_spot != _quadrant(0.05, 0.05)
    assert blind_spot != _quadrant(0.95, 0.95)


# ─────────────────────────── пустой корпус и агрегация ────────────────────────


@pytest.mark.asyncio
async def test_missing_collection_yields_zero_without_raising() -> None:
    """Коллекции ещё нет: own_stake = 0.0, а не исключение и не 500 на поиске."""
    scores = await QdrantFrontierClient.own_stake_scores(
        _scorer_self(None, corpus_exists=False),
        {"doc-a": [0.1, 0.2], "doc-b": [0.3, 0.4]},
        top_k=3,
    )
    assert scores == {"doc-a": 0.0, "doc-b": 0.0}


@pytest.mark.asyncio
async def test_empty_corpus_yields_zero_without_raising() -> None:
    """Коллекция есть, но пустая: соседей нет, счёт 0.0, исключения нет."""
    scores = await QdrantFrontierClient.own_stake_scores(
        _scorer_self([[], []]),
        {"doc-a": [0.1, 0.2], "doc-b": [0.3, 0.4]},
        top_k=3,
    )
    assert scores == {"doc-a": 0.0, "doc-b": 0.0}


@pytest.mark.asyncio
async def test_no_hits_yields_empty_mapping() -> None:
    scores = await QdrantFrontierClient.own_stake_scores(_scorer_self(None), {}, top_k=3)
    assert scores == {}


@pytest.mark.asyncio
async def test_qdrant_failure_degrades_to_none_not_to_zero() -> None:
    """Вторая ось не роняет поиск — но и не выдумывает ноль вместо замера.

    0.0 читается как «корпус проверен, своего замера нет». Когда Qdrant не ответил,
    корпус не проверяли, и разница между этими двумя случаями не косметическая:
    ноль уедет на карточку с degraded=False, а оттуда — снимком own_stake_at_pick
    в append-only card_feedback, из которого калибруются пороги квадрантов.
    """

    class _ExplodingClient:
        async def query_batch_points(self, **kwargs: Any) -> list[Any]:
            raise RuntimeError("qdrant is down")

    async def _exists() -> bool:
        return True

    broken = SimpleNamespace(
        own_corpus_collection="own_corpus__embeddingsgigar__dense_2560",
        client=_ExplodingClient(),
        _own_corpus_exists=_exists,
    )
    scores = await QdrantFrontierClient.own_stake_scores(broken, {"doc-a": [0.1]}, top_k=3)
    assert scores is None


@pytest.mark.asyncio
async def test_short_batch_response_degrades_instead_of_zero_filling() -> None:
    """Ответ короче запроса: выровнять нечем, недостающие doc_id — не нули."""
    # Два вектора, один ответ: zip() молча обрезал бы, оставив doc-b на 0.0.
    scores = await QdrantFrontierClient.own_stake_scores(
        _scorer_self([[0.9]]),
        {"doc-a": [0.1], "doc-b": [0.2]},
        top_k=3,
    )
    assert scores is None


@pytest.mark.asyncio
async def test_attach_reports_degraded_when_scorer_gives_up(monkeypatch) -> None:
    """Сигнал деградации доезжает до ответа: ни ключей на хитах, ни degraded=False."""

    class _DegradingClient(_FakeAttachClient):
        async def own_stake_scores(
            self,
            vectors: dict[str, list[float]],
            *,
            top_k: int,
        ) -> dict[str, float] | None:
            return None

    hits = [_hit("doc-a", 0.62), _hit("doc-b", 0.41)]
    fake = _DegradingClient(vectors={"doc-a": [0.1], "doc-b": [0.2]})
    monkeypatch.setattr(sf, "QdrantFrontierClient", lambda: fake)

    meta = await sf._attach_own_stake(hits, _settings())

    assert meta["degraded"] is True
    assert meta["scored_hits"] == 0
    assert all("own_stake" not in hit for hit in hits)
    assert all("stake_quadrant" not in hit for hit in hits)


@pytest.mark.asyncio
async def test_aggregation_is_max_not_mean() -> None:
    """Случай, где среднее и максимум расходятся, и прав только максимум.

    doc-a: один точный замер автора (0.90) и два случайных касания (0.10, 0.05).
    doc-b: три размытых полусовпадения (0.42, 0.40, 0.38) и ни одного замера.

    Среднее переворачивает картину — 0.35 против 0.40 — и при пороге 0.60
    оба сигнала попадают в «своего замера нет». Правильный ответ ровно обратный:
    по doc-a у автора есть один свой замер, по doc-b — ни одного.
    """
    top_k = 3
    doc_a_neighbours = [0.90, 0.10, 0.05]
    doc_b_neighbours = [0.42, 0.40, 0.38]

    scores = await QdrantFrontierClient.own_stake_scores(
        _scorer_self([doc_a_neighbours, doc_b_neighbours]),
        {"doc-a": [0.1, 0.2], "doc-b": [0.3, 0.4]},
        top_k=top_k,
    )

    assert scores["doc-a"] == 0.90
    assert scores["doc-b"] == 0.42

    mean_a = sum(doc_a_neighbours) / len(doc_a_neighbours)
    mean_b = sum(doc_b_neighbours) / len(doc_b_neighbours)
    assert mean_a < mean_b, "тест бессмыслен, если среднее не расходится с максимумом"

    # Максимум даёт верные квадранты…
    assert _quadrant(0.9, scores["doc-a"]) == "post"
    assert _quadrant(0.9, scores["doc-b"]) == "run_your_own"
    # …а среднее — одинаково неверные для обоих.
    assert _quadrant(0.9, mean_a) == "run_your_own"
    assert _quadrant(0.9, mean_b) == "run_your_own"


@pytest.mark.asyncio
async def test_scores_are_clamped_into_unit_range() -> None:
    """Косинус приходит из [-1, 1]; отрицательный неотличим от отсутствия замера."""
    scores = await QdrantFrontierClient.own_stake_scores(
        _scorer_self([[-0.8, -0.2], [1.4]]),
        {"doc-a": [0.1], "doc-b": [0.2]},
        top_k=2,
    )
    assert scores["doc-a"] == 0.0
    assert scores["doc-b"] == 1.0


# ───────────────────────── инвариант 1: score и порядок ───────────────────────


def _order_and_scores(hits: list[dict[str, Any]]) -> str:
    """Канонический снимок того, что защищает инвариант 1."""
    return json.dumps(
        [{"id": hit["id"], "score": hit["score"]} for hit in hits],
        sort_keys=True,
    )


def _without_new_keys(hits: list[dict[str, Any]]) -> str:
    return json.dumps(
        [
            {key: value for key, value in hit.items() if key not in {"own_stake", "stake_quadrant"}}
            for hit in hits
        ],
        sort_keys=True,
        default=str,
    )


@pytest.mark.asyncio
async def test_invariant_one_attach_changes_neither_order_nor_score(monkeypatch) -> None:
    """Один и тот же список хитов с включённой и выключенной осью.

    Выключенная ось — это просто «`_attach_own_stake` не вызван» (см.
    `run_search_request`), поэтому сравнение «до врезки / после врезки» и есть
    сравнение false/true. Совпадать обязано всё, кроме двух новых ключей.
    """
    hits = [
        _hit("doc-a", 0.62),
        _hit("doc-b", 0.41),
        _hit("doc-c", 0.08),
    ]
    baseline_hits = copy.deepcopy(hits)

    fake = _FakeAttachClient(
        vectors={"doc-a": [0.1, 0.2], "doc-b": [0.3, 0.4], "doc-c": [0.5, 0.6]},
        # Специально «перевёрнутый» порядок: если own_stake хоть как-то попадёт
        # в сортировку, порядок doc-a / doc-b / doc-c обязан развалиться.
        scores={"doc-a": 0.01, "doc-b": 0.55, "doc-c": 0.99},
    )
    monkeypatch.setattr(sf, "QdrantFrontierClient", lambda: fake)

    meta = await sf._attach_own_stake(hits, _settings())

    assert meta["degraded"] is False
    assert _order_and_scores(hits) == _order_and_scores(baseline_hits)
    assert _without_new_keys(hits) == _without_new_keys(baseline_hits)
    assert [hit["id"] for hit in hits] == [hit["id"] for hit in baseline_hits]
    # Новые ключи действительно появились — иначе тест проходил бы вхолостую.
    assert [hit["own_stake"] for hit in hits] == [0.01, 0.55, 0.99]
    assert hits[2]["stake_quadrant"] == "personal_blind_spot"


@pytest.mark.asyncio
async def test_invariant_one_holds_when_own_stake_would_reorder(monkeypatch) -> None:
    """Самый высокий own_stake достаётся самому слабому хиту — он обязан остаться внизу."""
    hits = [_hit("doc-a", 0.9), _hit("doc-b", 0.5), _hit("doc-c", 0.1)]
    fake = _FakeAttachClient(
        vectors={"doc-a": [1.0], "doc-b": [1.0], "doc-c": [1.0]},
        scores={"doc-a": 0.0, "doc-b": 0.2, "doc-c": 1.0},
    )
    monkeypatch.setattr(sf, "QdrantFrontierClient", lambda: fake)

    await sf._attach_own_stake(hits, _settings())

    assert [hit["payload"]["post_id"] for hit in hits] == ["doc-a", "doc-b", "doc-c"]
    assert [hit["score"] for hit in hits] == [0.9, 0.5, 0.1]


class _EndToEndSearchQdrant:
    """Дублёр QdrantFrontierClient на всём пути `run_search_request`.

    `run_search_request` создаёт клиент дважды (сам поиск и `_attach_own_stake`),
    фабрика отдаёт один и тот же экземпляр — так виден и порядок вызовов.
    """

    own_corpus_collection = "own_corpus__embeddingsgigar__dense_2560"

    def __init__(
        self,
        hits: list[dict[str, Any]],
        *,
        stake_scores: dict[str, float],
        size: int = 419,
    ) -> None:
        self._hits = hits
        self._stake_scores = stake_scores
        self._size = size
        self.hybrid_search_calls = 0

    async def hybrid_search(
        self,
        vector: list[float],
        workspace: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.hybrid_search_calls += 1
        # Копия: сортировка в run_search_request правит список на месте.
        return copy.deepcopy(self._hits)

    async def own_corpus_size(self) -> int:
        return self._size

    async def fetch_documents(self, doc_ids: list[str]) -> list[dict[str, Any]]:
        return [
            {
                "id": str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id)),
                "payload": {"doc_id": doc_id, "post_id": doc_id},
                "vector": [0.1, 0.2],
            }
            for doc_id in doc_ids
        ]

    async def own_stake_scores(
        self,
        vectors: dict[str, list[float]],
        *,
        top_k: int,
    ) -> dict[str, float]:
        return {doc_id: self._stake_scores[doc_id] for doc_id in vectors}

    async def close(self) -> None:
        return None


class _ForbiddenSearxng:
    """SearXNG на этом пути не должен вызываться вообще: сеть в unit-тесте — это флаки."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("SearXNG must not be reached from run_search_request in this test")


async def _run_search_end_to_end(
    monkeypatch,
    *,
    own_stake_enabled: bool,
    hits: list[dict[str, Any]],
    stake_scores: dict[str, float],
) -> tuple[dict[str, Any], _EndToEndSearchQdrant]:
    """`run_search_request` целиком, наружу не ходит ничего: эмбеддер, Qdrant, БД, SearXNG."""
    fake = _EndToEndSearchQdrant(hits, stake_scores=stake_scores)

    async def _embedding(query: str, settings: Any) -> list[float]:
        return [0.1, 0.2]

    async def _source_scores(source_ids: set[str]) -> dict[str, dict[str, float]]:
        return {}

    settings = SimpleNamespace(
        gigachat_embeddings_model="EmbeddingsGigaR",
        searxng_enabled=False,
        own_stake_enabled=own_stake_enabled,
        own_stake_top_k=3,
        own_stake_high=OWN_STAKE_HIGH,
        relevance_high=RELEVANCE_HIGH,
    )

    monkeypatch.setattr(sf, "get_settings", lambda: settings)
    monkeypatch.setattr(sf, "_get_embedding", _embedding)
    monkeypatch.setattr(sf, "_load_source_scores", _source_scores)
    monkeypatch.setattr(sf, "QdrantFrontierClient", lambda: fake)
    monkeypatch.setattr(sf, "SearXNGClient", _ForbiddenSearxng)

    response = await sf.run_search_request(
        SearchRequest(query="assisted driving handover", workspace="disruption", limit=10)
    )
    return response, fake


@pytest.mark.asyncio
async def test_invariant_one_run_search_request_sort_key_ignores_own_stake(monkeypatch) -> None:
    """Единственный тест, который держит саму строку `hydrated.sort(...)`.

    Все остальные проверки инварианта 1 дёргают `_attach_own_stake` или
    `_final_rank_score` — ни то, ни другое переставить выдачу не может по устройству,
    так что зелёными они останутся и после того, как ключ сортировки начнут читать
    из own_stake. Здесь прогоняется весь `run_search_request`, и проверяются две вещи
    сразу:

    1) выдача при OWN_STAKE_ENABLED=false и =true совпадает по id и по `score`
       (own_stake не влияет на порядок);
    2) сама выдача упорядочена по `score` по убыванию, а не по порядку из
       `hybrid_search` (ключ сортировки — именно `score`).

    Без пункта 2 подмена ключа на own_stake прошла бы незамеченной: на момент
    сортировки ключа own_stake на хитах ещё нет, `.get(...)` вернул бы всем 0.0,
    и обе выдачи одинаково сохранили бы порядок Qdrant.
    """
    # Порядок из hybrid_search намеренно НЕ отсортирован по score.
    raw_hits = [_hit("doc-b", 0.41), _hit("doc-a", 0.62), _hit("doc-c", 0.08)]
    # own_stake намеренно перевёрнут относительно score: самый слабый хит — самый «свой».
    stake_scores = {"doc-a": 0.01, "doc-b": 0.55, "doc-c": 0.99}

    off, fake_off = await _run_search_end_to_end(
        monkeypatch, own_stake_enabled=False, hits=raw_hits, stake_scores=stake_scores
    )
    on, fake_on = await _run_search_end_to_end(
        monkeypatch, own_stake_enabled=True, hits=raw_hits, stake_scores=stake_scores
    )

    assert fake_off.hybrid_search_calls == 1
    assert fake_on.hybrid_search_calls == 1

    # 1. Ось выключена / включена — id и score побайтово те же.
    assert _order_and_scores(off["results"]) == _order_and_scores(on["results"])
    assert [hit["id"] for hit in off["results"]] == [hit["id"] for hit in on["results"]]
    assert [hit["score"] for hit in off["results"]] == [hit["score"] for hit in on["results"]]

    # 2. Ключ сортировки — score. Это и есть то, что мутация ломает.
    ordered_ids = [hit["payload"]["post_id"] for hit in on["results"]]
    assert ordered_ids == ["doc-a", "doc-b", "doc-c"]
    assert [hit["score"] for hit in on["results"]] == [0.62, 0.41, 0.08]
    incoming_ids = [hit["payload"]["post_id"] for hit in raw_hits]
    assert ordered_ids != incoming_ids, "тест вхолостую: hybrid_search уже отдал нужный порядок"

    # Ось действительно работала — иначе пункт 1 сравнивал бы выдачу с самой собой.
    assert "own_corpus" not in off
    assert on["own_corpus"]["degraded"] is False
    assert on["own_corpus"]["scored_hits"] == 3
    assert [hit["own_stake"] for hit in on["results"]] == [0.01, 0.55, 0.99]
    assert all("own_stake" not in hit for hit in off["results"])
    # Тот самый квадрант, ради которого инвариант 1 и существует.
    assert on["results"][2]["stake_quadrant"] == "personal_blind_spot"


def test_own_stake_in_payload_cannot_reach_the_rank_formula() -> None:
    """Вторая половина инварианта 1 — на уровне самой формулы ранжирования."""
    payload = {"source_score": 0.5, "published_at": "2020-01-01T00:00:00+00:00"}
    plain, plain_breakdown = _final_rank_score(0.8, dict(payload))
    with_stake, stake_breakdown = _final_rank_score(
        0.8,
        {**payload, "own_stake": 0.99, "stake_quadrant": "post"},
    )

    assert plain == with_stake
    assert plain_breakdown == stake_breakdown
    assert "own_stake" not in stake_breakdown


@pytest.mark.asyncio
async def test_attach_reports_empty_corpus_as_zero_not_exception(monkeypatch) -> None:
    """Пустой корпус: own_stake = 0.0 на каждом хите, ответ не деградирован."""
    hits = [_hit("doc-a", 0.5)]
    fake = _FakeAttachClient(vectors={"doc-a": [0.1]}, scores={}, size=0)
    monkeypatch.setattr(sf, "QdrantFrontierClient", lambda: fake)

    meta = await sf._attach_own_stake(hits, _settings())

    assert meta["degraded"] is False
    assert meta["size"] == 0
    assert hits[0]["own_stake"] == 0.0
    assert hits[0]["stake_quadrant"] == "run_your_own"


@pytest.mark.asyncio
async def test_attach_marks_unmeasured_hits_as_none(monkeypatch) -> None:
    """Вектор хита не достался — это «не измеряли», а не «замера нет»."""
    hits = [_hit("doc-a", 0.5), _hit("doc-b", 0.4)]
    fake = _FakeAttachClient(vectors={"doc-a": [0.1]}, scores={"doc-a": 0.8})
    monkeypatch.setattr(sf, "QdrantFrontierClient", lambda: fake)

    meta = await sf._attach_own_stake(hits, _settings())

    assert hits[0]["own_stake"] == 0.8
    assert hits[1]["own_stake"] is None
    assert hits[1]["stake_quadrant"] is None
    assert meta["scored_hits"] == 1


# ──────────────── own_corpus на пути search_balanced (три подзапроса) ─────────


def test_balanced_merge_keeps_size_and_sums_scored_hits() -> None:
    """Размер корпуса и пороги обязаны доехать до карточки вместе с числом.

    Хит уносит own_stake сам, а `size` живёт на уровне ответа: без него 0.71 по
    корпусу в полсотни чанков неотличим от замера по зрелому корпусу (риск 1 ТЗ B).
    """
    thresholds = {"own_stake_high": 0.60, "relevance_high": 0.40}
    merged = _merge_own_corpus(
        {"size": 47, "top_k": 3, "scored_hits": 10, "thresholds": thresholds, "degraded": False},
        {"size": 47, "top_k": 3, "scored_hits": 4, "thresholds": thresholds, "degraded": False},
        {"size": 47, "top_k": 3, "scored_hits": 2, "thresholds": thresholds, "degraded": False},
    )

    assert merged == {
        "size": 47,
        "top_k": 3,
        "scored_hits": 16,
        "thresholds": thresholds,
        "degraded": False,
    }


def test_balanced_merge_degrades_if_any_subsearch_degraded() -> None:
    """Частично деградировавший ответ не имеет права выглядеть чистым."""
    merged = _merge_own_corpus(
        {"size": 47, "top_k": 3, "scored_hits": 10, "thresholds": {}, "degraded": False},
        {"size": None, "top_k": 3, "scored_hits": 0, "thresholds": {}, "degraded": True},
    )

    assert merged["degraded"] is True
    # Измеренный размер не теряется из-за соседа, который до него не дошёл.
    assert merged["size"] == 47


def test_balanced_merge_is_absent_when_axis_is_off() -> None:
    """OWN_STAKE_ENABLED=false — ключа в ответе нет, сравнение выдач остаётся честным."""
    assert _merge_own_corpus() is None


# ────────────────────── ассерт purpose="document" при эмбеддинге ──────────────


def test_wrong_purpose_is_refused() -> None:
    with pytest.raises(ValueError) as excinfo:
        ioc.assert_document_purpose("query", "EmbeddingsGigaR")
    assert "document" in str(excinfo.value)

    for wrong in ("query", "passage", "doc", "", "Document"):
        with pytest.raises(ValueError):
            ioc.assert_document_purpose(wrong, "EmbeddingsGigaR")


def test_document_purpose_is_accepted() -> None:
    ioc.assert_document_purpose("document", "EmbeddingsGigaR")
    ioc.assert_document_purpose("document", "Embeddings")  # модель без префиксов
    ioc.assert_document_purpose("document", "unknown-model")


def test_wrong_purpose_produces_no_error_downstream() -> None:
    """Почему ассерт вообще нужен: перепутанный purpose нигде не бросает.

    `embedding_input_text` на неизвестном purpose молча отдаёт текст без префикса,
    а на "query" — с чужим префиксом. Оба варианта дают внешне правдоподобный вектор.
    """
    model = "EmbeddingsGigaR"
    assert embedding_input_text(model, "x", purpose="document") == "search_document: x"
    assert embedding_input_text(model, "x", purpose="query") == "search_query: x"
    assert embedding_input_text(model, "x", purpose="passage") == "x"


@pytest.mark.asyncio
async def test_embed_helper_asserts_on_every_call() -> None:
    router = _FakeRouter(dim=4)

    vector = await ioc.embed_chunk_text(router, "chunk", model="EmbeddingsGigaR", expected_dim=4)

    assert vector == [0.1, 0.1, 0.1, 0.1]
    assert router.calls == [{"text": "chunk", "purpose": "document"}]


@pytest.mark.asyncio
async def test_embed_helper_refuses_before_spending_a_token() -> None:
    router = _FakeRouter()
    with pytest.raises(ValueError):
        await ioc.embed_chunk_text(router, "chunk", model="EmbeddingsGigaR", purpose="query")
    assert router.calls == [], "провайдер не должен быть вызван при неверном purpose"


@pytest.mark.asyncio
async def test_embed_helper_refuses_dimension_drift() -> None:
    router = _FakeRouter(dim=8)
    with pytest.raises(RuntimeError) as excinfo:
        await ioc.embed_chunk_text(router, "chunk", model="EmbeddingsGigaR", expected_dim=2560)
    assert "dimension" in str(excinfo.value)


def test_embed_purpose_constant_is_document() -> None:
    assert ioc.EMBED_PURPOSE == "document"


# ──────────────────────── исключения индексатора корпуса ──────────────────────


#: Один и тот же путь под `secrets/` в четырёх записях. Предохранитель обязан
#: срабатывать на каждой — и на Windows, и на POSIX.
SECRET_PATH_SPELLINGS: tuple[tuple[str, str], ...] = (
    ("windows", r"D:\telegramChanel\secrets\credentials.md"),
    ("posix", "/home/ilya/telegramChanel/secrets/credentials.md"),
    ("mixed", r"D:\telegramChanel/secrets\credentials.md"),
    ("case", r"D:\telegramChanel\Secrets\credentials.md"),
)


@pytest.mark.parametrize(("spelling", "raw"), SECRET_PATH_SPELLINGS)
@pytest.mark.parametrize("flavour", [Path, PurePosixPath, PureWindowsPath, str])
def test_secrets_path_is_refused_outright(spelling: str, raw: str, flavour: Any) -> None:
    """Отказ от `secrets/` не имеет права зависеть от ОС интерпретатора.

    `PurePath.parts` разбирает путь по правилам ТЕКУЩЕЙ платформы: под POSIX
    `D:\\telegramChanel\\secrets\\credentials.md` — один неделимый компонент, и старая
    реализация на этой строке молча отвечала False. Тест раньше проходил только потому,
    что запускался под Windows; `PurePosixPath` в списке flavour'ов — это и есть Linux
    в CI, воспроизведённый без Linux.
    """
    secret = flavour(raw)
    assert ioc.is_secret_path(secret) is True, f"{spelling} spelling slipped past the guard"
    with pytest.raises(ioc.SecretsPathRefused):
        ioc.assert_not_secret_path(secret)


@pytest.mark.parametrize("flavour", [Path, PurePosixPath, PureWindowsPath, str])
def test_ordinary_paths_are_not_mistaken_for_secrets(flavour: Any) -> None:
    """Обратная сторона: предохранитель не должен глотать нормальный корпус."""
    for raw in (
        r"D:\telegramChanel\content\inbox.md",
        "/home/ilya/telegramChanel/content/inbox.md",
        r"D:\Workspace\frontier-intelligence\docs\README.md",
    ):
        assert ioc.is_secret_path(flavour(raw)) is False
        ioc.assert_not_secret_path(flavour(raw))


@pytest.mark.parametrize(
    "name",
    [".env", "id_rsa", "credentials.md", "SECRETS.md", "analysis-raw.json", "app.session"],
)
def test_filename_globs_hold_on_a_full_path_in_either_separator(name: str) -> None:
    """Второй контур — тот же класс ошибки: имя может приехать целым путём.

    `fnmatch("/opt/repo/id_rsa", "id_rsa*")` — False, и файл ключа прошёл бы отбор.
    Проверка идёт по последнему компоненту, разобранному по обоим разделителям.
    """
    assert ioc.is_excluded_filename(name) is True
    assert ioc.is_excluded_filename(f"/opt/repo/{name}") is True
    assert ioc.is_excluded_filename(rf"D:\repo\{name}") is True
    assert ioc.is_excluded_filename(rf"D:\repo/sub\{name}") is True
    # Обычный файл корпуса от этого не пострадал.
    assert ioc.is_excluded_filename("/opt/repo/notes.md") is False


def test_secrets_directory_is_pruned_during_the_walk(tmp_path: Path) -> None:
    root = tmp_path / "channel"
    (root / "secrets").mkdir(parents=True)
    (root / "content").mkdir(parents=True)
    (root / "secrets" / "notes.md").write_text("x" * 500, encoding="utf-8")
    (root / "content" / "inbox.md").write_text("- " + "y" * 500, encoding="utf-8")

    source = ioc.CorpusSource(
        slug="channel",
        root=root,
        default_doc_kind="channel_analytics",
        default_tier=1,
        channel_layout=True,
    )
    skips = ioc.SkipCounters()
    collected = ioc.collect_files([source], skips=skips)

    paths = {entry.corpus_path for entry in collected}
    assert paths == {"channel/content/inbox.md"}
    assert not any("secret" in path for path in paths)
    assert skips.by_reason.get("dir_pruned", 0) >= 1


def test_tripwire_fires_if_the_prune_ever_regresses(tmp_path: Path, monkeypatch) -> None:
    """Если обход однажды перестанет вырезать secrets/, скрипт обязан ОТКАЗАТЬСЯ.

    Проверяется именно предохранитель, а не документация: prune отключается, и
    единственное, что стоит между корпусом и credentials.md, — assert_not_secret_path.
    """
    root = tmp_path / "channel"
    (root / "secrets").mkdir(parents=True)
    (root / "secrets" / "notes.md").write_text("x" * 500, encoding="utf-8")

    monkeypatch.setattr(ioc, "is_excluded_dir", lambda name: False)
    monkeypatch.setattr(ioc, "is_excluded_filename", lambda name: False)

    source = ioc.CorpusSource(
        slug="channel",
        root=root,
        default_doc_kind="channel_analytics",
        default_tier=1,
        channel_layout=True,
    )
    with pytest.raises(ioc.SecretsPathRefused):
        ioc.collect_files([source], skips=ioc.SkipCounters())


def test_raw_json_is_skipped() -> None:
    for name in ("analysis-raw.json", "brand-raw.json", "frontier-spec-raw.json"):
        assert ioc.is_indexable_suffix(name) is False
        assert ioc.is_excluded_filename(name) is True


def test_secret_shaped_filenames_are_skipped_even_as_markdown() -> None:
    """Именной глоб несёт нагрузку: SECRETS.md — настоящий .md, allowlist его пропустит."""
    for name in ("SECRETS.md", "credentials.md", "API_TOKEN.md", "db-password.md"):
        assert ioc.is_indexable_suffix(name) is True
        assert ioc.is_excluded_filename(name) is True


def test_env_and_key_files_are_skipped() -> None:
    for name in (".env", ".env.example", ".env.production", "server.pem", "id_rsa", "app.session"):
        assert ioc.is_excluded_filename(name) is True


def test_infrastructure_directories_are_excluded() -> None:
    excluded = ("node_modules", "vendor", "dist", "build", ".git", ".github", ".claude", "secrets")
    for name in excluded:
        assert ioc.is_excluded_dir(name) is True
    for name in ("docs", "content", "analytics", "channel"):
        assert ioc.is_excluded_dir(name) is False


def test_raw_json_next_to_markdown_is_dropped_but_markdown_survives(tmp_path: Path) -> None:
    root = tmp_path / "channel"
    (root / "analytics").mkdir(parents=True)
    (root / "analytics" / "brand.md").write_text("# Brand\n\n" + "z" * 600, encoding="utf-8")
    (root / "analytics" / "patterns-raw.json").write_text('{"a": 1}', encoding="utf-8")

    source = ioc.CorpusSource(
        slug="channel",
        root=root,
        default_doc_kind="channel_analytics",
        default_tier=1,
        channel_layout=True,
    )
    collected = ioc.collect_files([source], skips=ioc.SkipCounters())
    assert [entry.corpus_path for entry in collected] == ["channel/analytics/brand.md"]
    assert collected[0].doc_kind == "channel_analytics"


def test_secret_content_guard_skips_a_markdown_file(tmp_path: Path) -> None:
    entry = ioc.CorpusFile(
        source_slug="repo",
        path=tmp_path / "README.md",
        corpus_path="repo/README.md",
        doc_kind="repo_readme",
        tier=1,
    )
    entry.path.write_text("api_key = " + "A" * 32 + "\n", encoding="utf-8")
    skips = ioc.SkipCounters()

    assert ioc.read_corpus_file(entry, skips) is None
    assert skips.by_reason.get("secret_content") == 1


def test_older_version_families_are_dropped() -> None:
    paths = [
        Path("/repo/pricing-strategy.md"),
        Path("/repo/pricing-strategy-v2.md"),
        Path("/repo/pricing-strategy-v3.md"),
        Path("/repo/icp-profiles.md"),
        Path("/repo/standalone.md"),
    ]
    kept, dropped = ioc.prune_version_families(paths)
    kept_names = {path.name for path in kept}
    dropped_names = {path.name for path in dropped}

    assert kept_names == {"pricing-strategy-v3.md", "icp-profiles.md", "standalone.md"}
    assert dropped_names == {"pricing-strategy.md", "pricing-strategy-v2.md"}


# ──────────────────────────── идемпотентность точек ───────────────────────────


def test_point_id_is_stable_for_the_same_path_and_index() -> None:
    first = ioc.own_corpus_point_id("channel/content/inbox.md", 3)
    second = ioc.own_corpus_point_id("channel/content/inbox.md", 3)
    assert first == second
    uuid.UUID(first)  # это валидный uuid, а не произвольная строка


def test_point_id_differs_per_chunk_index_and_path() -> None:
    base = ioc.own_corpus_point_id("channel/content/inbox.md", 3)
    assert base != ioc.own_corpus_point_id("channel/content/inbox.md", 4)
    assert base != ioc.own_corpus_point_id("channel/content/ideas.md", 3)


def test_point_id_matches_the_qdrant_client_formula() -> None:
    """Скрипт и клиент обязаны считать один и тот же id.

    Разойдись формулы — повторный прогон не перезаписывал бы точки, а дублировал их,
    и «повторный прогон не меняет N» перестал бы что-либо значить.
    """
    path, chunk_idx = "channel/content/inbox.md", 7
    expected = str(uuid.uuid5(uuid.NAMESPACE_URL, f"own:{path}:{chunk_idx}"))
    assert ioc.own_corpus_point_id(path, chunk_idx) == expected

    chunk = ioc.Chunk(
        corpus_path=path,
        chunk_idx=chunk_idx,
        doc_kind="inbox",
        tier=1,
        source_slug="channel",
        heading="",
        text="body",
    )
    assert chunk.point_id == expected


def test_chunk_payload_carries_the_spec_contract_and_no_workspace_id() -> None:
    chunk = ioc.Chunk(
        corpus_path="channel/content/inbox.md",
        chunk_idx=0,
        doc_kind="inbox",
        tier=1,
        source_slug="channel",
        heading="Inbox",
        text="body",
    )
    payload = chunk.payload("2026-08-03T00:00:00+00:00")

    assert {"doc_kind", "path", "chunk_idx", "indexed_at"} <= set(payload)
    assert payload["doc_kind"] == "inbox"
    assert payload["path"] == "channel/content/inbox.md"
    assert payload["chunk_idx"] == 0
    # Инвариант 2: корпус живёт вне общего поиска, workspace_id ему не положен.
    assert "workspace_id" not in payload


# ──────────────────────────────── нарезка текста ──────────────────────────────


def test_long_bullet_becomes_its_own_chunk() -> None:
    bullet_a = "- " + "a" * 500
    bullet_b = "- " + "b" * 500
    chunks = ioc.chunk_markdown(f"# Inbox\n\n{bullet_a}\n{bullet_b}\n")
    bodies = [body for _, body in chunks]

    assert len(bodies) == 2
    assert bodies[0].startswith("- aaa")
    assert bodies[1].startswith("- bbb")


def test_headings_become_a_breadcrumb_on_every_chunk() -> None:
    text = "# Top\n\n## Section\n\n" + ("word " * 400)
    chunks = ioc.chunk_markdown(text)
    assert chunks
    assert all(heading == "Top > Section" for heading, _ in chunks)


def test_no_chunk_exceeds_the_hard_cap_and_none_is_a_stub() -> None:
    text = "# H1\n\n" + "\n\n".join(f"Sentence number {index}. " * 30 for index in range(40))
    chunks = ioc.chunk_markdown(text)

    assert chunks
    for _, body in chunks:
        assert len(body) <= ioc.CHUNK_MERGE_MAX_CHARS
        assert len(body) >= ioc.CHUNK_DROP_CHARS


def test_oversized_block_is_split_not_truncated() -> None:
    sentences = " ".join(f"Sentence number {index} carries its own words." for index in range(200))
    pieces = ioc.split_oversized(sentences)

    assert len(pieces) > 1
    assert all(len(piece) <= ioc.CHUNK_MAX_CHARS for piece in pieces)
    # Ничего не потеряно: усечение молча удалило бы авторский текст.
    rejoined = " ".join(pieces).split()
    assert rejoined == sentences.split()


def test_tiny_file_yields_one_clean_chunk() -> None:
    text = "# Subscribers\n\n" + "The metric is defined as weekly active readers. " * 4
    chunks = ioc.chunk_markdown(text)
    assert len(chunks) == 1
    assert chunks[0][0] == "Subscribers"


# ───────────────────────── коллекция и раскладка корпуса ──────────────────────


def test_resolved_collection_is_versioned_and_never_the_shared_index() -> None:
    assert (
        ioc.resolve_collection_name("own_corpus", "EmbeddingsGigaR")
        == "own_corpus__embeddingsgigar__dense_2560"
    )
    # Пустая база не должна схлопываться в frontier_docs — это утечка корпуса
    # в общий индекс, а не косметика.
    assert ioc.resolve_collection_name("", "EmbeddingsGigaR").startswith("own_corpus__")


def test_channel_layout_maps_files_to_doc_kinds() -> None:
    cases = {
        "content/inbox.md": ("inbox", 1),
        "content/ideas.md": ("idea", 1),
        "content/published/2026-01-post.md": ("published", 1),
        "content/drafts/wip.md": ("draft", 1),
        "channel/anketa.md": ("author_profile", 1),
        "channel/profile.md": ("author_profile", 1),
        "analytics/brand.md": ("channel_analytics", 1),
        "CLAUDE.md": ("repo_readme", 1),
    }
    for relative, expected in cases.items():
        assert ioc.resolve_channel_doc_kind(PurePosixPath(relative)) == expected


def test_dry_run_report_reports_the_stability_floor(tmp_path: Path) -> None:
    root = tmp_path / "channel"
    (root / "content").mkdir(parents=True)
    (root / "content" / "inbox.md").write_text(
        "# Inbox\n\n" + "\n\n".join("- " + "x" * 500 for _ in range(3)),
        encoding="utf-8",
    )
    source = ioc.CorpusSource(
        slug="channel",
        root=root,
        default_doc_kind="channel_analytics",
        default_tier=1,
        channel_layout=True,
    )
    skips = ioc.SkipCounters()
    files = ioc.collect_files([source], skips=skips)
    planned = [
        (entry, ioc.chunk_corpus_file(entry, entry.path.read_text(encoding="utf-8")))
        for entry in files
    ]

    report = ioc.build_plan_report(
        planned,
        skips=skips,
        sources=[source],
        collection="own_corpus__x",
    )

    assert report["totals"]["files"] == 1
    assert report["by_doc_kind"]["inbox"]["chunks"] == report["totals"]["chunks"]
    assert report["spec_stability_floor_chunks"] == 60
    assert report["clears_stability_floor"] is (report["totals"]["chunks"] >= 60)
