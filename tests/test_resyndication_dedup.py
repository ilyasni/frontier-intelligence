"""
Схлопывание ре-синдикации в выдаче поиска (пункт 28).

Замер 05.08.2026 на живой базе: за 30 дней в disruption 11 220 постов из 47 301
(23.7%) лежат в 4779 группах с одинаковым содержимым и РАЗНЫМИ source_id.
В реальной выдаче это 10-13% топ-30 — три запроса дали 26-27 различных
canonical-url на 30 хитов. Все пары: разный post_id, разный source_id,
одинаковый canonical url.

Для модели-потребителя это хуже шума: повтор читается как независимое
подтверждение, и уверенность растёт там, где первоисточник один.

Две ловушки, каждая со своим тестом:

  * схлопывание ПОСЛЕ среза по limit молча недодаёт клиенту (limit=10 → 7-9);
  * схлопывание ПОСЛЕ сборки промпта оставляет модель считать копии за
    подтверждения, и тесты на составе выдачи при этом зелёные.
"""

from __future__ import annotations

import pytest

from mcp.tools.search_frontier import (
    _collapse_resyndication,
    _dedup_key,
    _resyndication_dedup_enabled,
)

pytestmark = pytest.mark.unit


def _hit(score: float, url: str | None, post_id: str, source_id: str) -> dict:
    payload = {"post_id": post_id, "source_id": source_id}
    if url is not None:
        payload["url"] = url
    return {"score": score, "payload": payload}


# ── Ключ схлопывания ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("url", "collapses", "why"),
    [
        ("https://example.com/post", True, "обычная статья"),
        ("https://example.com/post?utm_source=rss", True, "трекинг снимается canonical_url"),
        ("https://m.example.com/post", True, "мобильный префикс снимается"),
        ("https://example.com/", False, "голый хост — под него подпадают разные материалы"),
        ("https://example.com", False, "голый хост без слэша"),
        ("not-a-url", False, "не разбирается как URL"),
        (None, False, "url в payload отсутствует"),
    ],
)
def test_dedup_key_selection(url, collapses, why) -> None:
    key = _dedup_key(_hit(0.5, url, "p", "s"))
    assert (key is not None) is collapses, why


def test_hits_without_key_never_collapse_together() -> None:
    """Иначе все хиты без url слиплись бы в один — потеря вместо дедупа."""
    hits = [
        _hit(0.9, None, "p1", "s1"),
        _hit(0.8, None, "p2", "s2"),
        _hit(0.7, "https://example.com/", "p3", "s3"),
    ]
    kept, meta = _collapse_resyndication(hits)
    assert len(kept) == 3
    assert meta["collapsed"] == 0


# ── Схлопывание ──────────────────────────────────────────────────────────────


def test_reprints_collapse_to_the_highest_score() -> None:
    """Представителем становится хит с максимальным score — список уже отсортирован."""
    hits = [
        _hit(0.9, "https://arxiv.org/abs/2607.18637", "p1", "rss_arxiv_cs_ai"),
        _hit(0.5, "https://arxiv.org/abs/2607.18637?utm_source=rss", "p2", "rss_arxiv_cs_ro"),
        _hit(0.4, "https://other.example/post", "p3", "s3"),
    ]
    kept, meta = _collapse_resyndication(hits)

    assert [h["score"] for h in kept] == [0.9, 0.4], "порядок по score обязан сохраниться"
    assert kept[0]["echo_count"] == 2
    assert kept[0]["echo_source_ids"] == ["rss_arxiv_cs_ai", "rss_arxiv_cs_ro"]
    assert kept[0]["echoes"][0]["post_id"] == "p2"
    assert meta == {"raw_hits": 3, "kept": 2, "collapsed": 1, "groups": 1}


def test_unique_hits_are_marked_as_single() -> None:
    """`echo_count = 1` должен стоять у всех, иначе клиент не отличит «уникален»
    от «поле не проставлено»."""
    kept, meta = _collapse_resyndication(
        [_hit(0.9, "https://a.example/x", "p1", "s1"), _hit(0.8, "https://b.example/y", "p2", "s2")]
    )
    assert all(h["echo_count"] == 1 for h in kept)
    assert all(h["echoes"] == [] for h in kept)
    assert meta["groups"] == 0


def test_different_paths_on_one_host_stay_apart() -> None:
    """Защита от пересхлопывания: один сайт — не один материал."""
    kept, _ = _collapse_resyndication(
        [
            _hit(0.9, "https://tass.ru/ekonomika/27905741", "p1", "rss_tass_transport"),
            _hit(0.8, "https://tass.ru/ekonomika/27905999", "p2", "rss_tass_it"),
        ]
    )
    assert len(kept) == 2


def test_same_article_across_sources_collapses() -> None:
    """Ровно тот случай, ради которого всё делается."""
    kept, meta = _collapse_resyndication(
        [
            _hit(0.57, "https://www.datagubbe.se/scenegui", "p1", "api_hn_topstories"),
            _hit(0.38, "https://www.datagubbe.se/scenegui", "p2", "api_hn_beststories"),
        ]
    )
    assert len(kept) == 1
    assert kept[0]["echo_count"] == 2
    assert meta["collapsed"] == 1


# ── Место врезки: до среза и до всех потребителей ────────────────────────────


def test_collapse_happens_before_the_limit_slice_and_before_consumers() -> None:
    """Главный инвариант, и он статический — иначе тесты зеленеют, а цель не достигнута.

    Схлопывание после `[:limit]` молча недодаёт клиенту. Схлопывание после
    `_synthesize_results` оставляет модель считать копии за подтверждения, и
    проверка состава выдачи этого не увидит.
    """
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "mcp" / "tools" / "search_frontier.py"
    ).read_text(encoding="utf-8")
    body = source[source.find("async def run_search_request("):]

    pos_collapse = body.find("_collapse_resyndication(hydrated)")
    pos_slice = body.find("hydrated[:effective_limit]")
    pos_own_stake = body.find("_attach_own_stake(hydrated")
    pos_synth = body.find("_synthesize_results(req, hydrated")
    pos_entity = body.find("entity_evidence(hydrated")

    assert pos_collapse > 0, "схлопывание не вызывается в run_search_request"
    for name, pos in (
        ("срез по limit", pos_slice),
        ("own_stake", pos_own_stake),
        ("синтез", pos_synth),
        ("entity_evidence", pos_entity),
    ):
        assert pos > 0, f"не найден потребитель: {name}"
        assert pos_collapse < pos, (
            f"схлопывание стоит ПОСЛЕ «{name}» — значит {name} видит перепечатки "
            "как отдельные подтверждения"
        )


def test_overfetch_is_wired_into_the_search_call() -> None:
    """Без над-выборки дедуп превращает limit=10 в 7-9 молча."""
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "mcp" / "tools" / "search_frontier.py"
    ).read_text(encoding="utf-8")
    assert "limit=fetch_limit" in source, (
        "hybrid_search по-прежнему зовётся с effective_limit: после схлопывания "
        "выдача окажется короче запрошенной, и клиент об этом не узнает"
    )
    assert "_DEDUP_OVERFETCH" in source and "_DEDUP_MAX_FETCH" in source


def test_dedup_can_be_switched_off_without_a_config_field() -> None:
    """getattr, а не settings.field: образ mcp может уехать вперёд конфига."""

    class _NoField:
        pass

    class _Off:
        search_resyndication_dedup_enabled = False

    assert _resyndication_dedup_enabled(_NoField()) is True
    assert _resyndication_dedup_enabled(_Off()) is False
