"""
Провенанс доезжает до брифа, и «не измерено» не превращается в нули (пункт 25).

`_compact_workspace` вырезал провенанс whitelist'ом, поэтому синтезатор оценивал
силу сигнала, не зная, стоят ли за ним независимые источники или одна перепечатка
в пяти местах. Слой был построен целиком (`shared/provenance.py`, колонки в трёх
таблицах, расчёт с 02.08.2026) и не доходил ни до одной поверхности принятия
решений.

Ключевое требование — отдавать `None`, а не словарь нулей. На 05.08.2026 измерены
48 трендов из 404: словарь нулей отправил бы синтезатору 88% выдуманных измерений,
и модель приняла бы их за факт «источники полностью зависимы». Это ровно та ложь,
которую пункт 26 убрал на уровне отдачи, и она не должна вернуться через бриф.
"""

from __future__ import annotations

import pytest

from mcp.tools.frontier_brief import _compact_workspace, _provenance_block

pytestmark = pytest.mark.unit


def _measured_item() -> dict:
    return {
        "id": "trend:1",
        "title": "EV charging standards converge",
        "signal_stage": "stable",
        "signal_score": 0.71,
        "burst_score": 0.3,
        "keywords": ["ev", "charging"],
        "provenance_measured": True,
        "independence_score": 0.65,
        "deduped_source_count": 2,
        "distinct_originators": 2,
        "echo_ratio": 0.5,
    }


def _unmeasured_item() -> dict:
    """Как отдаёт observability после пункта 26: флаг False, значения null."""
    return {
        "id": "trend:old",
        "title": "Older trend",
        "signal_stage": "stable",
        "signal_score": 0.4,
        "burst_score": 0.1,
        "keywords": [],
        "provenance_measured": False,
        "independence_score": None,
        "deduped_source_count": None,
        "distinct_originators": None,
        "echo_ratio": None,
    }


def test_measured_signal_carries_the_block() -> None:
    block = _provenance_block(_measured_item())
    assert block == {
        "independence_score": 0.65,
        "deduped_source_count": 2,
        "distinct_originators": 2,
        "echo_ratio": 0.5,
    }


def test_unmeasured_signal_gets_no_block_at_all() -> None:
    """None, а не словарь нулей — иначе 88% выдачи станут выдуманными измерениями."""
    assert _provenance_block(_unmeasured_item()) is None


def test_absent_flag_is_treated_as_not_measured() -> None:
    """Строка из старого кэша или из ручки, не прошедшей _mark_provenance."""
    assert _provenance_block({"id": "x", "independence_score": 0.9}) is None


def test_compact_workspace_passes_provenance_for_both_blocks() -> None:
    payload = {
        "workspace": "disruption",
        "clusters": {
            "trends": [_measured_item(), _unmeasured_item()],
            "emerging": [_measured_item()],
        },
    }
    out = _compact_workspace(payload)

    assert out["trends"][0]["provenance"]["independence_score"] == 0.65
    assert out["trends"][1]["provenance"] is None, (
        "неизмеренный тренд получил блок — синтезатор примет нули за факт"
    )
    assert out["emerging"][0]["provenance"]["echo_ratio"] == 0.5, (
        "emerging остался без провенанса: whitelist вырезает его так же, как у трендов"
    )


def test_compact_workspace_keeps_its_previous_shape() -> None:
    """Остальные поля брифа не должны поехать вместе с правкой."""
    out = _compact_workspace(
        {"workspace": "design", "summary": {"posts": 1}, "clusters": {"trends": [_measured_item()]}}
    )
    trend = out["trends"][0]
    assert trend["id"] == "trend:1"
    assert trend["signal_score"] == 0.71
    assert trend["keywords"] == ["ev", "charging"]
    assert out["workspace"] == "design"
    assert out["summary"] == {"posts": 1}


def test_prompt_explains_how_to_read_provenance() -> None:
    """Блок без инструкции читается как обычная метрика.

    Низкий `independence_score` модель приняла бы за слабый сигнал, а это разные
    вещи: слабый сигнал — мало материала; низкая независимость — много материала
    из одного первоисточника. Ещё важнее сказать, что ОТСУТСТВИЕ блока означает
    «не считали», а не «источники зависимы».
    """
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "mcp" / "tools" / "frontier_brief.py"
    ).read_text(encoding="utf-8")
    prompt_area = source[source.find("Return JSON with keys"):]
    for fragment in ("provenance", "echo_ratio", "re-syndication", "never computed"):
        assert fragment in prompt_area, (
            f"в промпте синтеза нет объяснения про {fragment!r}: модель получит "
            "числа без указания, как их читать"
        )
