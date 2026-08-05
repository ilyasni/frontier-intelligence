"""
«Не измерено» перестаёт выглядеть как «полностью синдицировано» (пункт 26).

Колонки провенанса заведены миграцией с `DEFAULT 0.0`, а считаться начали
02.08.2026. Замер 05.08.2026: trend_clusters 48 измеренных из 404,
emerging_signals 7368 из 34039, semantic_clusters 5721 из 51427. Остальное
уезжало клиенту как `independence_score = 0.0` — то есть «источники полностью
зависимы, сплошная перепечатка», ровно противоположно истине «мы не считали».
У `ai_trends` и `design` неизмеренными были ВСЕ trend_clusters: сто процентов
фальшивых нулей.

Правка стоит в одной точке (`_fetch_rows` / `_fetch_one`), а не в местах отдачи.
Реестр насчитал семь таких мест — это только те, где имя колонки набрано
буквально; ещё шесть путей отдают строку целиком через `SELECT *`. Тест ниже
держит именно это: правка обязана работать и для `SELECT *`-ручек, иначе
завтрашняя новая ручка добавит четырнадцатое место.
"""

from __future__ import annotations

import pytest

from mcp.tools.observability import _mark_provenance, _provenance_measured

pytestmark = pytest.mark.unit


def _measured_row() -> dict:
    return {
        "id": "trend:abc",
        "workspace_id": "disruption",
        "deduped_source_count": 3,
        "distinct_voices": 3,
        "echo_ratio": 0.25,
        "arrival_dispersion": 0.8,
        "distinct_originators": 2,
        "independence_score": 0.61,
    }


def _unmeasured_row() -> dict:
    """Строка, как её отдаёт БД до расчёта: DEFAULT-нули, а не NULL."""
    return {
        "id": "trend:old",
        "workspace_id": "design",
        "deduped_source_count": 0,
        "distinct_voices": 0,
        "echo_ratio": 0.0,
        "arrival_dispersion": 0.0,
        "distinct_originators": None,
        "independence_score": 0.0,
    }


def test_unmeasured_becomes_null_not_zero() -> None:
    out = _mark_provenance(_unmeasured_row())
    assert out["provenance_measured"] is False
    for field in ("deduped_source_count", "distinct_voices", "echo_ratio",
                  "arrival_dispersion", "independence_score", "distinct_originators"):
        assert out[field] is None, (
            f"{field} остался нулём — клиент по-прежнему не отличит «не измерено» "
            "от «полностью синдицировано»"
        )


def test_measured_row_passes_through_untouched() -> None:
    out = _mark_provenance(_measured_row())
    assert out["provenance_measured"] is True
    assert out["independence_score"] == 0.61
    assert out["deduped_source_count"] == 3
    assert out["distinct_originators"] == 2


def test_rows_without_provenance_columns_are_not_touched() -> None:
    """posts, sources, missing_signals не должны получить лишний ключ.

    Иначе ответ каждой ручки распухнет полем, которое к ней не относится,
    и клиент решит, что провенанс у постов бывает.
    """
    row = {"id": "post:1", "workspace_id": "disruption", "content": "x"}
    out = _mark_provenance(dict(row))
    assert out == row
    assert "provenance_measured" not in out


@pytest.mark.parametrize(
    ("row", "expected", "why"),
    [
        ({"deduped_source_count": 2, "independence_score": 0.0}, True,
         "deduped посчитан — строка измерена, даже если score вышел нулём"),
        ({"deduped_source_count": 0, "distinct_voices": 1}, True,
         "distinct_voices посчитан"),
        ({"deduped_source_count": 0, "independence_score": 0.05}, True,
         "score живой при занулённом deduped — клампинг в semantic_clustering это умеет"),
        ({"deduped_source_count": 0, "distinct_voices": 0, "independence_score": 0.0}, False,
         "все три нули — DEFAULT миграции"),
        ({"deduped_source_count": None, "independence_score": None}, False,
         "None вместо чисел не должен считаться измерением"),
    ],
)
def test_predicate_is_triple_not_single_column(row, expected, why) -> None:
    """Предикат опирается на три признака, а не на один.

    Эмпирически они сейчас совпадают побитово — расхождений ноль во всех трёх
    таблицах в обе стороны. Но механизм расхождения существует:
    `_provenance_fields` клампит deduped через `min(deduped, raw_source_count)`,
    и при отсутствии `source_count` в БД ляжет ноль при живом score. Маркер,
    построенный на одном столбце, был бы завязан на случайность.
    """
    assert _provenance_measured(row) is expected, why


def test_marker_covers_select_star_handlers() -> None:
    """Статически: правка стоит в общей выборке, а не в местах отдачи.

    Реестр насчитал семь мест с буквальным именем колонки; ещё шесть путей
    отдают строку целиком через `SELECT *` (get_cluster_details,
    get_signal_timeline). Правка «по семи местам» оставила бы их отдавать
    сырой ноль — и это ловится здесь, а не на проде.
    """
    import ast
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "mcp" / "tools" / "observability.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    fetchers = {
        node.name: ast.dump(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name in {"_fetch_rows", "_fetch_one"}
    }
    assert set(fetchers) == {"_fetch_rows", "_fetch_one"}, (
        f"общие выборки переименованы или исчезли: {sorted(fetchers)}"
    )
    missing = [name for name, body in fetchers.items() if "_mark_provenance" not in body]
    assert not missing, (
        f"{missing} не пропускают строки через _mark_provenance. Тогда ручки на "
        "`SELECT *` снова начнут отдавать 0.0 вместо null, и заметить это можно "
        "будет только по жалобе клиента."
    )
