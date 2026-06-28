"""Тесты Контура D — обслуживание графа (метрики + слияние под apply-гейтом).

apply=False (cron) — только метрики, граф не мутируется. apply=True (оператор) —
слияние + пересчёт здоровья. Neo4j мокается.
"""

from __future__ import annotations

import pytest

from worker.services import graph_maintenance as gm

pytestmark = pytest.mark.unit


class _Neo:
    def __init__(self, health=None, duplicates=None, merge_result=None, raise_on_health=False):
        self._health = health or {"concept_count": 10, "orphan_count": 2, "duplicate_clusters": 1}
        self._duplicates = duplicates if duplicates is not None else [{"acro": "hmi"}, {"acro": "llm"}]
        self._merge_result = merge_result or {"merged_groups": 7}
        self._raise_on_health = raise_on_health
        self.health_calls = 0
        self.merge_calls: list[dict] = []
        self.closed = False

    async def graph_health(self, ws):
        self.health_calls += 1
        if self._raise_on_health:
            raise RuntimeError("neo down")
        return dict(self._health)

    async def find_duplicate_entities(self, ws, limit=50):
        return list(self._duplicates)

    async def merge_duplicate_entities(self, ws, limit=200):
        self.merge_calls.append({"ws": ws, "limit": limit})
        return dict(self._merge_result)

    async def close(self):
        self.closed = True


def _patch(monkeypatch, neo):
    metrics: list[tuple] = []
    monkeypatch.setattr(gm, "Neo4jFrontierClient", lambda: neo)
    monkeypatch.setattr(
        gm, "set_graph_health_metric", lambda *a: metrics.append(a)
    )
    return metrics


async def test_apply_false_only_metrics_no_merge(monkeypatch):
    neo = _Neo()
    metrics = _patch(monkeypatch, neo)
    out = await gm.run_graph_maintenance("disruption", apply=False)
    assert out["status"] == "ok" and out["apply"] is False
    assert neo.merge_calls == []  # граф не трогали
    assert out["merged_groups"] == 0
    assert out["duplicate_groups_found"] == 2
    assert neo.health_calls == 1
    assert len(metrics) == 3  # по одному гейджу на метрику здоровья
    assert neo.closed


async def test_apply_true_merges_and_recalcs_health(monkeypatch):
    neo = _Neo(merge_result={"merged_groups": 7})
    metrics = _patch(monkeypatch, neo)
    out = await gm.run_graph_maintenance("disruption", apply=True)
    assert out["merged_groups"] == 7
    assert neo.merge_calls == [{"ws": "disruption", "limit": gm.GRAPH_MERGE_MAX_GROUPS}]
    assert neo.health_calls == 2  # до и после слияния
    assert len(metrics) == 6  # 3 + 3
    assert neo.closed


async def test_apply_true_empty_groups_skips_merge(monkeypatch):
    neo = _Neo(duplicates=[])
    _patch(monkeypatch, neo)
    out = await gm.run_graph_maintenance("disruption", apply=True)
    assert neo.merge_calls == []  # нет дублей — нечего сливать
    assert out["merged_groups"] == 0
    assert neo.health_calls == 1


async def test_close_called_on_exception(monkeypatch):
    neo = _Neo(raise_on_health=True)
    _patch(monkeypatch, neo)
    with pytest.raises(RuntimeError):
        await gm.run_graph_maintenance("disruption", apply=False)
    assert neo.closed  # finally закрыл клиент


async def test_return_structure(monkeypatch):
    neo = _Neo()
    _patch(monkeypatch, neo)
    out = await gm.run_graph_maintenance("disruption", apply=False)
    for key in (
        "workspace_id", "status", "apply", "health",
        "duplicate_groups_found", "duplicate_groups_sample", "merged_groups",
    ):
        assert key in out
    assert out["duplicate_groups_sample"] == [{"acro": "hmi"}, {"acro": "llm"}]
