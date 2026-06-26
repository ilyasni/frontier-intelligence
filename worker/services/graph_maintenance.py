"""Контур D (RSI): обслуживание концепт-графа Neo4j.

NEL копит дубли сущностей («HMI» / «hmi» / «H.M.I.»), GraphRAG деградирует молча.
Здесь: метрики здоровья графа (orphan-ноды, дубль-кластеры, плотность) на Grafana и
entity-resolution merge нормализационных дублей в канонический узел с aliases.

Метрики считаются автоматически (cron, безопасно). Слияние (мутация графа) —
только при apply=True (операторский job), человек-гейт и для графа тоже.
Семантические дубли («HMI» vs «Human-Machine Interface») нормализация НЕ ловит —
это будущий шаг через embedding/LLM, пока ловятся регистр/пробелы/пунктуация.
"""

from __future__ import annotations

import logging
from typing import Any

from shared.metrics import set_graph_health_metric
from worker.integrations.neo4j_client import Neo4jFrontierClient

logger = logging.getLogger(__name__)

GRAPH_MERGE_MAX_GROUPS = 200


async def run_graph_maintenance(workspace_id: str, *, apply: bool = False) -> dict[str, Any]:
    client = Neo4jFrontierClient()
    try:
        health = await client.graph_health(workspace_id)
        for metric, value in health.items():
            set_graph_health_metric("worker", workspace_id, metric, float(value))

        duplicate_groups = await client.find_duplicate_entities(workspace_id, limit=50)
        merged_groups = 0
        if apply and duplicate_groups:
            merged = await client.merge_duplicate_entities(workspace_id, limit=GRAPH_MERGE_MAX_GROUPS)
            merged_groups = int(merged.get("merged_groups") or 0)
            # Пересчёт здоровья после слияния.
            health = await client.graph_health(workspace_id)
            for metric, value in health.items():
                set_graph_health_metric("worker", workspace_id, metric, float(value))
    finally:
        await client.close()

    logger.info(
        "graph_maintenance workspace=%s apply=%s concepts=%s orphans=%s dup_clusters=%s merged=%s",
        workspace_id, apply, health.get("concept_count"), health.get("orphan_count"),
        health.get("duplicate_clusters"), merged_groups,
    )
    return {
        "workspace_id": workspace_id,
        "status": "ok",
        "apply": apply,
        "health": health,
        "duplicate_groups_found": len(duplicate_groups),
        "duplicate_groups_sample": duplicate_groups[:10],
        "merged_groups": merged_groups,
    }
