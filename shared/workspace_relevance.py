"""
Слияние relevance_weights для workspaces.

При сохранении из админки нельзя затирать веса по категориям (technology, design, …),
оставляя только threshold — см. POST upsert и PATCH в admin routers.

Реализация согласована с практикой FastAPI+Pydantic partial update (model_dump exclude_unset);
SQL — параметризованный text() (SQLAlchemy 2.x).
"""
from __future__ import annotations

import json
from typing import Any


def parse_jsonb_object(value: Any) -> dict:
    """Нормализует значение JSONB из asyncpg/SQLAlchemy в dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            out = json.loads(value)
            return dict(out) if isinstance(out, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def merge_relevance_weights(
    existing: Any,
    *,
    new_threshold: float | None = None,
) -> dict:
    """
    Копирует существующие ключи relevance_weights и обновляет threshold при передаче.
    """
    base = parse_jsonb_object(existing)
    out = {**base}
    if new_threshold is not None:
        out["threshold"] = float(new_threshold)
    return out
