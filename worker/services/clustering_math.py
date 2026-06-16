"""Pure, dependency-free helpers for semantic clustering.

These primitives (cosine similarity, centroid, freshness decay, Jaccard, time
bucketing, tokenization, hashing, JSON normalization) carry no project state — no
DB, Qdrant, or settings — so they are extracted here to be unit-tested in isolation
and reused without importing the full clustering service.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from typing import Any


def digest(value: str, prefix: str) -> str:
    return f"{prefix}:{hashlib.sha1(value.encode('utf-8')).hexdigest()[:16]}"


def cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return 0.0 if na == 0 or nb == 0 else dot / (na * nb)


def freshness(dt: datetime) -> float:
    age = max((datetime.now(UTC) - dt).total_seconds() / 3600.0, 0.0)
    if age <= 24:
        return 1.0
    if age <= 72:
        return 0.75
    if age <= 168:
        return 0.45
    return 0.2


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / max(len(a | b), 1)


def bucket_start(dt: datetime, bucket_hours: int) -> datetime:
    base = dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    if bucket_hours >= 24:
        return base.replace(hour=0)
    hour = base.hour - (base.hour % max(bucket_hours, 1))
    return base.replace(hour=hour)


def terms(text: str) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-_]{2,}", (text or "").lower())


def centroid(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        return []
    dims = len(vectors[0])
    acc = [0.0] * dims
    for vector in vectors:
        for idx, value in enumerate(vector):
            acc[idx] += value
    count = float(len(vectors))
    return [value / count for value in acc]


def json_ready(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            default=lambda item: item.isoformat() if isinstance(item, datetime) else str(item),
        )
    )
