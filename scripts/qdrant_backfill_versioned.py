from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue, PointStruct

from scripts.qdrant_alias_cutover import qdrant_alias_prepare, qdrant_alias_status
from shared.config import Settings, get_settings


def _embedding_filter(settings: Settings) -> Filter | None:
    if not settings.qdrant_filter_embedding_version:
        return None
    embedding_version = str(settings.gigachat_embeddings_model or "").strip()
    if not embedding_version:
        return None
    return Filter(
        must=[
            FieldCondition(
                key="embedding_version",
                match=MatchValue(value=embedding_version),
            )
        ]
    )


async def _count_points(
    client: AsyncQdrantClient,
    *,
    collection_name: str,
    count_filter: Filter | None = None,
) -> int:
    result = await client.count(
        collection_name=collection_name,
        count_filter=count_filter,
        exact=True,
    )
    return int(getattr(result, "count", 0) or 0)


async def _backfill_kind(
    settings: Settings,
    *,
    kind: str,
    batch_size: int = 256,
    max_batches: int | None = None,
) -> dict[str, Any]:
    await qdrant_alias_prepare(settings)
    status = await qdrant_alias_status(settings, min_ratio=0.95)
    state = status[kind]
    source_collection = str(state["active_collection"])
    target_collection = str(state["target_collection"])
    if source_collection == target_collection:
        return {
            "kind": kind,
            "skipped": True,
            "reason": "source_equals_target",
            "source_collection": source_collection,
            "target_collection": target_collection,
        }

    client = AsyncQdrantClient(url=settings.qdrant_url, timeout=30)
    try:
        scroll_filter = _embedding_filter(settings) if kind == "documents" else None
        source_count_at_start = await _count_points(
            client,
            collection_name=source_collection,
            count_filter=scroll_filter,
        )
        target_count_before = await _count_points(
            client,
            collection_name=target_collection,
            count_filter=scroll_filter,
        )

        copied_points = 0
        batches = 0
        offset: Any = None
        while True:
            records, offset = await client.scroll(
                collection_name=source_collection,
                scroll_filter=scroll_filter,
                offset=offset,
                limit=batch_size,
                with_payload=True,
                with_vectors=True,
            )
            if not records:
                break

            points = [
                PointStruct(
                    id=record.id,
                    vector=record.vector,
                    payload=record.payload or {},
                )
                for record in records
            ]
            await client.upsert(
                collection_name=target_collection,
                points=points,
                wait=True,
            )
            copied_points += len(points)
            batches += 1
            if max_batches is not None and batches >= max_batches:
                break
            if offset is None:
                break

        target_count_after = await _count_points(
            client,
            collection_name=target_collection,
            count_filter=scroll_filter,
        )
        post_status = await qdrant_alias_status(settings, min_ratio=0.95)
        return {
            "kind": kind,
            "source_collection": source_collection,
            "target_collection": target_collection,
            "embedding_version": str(settings.gigachat_embeddings_model or "").strip(),
            "source_count_at_start": source_count_at_start,
            "target_count_before": target_count_before,
            "target_count_after": target_count_after,
            "copied_points": copied_points,
            "batches": batches,
            "batch_size": batch_size,
            "post_status": post_status[kind],
        }
    finally:
        await client.close()


async def qdrant_backfill_versioned(
    settings: Settings,
    *,
    kinds: list[str],
    batch_size: int = 256,
    max_batches: int | None = None,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for kind in kinds:
        results[kind] = await _backfill_kind(
            settings,
            kind=kind,
            batch_size=batch_size,
            max_batches=max_batches,
        )
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill active Qdrant collections into versioned target collections."
    )
    parser.add_argument(
        "--kind",
        choices=("documents", "trends", "all"),
        default="all",
    )
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--max-batches", type=int, default=None)
    return parser.parse_args()


def _selected_kinds(kind: str) -> list[str]:
    return ["documents", "trends"] if kind == "all" else [kind]


def main() -> None:
    args = _parse_args()
    result = asyncio.run(
        qdrant_backfill_versioned(
            get_settings(),
            kinds=_selected_kinds(args.kind),
            batch_size=max(1, int(args.batch_size)),
            max_batches=args.max_batches,
        )
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
