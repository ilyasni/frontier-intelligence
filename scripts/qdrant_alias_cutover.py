from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import CreateAlias, CreateAliasOperation, DeleteAlias, DeleteAliasOperation

from scripts.init_storage import init_qdrant
from shared.config import Settings, get_settings
from shared.embedding_models import embedding_profile, qdrant_collection_name_for_embedding

# Safe operational helper for zero-downtime embedding cutovers via Qdrant aliases.


@dataclass(frozen=True)
class CollectionContract:
    kind: str
    base_collection: str
    alias_name: str
    target_collection: str
    embedding_version: str
    expected_dimension: int | None
    expected_distance: str
    expects_sparse: bool


def _distance_name(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _extract_dense_schema(collection_info: Any) -> tuple[int | None, str]:
    vectors = getattr(
        getattr(getattr(collection_info, "config", None), "params", None),
        "vectors",
        None,
    )
    if isinstance(vectors, dict):
        dense = vectors.get("dense")
        return (
            int(getattr(dense, "size", 0) or 0) or None,
            _distance_name(getattr(dense, "distance", "")),
        )
    if vectors is not None:
        return (
            int(getattr(vectors, "size", 0) or 0) or None,
            _distance_name(getattr(vectors, "distance", "")),
        )
    return None, ""


def _extract_sparse_enabled(collection_info: Any) -> bool:
    sparse_vectors = getattr(
        getattr(getattr(collection_info, "config", None), "params", None),
        "sparse_vectors",
        None,
    )
    if isinstance(sparse_vectors, dict):
        return "sparse" in sparse_vectors
    return bool(sparse_vectors)


def _collection_contracts(settings: Settings) -> dict[str, CollectionContract]:
    profile = embedding_profile(settings.gigachat_embeddings_model)
    expected_dimension = profile.get("dimension")
    expected_distance = str(profile.get("distance_metric") or "cosine").strip().lower()
    return {
        "documents": CollectionContract(
            kind="documents",
            base_collection=settings.qdrant_collection,
            alias_name=str(settings.qdrant_collection_alias or "").strip(),
            target_collection=qdrant_collection_name_for_embedding(
                settings.qdrant_collection,
                settings.gigachat_embeddings_model,
            ),
            embedding_version=str(settings.gigachat_embeddings_model or "").strip(),
            expected_dimension=int(expected_dimension) if expected_dimension else None,
            expected_distance=expected_distance,
            expects_sparse=bool(settings.sparse_vectors_enabled),
        ),
        "trends": CollectionContract(
            kind="trends",
            base_collection=settings.qdrant_trends_collection,
            alias_name=str(settings.qdrant_trends_collection_alias or "").strip(),
            target_collection=qdrant_collection_name_for_embedding(
                settings.qdrant_trends_collection,
                settings.gigachat_embeddings_model,
            ),
            embedding_version=str(settings.gigachat_embeddings_model or "").strip(),
            expected_dimension=int(expected_dimension) if expected_dimension else None,
            expected_distance=expected_distance,
            expects_sparse=False,
        ),
    }


def _preflight_errors(
    *,
    expected_dimension: int | None,
    expected_distance: str,
    expects_sparse: bool,
    current_count: int,
    target_count: int,
    target_dimension: int | None,
    target_distance: str,
    target_sparse_enabled: bool,
    min_ratio: float,
) -> list[str]:
    errors: list[str] = []
    if expected_dimension is not None and target_dimension != expected_dimension:
        errors.append(
            f"target_dimension_mismatch actual={target_dimension} expected={expected_dimension}"
        )
    if expected_distance and target_distance and target_distance != expected_distance:
        errors.append(
            f"target_distance_mismatch actual={target_distance} expected={expected_distance}"
        )
    if expects_sparse and not target_sparse_enabled:
        errors.append("target_sparse_vector_missing")
    if current_count > 0 and target_count < current_count * min_ratio:
        errors.append(
            f"target_point_ratio_too_low current={current_count} target={target_count} "
            f"min_ratio={min_ratio}"
        )
    return errors


def _alias_swap_operations(
    *,
    alias_name: str,
    current_target: str | None,
    target_collection: str,
) -> list[Any]:
    if not alias_name:
        return []
    if current_target == target_collection:
        return []
    operations: list[Any] = []
    if current_target:
        operations.append(
            DeleteAliasOperation(delete_alias=DeleteAlias(alias_name=alias_name))
        )
    operations.append(
        CreateAliasOperation(
            create_alias=CreateAlias(
                collection_name=target_collection,
                alias_name=alias_name,
            )
        )
    )
    return operations


async def _alias_target(client: AsyncQdrantClient, alias_name: str) -> str | None:
    if not alias_name:
        return None
    aliases = await client.get_aliases()
    for alias in getattr(aliases, "aliases", []) or []:
        if str(getattr(alias, "alias_name", "")).strip() == alias_name:
            collection_name = str(getattr(alias, "collection_name", "")).strip()
            return collection_name or None
    return None


async def _collection_snapshot(
    client: AsyncQdrantClient,
    collection_name: str,
) -> dict[str, Any]:
    exists = await client.collection_exists(collection_name)
    if not exists:
        return {
            "collection_name": collection_name,
            "exists": False,
        }
    info = await client.get_collection(collection_name=collection_name)
    count = await client.count(collection_name=collection_name, exact=True)
    dimension, distance = _extract_dense_schema(info)
    return {
        "collection_name": collection_name,
        "exists": True,
        "points_count": int(getattr(count, "count", 0) or 0),
        "indexed_vectors_count": int(getattr(info, "indexed_vectors_count", 0) or 0),
        "dense_dimension": dimension,
        "distance_metric": distance,
        "sparse_enabled": _extract_sparse_enabled(info),
    }


async def qdrant_alias_status(
    settings: Settings,
    *,
    min_ratio: float = 0.95,
) -> dict[str, Any]:
    client = AsyncQdrantClient(url=settings.qdrant_url, timeout=30)
    try:
        report: dict[str, Any] = {}
        for contract in _collection_contracts(settings).values():
            alias_target = await _alias_target(client, contract.alias_name)
            active_collection = alias_target or contract.base_collection
            active_snapshot = await _collection_snapshot(client, active_collection)
            target_snapshot = (
                active_snapshot
                if active_collection == contract.target_collection
                else await _collection_snapshot(client, contract.target_collection)
            )
            errors = _preflight_errors(
                expected_dimension=contract.expected_dimension,
                expected_distance=contract.expected_distance,
                expects_sparse=contract.expects_sparse,
                current_count=int(active_snapshot.get("points_count") or 0),
                target_count=int(target_snapshot.get("points_count") or 0),
                target_dimension=target_snapshot.get("dense_dimension"),
                target_distance=str(target_snapshot.get("distance_metric") or ""),
                target_sparse_enabled=bool(target_snapshot.get("sparse_enabled")),
                min_ratio=min_ratio,
            )
            report[contract.kind] = {
                "kind": contract.kind,
                "alias_name": contract.alias_name,
                "alias_target": alias_target,
                "alias_enabled": bool(contract.alias_name),
                "base_collection": contract.base_collection,
                "active_collection": active_collection,
                "target_collection": contract.target_collection,
                "embedding_version": contract.embedding_version,
                "expected_dimension": contract.expected_dimension,
                "expected_distance": contract.expected_distance,
                "expects_sparse": contract.expects_sparse,
                "active": active_snapshot,
                "target": target_snapshot,
                "cutover_ready": not errors,
                "preflight_errors": errors,
            }
        return report
    finally:
        await client.close()


async def qdrant_alias_prepare(settings: Settings) -> dict[str, Any]:
    await init_qdrant(settings, force_versioned_targets=True)
    return await qdrant_alias_status(settings)


async def qdrant_alias_swap(
    settings: Settings,
    *,
    kinds: list[str],
    min_ratio: float = 0.95,
    force: bool = False,
) -> dict[str, Any]:
    status = await qdrant_alias_status(settings, min_ratio=min_ratio)
    selected = {kind: status[kind] for kind in kinds}
    client = AsyncQdrantClient(url=settings.qdrant_url, timeout=30)
    try:
        swapped: dict[str, Any] = {}
        for kind in kinds:
            state = selected[kind]
            alias_name = str(state["alias_name"] or "")
            if not alias_name:
                swapped[kind] = {"skipped": True, "reason": "alias_disabled"}
                continue
            errors = list(state["preflight_errors"] or [])
            if errors and not force:
                raise RuntimeError(f"{kind} cutover blocked: {'; '.join(errors)}")
            operations = _alias_swap_operations(
                alias_name=alias_name,
                current_target=state.get("alias_target"),
                target_collection=str(state["target_collection"]),
            )
            if not operations:
                swapped[kind] = {
                    "skipped": True,
                    "reason": "already_active",
                    "alias_name": alias_name,
                    "target_collection": state["target_collection"],
                }
                continue
            await client.update_collection_aliases(change_aliases_operations=operations)
            swapped[kind] = {
                "swapped": True,
                "alias_name": alias_name,
                "target_collection": state["target_collection"],
                "forced": force,
            }
        return {
            "requested_kinds": kinds,
            "swapped": swapped,
            "post_swap_status": await qdrant_alias_status(settings, min_ratio=min_ratio),
        }
    finally:
        await client.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect and safely cut over Qdrant aliases for embedding model migrations."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("status", "prepare", "preflight", "swap"):
        sub = subparsers.add_parser(command)
        sub.add_argument(
            "--kind",
            choices=("documents", "trends", "all"),
            default="all",
        )
        sub.add_argument("--min-ratio", type=float, default=0.95)
        if command == "swap":
            sub.add_argument("--force", action="store_true")
    return parser.parse_args()


def _selected_kinds(kind: str) -> list[str]:
    return ["documents", "trends"] if kind == "all" else [kind]


def main() -> None:
    args = _parse_args()
    settings = get_settings()
    if args.command == "status":
        result = asyncio.run(qdrant_alias_status(settings, min_ratio=args.min_ratio))
    elif args.command == "prepare":
        result = asyncio.run(qdrant_alias_prepare(settings))
    elif args.command == "preflight":
        result = asyncio.run(qdrant_alias_status(settings, min_ratio=args.min_ratio))
        selected = [result[kind] for kind in _selected_kinds(args.kind)]
        errors = [
            f"{item['kind']}: {'; '.join(item['preflight_errors'])}"
            for item in selected
            if item.get("preflight_errors")
        ]
        if errors:
            print(json.dumps(result, ensure_ascii=False, indent=2))
            raise SystemExit("\n".join(errors))
    else:
        result = asyncio.run(
            qdrant_alias_swap(
                settings,
                kinds=_selected_kinds(args.kind),
                min_ratio=args.min_ratio,
                force=bool(args.force),
            )
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
