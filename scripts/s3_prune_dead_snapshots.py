"""Удаление из S3 снапшотов Qdrant-коллекций, вытесненных версионированными.

Зачем: `backup-stack.sh` до 2026-08-02 снапшотил ВСЕ коллекции, включая базовые
(`frontier_docs`, `trend_clusters`), которые давно вытеснены версионированными
(`frontier_docs__embeddingsgigar__dense_2560` и т.п. — на них указывают алиасы).
В базовые коллекции не пишут, а их снапшоты занимали 611 MiB за ночь и вместе с
ростом основного снапшота упирали бакет в квоту: 01.08 и 02.08 самый важный файл
(2.3 GiB dense-снапшот) не доехал вовсе.

Скрипт удаляет ТОЛЬКО объекты вида `backups/<дата>/qdrant_<база>.snapshot`, где
`<база>` — коллекция, вытесненная алиасированным потомком. Живые снапшоты и
любые другие префиксы не трогает.

Без --apply печатает план и ничего не удаляет.

Запуск:
    docker compose --profile core --profile worker run --rm -T worker \\
        python scripts/s3_prune_dead_snapshots.py            # dry-run
    ... python scripts/s3_prune_dead_snapshots.py --apply    # удалить
"""

from __future__ import annotations

import argparse
import os
import re

import boto3
import httpx
from botocore.config import Config

# Автономно, как s3_lifecycle_apply.py: креды из окружения, не из Settings —
# на сервере .env содержит инлайн-комментарии, на которых pydantic падает.


def _make_s3():
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    bucket = os.environ.get("S3_BUCKET_NAME")
    access_key = os.environ.get("S3_ACCESS_KEY_ID")
    secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")
    if not (endpoint and bucket and access_key and secret_key):
        return None, None
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.environ.get("S3_REGION") or None,
        config=Config(s3={"addressing_style": os.environ.get("S3_ADDRESSING_STYLE", "path")}),
    )
    return client, bucket


def _dead_collections(qdrant_url: str) -> set[str]:
    """Базовые коллекции, вытесненные алиасированным потомком `<база>__...`."""
    headers = {}
    api_key = os.environ.get("QDRANT__SERVICE__API_KEY")
    if api_key:
        headers["api-key"] = api_key

    with httpx.Client(timeout=20.0, headers=headers) as client:
        colls = [
            c["name"]
            for c in client.get(f"{qdrant_url}/collections").json()["result"]["collections"]
        ]
        targets = {
            a["collection_name"]
            for a in client.get(f"{qdrant_url}/aliases").json()["result"]["aliases"]
        }

    return {
        name
        for name in colls
        if name not in targets and any(t.startswith(name + "__") for t in targets)
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Действительно удалить. Без флага — только план.",
    )
    args = parser.parse_args()

    s3, bucket = _make_s3()
    if not s3:
        print("S3_* переменные не заданы — нечего делать.")
        return 1

    qdrant_url = (os.environ.get("QDRANT_URL") or "http://qdrant:6333").rstrip("/")
    dead = _dead_collections(qdrant_url)
    if not dead:
        print("Вытесненных коллекций не найдено — удалять нечего.")
        return 0

    print("Вытесненные коллекции (их снапшоты — мусор):")
    for name in sorted(dead):
        print(f"  {name}")
    print()

    # backups/<что угодно>/qdrant_<коллекция>.snapshot
    patterns = {
        name: re.compile(rf"^backups/[^/]+/qdrant_{re.escape(name)}\.snapshot$") for name in dead
    }

    victims: list[tuple[str, int]] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="backups/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if any(rx.match(key) for rx in patterns.values()):
                victims.append((key, obj["Size"]))

    if not victims:
        print("Подходящих объектов нет — бакет уже чист.")
        return 0

    total = sum(size for _, size in victims)
    print(f"{'УДАЛЯЮ' if args.apply else 'БУДЕТ УДАЛЕНО (dry-run)'}: {len(victims)} объектов, "
          f"{total / 1024 / 1024:.1f} MiB")
    for key, size in sorted(victims):
        print(f"  {size / 1024 / 1024:>8.1f} MiB  {key}")

    if not args.apply:
        print("\nЭто dry-run. Запусти с --apply, чтобы удалить.")
        return 0

    for batch_start in range(0, len(victims), 1000):
        batch = victims[batch_start : batch_start + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key, _ in batch], "Quiet": True},
        )
    print(f"\nУдалено {len(victims)} объектов, освобождено {total / 1024 / 1024:.1f} MiB.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
