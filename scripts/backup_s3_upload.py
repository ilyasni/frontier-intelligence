#!/usr/bin/env python3
"""Выгрузка каталога бэкапа в S3/Cloud.ru. Запускается внутри worker-образа (boto3 есть).

Usage: python backup_s3_upload.py <local_dir> <s3_key_prefix>
       python backup_s3_upload.py --prune-only <s3_key_prefix> [--dry-run]
Креды берутся из окружения (передаётся через docker run --env-file .env):
  S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET_NAME, S3_REGION (опц.)
  S3_BACKUP_KEEP_DAYS — сколько ПРОШЛЫХ суточных комплектов держать (по умолчанию 1)
"""
import os
import sys
import pathlib

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def _make_s3():
    """Клиент + имя бакета из окружения. Возвращает (None, None) при нехватке кредов."""
    try:
        endpoint = os.environ["S3_ENDPOINT_URL"]
        access_key = os.environ["S3_ACCESS_KEY_ID"]
        secret_key = os.environ["S3_SECRET_ACCESS_KEY"]
        bucket = os.environ["S3_BUCKET_NAME"]
    except KeyError as exc:
        print(f"missing env var: {exc}", file=sys.stderr)
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


def _delete_keys(s3, bucket: str, keys: list[str]) -> int:
    """Удалить ключи пачками по 1000. При отказе batch API — по одному."""
    removed = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        try:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
            removed += len(chunk)
        except ClientError as exc:
            print(f"batch delete отклонён ({exc}), удаляю по одному", file=sys.stderr)
            for key in chunk:
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                    removed += 1
                except ClientError as exc_one:
                    print(f"не удалось удалить {key}: {exc_one}", file=sys.stderr)
    return removed


def _prune_old_days(s3, bucket: str, prefix: str, dry_run: bool = False) -> int:
    """Удалить устаревшие суточные комплекты в backups/ ДО заливки нового.

    Lifecycle-правило бакета (backups-expire, Days=1) отрабатывает асинхронно:
    провайдер сметает объекты когда угодно в пределах суток после истечения. К
    03:30, когда стартует заливка, место освободиться не успевает. Замер 17.08.2026:
    в бакете лежало ДВОЕ суток бэкапов (8.7 ГиБ) при объявленном ретеншне в одни,
    свободного места 3.7 ГиБ против 4.7 ГиБ суточного комплекта — то есть ночной
    прогон упал бы на выгрузке. Пик здесь считается по ТРЁМ комплектам (два старых
    плюс заливаемый), а квота в 15 ГиБ рассчитана на два.

    Синхронная уборка делает освобождение места детерминированным. Прогон идёт
    ПЕРЕД заливкой: если выгрузка потом упадёт, предыдущий комплект остаётся цел.
    Сам заливаемый день не трогаем никогда.
    """
    root = prefix.rsplit("/", 1)[0] if "/" in prefix else prefix
    current_day = prefix[len(root) :].strip("/")
    keep = int(os.environ.get("S3_BACKUP_KEEP_DAYS", "1"))

    paginator = s3.get_paginator("list_objects_v2")
    days: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{root}/", Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            day = common["Prefix"][len(root) + 1 :].strip("/")
            if day:
                days.add(day)

    # Заливаемый день из кандидатов исключён: он ещё не полон, а после заливки он же
    # и есть самая свежая копия. Имена суток — ISO-даты, поэтому сортировка строк
    # совпадает с хронологией.
    others = sorted(d for d in days if d != current_day)
    doomed = others[:-keep] if keep > 0 else others
    if not doomed:
        print(f"prune: в {root}/ держим {others or '—'} (keep={keep}), удалять нечего")
        return 0

    freed = 0
    for day in doomed:
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{root}/{day}/"):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
                freed += obj["Size"]
        if not keys:
            continue
        if dry_run:
            print(f"prune[dry-run]: снёс бы {root}/{day}/ — {len(keys)} объектов")
            continue
        removed = _delete_keys(s3, bucket, keys)
        print(f"prune: удалён {root}/{day}/ — {removed}/{len(keys)} объектов")

    kept = [d for d in others if d not in doomed]
    print(
        f"prune: освобождено {freed / 1024 ** 3:.2f} GiB"
        f"{' (dry-run, ничего не удалено)' if dry_run else ''}; "
        f"оставлено прошлых суток: {kept or '—'}"
    )
    return freed


def main() -> int:
    argv = sys.argv[1:]
    dry_run = "--dry-run" in argv
    argv = [a for a in argv if a != "--dry-run"]
    prune_only = "--prune-only" in argv
    argv = [a for a in argv if a != "--prune-only"]

    if prune_only:
        if len(argv) != 1:
            print("usage: backup_s3_upload.py --prune-only <s3_key_prefix> [--dry-run]", file=sys.stderr)
            return 2
        src, prefix = None, argv[0].strip("/")
    else:
        if len(argv) != 2:
            print("usage: backup_s3_upload.py <local_dir> <s3_key_prefix>", file=sys.stderr)
            return 2
        src, prefix = pathlib.Path(argv[0]), argv[1].strip("/")

    s3, bucket = _make_s3()
    if s3 is None:
        return 3

    # Уборка не должна ронять бэкап: место могло и не понадобиться, а выгрузка нужна всегда.
    try:
        _prune_old_days(s3, bucket, prefix, dry_run=dry_run)
    except ClientError as exc:
        print(f"prune пропущен из-за ошибки S3: {exc}", file=sys.stderr)

    if prune_only:
        return 0

    uploaded = 0
    for path in sorted(src.rglob("*")):
        if not path.is_file():
            continue
        key = f"{prefix}/{path.relative_to(src).as_posix()}"
        s3.upload_file(str(path), bucket, key)
        print(f"uploaded s3://{bucket}/{key} ({path.stat().st_size} bytes)")
        uploaded += 1

    if uploaded == 0:
        print("nothing uploaded (empty dir)", file=sys.stderr)
        return 1
    print(f"done: {uploaded} files -> s3://{bucket}/{prefix}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
