#!/usr/bin/env python3
"""Выгрузка каталога бэкапа в S3/Cloud.ru. Запускается внутри worker-образа (boto3 есть).

Usage: python backup_s3_upload.py <local_dir> <s3_key_prefix>
Креды берутся из окружения (передаётся через docker run --env-file .env):
  S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET_NAME, S3_REGION (опц.)
"""
import os
import sys
import pathlib

import boto3
from botocore.config import Config


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: backup_s3_upload.py <local_dir> <s3_key_prefix>", file=sys.stderr)
        return 2

    src = pathlib.Path(sys.argv[1])
    prefix = sys.argv[2].strip("/")

    try:
        endpoint = os.environ["S3_ENDPOINT_URL"]
        access_key = os.environ["S3_ACCESS_KEY_ID"]
        secret_key = os.environ["S3_SECRET_ACCESS_KEY"]
        bucket = os.environ["S3_BUCKET_NAME"]
    except KeyError as exc:
        print(f"missing env var: {exc}", file=sys.stderr)
        return 3

    region = os.environ.get("S3_REGION") or None
    addressing = os.environ.get("S3_ADDRESSING_STYLE", "path")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(s3={"addressing_style": addressing}),
    )

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
