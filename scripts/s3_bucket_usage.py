from __future__ import annotations

import argparse
import os
from collections import defaultdict

import boto3
from botocore.config import Config

# Автономно (как scripts/backup_s3_upload.py): креды из окружения, не из полного
# Settings — на сервере .env содержит инлайн-комментарии в не-S3 полях, из-за
# которых pydantic Settings падает при `docker run --env-file .env`.


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


def _human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PiB"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Разбор наполнения S3-бакета: сумма/кол-во по top-префиксам, "
        "разбивка backups/ по дням и media/ по workspace, топ крупных объектов.",
    )
    parser.add_argument("--top", type=int, default=25, help="Сколько крупнейших объектов показать.")
    args = parser.parse_args()

    s3, bucket = _make_s3()
    if s3 is None:
        print("S3 is not configured (need S3_ENDPOINT_URL/S3_BUCKET_NAME/S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY)")
        return 2

    paginator = s3.get_paginator("list_objects_v2")

    top: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    backups_day: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    media_ws: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    total_count = 0
    total_bytes = 0
    oldest: tuple | None = None
    newest: tuple | None = None
    largest: list[tuple[int, str]] = []

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            size = int(obj.get("Size", 0))
            lm = obj.get("LastModified")
            total_count += 1
            total_bytes += size

            seg = key.split("/", 2)
            p0 = seg[0] if len(seg) > 1 else "(root)"
            top[p0][0] += 1
            top[p0][1] += size
            if p0 == "backups" and len(seg) >= 2:
                backups_day[seg[1]][0] += 1
                backups_day[seg[1]][1] += size
            if p0 == "media" and len(seg) >= 2:
                media_ws[seg[1]][0] += 1
                media_ws[seg[1]][1] += size

            if lm is not None:
                if oldest is None or lm < oldest[0]:
                    oldest = (lm, key)
                if newest is None or lm > newest[0]:
                    newest = (lm, key)
            largest.append((size, key))

    largest.sort(reverse=True)
    largest = largest[: max(1, args.top)]

    print(f"BUCKET={bucket}")
    print(f"TOTAL: {total_count} objects, {_human(total_bytes)} ({total_bytes} bytes)\n")

    print("== BY TOP PREFIX ==")
    for p, (c, b) in sorted(top.items(), key=lambda kv: kv[1][1], reverse=True):
        print(f"  {p:<16} {c:>8} obj   {_human(b):>10}")
    print()

    if backups_day:
        print("== backups/ BY DAY ==")
        for d, (c, b) in sorted(backups_day.items()):
            print(f"  {d:<16} {c:>4} obj   {_human(b):>10}")
        print(f"  (days={len(backups_day)})\n")

    if media_ws:
        print("== media/ BY WORKSPACE ==")
        for w, (c, b) in sorted(media_ws.items(), key=lambda kv: kv[1][1], reverse=True):
            print(f"  {w:<20} {c:>8} obj   {_human(b):>10}")
        print()

    if oldest:
        print(f"OLDEST: {oldest[0]}  {oldest[1]}")
    if newest:
        print(f"NEWEST: {newest[0]}  {newest[1]}")
    print(f"\n== TOP {len(largest)} LARGEST OBJECTS ==")
    for size, key in largest:
        print(f"  {_human(size):>10}  {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
