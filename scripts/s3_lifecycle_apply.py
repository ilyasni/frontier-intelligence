from __future__ import annotations

import argparse
import json
import os
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

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

# Декларативный источник истины для lifecycle бакета (Cloud.ru S3).
# put_bucket_lifecycle_configuration ЗАМЕНЯЕТ конфиг целиком — этот список
# должен содержать ВСЕ правила. Правило живёт на бакете (server-side), не в .env;
# при пересоздании бакета прогнать `python scripts/s3_lifecycle_apply.py --apply`.
#
# Сроки истечения (Days = возраст объекта):
#   media/    30д  — Telegram-медиа
#   vision/   14д  — vision-summary альбомов (gzip)
#   crawl/     7д  — crawl4ai (html/md.gz)
#   backups/   2д  — ночные дампы стека (~3.9 GiB/день); интерим под квоту бакета ~15 GiB.
#                    Чтобы держать дольше — поднять квоту бакета ИЛИ исключить
#                    dense_2560-снапшоты (2.1 GiB) из S3-заливки в backup-stack.sh.
#   raw/       —    без правила (мелочь, ~27 MiB); при росте добавить сюда.
# AbortIncompleteMultipartUpload=1д на каждом правиле + глобально — чистит
# недозалитые multipart-обрубки (иначе они молча копят место под квотой).
DESIRED_RULES: list[dict[str, Any]] = [
    {
        "ID": "media-expire",
        "Filter": {"Prefix": "media/"},
        "Status": "Enabled",
        "Expiration": {"Days": 30},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    },
    {
        "ID": "vision-expire",
        "Filter": {"Prefix": "vision/"},
        "Status": "Enabled",
        "Expiration": {"Days": 14},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    },
    {
        "ID": "crawl-expire",
        "Filter": {"Prefix": "crawl/"},
        "Status": "Enabled",
        "Expiration": {"Days": 7},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    },
    {
        "ID": "backups-expire",
        "Filter": {"Prefix": "backups/"},
        "Status": "Enabled",
        "Expiration": {"Days": 2},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    },
    {
        "ID": "abort-multipart-global",
        "Filter": {},
        "Status": "Enabled",
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    },
]


def _fetch_current(s3: Any, bucket: str) -> list[dict[str, Any]]:
    try:
        resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        return list(resp.get("Rules", []))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchLifecycleConfiguration":
            return []
        raise


def _summarize(rules: list[dict[str, Any]]) -> str:
    lines = []
    for r in rules:
        exp = r.get("Expiration", {}).get("Days", "-")
        pfx = r.get("Filter", {}).get("Prefix", "(all)")
        abort = r.get("AbortIncompleteMultipartUpload", {}).get("DaysAfterInitiation", "-")
        lines.append(
            f"  {r.get('ID', '?'):<24} prefix={pfx:<10} expire={exp}d "
            f"abort_mpu={abort}d status={r.get('Status', '?')}"
        )
    return "\n".join(lines) if lines else "  (нет правил)"


def _normalize(rules: list[dict[str, Any]]) -> str:
    """Стабильная строка для сравнения current vs desired (порядок правил не важен)."""
    return json.dumps(sorted(rules, key=lambda r: r.get("ID", "")), sort_keys=True, ensure_ascii=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Применить декларативный lifecycle к S3-бакету (см. DESIRED_RULES). "
        "Без --apply — только dry-run отчёт (current vs desired).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Записать DESIRED_RULES в бакет. Без флага — только показать разницу.",
    )
    args = parser.parse_args()

    s3, bucket = _make_s3()
    if s3 is None:
        print("S3 is not configured (need S3_ENDPOINT_URL/S3_BUCKET_NAME/S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY)")
        return 2

    current = _fetch_current(s3, bucket)
    print(f"bucket={bucket}")
    print("== CURRENT ==")
    print(_summarize(current))
    print("== DESIRED ==")
    print(_summarize(DESIRED_RULES))

    if _normalize(current) == _normalize(DESIRED_RULES):
        print("\nСовпадает — менять нечего.")
        return 0

    if not args.apply:
        print("\nОтличается. Запусти с --apply, чтобы записать DESIRED_RULES.")
        return 0

    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": DESIRED_RULES},
    )
    applied = _fetch_current(s3, bucket)
    print("\n== APPLIED ==")
    print(_summarize(applied))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
