from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.config import get_settings
from shared.s3 import make_s3_client

TARGET_SUFFIX = "/albums/null/summary.json.gz"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit or delete stale vision/*/albums/null/summary.json.gz objects.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete matched objects. Without this flag the script only prints a dry-run report.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of matched objects to process.",
    )
    args = parser.parse_args()

    settings = get_settings()
    if not settings.s3_bucket_name:
        print("S3_BUCKET_NAME is not configured")
        return 2

    s3, bucket = make_s3_client(settings)
    paginator = s3.get_paginator("list_objects_v2")

    matched: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix="vision/"):
        for obj in page.get("Contents", []):
            key = str(obj.get("Key") or "")
            if key.endswith(TARGET_SUFFIX):
                matched.append(key)
                if len(matched) >= max(1, args.limit):
                    break
        if len(matched) >= max(1, args.limit):
            break

    print(f"matched={len(matched)} bucket={bucket} apply={args.apply}")
    for key in matched:
        print(key)

    if args.apply and matched:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in matched], "Quiet": False},
        )
        print(f"deleted={len(matched)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
