from __future__ import annotations

from typing import Any, Optional

import boto3
from botocore.config import Config as BotoConfig


def make_s3_client(settings) -> tuple[Optional[Any], Optional[str]]:
    if not settings.s3_bucket_name:
        return None, None

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        region_name=settings.s3_region,
        config=BotoConfig(
            signature_version=settings.aws_signature_version,
            connect_timeout=settings.s3_connect_timeout_sec,
            read_timeout=settings.s3_read_timeout_sec,
            retries={
                "max_attempts": settings.s3_max_retry_attempts,
                "mode": "standard",
            },
            s3={"addressing_style": settings.s3_addressing_style},
        ),
    )
    return client, settings.s3_bucket_name
