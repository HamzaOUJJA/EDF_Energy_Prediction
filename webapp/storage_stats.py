# =======================================================================
# webapp/storage_stats.py — MinIO bucket size/object-count stats + Postgres
# DW size. Polled on its own slower interval (buckets can be large).
# =======================================================================

from __future__ import annotations

from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

import config
import db

BUCKETS = ["edf-bronze", "edf-silver", "edf-gold", "rapport-eda", "models"]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4", connect_timeout=3, read_timeout=10),
    )


def _bucket_stats(client, bucket: str) -> dict[str, Any]:
    try:
        total_bytes = 0
        object_count = 0
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                total_bytes += obj["Size"]
                object_count += 1
        return {"bucket": bucket, "object_count": object_count, "size_bytes": total_bytes}
    except (BotoCoreError, ClientError) as exc:
        return {"bucket": bucket, "object_count": None, "size_bytes": None, "error": str(exc)}


def minio_bucket_stats() -> list[dict[str, Any]]:
    client = _s3_client()
    return [_bucket_stats(client, bucket) for bucket in BUCKETS]


def postgres_database_size() -> dict[str, Any] | None:
    row = db.query_one(
        "SELECT pg_database_size(%s) AS size_bytes",
        (config.PG_DATABASE,),
    )
    return row


def get_storage_summary() -> dict[str, Any]:
    return {
        "buckets": minio_bucket_stats(),
        "postgres": postgres_database_size(),
    }
