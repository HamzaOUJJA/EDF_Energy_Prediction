# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/common/object_storage.py — Persistence MinIO (S3) + local mirror for EDA reports and ML models.
# =======================================================================


from __future__ import annotations

import logging
import os
from pathlib import Path

from spark.common.config import (
    MINIO_ENDPOINT,
    MODEL_PATH,
    REPORT_EDA_BUCKET,
    REPORT_EDA_S3_PREFIX,
    resolve_model_local_path,
)

logger = logging.getLogger(__name__)


def minio_endpoint() -> str:
    """Endpoint S3/MinIO.

    - Dans Airflow Docker : ``http://minio:9000`` (service compose).
    - En CLI hôte : bascule sur ``localhost`` si ``minio`` ne résout pas.
    """
    ep = os.getenv("MINIO_ENDPOINT", MINIO_ENDPOINT).strip()
    if "minio:" not in ep:
        return ep
    try:
        import socket

        socket.getaddrinfo("minio", 9000, type=socket.SOCK_STREAM)
        return ep
    except OSError:
        return ep.replace("://minio:", "://localhost:")


def minio_credentials() -> tuple[str, str]:
    """Returns the MinIO credentials"""
    access = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "edfadmin"))
    secret = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "edfpassword123"))
    return access, secret


def get_s3_client():
    import boto3
    from botocore.client import Config

    access, secret = minio_credentials()
    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint(),
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
    )


def parse_s3a_uri(uri: str) -> tuple[str, str]:
    """Parses an S3A URI and returns the bucket and prefix"""
    if not uri.startswith("s3a://"):
        raise ValueError(f"URI S3A attendue, reçu : {uri}")
    rest = uri[6:]
    bucket, _, prefix = rest.partition("/")
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return bucket, prefix


def upload_file(local_path: Path | str, bucket: str, key: str) -> str:
    """Uploads a file to MinIO"""
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(path)

    client = get_s3_client()
    client.upload_file(str(path), bucket, key)
    uri = f"s3a://{bucket}/{key}"
    logger.info("Upload MinIO : %s → %s", path.name, uri)
    return uri


def delete_object(bucket: str, key: str) -> None:
    """Deletes an object from MinIO"""
    try:
        get_s3_client().delete_object(Bucket=bucket, Key=key)
    except Exception as exc:
        logger.debug("Suppression MinIO ignorée (%s): %s", key, exc)


def report_s3_key(filename: str) -> str:
    """Returns the S3 key for a report file"""
    prefix = REPORT_EDA_S3_PREFIX.strip("/")
    return f"{prefix}/{filename}" if prefix else filename


def persist_report_file(local_path: Path | str) -> str | None:
    """Persists a report file to MinIO"""
    path = Path(local_path)
    try:
        return upload_file(path, REPORT_EDA_BUCKET, report_s3_key(path.name))
    except Exception as exc:
        logger.warning("Upload rapport EDA échoué (%s) : %s", path.name, exc)
        return None


def remove_report_from_s3(filename: str) -> None:
    """Removes a report file from MinIO"""
    delete_object(REPORT_EDA_BUCKET, report_s3_key(filename))


def sync_report_pngs(local_dir: Path | str, filenames: list[str]) -> list[str]:
    """Upload les PNG listés depuis un répertoire local vers MinIO rapport-eda."""
    root = Path(local_dir)
    uploaded: list[str] = []
    for name in filenames:
        path = root / name
        if not path.is_file():
            continue
        uri = persist_report_file(path)
        if uri:
            uploaded.append(uri)
            logger.info("MinIO sync OK : %s", uri)
    return uploaded


def sync_s3_prefix_to_local(s3_uri: str, local_dir: Path | str) -> int:
    """Synchronizes a S3 prefix to a local directory"""
    bucket, prefix = parse_s3a_uri(s3_uri)
    dest = Path(local_dir)
    dest.mkdir(parents=True, exist_ok=True)

    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/"):
                continue
            relative = key[len(prefix) :] if prefix and key.startswith(prefix) else key
            target = dest / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(target))
            count += 1

    logger.info(
        "Sync MinIO → local : %d objet(s) depuis s3a://%s/%s vers %s",
        count,
        bucket,
        prefix,
        dest,
    )
    return count


def sync_model_artifacts(
    model_s3_uri: str | None = None,
    local_dir: Path | str | None = None,
) -> int:
    """Synchronizes model artifacts from MinIO to a local directory"""
    uri = model_s3_uri or MODEL_PATH
    local = Path(local_dir) if local_dir else resolve_model_local_path()
    return sync_s3_prefix_to_local(uri, local)


def sync_ml_report_artifacts(local_dir: Path | str | None = None) -> int:
    """Synchronise ``{MODEL_PATH}/_report/`` depuis MinIO vers le répertoire local."""
    uri = f"{MODEL_PATH.rstrip('/')}/_report/"
    dest = Path(local_dir) if local_dir else resolve_model_local_path() / "_report"
    dest.mkdir(parents=True, exist_ok=True)
    return sync_s3_prefix_to_local(uri, dest)
