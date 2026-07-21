# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/common/spark_session.py — Factory SparkSession (MinIO S3A, SQL tuning).
# =======================================================================

from __future__ import annotations

from pyspark.sql import SparkSession

from spark.common.config import (
    MINIO_ACCESS,
    MINIO_ENDPOINT,
    MINIO_SECRET,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_MASTER_URL,
)


def build_spark_session(
    app_name: str,
    *,
    ssl_enabled: bool = False,
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """Returns the active Spark session or creates a configured one for MinIO.

    In cluster mode (Airflow REST), ``getActiveSession()`` returns the session
    already created by SparkSubmit — the jars/S3A config comes from the REST payload.

    Parameters
    ----------
    app_name
        Name of the Spark application.
    ssl_enabled
        Enable TLS for S3A (production outside Docker local).
    extra_configs
        Additional Spark key/value configurations (e.g. ML partitions).
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    builder = (
        SparkSession.builder
        .master(SPARK_MASTER_URL)
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )

    if SPARK_DRIVER_MEMORY:
        builder = builder.config("spark.driver.memory", SPARK_DRIVER_MEMORY)
    if SPARK_EXECUTOR_MEMORY:
        builder = builder.config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)

    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
