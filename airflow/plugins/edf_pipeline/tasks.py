# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/tasks.py — reusable Airflow tasks (Makefile = prod).
# =======================================================================

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import boto3

from edf_pipeline.config import DAG_ID_ETL_STREAMING, DAG_ID_PIPELINE_COMPLET
from edf_pipeline.ml_readiness import MIN_TRAINING_ROWS, count_ml_ready_rows
from edf_pipeline.quality import (
    get_streaming_daily_checks,
    run_checks,
    run_post_etl_quality_checks,
    validate_xls_sources_or_raise,
)
from rte_pipeline.kafka_parquet import (
    RTE_SCHEMA,
    build_streaming_s3_key,
    records_to_parquet,
)

logger = logging.getLogger(__name__)
DATA_DIR: str = os.getenv("DATA_DIR", "/opt/airflow/data/raw")
KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_EP: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_AK: str = os.getenv("MINIO_ACCESS_KEY", "edfadmin")
MINIO_SK: str = os.getenv("MINIO_SECRET_KEY", "edfpassword123")
BRONZE_BUCKET: str = "edf-bronze"


def _ensure_project_path() -> None:
    """Ensure the project path is in the Python path."""
    root = "/opt/airflow/project"
    if root not in sys.path:
        sys.path.insert(0, root)


def validate_xls_sources(**context) -> dict:
    """Validate the XLS consumption (date ranges, missing years) before Bronze."""
    summary = validate_xls_sources_or_raise(DATA_DIR)
    logger.info(
        "%d XLS file(s) — consumption years : %s",
        summary["file_count"],
        summary["years_covered"],
    )
    return summary


def run_post_etl_quality(**context) -> dict:
    """Post-ETL quality checks — critical failures blocking (dev + prod)."""
    fail_on_warning = os.getenv("QUALITY_FAIL_ON_WARNING", "false").lower() in (
        "1",
        "true",
        "yes",
    )
    return run_post_etl_quality_checks(
        source=f"DAG {DAG_ID_PIPELINE_COMPLET}",
        fail_on_critical=True,
        fail_on_warning=fail_on_warning,
    )


def check_ml_readiness(
    task_train: str = "ml.run_gold_to_model",
    task_skip: str = "ml.skip_ml_training",
    **context,
) -> str:
    """Branch : Silver ML-ready volume sufficient."""
    count = count_ml_ready_rows()
    context["ti"].xcom_push(key="n_rows_available", value=count)

    if count >= MIN_TRAINING_ROWS:
        logger.info(
            "ML : %d Silver ML-ready lines >= %d -> training",
            count,
            MIN_TRAINING_ROWS,
        )
        return task_train

    logger.warning(
        "ML ignored : %d lines < %d (ML_MIN_TRAINING_ROWS threshold)",
        count,
        MIN_TRAINING_ROWS,
    )
    return task_skip


def check_kafka_available(**context) -> str:
    """Branch streaming : Kafka reachable or graceful end."""
    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            consumer_timeout_ms=5_000,
        )
        consumer.topics()
        consumer.close()
        logger.info("Kafka available : %s", KAFKA_BOOTSTRAP)
        return "publish_to_kafka"
    except Exception as exc:
        logger.error("Kafka inaccessible (%s) : %s", KAFKA_BOOTSTRAP, exc)
        return "kafka_unavailable"


def publish_to_kafka(**context) -> dict:
    """Publish the XLS of the day on Kafka (streaming path)."""
    _ensure_project_path()
    from rte_pipeline.producers.rte_producer import ingest_all

    raw_dir = str(Path(DATA_DIR))
    if not Path(raw_dir).is_dir():
        raw_dir = str(Path(DATA_DIR) / "raw")
    logger.info("Publication Kafka from : %s", raw_dir)
    stats = ingest_all(raw_dir)
    total_sent = sum(s.get("sent", 0) for s in stats.values())
    context["ti"].xcom_push(key="kafka_sent", value=total_sent)
    return stats


def consume_kafka_to_bronze(**context) -> int:
    """Consume Kafka and write the daily delta in Parquet Bronze (rte_pipeline)."""
    from kafka import KafkaConsumer

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_EP,
        aws_access_key_id=MINIO_AK,
        aws_secret_access_key=MINIO_SK,
    )
    consumer = KafkaConsumer(
        "rte.raw",
        "rte.tempo",
        "rte.realtime",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="airflow-daily-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=60_000,
    )

    by_topic: dict[str, list] = {}
    for msg in consumer:
        by_topic.setdefault(msg.topic, []).append(msg.value)
    consumer.close()

    run_date = context["ds"]
    total = 0
    for topic, records in by_topic.items():
        if not records:
            continue
        payload = records_to_parquet(records, RTE_SCHEMA)
        if not payload:
            continue
        key = build_streaming_s3_key(topic, run_date)
        s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=payload)
        total += len(records)
        logger.info("%d msgs → s3://%s/%s", len(records), BRONZE_BUCKET, key)

    context["ti"].xcom_push(key="bronze_written", value=total)
    return total


def run_daily_quality_checks(**context) -> list:
    """Daily quality checks on the window (streaming pipeline)."""
    summary = run_checks(
        get_streaming_daily_checks(context["ds"]),
        source=f"DAG {DAG_ID_ETL_STREAMING}",
        fail_on_critical=False,
        fail_on_warning=False,
    )
    if summary["failed_warning"] or summary["failed_critical"]:
        logger.warning(
            "Daily quality checks failed (non blocking) : %s",
            summary["failed"],
        )
    return summary["checks"]


def skip_ml_training(**context) -> dict:
    """No-op when the Gold data is insufficient."""
    return {"status": "skipped", "reason": "insufficient_training_rows"}
