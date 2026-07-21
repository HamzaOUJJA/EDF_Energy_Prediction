# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/common/job_runner.py — Bootstrap partagé pour les jobs Spark (logging + session).
# =======================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from spark.common.spark_session import build_spark_session


def configure_job_logging(name: str) -> logging.Logger:
    """Configure the logging once and return the job logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        force=True,
    )
    return logging.getLogger(name)


def get_job_spark(
    app_name: str,
    *,
    ssl_enabled: bool = False,
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """Standardized Spark session for all ETL/ML jobs."""
    return build_spark_session(
        app_name,
        ssl_enabled=ssl_enabled,
        extra_configs=extra_configs,
    )


def utc_now() -> datetime:
    """UTC timestamp (replaces deprecated datetime.utcnow)."""
    return datetime.now(timezone.utc)
