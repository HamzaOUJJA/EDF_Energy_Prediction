# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/transform/bronze.py — Bronze transformations
# =======================================================================

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from spark.common.config import BRONZE_INCLUDE_STREAMING, bronze_raw_path

logger = logging.getLogger(__name__)


def bronze_streaming_path(base: str) -> str:
    """Prefix Parquet Kafka under ``{bronze}/streaming/``."""
    return f"{base.rstrip('/')}/streaming/"


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Read Bronze batch ``raw/`` and merge streaming if enabled."""
    raw_path = bronze_raw_path(path)
    logger.info("Reading Bronze (batch raw) : %s", raw_path)
    df = spark.read.parquet(raw_path)
    n_raw = df.count()
    logger.info("   -> %s raw lines", f"{n_raw:,}")

    if not BRONZE_INCLUDE_STREAMING:
        return df

    stream_path = bronze_streaming_path(path)
    try:
        df_stream = spark.read.parquet(stream_path)
        n_stream = df_stream.count()
        if n_stream > 0:
            df = df.unionByName(df_stream, allowMissingColumns=True)
            logger.info(
                "   -> +%s streaming lines (%s)",
                f"{n_stream:,}",
                stream_path,
            )
    except Exception as exc:
        logger.info("   Streaming ignored (%s) : %s", stream_path, exc)

    return df
