# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/jobs/bronze_to_silver.py — Bronze to Silver transformations.
# =======================================================================

from __future__ import annotations

import sys
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.config import (
    BRONZE_PATH,
    SILVER_PATH,
    SILVER_PG_MERGE_KEYS,
    SILVER_PG_WRITE_MODE,
    bronze_raw_path,
)
from spark.common.job_runner import configure_job_logging, get_job_spark, utc_now
from spark.common.postgres import write_to_postgres as _write_pg
from spark.transform.bronze import read_bronze
from spark.transform.silver import (
    SILVER_OUTPUT_COLUMNS,
    SILVER_POSTGRES_COLUMNS,
    clean,
    engineer_features,
)

logger = configure_job_logging("bronze_to_silver")


def write_silver(df: DataFrame, path: str) -> int:
    """Write Silver to Parquet."""
    logger.info("Writing Silver -> %s", path)
    available = [c for c in SILVER_OUTPUT_COLUMNS if c in df.columns]
    df_out = df.select(*available)
    (
        df_out.repartition(F.col("year"), F.col("month"))
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .option("compression", "snappy")
        .parquet(path)
    )
    count = df_out.count()
    logger.info("   -> %s lines Silver", f"{count:,}")
    return count


def run(
    bronze_path: str | None = None,
    silver_path: str | None = None,
    load_postgres: bool = True,
) -> dict[str, Any]:
    """Bronze to Silver transformations."""
    bronze_path = bronze_path or BRONZE_PATH
    silver_path = silver_path or SILVER_PATH

    started_at = utc_now()
    logger.info("═" * 78)
    logger.info("  EDF ETL -> Silver")
    logger.info("  Bronze : %s (+ streaming if enabled)", bronze_raw_path(bronze_path))
    logger.info("  Silver : %s", silver_path)
    logger.info("  Silver PG : %s", SILVER_PG_WRITE_MODE)
    logger.info("═" * 78)

    spark = get_job_spark("EDF_Bronze_to_Silver")
    df_bronze = read_bronze(spark, bronze_path)
    df_silver = engineer_features(clean(df_bronze))
    df_silver.cache()

    written = write_silver(df_silver, silver_path)
    if load_postgres:
        try:
            pg_cols = [c for c in SILVER_POSTGRES_COLUMNS if c in df_silver.columns]
            _write_pg(
                df_silver,
                "dw.fact_consumption_silver",
                columns=pg_cols,
                mode=SILVER_PG_WRITE_MODE,
                merge_keys=SILVER_PG_MERGE_KEYS,
                staging_table=None,
                touch_column="processed_at",
            )
        except Exception as exc:
            logger.error("Loading Postgres failed: %s", exc)

    df_silver.unpersist()
    duration_s = (utc_now() - started_at).total_seconds()
    return {
        "status": "success",
        "rows": written,
        "duration_s": round(duration_s, 2),
    }


if __name__ == "__main__":
    cli_bronze = sys.argv[1] if len(sys.argv) > 1 else BRONZE_PATH
    cli_silver = sys.argv[2] if len(sys.argv) > 2 else SILVER_PATH
    run(cli_bronze, cli_silver)
