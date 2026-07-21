# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/jobs/xls_to_bronze.py — Ingestion batch of RTE éco2mix (XLS/TSV) to MinIO Bronze (Parquet partitioned).
# =======================================================================

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from rte_pipeline.parsing.xls import NUMERIC_COLUMNS, parse_tempo_file, parse_xls_file
from spark.common.config import BRONZE_PATH, DATA_DIR
from spark.common.job_runner import configure_job_logging
from spark.common.spark_session import build_spark_session

logger = configure_job_logging("xls_to_bronze")

NUMERIC_COLS = list(NUMERIC_COLUMNS)


def _filter_records_by_year(
    records: list[dict[str, Any]],
    years: set[int] | None,
    *,
    datetime_key: str = "datetime",
) -> list[dict[str, Any]]:
    """Keep only the records whose year (from ``datetime_key``) is in ``years``."""
    if not years:
        return records
    return [
        r for r in records
        if r.get(datetime_key) and int(str(r[datetime_key])[:4]) in years
    ]


def ingest_files_to_bronze(
    data_dir: str,
    bronze_path: str | None = None,
    years: set[int] | None = None,
) -> int:
    """Ingest XLS RTE to Bronze Parquet.

    Parameters
    ----------
    years
        If provided, only records whose year is in this set are (re)written —
        other years already present in Bronze are left untouched (dynamic
        partition overwrite, see below). ``None``/empty = all years.

    Returns
    -------
    int
        Number of lines written.
    """
    bronze_path = bronze_path or BRONZE_PATH
    data_path = Path(data_dir)
    spark = build_spark_session("EDF_XLS_to_Bronze", ssl_enabled=False)
    # Static overwrite (Spark default) would wipe the whole target directory on
    # every write; dynamic mode replaces only the partitions actually written,
    # which is required once `years` can restrict a run to a subset of years.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    xls_files = sorted(
        f for f in data_path.glob("eCO2mix_RTE_*.xls")
        if "tempo" not in f.name.lower()
    )
    logger.info(f"{len(xls_files)} XLS RTE file(s) detected in {data_path}")

    if not xls_files:
        logger.warning("No XLS file to process")
        return 0

    all_records: list[dict[str, Any]] = []
    for f in xls_files:
        all_records.extend(parse_xls_file(str(f)))

    all_records = _filter_records_by_year(all_records, years)

    if not all_records:
        logger.warning("Parsing completed: no usable data")
        return 0

    logger.info(f"Total parsed: {len(all_records):,} lines")

    logger.info("Conversion Spark + typing...")
    df: DataFrame = spark.createDataFrame(all_records)
    df = df.withColumn(
        "datetime",
        F.coalesce(
            F.to_timestamp(F.col("datetime"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("datetime"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("datetime")),
        ),
    )
    for col in NUMERIC_COLS:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    df = (
        df.withColumn("year", F.year("datetime"))
          .withColumn("month", F.month("datetime"))
    )

    target = f"{bronze_path.rstrip('/')}/raw/"
    logger.info(f"Writing Bronze → {target}")
    (
        df.repartition("year", "month")
          .write
          .mode("overwrite")
          .partitionBy("year", "month")
          .option("compression", "snappy")
          .parquet(target)
    )

    written = df.count()
    logger.info(f"{written:,} lines written in Bronze")
    return written


def ingest_tempo_to_bronze(
    data_dir: str,
    bronze_path: str | None = None,
    years: set[int] | None = None,
) -> int:
    """Ingest Tempo EDF (BLUE/WHITE/RED days) to Bronze."""
    bronze_path = bronze_path or BRONZE_PATH
    data_path = Path(data_dir)
    spark = build_spark_session("EDF_XLS_to_Bronze", ssl_enabled=False)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    tempo_files = sorted(data_path.glob("eCO2mix_RTE_tempo*.xls"))
    records: list[dict[str, Any]] = []
    for tempo_path in tempo_files:
        for row in parse_tempo_file(str(tempo_path)):
            records.append({
                "date": row["date"],
                "tempo_color": row["tempo_color"],
                "source_file": row["source_file"],
            })

    records = _filter_records_by_year(records, years, datetime_key="date")

    if not records:
        logger.info("No Tempo file")
        return 0

    df = (
        spark.createDataFrame(records)
        .withColumn("date_parsed", F.to_date("date", "yyyy-MM-dd"))
        .withColumn("year", F.year("date_parsed"))
        .withColumn("month", F.month("date_parsed"))
    )

    target = f"{bronze_path.rstrip('/')}/tempo/"
    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("compression", "snappy")
        .parquet(target)
    )

    count = df.count()
    logger.info(f"{count} Tempo days written in Bronze")
    return count


def run_pipeline(
    data_dir: str | None = None,
    bronze_path: str | None = None,
    years: set[int] | None = None,
) -> dict[str, Any]:
    """Orchestrator of XLS -> Bronze (main entry point).

    Parameters
    ----------
    years
        Restrict ingestion to these years (dynamic partition overwrite keeps
        other years already in Bronze untouched). ``None``/empty = all years.
    """
    data_dir = data_dir or DATA_DIR
    bronze_path = bronze_path or BRONZE_PATH

    started_at = datetime.utcnow()
    logger.info("═" * 78)
    logger.info("  EDF ETL -> Bronze")
    logger.info(f"  Source     : {data_dir}")
    logger.info(f"  Destination: {bronze_path}")
    logger.info(f"  Years      : {sorted(years) if years else 'all'}")
    logger.info("═" * 78)

    count_main = ingest_files_to_bronze(data_dir, bronze_path, years)
    count_tempo = ingest_tempo_to_bronze(data_dir, bronze_path, years)

    duration_s = (datetime.utcnow() - started_at).total_seconds()
    summary = {
        "status": "success",
        "main_rows": count_main,
        "tempo_rows": count_tempo,
        "duration_s": round(duration_s, 2),
    }
    logger.info(f"Pipeline Bronze completed in {duration_s:.1f}s -> {summary}")
    return summary


def _parse_years_arg(raw: str) -> set[int] | None:
    """Parse a comma-separated years CLI arg (e.g. ``"2022,2023"``) into a set."""
    values = {v.strip() for v in raw.split(",") if v.strip()}
    return {int(v) for v in values} if values else None


if __name__ == "__main__":
    cli_data_dir = sys.argv[1] if len(sys.argv) > 1 else DATA_DIR
    cli_bronze = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else BRONZE_PATH
    cli_years = _parse_years_arg(sys.argv[3]) if len(sys.argv) > 3 else None
    run_pipeline(cli_data_dir, cli_bronze, cli_years)
