# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/common/postgres.py — Write JDBC to PostgreSQL (data warehouse).
# =======================================================================

from __future__ import annotations

import logging
import os
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.common.config import (
    PG_PASS,
    PG_URL,
    PG_USER,
    SILVER_PG_MERGE_KEYS,
    SILVER_PG_STAGING_TABLE,
)

logger = logging.getLogger(__name__)

_JDBC_OPTS = {
    "url": PG_URL,
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")


def _pg_psycopg2_dsn() -> str:
    """DSN for psycopg2 (driver-side SQL after JDBC staging load)."""
    explicit = os.getenv("POSTGRES_CONN", "").strip()
    if explicit:
        return explicit
    if PG_URL.startswith("jdbc:"):
        return PG_URL[len("jdbc:") :]
    return f"postgresql://{PG_USER}:{PG_PASS}@postgres:5432/edf_dw"


def _assert_safe_identifier(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def _execute_pg_sql(sql: str) -> int:
    import psycopg2

    with psycopg2.connect(_pg_psycopg2_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rowcount = cur.rowcount
        conn.commit()
    return rowcount


def _jdbc_reader(spark, dbtable: str):
    """Build a JDBC reader for a table or subquery."""
    reader = spark.read.format("jdbc")
    for key, value in _JDBC_OPTS.items():
        reader = reader.option(key, value)
    return reader.option("dbtable", dbtable)


def _existing_keys_subquery(table: str, keys: list[str], df: DataFrame) -> str:
    """Scope existing keys to the batch datetime window when possible."""
    key_list = ", ".join(_assert_safe_identifier(k) for k in keys)
    table = _assert_safe_identifier(table)
    if "datetime" in keys and "datetime" in df.columns:
        bounds = df.agg(
            F.min("datetime").alias("min_dt"),
            F.max("datetime").alias("max_dt"),
        ).collect()[0]
        if bounds["min_dt"] is not None and bounds["max_dt"] is not None:
            min_dt = bounds["min_dt"]
            max_dt = bounds["max_dt"]
            return (
                f"(SELECT DISTINCT {key_list} FROM {table} "
                f"WHERE datetime >= TIMESTAMP '{min_dt}' "
                f"AND datetime <= TIMESTAMP '{max_dt}') AS existing_keys"
            )
    return f"(SELECT DISTINCT {key_list} FROM {table}) AS existing_keys"


def _write_append(
    df: DataFrame,
    table: str,
    *,
    columns: list[str],
    batch_size: int,
) -> None:
    table = _assert_safe_identifier(table)
    logger.info("Loading PostgreSQL -> %s (%d columns, append)", table, len(columns))
    (
        df.select(*columns)
        .write.format("jdbc")
        .options(**_JDBC_OPTS)
        .option("dbtable", table)
        .option("batchsize", str(batch_size))
        .mode("append")
        .save()
    )


def _merge_append_to_postgres(
    df: DataFrame,
    table: str,
    *,
    columns: list[str],
    merge_keys: list[str],
    batch_size: int,
) -> None:
    """Insert only rows whose merge keys are not already present in PostgreSQL."""
    spark = df.sparkSession
    subquery = _existing_keys_subquery(table, merge_keys, df)
    existing = _jdbc_reader(spark, subquery).load()

    new_df = df.join(existing, on=merge_keys, how="left_anti")
    total = df.count()
    new_count = new_df.count()
    logger.info(
        "PostgreSQL merge -> %s: %d new row(s) / %d in batch (keys=%s)",
        table,
        new_count,
        total,
        ",".join(merge_keys),
    )
    if new_count == 0:
        return

    _write_append(new_df, table, columns=columns, batch_size=batch_size)


def _upsert_via_staging(
    df: DataFrame,
    table: str,
    *,
    columns: list[str],
    merge_keys: list[str],
    batch_size: int,
    staging_table: str,
    touch_column: str | None = None,
) -> None:
    """Staging table + ``INSERT … ON CONFLICT DO UPDATE`` (sync PG with batch)."""
    table = _assert_safe_identifier(table)
    staging_table = _assert_safe_identifier(staging_table)
    for key in merge_keys:
        _assert_safe_identifier(key)
    for col in columns:
        _assert_safe_identifier(col)

    total = df.count()
    if total == 0:
        logger.info("PostgreSQL upsert -> %s: empty batch, skipped", table)
        return

    _execute_pg_sql(f"TRUNCATE TABLE {staging_table}")
    _write_append(df, staging_table, columns=columns, batch_size=batch_size)

    insert_cols = ", ".join(columns)
    conflict_cols = ", ".join(merge_keys)
    update_cols = [c for c in columns if c not in merge_keys]
    set_parts = [f"{col} = EXCLUDED.{col}" for col in update_cols]
    if touch_column:
        touch_column = _assert_safe_identifier(touch_column)
        if touch_column not in merge_keys and touch_column not in update_cols:
            set_parts.append(f"{touch_column} = NOW()")
    set_clause = ", ".join(set_parts)

    sql = f"""
        INSERT INTO {table} ({insert_cols})
        SELECT {insert_cols} FROM {staging_table}
        ON CONFLICT ({conflict_cols}) DO UPDATE SET
            {set_clause}
    """
    affected = _execute_pg_sql(sql)
    logger.info(
        "PostgreSQL upsert -> %s: batch=%d row(s), driver rowcount=%d (keys=%s)",
        table,
        total,
        affected,
        conflict_cols,
    )


def write_to_postgres(
    df: DataFrame,
    table: str,
    *,
    columns: list[str] | None = None,
    mode: str = "append",
    batch_size: int = 5000,
    merge_keys: list[str] | None = None,
    staging_table: str | None = None,
    touch_column: str | None = None,
) -> None:
    """Write a Spark DataFrame to PostgreSQL via JDBC.

    Parameters
    ----------
    df
        DataFrame source.
    table
        Target table (e.g. ``dw.fact_consumption_silver``).
    columns
        Subset of columns to write; by default all columns of the DF.
    mode
        ``append``, ``overwrite`` (truncate), ``merge`` (insert-if-not-exists),
        or ``upsert`` (insert + update on conflict).
    batch_size
        JDBC batch size.
    merge_keys
        Key columns for ``merge`` / ``upsert`` (default: ``datetime``).
    staging_table
        Staging table for ``upsert`` (required for Gold ; Silver default below).
    touch_column
        Optional timestamp column refreshed on update (e.g. ``processed_at``).
    """
    if not PG_URL or not PG_USER:
        logger.warning("PostgreSQL connection not configured — skipping step")
        return

    selected = columns or df.columns
    available = [c for c in selected if c in df.columns]
    if not available:
        logger.warning("No columns to load into %s", table)
        return

    keys = merge_keys or list(SILVER_PG_MERGE_KEYS) or ["datetime"]
    normalized_mode = mode.lower()

    if normalized_mode in ("merge", "upsert"):
        missing = [k for k in keys if k not in available]
        if missing:
            raise ValueError(f"{normalized_mode} keys missing from DataFrame columns: {missing}")

    if normalized_mode == "merge":
        _merge_append_to_postgres(
            df,
            table,
            columns=available,
            merge_keys=keys,
            batch_size=batch_size,
        )
        return

    if normalized_mode == "upsert":
        effective_staging = staging_table or SILVER_PG_STAGING_TABLE
        _upsert_via_staging(
            df,
            table,
            columns=available,
            merge_keys=keys,
            batch_size=batch_size,
            staging_table=effective_staging,
            touch_column=touch_column,
        )
        return

    logger.info("Loading PostgreSQL -> %s (%d columns, %s)", table, len(available), normalized_mode)
    writer = (
        df.select(*available)
        .write.format("jdbc")
        .options(**_JDBC_OPTS)
        .option("dbtable", table)
        .option("batchsize", str(batch_size))
    )
    # overwrite JDBC = DROP TABLE by default — incompatible with DW views (v_annual_summary…)
    if normalized_mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.mode(normalized_mode).save()
