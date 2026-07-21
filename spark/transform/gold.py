# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/transform/gold.py — Gold transformations
# =======================================================================

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def read_silver(spark: SparkSession, path: str) -> DataFrame:
    logger.info("Reading Silver: %s", path)
    df = spark.read.parquet(path)
    logger.info("   -> %s lines", f"{df.count():,}")
    return df


def compute_daily_aggregates(df: DataFrame) -> DataFrame:
    logger.info("Daily aggregates Gold...")

    df = df.withColumn("date", F.to_date("datetime"))

    base = df.groupBy(
        "date", "year", "month", "day_of_week", "season", "is_weekend"
    ).agg(
        F.avg("consumption_mw").alias("consumption_mean_mw"),
        F.min("consumption_mw").alias("consumption_min_mw"),
        F.max("consumption_mw").alias("consumption_max_mw"),
        F.stddev("consumption_mw").alias("consumption_std_mw"),
        (F.sum("consumption_mw") * 0.5 / 1000).alias("consumption_total_gwh"),
        F.avg("forecast_error_pct").alias("forecast_mape_pct"),
        F.avg(F.abs(F.col("forecast_error_mw"))).alias("forecast_mae_mw"),
        F.avg("nuclear_mw").alias("nuclear_mean_mw"),
        F.avg("wind_mw").alias("wind_mean_mw"),
        F.avg("solar_mw").alias("solar_mean_mw"),
        F.avg("hydro_mw").alias("hydro_mean_mw"),
        F.avg("renewable_share_pct").alias("renewable_share_pct"),
        F.avg("nuclear_share_pct").alias("nuclear_share_pct"),
        F.avg("co2_rate").alias("co2_mean_gkwh"),
        F.min("co2_rate").alias("co2_min_gkwh"),
        F.max("co2_rate").alias("co2_max_gkwh"),
        F.count("*").alias("records_count"),
        F.sum(F.when(F.col("is_interpolated"), 1).otherwise(0)).alias(
            "interpolated_count"
        ),
        F.avg("quality_score").alias("quality_score_mean"),
    )

    peak_hours = (
        df.withColumn(
            "rk",
            F.rank().over(
                Window.partitionBy(F.to_date("datetime")).orderBy(
                    F.col("consumption_mw").desc()
                )
            ),
        )
        .filter(F.col("rk") == 1)
        .select(
            F.to_date("datetime").alias("date"),
            F.col("hour").alias("peak_hour"),
            F.col("consumption_mw").alias("peak_consumption_mw"),
        )
        .dropDuplicates(["date"])
    )

    aggregates = base.join(peak_hours, on="date", how="left")
    logger.info("   -> %s days aggregated", f"{aggregates.count():,}")
    return aggregates


def compute_monthly_aggregates(daily_df: DataFrame) -> DataFrame:
    logger.info("Monthly aggregates Gold (YoY)...")

    monthly = daily_df.groupBy("year", "month", "season").agg(
        F.avg("consumption_mean_mw").alias("consumption_mean_mw"),
        (F.sum("consumption_total_gwh") / 1000).alias("consumption_total_twh"),
        F.avg("renewable_share_pct").alias("renewable_pct"),
        F.avg("nuclear_share_pct").alias("nuclear_share_pct"),
        F.avg("co2_mean_gkwh").alias("co2_mean_gkwh"),
        F.count("date").alias("trading_days"),
    )

    w_yoy = Window.partitionBy("month").orderBy("year")
    monthly = monthly.withColumn(
        "consumption_yoy_pct",
        F.when(
            F.lag("consumption_total_twh").over(w_yoy).isNotNull(),
            (
                F.col("consumption_total_twh")
                - F.lag("consumption_total_twh").over(w_yoy)
            )
            / F.lag("consumption_total_twh").over(w_yoy)
            * 100,
        ).otherwise(None),
    )

    logger.info("   -> %s months aggregated", f"{monthly.count():,}")
    return monthly


def write_gold_parquet(
    df: DataFrame, path: str, partition_cols: list[str]
) -> None:
    logger.info("Writing Gold -> %s", path)
    (
        df.write.mode("overwrite")
        .partitionBy(*partition_cols)
        .option("compression", "snappy")
        .parquet(path)
    )
