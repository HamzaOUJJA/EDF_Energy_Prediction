# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/ml/training.py — Spark ML model training and evaluation
# =======================================================================

from __future__ import annotations

import gc
import logging
from datetime import datetime
from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.config import GOLD_PATH
from spark.ml.constants import (
    CANDIDATE_FEATURES,
    LABEL_COL,
    ML_GBT_MAX_DEPTH,
    ML_GBT_MAX_ITER,
    ML_RF_MAX_DEPTH,
    ML_RF_NUM_TREES,
    ML_SPARK_PARTITIONS,
    RANDOM_SEED,
    TRAIN_RATIO,
)

logger = logging.getLogger("gold_to_model")

# Load training data
def load_training_data(spark: SparkSession, silver_path: str) -> DataFrame:
    """Load Silver layer and keep only usable ML rows."""
    logger.info("Loading training data: %s", silver_path)
    df = spark.read.parquet(silver_path)
    initial = df.count()

    df = df.filter(F.col(LABEL_COL).isNotNull())
    df = df.filter(F.col("lag_24h_mw").isNotNull())
    logger.info("   Usable ML rows: %s / %s", f"{df.count():,}", f"{initial:,}")
    return df

# Build feature pipeline
def build_feature_pipeline(feature_cols: list[str]) -> VectorAssembler:
    return VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

# Time-based split
def time_based_split(
    df: DataFrame, train_ratio: float = TRAIN_RATIO
) -> tuple[DataFrame, DataFrame]:
    """Split temporal TRAIN/TEST (without global shuffle)."""
    total = df.count()
    train_count = max(1, int(total * train_ratio))

    cutoff_row = (
        df.select("datetime")
        .orderBy("datetime")
        .limit(train_count)
        .agg(F.max("datetime").alias("cutoff"))
        .collect()[0]
    )
    cutoff = cutoff_row["cutoff"]

    train = df.filter(F.col("datetime") <= cutoff).repartition(ML_SPARK_PARTITIONS)
    test = df.filter(F.col("datetime") > cutoff).repartition(ML_SPARK_PARTITIONS)

    logger.info(
        "   TRAIN : ~%s rows | TEST : ~%s rows | cutoff=%s",
        f"{train_count:,}",
        f"{total - train_count:,}",
        cutoff,
    )
    return train, test


# Materialize splits
def materialize_splits(
    spark: SparkSession,
    train: DataFrame,
    test: DataFrame,
    run_id: str,
) -> tuple[DataFrame, DataFrame, int, int]:
    """Write then read train/test to cut Spark lineage."""
    tmp = f"{GOLD_PATH.rstrip('/')}/_ml_cache/{run_id}"
    train_path = f"{tmp}/train"
    test_path = f"{tmp}/test"
    train.write.mode("overwrite").parquet(train_path)
    test.write.mode("overwrite").parquet(test_path)
    train_df = spark.read.parquet(train_path).repartition(ML_SPARK_PARTITIONS)
    test_df = spark.read.parquet(test_path).repartition(ML_SPARK_PARTITIONS)
    n_train = train_df.count()
    n_test = test_df.count()
    logger.info("   Materialized Parquet: %s train / %s test", f"{n_train:,}", f"{n_test:,}")
    return train_df, test_df, n_train, n_test


# Get models
def get_models() -> dict[str, Any]:
    return {
        "LinearRegression": LinearRegression(
            featuresCol="features", labelCol=LABEL_COL,
            maxIter=20, regParam=0.01, elasticNetParam=0.5,
        ),
        "DecisionTree": DecisionTreeRegressor(
            featuresCol="features", labelCol=LABEL_COL,
            maxDepth=8, seed=RANDOM_SEED,
        ),
        "RandomForest": RandomForestRegressor(
            featuresCol="features", labelCol=LABEL_COL,
            numTrees=ML_RF_NUM_TREES, maxDepth=ML_RF_MAX_DEPTH, seed=RANDOM_SEED,
        ),
        "GradientBoosting": GBTRegressor(
            featuresCol="features", labelCol=LABEL_COL,
            maxIter=ML_GBT_MAX_ITER, maxDepth=ML_GBT_MAX_DEPTH, seed=RANDOM_SEED,
        ),
    }


def evaluate_model(predictions: DataFrame, label_col: str = LABEL_COL) -> dict[str, float]:
    rmse_eval = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="rmse"
    )
    mae_eval = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="mae"
    )
    r2_eval = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="r2"
    )

    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)

    mape_row = (
        predictions
        .filter(F.col(label_col) > 0)
        .agg(
            (F.avg(F.abs((F.col(label_col) - F.col("prediction"))
                         / F.col(label_col))) * 100).alias("mape")
        )
        .first()
    )
    mape = float(mape_row["mape"]) if mape_row and mape_row["mape"] is not None else float("nan")

    return {"rmse": float(rmse), "mae": float(mae), "r2": float(r2), "mape": mape}

# Train and compare models
def train_and_compare(
    train: DataFrame,
    test: DataFrame,
    feature_cols: list[str],
    model_path: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Train all models, calculate metrics and save as we go."""
    assembler = build_feature_pipeline(feature_cols)

    results: dict[str, dict[str, Any]] = {}
    saved_paths: dict[str, str] = {}

    for name, estimator in get_models().items():
        logger.info("─" * 60)
        logger.info("Training: %s", name)
        t0 = datetime.utcnow()

        pipeline = Pipeline(stages=[assembler, estimator])
        model = pipeline.fit(train)
        train_time = (datetime.utcnow() - t0).total_seconds()

        preds = model.transform(test)
        metrics = evaluate_model(preds)
        metrics["train_time_s"] = round(train_time, 2)

        results[name] = metrics
        target = f"{model_path.rstrip('/')}/{name}/"
        model.write().overwrite().save(target)
        saved_paths[name] = target
        del model
        gc.collect()

        logger.info(
            "   OK %s | RMSE=%.1f | MAE=%.1f | MAPE=%.2f%% | R²=%.4f | train=%.1fs",
            name,
            metrics["rmse"],
            metrics["mae"],
            metrics["mape"],
            metrics["r2"],
            train_time,
        )

    return results, saved_paths

# Save best model
def save_best_model(
    saved_paths: dict[str, str],
    results: dict[str, dict[str, Any]],
    model_path: str,
) -> str:
    """Copy the best model (RMSE minimal) to ``{model_path}/best/``."""
    if not results or not saved_paths:
        raise ValueError("No trained model — impossible to select the best")

    best_name = min(results.items(), key=lambda x: x[1]["rmse"])[0]
    source = saved_paths.get(best_name)
    if not source:
        raise ValueError(f"Missing path for model {best_name}")

    dest = f"{model_path.rstrip('/')}/best/"
    spark = SparkSession.getActiveSession()
    if spark is None:
        logger.warning(
            "Session Spark inactive — best model kept only under %s",
            source,
        )
        return best_name

    PipelineModel.load(source).write().overwrite().save(dest)
    logger.info("Best model %s (RMSE=%.1f) → %s", best_name, results[best_name]["rmse"], dest)
    return best_name
