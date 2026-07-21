# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/config.py — configuration centralisée des DAGs EDF (environnement, planification, retries).
# =======================================================================

from __future__ import annotations

import os
from datetime import timedelta

# dev  -> manual trigger only (local development)
# prod -> automatic scheduling via the crons below
EDF_ENVIRONMENT: str = os.getenv("EDF_ENVIRONMENT", "dev").lower()

# Prod crons (overridable via .env)
PROD_PIPELINE_SCHEDULE: str = os.getenv("EDF_PIPELINE_SCHEDULE", "0 3 * * 0")
PROD_ETL_SCHEDULE: str = os.getenv("EDF_ETL_SCHEDULE", "0 2 * * *")
PROD_ML_SCHEDULE: str = os.getenv("EDF_ML_SCHEDULE", "0 4 * * 1")
PROD_QUALITY_SCHEDULE: str = os.getenv("EDF_QUALITY_SCHEDULE", "30 6 * * *")

DAG_ID_PIPELINE_COMPLET: str = "edf_pipeline_complet"
DAG_ID_ETL_STREAMING: str = "edf_etl_pipeline"
DAG_ID_ML: str = "edf_ml_pipeline"
DAG_ID_QUALITY: str = "edf_quality_monitoring"

# Spark cluster pool — one active Spark REST job at a time (professional mode).
AIRFLOW_SPARK_POOL: str = os.getenv("AIRFLOW_SPARK_POOL", "spark_cluster")
AIRFLOW_SPARK_POOL_SLOTS: int = int(os.getenv("AIRFLOW_SPARK_POOL_SLOTS", "1"))

DEFAULT_ARGS: dict = {
    "owner": "edf-etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=8),
}


def get_schedule(prod_cron: str) -> str | None:
    """Return the cron in production, ``None`` in dev (manual trigger)."""
    if EDF_ENVIRONMENT == "prod":
        return prod_cron
    return None


def get_pipeline_schedule() -> str | None:
    """Scheduling ``edf_pipeline_complet``."""
    return get_schedule(PROD_PIPELINE_SCHEDULE)


def get_etl_schedule() -> str | None:
    """Scheduling ``edf_etl_pipeline`` (streaming Kafka)."""
    return get_schedule(PROD_ETL_SCHEDULE)


def get_ml_schedule() -> str | None:
    """Scheduling ``edf_ml_pipeline``."""
    return get_schedule(PROD_ML_SCHEDULE)


def get_quality_schedule() -> str | None:
    """Scheduling ``edf_quality_monitoring``."""
    return get_schedule(PROD_QUALITY_SCHEDULE)
