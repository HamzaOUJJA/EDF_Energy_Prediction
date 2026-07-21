# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/common/config.py — Centralized configuration for Spark jobs (environment variables).
# =======================================================================

from __future__ import annotations

import os
from pathlib import Path


def _env(key: str, default: str) -> str:
    """Returns the environment variable value or the default value."""
    return os.getenv(key, default)

# MinIO / S3A configuration.
MINIO_ENDPOINT: str = _env("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS: str = _env(
    "MINIO_ACCESS_KEY",
    _env("MINIO_ROOT_USER", "edfadmin"),
)
MINIO_SECRET: str = _env(
    "MINIO_SECRET_KEY",
    _env("MINIO_ROOT_PASSWORD", "edfpassword123"),
)

# Spark (jobs executed on the standalone cluster) configuration.
SPARK_MASTER_URL: str = _env("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_PACKAGES: str = _env(
    "SPARK_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.7.4",
)
SPARK_DRIVER_MEMORY: str = _env("SPARK_DRIVER_MEMORY", "")
SPARK_EXECUTOR_MEMORY: str = _env("SPARK_EXECUTOR_MEMORY", "")

# Bronze path configuration.
BRONZE_PATH: str = _env("BRONZE_PATH", "s3a://edf-bronze/rte/")
SILVER_PATH: str = _env("SILVER_PATH", "s3a://edf-silver/rte/")
GOLD_PATH: str = _env("GOLD_PATH", "s3a://edf-gold/rte/")
MODELS_BUCKET: str = _env("MODELS_BUCKET", "models")
MODEL_PATH: str = _env(
    "ML_MODEL_PATH",
    _env("MODEL_PATH", f"s3a://{MODELS_BUCKET}/rte/"),
)
MODEL_LOCAL_PATH: str = _env(
    "MODEL_LOCAL_PATH",
    "/opt/spark-data/models/rte",
)


def project_root() -> Path:
    return Path(os.getenv("EDF_PROJECT_ROOT", Path.cwd()))


def resolve_model_local_path() -> Path:
    """Chemin local des modèles — conteneur Docker ou hôte ``data/models/rte``."""
    root = project_root()
    candidates: list[Path] = []

    env_path = os.getenv("MODEL_LOCAL_PATH", "").strip()
    if env_path:
        p = Path(env_path)
        candidates.append(p if p.is_absolute() else root / p)

    default = Path(MODEL_LOCAL_PATH)
    if default not in candidates:
        candidates.append(default)

    host_default = root / "data" / "models" / "rte"
    if host_default not in candidates:
        candidates.append(host_default)

    for path in candidates:
        if path.exists():
            return path
    return host_default


REPORT_EDA_BUCKET: str = _env("REPORT_EDA_BUCKET", "rapport-eda")
REPORT_EDA_S3_PREFIX: str = _env("REPORT_EDA_S3_PREFIX", "")
REPORT_EDA_LOCAL: str = _env("REPORT_EDA_LOCAL", "data/eda/report")
DATA_DIR: str = _env("DATA_DIR", "/opt/airflow/data")

# Merge Bronze streaming (Kafka) into Silver when true.
BRONZE_INCLUDE_STREAMING: bool = _env(
    "BRONZE_INCLUDE_STREAMING", "true"
).lower() in ("1", "true", "yes")

# Silver PostgreSQL write mode:
#   upsert    — insert + update on conflict (default, PG aligned with Parquet)
#   merge     — insert if key absent only
#   overwrite — truncate + full reload
#   append    — blind append (legacy, may duplicate)
SILVER_PG_WRITE_MODE: str = _env("SILVER_PG_WRITE_MODE", "upsert").lower()
SILVER_PG_MERGE_KEYS: list[str] = [
    k.strip()
    for k in _env("SILVER_PG_MERGE_KEYS", "datetime").split(",")
    if k.strip()
]
SILVER_PG_STAGING_TABLE: str = _env(
    "SILVER_PG_STAGING_TABLE",
    "dw.fact_consumption_silver_staging",
)

# Gold PostgreSQL write mode (same semantics as Silver).
GOLD_PG_WRITE_MODE: str = _env("GOLD_PG_WRITE_MODE", "upsert").lower()
GOLD_DAILY_STAGING_TABLE: str = _env(
    "GOLD_DAILY_STAGING_TABLE",
    "dw.agg_daily_staging",
)
GOLD_MONTHLY_STAGING_TABLE: str = _env(
    "GOLD_MONTHLY_STAGING_TABLE",
    "dw.agg_monthly_staging",
)

# PostgreSQL (JDBC) configuration.
PG_URL: str = _env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/edf_dw")
PG_USER: str = _env("POSTGRES_USER", "edf")
PG_PASS: str = _env("POSTGRES_PASSWORD", "edf123")


def bronze_raw_path(base: str | None = None) -> str:
    """Parquet batch XLS path (``.../raw/``) — excludes tempo and streaming."""
    explicit = os.getenv("BRONZE_RAW_PATH")
    if explicit:
        return explicit if explicit.endswith("/") else f"{explicit}/"
    root = (base or BRONZE_PATH).rstrip("/")
    return f"{root}/raw/"
