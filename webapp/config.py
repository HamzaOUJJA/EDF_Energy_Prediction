# =======================================================================
# webapp/config.py — Configuration for the EDF pipeline web app.
# =======================================================================

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(Path(__file__).resolve().parent / ".env")

# ----------------------------------------------------------------------
# Backend / server-side URLs — used by Flask itself to call these services.
# When the webapp runs as a docker-compose service, these are overridden to
# internal service hostnames (e.g. http://airflow-webserver:8080); when run
# locally on the host, the localhost defaults below are correct as-is.
# ----------------------------------------------------------------------
AIRFLOW_BASE_URL: str = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8081").rstrip("/")
AIRFLOW_USERNAME: str = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD: str = os.getenv("AIRFLOW_PASSWORD", "admin123")

PG_HOST: str = os.getenv("PG_HOST", "localhost")
PG_PORT: int = int(os.getenv("PG_PORT", "5432"))
PG_USER: str = os.getenv("PG_USER", "edf")
PG_PASSWORD: str = os.getenv("PG_PASSWORD", "edf123")
PG_DATABASE: str = os.getenv("PG_DATABASE", "edf_dw")

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "edfadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "edfpassword123")

SPARK_MASTER_INTERNAL_URL: str = os.getenv("SPARK_MASTER_INTERNAL_URL", "http://localhost:8082").rstrip("/")
KAFKA_BOOTSTRAP_HOST: str = os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost")
KAFKA_BOOTSTRAP_PORT: int = int(os.getenv("KAFKA_BOOTSTRAP_PORT", "29092"))
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

# ----------------------------------------------------------------------
# Browser-facing URLs — rendered as links / iframe src. The user's browser
# always runs on the host, so these must stay host-exposed ports regardless
# of whether Flask itself runs in Docker or locally — never override these
# to internal service hostnames.
# ----------------------------------------------------------------------
AIRFLOW_UI_URL: str = os.getenv("AIRFLOW_UI_URL", "http://localhost:8081").rstrip("/")
SPARK_MASTER_UI_URL: str = os.getenv("SPARK_MASTER_UI_URL", "http://localhost:8082").rstrip("/")
MINIO_CONSOLE_URL: str = os.getenv("MINIO_CONSOLE_URL", "http://localhost:9001").rstrip("/")
GRAFANA_BASE_URL: str = os.getenv("GRAFANA_BASE_URL", "http://localhost:3001").rstrip("/")

EDF_DAG_ID: str = os.getenv("EDF_DAG_ID", "edf_pipeline_complet")

DATA_DIR: Path = Path(os.getenv("DATA_DIR") or (REPO_ROOT / "data" / "raw"))

FLASK_HOST: str = os.getenv("FLASK_HOST", "127.0.0.1")
FLASK_PORT: int = int(os.getenv("FLASK_PORT", "5000"))
FLASK_DEBUG: bool = os.getenv("FLASK_DEBUG", "true").lower() in ("1", "true", "yes")
