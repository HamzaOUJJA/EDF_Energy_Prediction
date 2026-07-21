# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/ml/metrics_store.py — Persist ML metrics to PostgreSQL (psycopg2 — compatible mode local Spark).
# =======================================================================

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any
import psycopg2
from psycopg2.extras import execute_batch

# Get PostgreSQL connection
def _pg_conn() -> str:
    return os.getenv(
        "POSTGRES_CONN",
        "postgresql://edf:edf123@postgres:5432/edf_dw",
    )

logger = logging.getLogger("gold_to_model")

_MODEL_METRICS_DDL: tuple[str, ...] = (
    "CREATE SCHEMA IF NOT EXISTS etl",
    """
    CREATE TABLE IF NOT EXISTS etl.model_metrics (
        id BIGSERIAL PRIMARY KEY,
        run_id VARCHAR(50),
        model_name VARCHAR(100) NOT NULL,
        rmse DOUBLE PRECISION,
        mae DOUBLE PRECISION,
        mape_pct DOUBLE PRECISION,
        r2 DOUBLE PRECISION,
        train_time_s DOUBLE PRECISION,
        trained_at TIMESTAMP DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_model_metrics_trained_at "
    "ON etl.model_metrics (trained_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_model_metrics_model "
    "ON etl.model_metrics (model_name)",
)

_INSERT_SQL = """
    INSERT INTO etl.model_metrics
        (run_id, model_name, rmse, mae, mape_pct, r2, train_time_s, trained_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Ensure model metrics table
def ensure_model_metrics_table() -> None:
    """Create schema/table if necessary (idempotent)."""
    conn = psycopg2.connect(_pg_conn())
    cur = conn.cursor()
    try:
        for ddl in _MODEL_METRICS_DDL:
            cur.execute(ddl.strip())
        conn.commit()
    finally:
        cur.close()
        conn.close()

# Persist metrics to PostgreSQL
def persist_metrics_to_postgres(
    results: dict[str, dict[str, Any]],
    run_id: str | None = None,
) -> None:
    """Insert metrics into ``etl.model_metrics``."""
    if not results:
        logger.warning("No metrics to persist")
        return

    ensure_model_metrics_table()
    now = datetime.now(timezone.utc)
    rid = run_id or now.strftime("%Y%m%d_%H%M%S")

    batch = [
        (
            rid,
            name,
            float(m["rmse"]),
            float(m["mae"]),
            float(m["mape"]),
            float(m["r2"]),
            float(m["train_time_s"]),
            now,
        )
        for name, m in results.items()
    ]

    conn = psycopg2.connect(_pg_conn())
    cur = conn.cursor()
    try:
        execute_batch(cur, _INSERT_SQL, batch)
        conn.commit()
        logger.info(
            "Persist metrics → etl.model_metrics (%d models, run_id=%s)",
            len(batch),
            rid,
        )
    finally:
        cur.close()
        conn.close()
