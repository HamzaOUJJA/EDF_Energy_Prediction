# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/ml_readiness.py — ML readiness threshold (Makefile = prod).
# =======================================================================

from __future__ import annotations

import os

import psycopg2

from edf_pipeline.db import get_pg_conn

MIN_TRAINING_ROWS: int = int(os.getenv("ML_MIN_TRAINING_ROWS", "1000"))


def count_ml_ready_rows() -> int:
    """Count the number of ML-ready rows in the Silver table."""
    conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) FROM dw.fact_consumption_silver
        WHERE consumption_mw IS NOT NULL
          AND hour IS NOT NULL
          AND day_of_week IS NOT NULL
          AND month IS NOT NULL
        """
    )
    count = int(cur.fetchone()[0])
    cur.close()
    conn.close()
    return count


def ensure_ml_ready_or_raise() -> int:
    """Block the ML step if the Silver has not enough ML-ready rows."""
    count = count_ml_ready_rows()
    if count < MIN_TRAINING_ROWS:
        raise ValueError(
            f"ML ignored : {count} Silver ML-ready rows < {MIN_TRAINING_ROWS} "
            "(ML_MIN_TRAINING_ROWS). Populate Silver before make run-ml."
        )
    return count
