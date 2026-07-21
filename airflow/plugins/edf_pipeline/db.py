# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/db.py — database access (Makefile = prod).
# =======================================================================

from __future__ import annotations

import os


def get_pg_conn() -> str:
    """Get the PostgreSQL connection string from the environment."""
    return os.getenv(
        "POSTGRES_CONN",
        "postgresql://edf:edf123@postgres:5432/edf_dw",
    )
