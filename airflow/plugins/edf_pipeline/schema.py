# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/schema.py — idempotent PostgreSQL schema initialization (Makefile = prod).
# =======================================================================

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import psycopg2

from edf_pipeline.db import get_pg_conn

logger = logging.getLogger(__name__)


def _resolve_schema_dw_path() -> Path:
    """Resolve the schema_dw.sql path."""
    explicit = os.getenv("SCHEMA_DW_SQL", "").strip()
    if explicit:
        return Path(explicit)

    candidates = [
        Path(__file__).resolve().parents[3] / "infra/postgres/schema_dw.sql",
        Path("/opt/airflow/infra/postgres/schema_dw.sql"),
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError(
        "schema_dw.sql introuvable. Définissez SCHEMA_DW_SQL ou montez infra/postgres "
        "dans le conteneur Airflow."
    )


def _split_sql_statements(sql: str) -> list[str]:
    """Split a SQL script into statements (ignore line comments)."""
    lines: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    body = "\n".join(lines)
    parts = re.split(r";\s*\n", body)
    return [p.strip() for p in parts if p.strip()]


def _execute_sql_file(cur, path: Path) -> None:
    """Execute a SQL file."""
    sql = path.read_text(encoding="utf-8")
    for stmt in _split_sql_statements(sql):
        try:
            cur.execute(stmt)
        except Exception as exc:
            msg = str(exc).lower()
            if "already exists" in msg or "duplicate" in msg:
                logger.debug("Statement ignoré (déjà présent) : %s", exc)
            else:
                logger.warning("Statement SQL : %s", exc)


def init_postgres_schema(**context) -> dict:
    """Apply the DW schema completely from schema_dw.sql."""
    schema_path = _resolve_schema_dw_path()
    conn = psycopg2.connect(get_pg_conn())
    conn.autocommit = True
    cur = conn.cursor()
    logger.info("PostgreSQL initialization from %s", schema_path)
    _execute_sql_file(cur, schema_path)

    cur.close()
    conn.close()
    logger.info("PostgreSQL schema ready")
    return {"status": "success", "schema_file": str(schema_path)}


def ensure_model_metrics_schema(**context) -> dict:
    """Create ``etl.model_metrics`` (already in schema_dw.sql ; idempotent guard)."""
    from spark.ml.metrics_store import ensure_model_metrics_table

    ensure_model_metrics_table()
    logger.info("etl.model_metrics schema ready")
    return {"status": "success"}
