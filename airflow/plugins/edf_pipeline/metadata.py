# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/metadata.py — recording run metadata in ``etl.pipeline_runs`` (Makefile = prod).
# =======================================================================

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import psycopg2

from edf_pipeline.db import get_pg_conn

logger = logging.getLogger(__name__)


def record_pipeline_run(
    dag_id: str,
    run_type: str,
    status: str,
    *,
    rows_read: int | None = None,
    rows_written: int | None = None,
    metadata: dict | None = None,
    error_message: str | None = None,
) -> None:
    conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO etl.pipeline_runs
            (dag_id, run_type, status, finished_at, rows_read, rows_written,
             error_message, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            dag_id,
            run_type,
            status,
            datetime.now(timezone.utc),
            rows_read,
            rows_written,
            error_message,
            json.dumps(metadata or {}),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Run enregistré : %s / %s / %s", dag_id, run_type, status)


def _gold_rows_written(gold: dict) -> int | None:
    if not gold:
        return None
    if "rows_written" in gold:
        return gold.get("rows_written")
    daily = gold.get("daily_rows") or 0
    monthly = gold.get("monthly_rows") or 0
    if daily or monthly:
        return int(daily) + int(monthly)
    return gold.get("rows")


def _query_dw_layer_stats(cur) -> tuple[dict, dict, dict]:
    """Query the DW state after Spark execution (REST submit does not push business XComs)."""
    cur.execute("SELECT COUNT(*) FROM dw.fact_consumption_silver")
    silver_rows = int(cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM dw.agg_daily")
    gold_daily = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM dw.agg_monthly")
    gold_monthly = int(cur.fetchone()[0])

    silver = {"rows": silver_rows}
    gold = {"daily_rows": gold_daily, "monthly_rows": gold_monthly}
    bronze = {"source": "spark_rest", "silver_rows": silver_rows}
    return bronze, silver, gold


def _query_latest_ml_summary(cur) -> dict:
    cur.execute(
        """
        SELECT run_id FROM etl.model_metrics
        ORDER BY trained_at DESC
        LIMIT 1
        """
    )
    latest = cur.fetchone()
    if not latest:
        return {}

    ml_run_id = latest[0]
    cur.execute(
        """
        SELECT model_name, MIN(rmse), COUNT(*)
        FROM etl.model_metrics
        WHERE run_id = %s
        GROUP BY model_name
        ORDER BY MIN(rmse) ASC
        LIMIT 1
        """,
        (ml_run_id,),
    )
    row = cur.fetchone()
    if not row:
        return {"ml_run_id": ml_run_id}

    return {
        "ml_run_id": ml_run_id,
        "best_model": row[0],
        "best_rmse": float(row[1]),
        "models_trained": int(row[2]),
        "status": "success",
    }


def finalize_pipeline_run(**context) -> dict:
    """Aggregate quality + DW state and persist the final run."""
    ti = context["ti"]
    dag_id = context["dag"].dag_id

    quality = ti.xcom_pull(task_ids="quality.run_post_etl_quality") or {}
    ml_skip = ti.xcom_pull(task_ids="ml.skip_ml_training") or {}
    eda_data = ti.xcom_pull(task_ids="reporting.generate_eda_data_report") or {}
    eda_ml = ti.xcom_pull(task_ids="reporting.generate_eda_ml_report") or {}
    eda_ml_pending = ti.xcom_pull(task_ids="reporting.mark_eda_ml_pending") or {}

    conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()
    bronze, silver, gold = _query_dw_layer_stats(cur)
    ml = _query_latest_ml_summary(cur) if ml_skip.get("status") != "skipped" else {}
    cur.close()
    conn.close()

    quality_failed = [
        c["check"]
        for c in (quality.get("checks") or [])
        if not c.get("passed")
    ]
    quality_critical = quality.get("failed_critical") or []

    if quality_critical:
        status = "failed"
    elif quality_failed:
        status = "success_with_warnings"
    elif ml_skip.get("status") == "skipped":
        status = "success_with_warnings"
    else:
        status = "success"

    metadata = {
        "environment": os.getenv("EDF_ENVIRONMENT", "dev"),
        "source": "airflow",
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "quality_failed": quality_failed,
        "quality_critical": quality_critical,
        "ml": ml or ml_skip,
        "eda": {
            "data": eda_data,
            "ml": eda_ml or eda_ml_pending,
        },
        "logical_date": str(context.get("ds")),
    }

    record_pipeline_run(
        dag_id,
        "full_pipeline",
        status,
        rows_written=_gold_rows_written(gold),
        metadata=metadata,
    )
    return {"status": status, "metadata": metadata}


def record_spark_batch_run(source: str = "make pipeline-spark") -> dict:
    """Audit the Spark direct path (debug) — same ``etl.pipeline_runs`` table."""
    conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM dw.fact_consumption_silver")
    silver_rows = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM dw.agg_daily")
    gold_daily = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT check_name FROM etl.data_quality_checks
        WHERE passed = false
          AND checked_at >= NOW() - INTERVAL '2 hours'
        ORDER BY checked_at DESC
        """
    )
    quality_failed = [row[0] for row in cur.fetchall()]
    ml_meta = _query_latest_ml_summary(cur)

    status = "success" if not quality_failed else "success_with_warnings"
    metadata = {
        "environment": os.getenv("EDF_ENVIRONMENT", "dev"),
        "source": source,
        "silver_rows": silver_rows,
        "gold_daily": gold_daily,
        "quality_failed": quality_failed,
        **ml_meta,
    }

    record_pipeline_run(
        "make_pipeline_spark",
        "full_pipeline",
        status,
        rows_written=gold_daily,
        metadata=metadata,
    )

    cur.close()
    conn.close()
    logger.info("Spark run recorded : %s / %s", source, status)
    return {"status": status, "metadata": metadata}


def record_streaming_run(**context) -> None:
    """Audit the daily streaming run."""
    ti = context["ti"]
    run_date = context["ds"]

    conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) FROM dw.fact_consumption_silver
        WHERE DATE(datetime) = %s
        """,
        (run_date,),
    )
    silver_rows = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM dw.agg_daily")
    gold_daily = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM dw.agg_monthly")
    gold_monthly = int(cur.fetchone()[0] or 0)
    cur.close()
    conn.close()

    record_pipeline_run(
        context["dag"].dag_id,
        "streaming",
        "success",
        rows_read=ti.xcom_pull(key="kafka_sent") or 0,
        rows_written=ti.xcom_pull(key="bronze_written") or 0,
        metadata={
            "execution_date": run_date,
            "run_id": context["run_id"],
            "silver_rows_day": silver_rows,
            "gold_daily": gold_daily,
            "gold_monthly": gold_monthly,
        },
    )


def record_ml_run(**context) -> None:
    """Audit the ML run alone (metrics read from ``etl.model_metrics``)."""
    conn = psycopg2.connect(get_pg_conn())
    cur = conn.cursor()
    summary = _query_latest_ml_summary(cur)
    cur.execute("SELECT COUNT(*) FROM dw.fact_consumption_silver WHERE consumption_mw IS NOT NULL")
    ml_ready_rows = int(cur.fetchone()[0] or 0)
    cur.close()
    conn.close()

    record_pipeline_run(
        context["dag"].dag_id,
        "ml_training",
        summary.get("status", "success"),
        rows_read=ml_ready_rows,
        rows_written=summary.get("models_trained", 0),
        metadata={
            "execution_date": context["ds"],
            "run_id": context["run_id"],
            "best_model": summary.get("best_model"),
            "best_rmse": summary.get("best_rmse"),
            "ml_run_id": summary.get("ml_run_id"),
        },
    )
