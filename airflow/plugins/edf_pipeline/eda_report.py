# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/plugins/edf_pipeline/eda_report.py — Airflow tasks for EDA reporting.
# =======================================================================

from __future__ import annotations

import logging
import os
from typing import Any

from spark.common.eda_report import (
    generate_data_charts,
    generate_ml_charts,
    mark_ml_pending,
    report_output_dir,
)

logger = logging.getLogger(__name__)


def _fail_pipeline_on_eda_error() -> bool:
    return os.getenv("EDA_FAIL_PIPELINE", "false").lower() in ("1", "true", "yes")


def _handle_eda_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("status") == "failed" and _fail_pipeline_on_eda_error():
        raise RuntimeError(result.get("error", "EDA report failed"))
    return result


def generate_eda_data_report(**context) -> dict[str, Any]:
    """Generate data EDA charts (3 PNG, non-blocking by default)."""
    dag_id = context["dag"].dag_id
    try:
        paths = generate_data_charts(report_output_dir())
        payload = {
            "status": "success",
            "charts": len(paths),
            "paths": [str(p) for p in paths],
            "destination": str(report_output_dir()),
        }
        context["ti"].xcom_push(key="eda_data_charts", value=payload["paths"])
        logger.info(
            "EDA data report (%s): %d chart(s) in %s",
            dag_id,
            payload["charts"],
            payload["destination"],
        )
        return payload
    except Exception as exc:
        logger.exception("EDA data report failed (%s)", dag_id)
        result = {"status": "failed", "error": str(exc)}
        context["ti"].xcom_push(key="eda_data_status", value="failed")
        return _handle_eda_result(result)


def generate_eda_ml_report(**context) -> dict[str, Any]:
    """Generate ML dashboard tiles (up to 6 PNG) after training."""
    dag_id = context["dag"].dag_id
    try:
        paths = generate_ml_charts(report_output_dir(), pending_if_missing=False)
        if not paths:
            msg = "ML metrics absent after training — ML dashboard tiles not generated"
            logger.warning("%s (%s)", msg, dag_id)
            mark_ml_pending(report_output_dir())
            result = {"status": "warning", "reason": msg}
            context["ti"].xcom_push(key="eda_ml_status", value="missing_metrics")
            return _handle_eda_result(result)

        payload = {
            "status": "success",
            "paths": [str(p) for p in paths],
            "charts": len(paths),
            "destination": str(report_output_dir()),
        }
        context["ti"].xcom_push(key="eda_ml_charts", value=payload["paths"])
        logger.info("EDA ML report (%s): %d chart(s)", dag_id, payload["charts"])
        return payload
    except Exception as exc:
        logger.exception("EDA ML report failed (%s)", dag_id)
        result = {"status": "failed", "error": str(exc)}
        context["ti"].xcom_push(key="eda_ml_status", value="failed")
        return _handle_eda_result(result)


def mark_eda_ml_pending(**context) -> dict[str, Any]:
    """Mark chart 04 as pending when ML training was skipped."""
    mark_ml_pending(report_output_dir())
    logger.info(
        "EDA ML report pending — training skipped (%s)",
        context["dag"].dag_id,
    )
    return {"status": "skipped", "reason": "ml_training_skipped"}
