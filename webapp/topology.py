# =======================================================================
# webapp/topology.py — Maps the real edf_pipeline_complet DAG TaskGroups to
# a topology diagram: stage definitions, state rollup from Airflow task
# instances, and one small metric per stage.
# =======================================================================

from __future__ import annotations

from typing import Any, Callable

import db

# Task groups + task_ids exactly as defined in airflow/dags/edf_pipeline_complet.py.
# (Airflow's REST API exposes TaskGroup members as "<group_id>.<task_id>".)
STAGES: list[dict[str, Any]] = [
    {
        "id": "prerequisites",
        "label": "Prerequisites",
        "tooltip": "Validation and DW schema",
        "task_ids": [
            "prerequisites.validate_xls_sources",
            "prerequisites.init_postgres_schema",
        ],
    },
    {
        "id": "bronze",
        "label": "Bronze",
        "tooltip": "XLS -> Parquet MinIO",
        "task_ids": ["bronze.run_xls_to_bronze"],
    },
    {
        "id": "silver",
        "label": "Silver",
        "tooltip": "Bronze -> PostgreSQL Silver",
        "task_ids": ["silver.run_bronze_to_silver"],
    },
    {
        "id": "gold",
        "label": "Gold",
        "tooltip": "Daily / monthly aggregations",
        "task_ids": ["gold.run_silver_to_gold"],
    },
    {
        "id": "quality",
        "label": "Quality",
        "tooltip": "Post-ETL quality checks",
        "task_ids": ["quality.run_post_etl_quality"],
    },
    {
        "id": "ml",
        "label": "ML",
        "tooltip": "Model training (if enough data)",
        "task_ids": ["ml.check_ml_readiness", "ml.run_gold_to_model", "ml.skip_ml_training"],
    },
    {
        "id": "reporting",
        "label": "Reporting",
        "tooltip": "EDA / ML charts",
        "task_ids": [
            "reporting.generate_eda_data_report",
            "reporting.generate_eda_ml_report",
            "reporting.mark_eda_ml_pending",
        ],
    },
]

# States Airflow considers "done" (mirrors airflow_client.DONE_STATES).
_DONE_STATES = {"success", "failed", "skipped", "upstream_failed", "removed"}
_FAILED_STATES = {"failed", "upstream_failed"}


def _rollup_state(states: list[str | None]) -> str:
    """Aggregate a stage's task states into one status for the box color."""
    present = [s for s in states if s]
    if not present:
        return "pending"
    if any(s in _FAILED_STATES for s in present):
        return "failed"
    if any(s == "running" for s in present):
        return "running"
    if any(s in ("queued", "scheduled", "up_for_retry") for s in present):
        return "queued"
    if all(s in _DONE_STATES for s in present):
        return "success"
    return "pending"


def _silver_metric() -> dict[str, Any] | None:
    row = db.query_one("SELECT COUNT(*) AS rows FROM dw.fact_consumption_silver")
    return {"label": "Silver rows", "value": row["rows"]} if row else None


def _gold_metric() -> dict[str, Any] | None:
    row = db.query_one("SELECT COUNT(*) AS days FROM dw.agg_daily")
    return {"label": "Gold days", "value": row["days"]} if row else None


def _quality_metric() -> dict[str, Any] | None:
    row = db.query_one(
        """
        SELECT COUNT(*) FILTER (WHERE passed) AS passed, COUNT(*) AS total
        FROM etl.data_quality_checks
        WHERE checked_at >= NOW() - INTERVAL '1 day'
        """
    )
    if not row or not row["total"]:
        return None
    return {"label": "Checks passed", "value": f"{row['passed']}/{row['total']}"}


def _ml_metric() -> dict[str, Any] | None:
    row = db.query_one(
        """
        SELECT model_name, rmse
        FROM etl.model_metrics
        ORDER BY trained_at DESC, rmse ASC
        LIMIT 1
        """
    )
    if not row:
        return None
    return {"label": "Best model", "value": f"{row['model_name']} (RMSE {row['rmse']:.1f})"}


_METRIC_FNS: dict[str, Callable[[], dict[str, Any] | None]] = {
    "silver": _silver_metric,
    "gold": _gold_metric,
    "quality": _quality_metric,
    "ml": _ml_metric,
}


def build_topology(task_instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Roll up task_instances (from airflow_client.list_task_instances) per stage."""
    by_task = {t["task_id"]: t for t in task_instances}

    stages = []
    for stage in STAGES:
        tasks = [
            {
                "task_id": tid,
                "state": by_task.get(tid, {}).get("state"),
                "try_number": by_task.get(tid, {}).get("try_number") or 1,
            }
            for tid in stage["task_ids"]
        ]
        metric_fn = _METRIC_FNS.get(stage["id"])
        stages.append(
            {
                "id": stage["id"],
                "label": stage["label"],
                "tooltip": stage["tooltip"],
                "tasks": tasks,
                "state": _rollup_state([t["state"] for t in tasks]),
                "metric": metric_fn() if metric_fn else None,
            }
        )
    return stages
