# **************    Projet : EDF Prediction Platform       **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# airflow/dags/edf_quality_monitoring.py — quality monitoring DAG (Makefile = prod).
# =======================================================================

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from edf_pipeline.config import DEFAULT_ARGS, get_quality_schedule
from edf_pipeline.quality import generate_quality_report, run_monitoring_quality_checks

QUALITY_DEFAULT_ARGS = {
    **DEFAULT_ARGS,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="edf_quality_monitoring",
    description="Quality monitoring EDF RTE — global checks batch + streaming",
    default_args=QUALITY_DEFAULT_ARGS,
    schedule_interval=get_quality_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["edf", "quality", "monitoring"],
) as dag:

    start = EmptyOperator(task_id="start")
    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_monitoring_quality_checks,
    )
    report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=generate_quality_report,
        trigger_rule="all_done",
    )
    end = EmptyOperator(task_id="end")

    start >> quality >> report >> end
